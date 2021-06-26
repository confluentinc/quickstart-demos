package io.confluent.examples.streams.microservices;

import java.io.IOException;
import java.util.Optional;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import org.eclipse.jetty.server.Server;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ManagedAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderState;
import io.confluent.examples.streams.interactivequeries.HostStoreInfo;
import io.confluent.examples.streams.interactivequeries.MetadataService;
import io.confluent.examples.streams.microservices.domain.Schemas;
import io.confluent.examples.streams.microservices.domain.beans.OrderBean;
import io.confluent.examples.streams.microservices.util.Paths;

import static io.confluent.examples.streams.microservices.domain.Schemas.Topics.ORDERS;
import static io.confluent.examples.streams.microservices.domain.beans.OrderBean.fromBean;
import static io.confluent.examples.streams.microservices.domain.beans.OrderBean.toBean;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.*;
import static org.apache.kafka.streams.state.StreamsMetadata.NOT_AVAILABLE;

/**
 * This class provides a REST interface to write and read orders using a CQRS pattern
 * (https://martinfowler.com/bliki/CQRS.html). Three methods are exposed over HTTP:
 * <p>
 * - POST(Order) -> Writes and order and returns location of the resource.
 * <p>
 * - GET(OrderId) (Optional timeout) -> Returns requested order, blocking for timeout if no id present.
 * <p>
 * - GET(OrderId)/Validated (Optional timeout)
 * <p>
 * POST does what you might expect: it adds an Order to the system returning when Kafka sends the appropriate
 * acknowledgement.
 * <p>
 * GET accesses an inbuilt Materialized View, of Orders, which are kept in a
 * State Store inside the service. This CQRS-styled view is updated asynchronously wrt the HTTP
 * POST.
 * <p>
 * Calling GET(id) when the ID is not present will block the caller until either the order
 * is added to the view, or the passed TIMEOUT period elapses. This allows the caller to
 * read-their-own-writes.
 * <p>
 * In addition HTTP POST returns the location of the order submitted in the response.
 * <p>
 * Calling GET/id/validated will block until the FAILED/VALIDATED order is available in
 * the View.
 * <p>
 * The View can also be scaled out linearly simply by adding more instances of the
 * view service, and requests to any of the REST endpoints will be automatically forwarded to the
 * correct instance for the key requested orderId via Kafka's Queryable State feature.
 * <p>
 * Non-blocking IO is used for all operations other than the intialization of state stores on
 * startup or rebalance which will block calling Jetty thread.
 *<p>
 * NB This demo code only includes a partial implementation of the holding of outstanding requests
 * and as such would lead timeouts if used in a production use case.
 */
@Path("v1")
public class OrdersService implements Service {

  private static final Logger log = LoggerFactory.getLogger(OrdersService.class);
  private static final String CALL_TIMEOUT = "10000";
  private static final String ORDERS_STORE_NAME = "orders-store";
  private final String SERVICE_APP_ID = getClass().getSimpleName();
  private final Client client = ClientBuilder.newBuilder().register(JacksonFeature.class).build();
  private Server jettyServer;
  private final String host;
  private int port;
  private KafkaStreams streams = null;
  private MetadataService metadataService;
  private KafkaProducer<String, Order> producer;

  //In a real implementation we would need to (a) support outstanding requests for the same Id/filter from
  // different users and (b) periodically purge old entries from this map.
  private final Map<String, FilteredResponse<String, Order>> outstandingRequests = new ConcurrentHashMap<>();

  public OrdersService(final String host, final int port) {
    this.host = host;
    this.port = port;
  }

  public OrdersService(final String host) {
    this(host, 0);
  }

  /**
   * Create a table of orders which we can query. When the table is updated
   * we check to see if there is an outstanding HTTP GET request waiting to be
   * fulfilled.
   */
  private StreamsBuilder createOrdersMaterializedView() {
    final StreamsBuilder builder = new StreamsBuilder();
    builder.table(ORDERS.name(), Consumed.with(ORDERS.keySerde(), ORDERS.valueSerde()), Materialized.as(ORDERS_STORE_NAME))
        .toStream().foreach(this::maybeCompleteLongPollGet);
    return builder;
  }

  private void maybeCompleteLongPollGet(final String id, final Order order) {
    final FilteredResponse<String, Order> callback = outstandingRequests.get(id);
    if (callback != null && callback.predicate.test(id, order)) {
      callback.asyncResponse.resume(toBean(order));
    }
  }

  /**
   * Perform a "Long-Poll" styled get. This method will attempt to get the value for the passed key
   * blocking until the key is available or passed timeout is reached. Non-blocking IO is used to
   * implement this, but the API will block the calling thread if no metastore data is available
   * (for example on startup or during a rebalance)
   *
   * @param id - the key of the value to retrieve
   * @param timeout - the timeout for the long-poll
   * @param asyncResponse - async response used to trigger the poll early should the appropriate
   * value become available
   */
  @GET
  @ManagedAsync
  @Path("/orders/{id}")
  @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  public void getWithTimeout(@PathParam("id") final String id,
      @QueryParam("timeout") @DefaultValue(CALL_TIMEOUT) final Long timeout,
      @Suspended final AsyncResponse asyncResponse) {
    setTimeout(timeout, asyncResponse);

    final HostStoreInfo hostForKey = getKeyLocationOrBlock(id, asyncResponse);

    if (hostForKey == null) { //request timed out so return
      return;
    }
    //Retrieve the order locally or reach out to a different instance if the required partition is hosted elsewhere.
    if (thisHost(hostForKey)) {
      fetchLocal(id, asyncResponse, (k, v) -> true);
    } else {
      final String path = new Paths(hostForKey.getHost(), hostForKey.getPort()).urlGet(id);
      fetchFromOtherHost(path, asyncResponse, timeout);
    }
  }

  static class FilteredResponse<K, V> {
    private final AsyncResponse asyncResponse;
    private final Predicate<K, V> predicate;

    FilteredResponse(final AsyncResponse asyncResponse, final Predicate<K, V> predicate) {
      this.asyncResponse = asyncResponse;
      this.predicate = predicate;
    }
  }

  /**
   * Fetch the order from the local materialized view
   *
   * @param id ID to fetch
   * @param asyncResponse the response to call once completed
   * @param predicate a filter that for this fetch, so for example we might fetch only VALIDATED
   * orders.
   */
  private void fetchLocal(final String id, final AsyncResponse asyncResponse, final Predicate<String, Order> predicate) {
    log.info("running GET on this node");
    try {
      final Order order = ordersStore().get(id);
      if (order == null || !predicate.test(id, order)) {
        log.info("Delaying get as order not present for id " + id);
        outstandingRequests.put(id, new FilteredResponse<>(asyncResponse, predicate));
      } else {
        asyncResponse.resume(toBean(order));
      }
    } catch (final InvalidStateStoreException e) {
      //Store not ready so delay
      outstandingRequests.put(id, new FilteredResponse<>(asyncResponse, predicate));
    }
  }

  private ReadOnlyKeyValueStore<String, Order> ordersStore() {
    return streams.store(
            StoreQueryParameters.fromNameAndType(
                    ORDERS_STORE_NAME,
                    QueryableStoreTypes.keyValueStore()));
  }

  /**
   * Use Kafka Streams' Queryable State API to work out if a key/value pair is located on
   * this node, or on another Kafka Streams node. This returned HostStoreInfo can be used
   * to redirect an HTTP request to the node that has the data.
   * <p>
   * If metadata is available, which can happen on startup, or during a rebalance, block until it is.
   */
  private HostStoreInfo getKeyLocationOrBlock(final String id, final AsyncResponse asyncResponse) {
    HostStoreInfo locationOfKey;
    while (locationMetadataIsUnavailable(locationOfKey = getHostForOrderId(id))) {
      //The metastore is not available. This can happen on startup/rebalance.
      if (asyncResponse.isDone()) {
        //The response timed out so return
        return null;
      }
      try {
        //Sleep a bit until metadata becomes available
        Thread.sleep(Math.min(Long.parseLong(CALL_TIMEOUT), 200));
      } catch (final InterruptedException e) {
        e.printStackTrace();
      }
    }
    return locationOfKey;
  }

  private boolean locationMetadataIsUnavailable(final HostStoreInfo hostWithKey) {
    return NOT_AVAILABLE.host().equals(hostWithKey.getHost())
        && NOT_AVAILABLE.port() == hostWithKey.getPort();
  }

  private boolean thisHost(final HostStoreInfo host) {
    return host.getHost().equals(this.host) &&
        host.getPort() == port;
  }

  private void fetchFromOtherHost(final String path, final AsyncResponse asyncResponse, final long timeout) {
    log.info("Chaining GET to a different instance: " + path);
    try {
      final OrderBean bean = client.target(path)
                                   .queryParam("timeout", timeout)
                                   .request(MediaType.APPLICATION_JSON_TYPE)
                                   .get(new GenericType<OrderBean>() {
          });
      asyncResponse.resume(bean);
    } catch (final Exception swallowed) {
      log.warn("GET failed.", swallowed);
    }
  }

  @GET
  @ManagedAsync
  @Path("orders/{id}/validated")
  public void getPostValidationWithTimeout(@PathParam("id") final String id,
      @QueryParam("timeout") @DefaultValue(CALL_TIMEOUT) final Long timeout,
      @Suspended final AsyncResponse asyncResponse) {
    setTimeout(timeout, asyncResponse);

    final HostStoreInfo hostForKey = getKeyLocationOrBlock(id, asyncResponse);

    if (hostForKey == null) { //request timed out so return
      return;
    }
    //Retrieve the order locally or reach out to a different instance if the required partition is hosted elsewhere.
    if (thisHost(hostForKey)) {
      fetchLocal(id, asyncResponse,
          (k, v) -> (v.getState() == OrderState.VALIDATED || v.getState() == OrderState.FAILED));
    } else {
      fetchFromOtherHost(new Paths(hostForKey.getHost(), hostForKey.getPort()).urlGetValidated(id),
          asyncResponse, timeout);
    }
  }


  /**
   * Persist an Order to Kafka. Returns once the order is successfully written to R nodes where
   * R is the replication factor configured in Kafka.
   *
   * @param order the order to add
   * @param timeout the max time to wait for the response from Kafka before timing out the POST
   */
  @POST
  @ManagedAsync
  @Path("/orders")
  @Consumes(MediaType.APPLICATION_JSON)
  public void submitOrder(final OrderBean order,
      @QueryParam("timeout") @DefaultValue(CALL_TIMEOUT) final Long timeout,
      @Suspended final AsyncResponse response) {
    setTimeout(timeout, response);

    final Order bean = fromBean(order);

    // TODO 1.1: create a new `ProducerRecord` with a key specified by `bean.getId()` and value of the bean, to the orders topic whose name is specified by `ORDERS.name()`
    // ...

    // TODO 1.2: produce the newly created record using the existing `producer` and pass use the `OrdersService#callback` function to send the `response` and the record key
    // ...

  }

  @SuppressWarnings("unchecked")
  @Override
  public void start(final String bootstrapServers,
                    final String stateDir,
                    final Properties defaultConfig) {
    jettyServer = startJetty(port, this);
    port = jettyServer.getURI().getPort(); // update port, in case port was zero
    producer = startProducer(bootstrapServers, ORDERS, defaultConfig);
    streams = startKStreams(bootstrapServers, defaultConfig);
    log.info("Started Service " + getClass().getSimpleName());
    log.info("Order Service listening at:" + jettyServer.getURI().toString());
  }

  private KafkaStreams startKStreams(final String bootstrapServers,
                                     final Properties defaultConfig) {
    final KafkaStreams streams = new KafkaStreams(
        createOrdersMaterializedView().build(),
        config(bootstrapServers, defaultConfig));
    metadataService = new MetadataService(streams);
    streams.cleanUp(); //don't do this in prod as it clears your state stores

    streams.start();

    return streams;
  }

  private Properties config(final String bootstrapServers, final Properties defaultConfig) {
    final Properties props = baseStreamsConfig(
            bootstrapServers,
            "/tmp/kafka-streams",
            SERVICE_APP_ID,
            defaultConfig);
    props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, host + ":" + port);
    return props;
  }

  @Override
  public void stop() {
    if (streams != null) {
      streams.close();
    }
    if (producer != null) {
      producer.close();
    }
    if (jettyServer != null) {
      try {
        jettyServer.stop();
      } catch (final Exception e) {
        e.printStackTrace();
      }
    }
  }

  // for testing only
  void cleanLocalState() {
    if (streams != null) {
      streams.cleanUp();
    }
  }

  public int port() {
    return port;
  }

  private HostStoreInfo getHostForOrderId(final String orderId) {
    return metadataService
        .streamsMetadataForStoreAndKey(ORDERS_STORE_NAME, orderId, Serdes.String().serializer());
  }

  private Callback callback(final AsyncResponse response, final String orderId) {
    return (recordMetadata, e) -> {
      if (e != null) {
        response.resume(e);
      } else {
        try {
          //Return the location of the newly created resource
          final Response uri = Response.created(new URI("/v1/orders/" + orderId)).build();
          response.resume(uri);
        } catch (final URISyntaxException e2) {
          e2.printStackTrace();
        }
      }
    };
  }

  public static void main(final String[] args) throws Exception {
    final Options opts = new Options();
    opts.addOption(Option.builder("b")
            .longOpt("bootstrap-servers").hasArg().desc("Kafka cluster bootstrap server string").build())
        .addOption(Option.builder("s")
            .longOpt("schema-registry").hasArg().desc("Schema Registry URL").build())
        .addOption(Option.builder("h")
            .longOpt("hostname").hasArg().desc("This services HTTP host name").build())
        .addOption(Option.builder("p")
            .longOpt("port").hasArg().desc("This services HTTP port").build())
        .addOption(Option.builder("c")
            .longOpt("config-file").hasArg().desc("Java properties file with configurations for Kafka Clients").build())
        .addOption(Option.builder("d")
            .longOpt("state-dir").hasArg().desc("The directory for state storage").build())
        .addOption(Option.builder("h").longOpt("help").hasArg(false).desc("Show usage information").build());

    final CommandLine cl = new DefaultParser().parse(opts, args);
    if (cl.hasOption("h")) {
      final HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("Order Service", opts);
      return;
    }

    final String bootstrapServers = cl.getOptionValue("bootstrap-servers", DEFAULT_BOOTSTRAP_SERVERS);
    final String restHostname = cl.getOptionValue("hostname", "localhost");
    final int restPort = Integer.parseInt(cl.getOptionValue("port", "5432"));
    final String stateDir = cl.getOptionValue("state-dir", "/tmp/kafka-streams");

    final Properties defaultConfig = Optional.ofNullable(cl.getOptionValue("config-file", null))
            .map(path -> {
              try {
                return buildPropertiesFromConfigFile(path);
              } catch (final IOException e) {
                throw new RuntimeException(e);
              }
            })
            .orElse(new Properties());

    final String schemaRegistryUrl = cl.getOptionValue("schema-registry", DEFAULT_SCHEMA_REGISTRY_URL);
    defaultConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    Schemas.configureSerdes(defaultConfig);

    final OrdersService service = new OrdersService(restHostname, restPort);
    service.start(bootstrapServers, stateDir, defaultConfig);
    addShutdownHookAndBlock(service);
  }
}
