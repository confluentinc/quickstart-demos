package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderState;
import io.confluent.examples.streams.avro.microservices.OrderValidation;
import io.confluent.examples.streams.avro.microservices.OrderValue;
import io.confluent.examples.streams.microservices.domain.Schemas;

import java.io.IOException;
import java.util.Optional;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.commons.cli.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

import static io.confluent.examples.streams.avro.microservices.OrderValidationResult.FAIL;
import static io.confluent.examples.streams.avro.microservices.OrderValidationResult.PASS;
import static io.confluent.examples.streams.avro.microservices.OrderValidationType.FRAUD_CHECK;
import static io.confluent.examples.streams.microservices.domain.Schemas.Topics.ORDERS;
import static io.confluent.examples.streams.microservices.domain.Schemas.Topics.ORDER_VALIDATIONS;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.*;


/**
 * This service searches for potentially fraudulent transactions by calculating the total value of
 * orders for a customer within a time period, then checks to see if this is over a configured
 * limit. <p> i.e. if(SUM(order.value, 5Mins) > $5000) GroupBy customer -> Fail(orderId) else
 * Pass(orderId)
 */
public class FraudService implements Service {

  private static final Logger log = LoggerFactory.getLogger(FraudService.class);
  private final String SERVICE_APP_ID = getClass().getSimpleName();

  private static final int FRAUD_LIMIT = 2000;
  private KafkaStreams streams;

  @Override
  public void start(final String bootstrapServers,
                    final String stateDir,
                    final Properties defaultConfig) {
    streams = processStreams(bootstrapServers, stateDir, defaultConfig);
    streams.cleanUp(); //don't do this in prod as it clears your state stores

    streams.start();

    log.info("Started Service " + getClass().getSimpleName());
  }

  private KafkaStreams processStreams(final String bootstrapServers,
                                      final String stateDir,
                                      final Properties defaultConfig) {

    //Latch onto instances of the orders and inventory topics
    final StreamsBuilder builder = new StreamsBuilder();
    final KStream<String, Order> orders = builder
        .stream(ORDERS.name(), Consumed.with(ORDERS.keySerde(), ORDERS.valueSerde()))

        // TODO 4.1: filter this stream to include only orders in "CREATED" state, i.e., it should satisfy the predicate `OrderState.CREATED.equals(order.getState())`
        // ...

    //Create an aggregate of the total value by customer and hold it with the order. We use session windows to
    // detect periods of activity.
    final KTable<Windowed<Long>, OrderValue> aggregate = orders
        .groupBy((id, order) -> order.getCustomerId(), Grouped.with(Serdes.Long(), ORDERS.valueSerde()))
        .windowedBy(SessionWindows.with(Duration.ofHours(1)))
        .aggregate(OrderValue::new,
            //Calculate running total for each customer within this window
            (custId, order, total) -> new OrderValue(order,
                total.getValue() + order.getQuantity() * order.getPrice()),
            (k, a, b) -> simpleMerge(a, b), //include a merger as we're using session windows.
            Materialized.with(null, Schemas.ORDER_VALUE_SERDE));

    //Ditch the windowing and rekey
    final KStream<String, OrderValue> ordersWithTotals = aggregate
        .toStream((windowedKey, orderValue) -> windowedKey.key())
        .filter((k, v) -> v != null)//When elements are evicted from a session window they create delete events. Filter these out.
        .selectKey((id, orderValue) -> orderValue.getOrder().getId());

    //Now branch the stream into two, for pass and fail, based on whether the windowed total is over Fraud Limit

    // TODO 4.2: create a `KStream<String, OrderValue>` array from the `ordersWithTotals` stream by branching the records based on `OrderValue#getValue`
    // 1. First branched stream: FRAUD_CHECK will fail for predicate where order value >= FRAUD_LIMIT
    // 2. Second branched stream: FRAUD_CHECK will pass for predicate where order value < FRAUD_LIMIT
    // ...

    forks[0].mapValues(
        orderValue -> new OrderValidation(orderValue.getOrder().getId(), FRAUD_CHECK, FAIL))
        .to(ORDER_VALIDATIONS.name(), Produced
            .with(ORDER_VALIDATIONS.keySerde(), ORDER_VALIDATIONS.valueSerde()));

    forks[1].mapValues(
        orderValue -> new OrderValidation(orderValue.getOrder().getId(), FRAUD_CHECK, PASS))
        .to(ORDER_VALIDATIONS.name(), Produced
            .with(ORDER_VALIDATIONS.keySerde(), ORDER_VALIDATIONS.valueSerde()));

    //disable caching to ensure a complete aggregate changelog. This is a little trick we need to apply
    //as caching in Kafka Streams will conflate subsequent updates for the same key. Disabling caching ensures
    //we get a complete "changelog" from the aggregate(...) step above (i.e. every input event will have a
    //corresponding output event.
    final Properties props = baseStreamsConfig(bootstrapServers, stateDir, SERVICE_APP_ID, defaultConfig);
    props.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

    return new KafkaStreams(builder.build(), props);
  }

  private OrderValue simpleMerge(final OrderValue a, final OrderValue b) {
    return new OrderValue(b.getOrder(), (a == null ? 0D : a.getValue()) + b.getValue());
  }

  public static void main(final String[] args) throws Exception {

    final Options opts = new Options();
    opts.addOption(Option.builder("b")
            .longOpt("bootstrap-servers")
            .hasArg()
            .desc("Kafka cluster bootstrap server string (ex: broker:9092)")
            .build());
    opts.addOption(Option.builder("s")
            .longOpt("schema-registry")
            .hasArg()
            .desc("Schema Registry URL")
            .build());
    opts.addOption(Option.builder("c")
            .longOpt("config-file")
            .hasArg()
            .desc("Java properties file with configurations for Kafka Clients")
            .build());
    opts.addOption(Option.builder("t")
            .longOpt("state-dir")
            .hasArg()
            .desc("The directory for state storage")
            .build());
    opts.addOption(Option.builder("h").longOpt("help").hasArg(false).desc("Show usage information").build());

    final CommandLine cl = new DefaultParser().parse(opts, args);

    if (cl.hasOption("h")) {
      final HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("Fraud Service", opts);
      return;
    }
    final FraudService service = new FraudService();
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

    service.start(
            cl.getOptionValue("bootstrap-servers", DEFAULT_BOOTSTRAP_SERVERS),
            cl.getOptionValue("state-dir", "/tmp/kafka-streams-examples"),
            defaultConfig);
    addShutdownHookAndBlock(service);
  }

  @Override
  public void stop() {
    if (streams != null) {
      streams.close();
    }
  }

}
