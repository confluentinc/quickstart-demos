package io.confluent.examples.streams.microservices;

import static io.confluent.examples.streams.avro.microservices.Order.newBuilder;
import static io.confluent.examples.streams.avro.microservices.OrderState.VALIDATED;
import static io.confluent.examples.streams.avro.microservices.OrderValidationResult.FAIL;
import static io.confluent.examples.streams.avro.microservices.OrderValidationResult.PASS;
import static io.confluent.examples.streams.microservices.domain.Schemas.Topics.ORDERS;
import static io.confluent.examples.streams.microservices.domain.Schemas.Topics.ORDER_VALIDATIONS;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.*;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderState;
import io.confluent.examples.streams.avro.microservices.OrderValidation;
import io.confluent.examples.streams.microservices.domain.Schemas;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.commons.cli.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.Properties;


/**
 * A simple service which listens to to validation results from each of the Validation
 * services and aggregates them by order Id, triggering a pass or fail based on whether
 * all rules pass or not.
 */
public class ValidationsAggregatorService implements Service {

  private static final Logger log = LoggerFactory.getLogger(ValidationsAggregatorService.class);
  private final String SERVICE_APP_ID = getClass().getSimpleName();
  private final Consumed<String, OrderValidation> serdes1 = Consumed
      .with(ORDER_VALIDATIONS.keySerde(), ORDER_VALIDATIONS.valueSerde());
  private final Consumed<String, Order> serdes2 = Consumed.with(ORDERS.keySerde(),
      ORDERS.valueSerde());
  private final Grouped<String, OrderValidation> serdes3 = Grouped
      .with(ORDER_VALIDATIONS.keySerde(), ORDER_VALIDATIONS.valueSerde());
  private final StreamJoined<String, Long, Order> serdes4 = StreamJoined
      .with(ORDERS.keySerde(), Serdes.Long(), ORDERS.valueSerde());
  private final Produced<String, Order> serdes5 = Produced
      .with(ORDERS.keySerde(), ORDERS.valueSerde());
  private final Grouped<String, Order> serdes6 = Grouped
      .with(ORDERS.keySerde(), ORDERS.valueSerde());
  private final StreamJoined<String, OrderValidation, Order> serdes7 = StreamJoined
      .with(ORDERS.keySerde(), ORDER_VALIDATIONS.valueSerde(), ORDERS.valueSerde());

  private KafkaStreams streams;

  @Override
  public void start(final String bootstrapServers,
                    final String stateDir,
                    final Properties defaultConfig) {
    streams = aggregateOrderValidations(bootstrapServers, stateDir, defaultConfig);
    streams.cleanUp(); //don't do this in prod as it clears your state stores


    streams.start();

    log.info("Started Service " + getClass().getSimpleName());
  }

  private KafkaStreams aggregateOrderValidations(
      final String bootstrapServers,
      final String stateDir,
      final Properties defaultConfig) {
    final int numberOfRules = 3; //TODO put into a KTable to make dynamically configurable

    final StreamsBuilder builder = new StreamsBuilder();
    final KStream<String, OrderValidation> validations = builder
        .stream(ORDER_VALIDATIONS.name(), serdes1);
    final KStream<String, Order> orders = builder
        .stream(ORDERS.name(), serdes2)
        .filter((id, order) -> OrderState.CREATED.equals(order.getState()));

    //If all rules pass then validate the order
    validations
        .groupByKey(serdes3)

        // TODO 5.1: window the data using `KGroupedStream#windowedBy`, specifically using `SessionWindows.with` to define 5-minute windows
        // ...

        .aggregate(
            () -> 0L,
            (id, result, total) -> PASS.equals(result.getValidationResult()) ? total + 1 : total,
            (k, a, b) -> b == null ? a : b, //include a merger as we're using session windows.
            Materialized.with(null, Serdes.Long())
        )
        //get rid of window
        .toStream((windowedKey, total) -> windowedKey.key())
        //When elements are evicted from a session window they create delete events. Filter these.
        .filter((k1, v) -> v != null)
        //only include results were all rules passed validation
        .filter((k, total) -> total >= numberOfRules)
        //Join back to orders
        .join(orders, (id, order) ->
                //Set the order to Validated
                newBuilder(order).setState(VALIDATED).build()
            , JoinWindows.of(Duration.ofMinutes(5)), serdes4)
        //Push the validated order into the orders topic
        .to(ORDERS.name(), serdes5);

    //If any rule fails then fail the order
    validations.filter((id, rule) -> FAIL.equals(rule.getValidationResult()))
        .join(orders, (id, order) ->
                //Set the order to Failed and bump the version on it's ID
                newBuilder(order).setState(OrderState.FAILED).build(),
            JoinWindows.of(Duration.ofMinutes(5)), serdes7)
        //there could be multiple failed rules for each order so collapse to a single order

        // TODO 5.2: group the records by key using `KStream#groupByKey`, providing the existing Serialized instance for ORDERS
        // ...

        // TODO 5.3: use an aggregation operator `KTable#reduce` to collapse the records in this stream to a single order for a given key
        // ...

        //Push the validated order into the orders topic
        .toStream().to(ORDERS.name(), Produced.with(ORDERS.keySerde(), ORDERS.valueSerde()));

    return new KafkaStreams(builder.build(),
        baseStreamsConfig(bootstrapServers, stateDir, SERVICE_APP_ID, defaultConfig));
  }

  @Override
  public void stop() {
    if (streams != null) {
      streams.close();
    }
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
      formatter.printHelp("Validator Aggregator Service", opts);
      return;
    }
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

    final ValidationsAggregatorService service = new ValidationsAggregatorService();

    service.start(
            cl.getOptionValue("bootstrap-servers", DEFAULT_BOOTSTRAP_SERVERS),
            cl.getOptionValue("state-dir", "/tmp/kafka-streams-examples"),
            defaultConfig);
    addShutdownHookAndBlock(service);
  }
}
