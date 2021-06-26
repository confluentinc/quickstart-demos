package io.confluent.examples.streams.microservices;

import java.io.IOException;
import java.util.Optional;
import java.util.Properties;

import io.confluent.examples.streams.microservices.domain.Schemas;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.commons.cli.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderState;
import io.confluent.examples.streams.avro.microservices.OrderValidation;
import io.confluent.examples.streams.avro.microservices.Product;
import io.confluent.examples.streams.microservices.domain.Schemas.Topics;
import io.confluent.examples.streams.microservices.util.MicroserviceUtils;

import static io.confluent.examples.streams.avro.microservices.OrderValidationResult.FAIL;
import static io.confluent.examples.streams.avro.microservices.OrderValidationResult.PASS;
import static io.confluent.examples.streams.avro.microservices.OrderValidationType.INVENTORY_CHECK;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.*;

/**
 * This service validates incoming orders to ensure there is sufficient stock to
 * fulfill them. This validation process considers both the inventory in the warehouse
 * as well as a set "reserved" items which is maintained by this service. Reserved
 * items are those that are in the warehouse, but have been allocated to a pending
 * order.
 * <p>
 * Currently there is nothing implemented that decrements the reserved items. This
 * would happen, inside this service, in response to an order being shipped.
 */
public class InventoryService implements Service {

  private static final Logger log = LoggerFactory.getLogger(InventoryService.class);
  public static final String SERVICE_APP_ID = "InventoryService";
  public static final String RESERVED_STOCK_STORE_NAME = "store-of-reserved-stock";
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

  @Override
  public void stop() {
    if (streams != null) {
      streams.close();
    }
  }

  private KafkaStreams processStreams(final String bootstrapServers,
                                      final String stateDir,
                                      final Properties defaultConfig) {

    //Latch onto instances of the orders and inventory topics
    final StreamsBuilder builder = new StreamsBuilder();
    final KStream<String, Order> orders = builder
      .stream(Topics.ORDERS.name(),
        Consumed.with(Topics.ORDERS.keySerde(), Topics.ORDERS.valueSerde()));
    final KTable<Product, Integer> warehouseInventory = builder
      .table(Topics.WAREHOUSE_INVENTORY.name(),
        Consumed.with(Topics.WAREHOUSE_INVENTORY.keySerde(), Topics.WAREHOUSE_INVENTORY.valueSerde()));

    //Create a store to reserve inventory whilst the order is processed.
    //This will be prepopulated from Kafka before the service starts processing
    final StoreBuilder<KeyValueStore<Product, Long>> reservedStock = Stores

      // TODO 6.1: create a state store called `RESERVED_STOCK_STORE_NAME`, using `Stores#keyValueStoreBuilder` and `Stores#persistentKeyValueStore`
      // 1. the key Serde is derived from the topic specified by `WAREHOUSE_INVENTORY`
      // 2. the value Serde is derived from `Serdes.Long()` because it represents a count
      // ...

      .withLoggingEnabled(new HashMap<>());
    builder.addStateStore(reservedStock);

    //First change orders stream to be keyed by Product (so we can join with warehouse inventory)
    orders.selectKey((id, order) -> order.getProduct())
      //Limit to newly created orders
      .filter((id, order) -> OrderState.CREATED.equals(order.getState()))
      //Join Orders to Inventory so we can compare each order to its corresponding stock value
      .join(warehouseInventory, KeyValue::new, Joined.with(Topics.WAREHOUSE_INVENTORY.keySerde(),
        Topics.ORDERS.valueSerde(), Serdes.Integer()))
      //Validate the order based on how much stock we have both in the warehouse and locally 'reserved' stock
      .transform(InventoryValidator::new, RESERVED_STOCK_STORE_NAME)
      //Push the result into the Order Validations topic
      .to(Topics.ORDER_VALIDATIONS.name(), Produced.with(Topics.ORDER_VALIDATIONS.keySerde(),
        Topics.ORDER_VALIDATIONS.valueSerde()));

    return new KafkaStreams(builder.build(),
      MicroserviceUtils.baseStreamsConfig(bootstrapServers, stateDir, SERVICE_APP_ID, defaultConfig));
  }

  private static class InventoryValidator implements
    Transformer<Product, KeyValue<Order, Integer>, KeyValue<String, OrderValidation>> {

    private KeyValueStore<Product, Long> reservedStocksStore;

    @Override
    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context) {
      reservedStocksStore = (KeyValueStore<Product, Long>) context
        .getStateStore(RESERVED_STOCK_STORE_NAME);
    }

    @Override
    public KeyValue<String, OrderValidation> transform(final Product productId,
                                                       final KeyValue<Order, Integer> orderAndStock) {
      //Process each order/inventory pair one at a time
      final OrderValidation validated;
      final Order order = orderAndStock.key;
      final Integer warehouseStockCount = orderAndStock.value;

      //Look up locally 'reserved' stock from our state store
      Long reserved = reservedStocksStore.get(order.getProduct());
      if (reserved == null) {
        reserved = 0L;
      }

      //If there is enough stock available (considering both warehouse inventory and reserved stock) validate the order
      if (warehouseStockCount - reserved - order.getQuantity() >= 0) {
        //reserve the stock by adding it to the 'reserved' store

        // TODO 6.2: update the reserved stock in the KeyValueStore called `reservedStocksStore`
        // 1. the key is the product in the order, using `OrderBean#getProduct`
        // 2. the value is the sum of the current reserved stock and the quantity in the order, using `OrderBean#getQuantity`

        //validate the order
        validated = new OrderValidation(order.getId(), INVENTORY_CHECK, PASS);
      } else {
        //fail the order
        validated = new OrderValidation(order.getId(), INVENTORY_CHECK, FAIL);
      }
      return KeyValue.pair(validated.getOrderId(), validated);
    }

    @Override
    public void close() {
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
      formatter.printHelp("Inventory Service", opts);
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

    final InventoryService service = new InventoryService();
    service.start(
            cl.getOptionValue("bootstrap-servers", DEFAULT_BOOTSTRAP_SERVERS),
            cl.getOptionValue("state-dir", "/tmp/kafka-streams-examples"),
            defaultConfig);
    addShutdownHookAndBlock(service);
  }
}
