import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.LogicalTypes;
import org.apache.kafka.streams.*;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class MinutesAlertsApp {
    public static final String PRODUCT_TOPIC_NAME = "products";
    public static final String PURCHASE_TOPIC_NAME = "purchases";
    public static final String RESULT_TOPIC = "sum_minutes_alerts";
    public static final String BROKER = "localhost:9092";
    public static final String SCHEMA_REGISTRY_URL = "http://localhost:8090";
    public static final int WINDOW_SIZE = 1;
    public static final long MAX_SUM = 3000;


    public static void main(String[] args) {
        var client = new CachedSchemaRegistryClient(SCHEMA_REGISTRY_URL, 16);
        var serDeProps = Map.of(
                KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, "true",
                KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL
        );
        Topology topology = buildTopology(client, serDeProps);
        KafkaStreams kafkaStreams = new KafkaStreams(topology, getStreamsConfig());
        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("shutdown-hook") {
            @Override
            public void run() {
                kafkaStreams.close();
                latch.countDown();
            }
        });
        kafkaStreams.setUncaughtExceptionHandler((thread, ex) -> {
            ex.printStackTrace();
            kafkaStreams.close();
            latch.countDown();
        });

        try {
            kafkaStreams.start();
            latch.await();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }

    public static Properties getStreamsConfig() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "MinutesAlert");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER);
        return props;
    }

    public static Topology buildTopology(SchemaRegistryClient client, Map<String, String> serDeConfig) {
        var builder = new StreamsBuilder();
        var avroSerde = new GenericAvroSerde(client);
        avroSerde.configure(serDeConfig, false);

        var purchasesStream = builder.stream(
                PURCHASE_TOPIC_NAME,
                Consumed.with(new Serdes.StringSerde(), avroSerde)
        );
        var productsTable = builder.globalTable(
                PRODUCT_TOPIC_NAME,
                Consumed.with(new Serdes.StringSerde(), avroSerde)
        );
        var purchaseWithJoinedProduct = purchasesStream.leftJoin(
                productsTable,
                (key, val) -> val.get("productid").toString(),
                MinutesAlertsApp::joinProduct
        );
        Duration oneMinute = Duration.ofMinutes(WINDOW_SIZE);
        purchaseWithJoinedProduct
                .filter((key, val) -> val.success)
                .mapValues(val -> val.result)
                .groupBy((key, val) -> val.get("productid").toString(), Grouped.with(new Serdes.StringSerde(), avroSerde))
                .windowedBy(TimeWindows.of(oneMinute).advanceBy(oneMinute))
                .aggregate(
                        () -> 0L,
                        (key, val, agg) -> agg += (Long) val.get("purchase_quantity") * (Long) val.get("product_price"),
                        Materialized.with(new Serdes.StringSerde(), new Serdes.LongSerde())
                )
                .filter((key, val) -> val > MAX_SUM)
                .toStream()
                .map((key, val) -> {
                    Schema schema = SchemaBuilder.record("SumAlert").fields()
                            .name("window_start")
                            .type(LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.LONG)))
                            .noDefault()
                            .requiredLong("sum")
                            .endRecord();
                    GenericRecord record = new GenericData.Record(schema);
                    record.put("window_start", key.window().start());
                    record.put("sum", val);
                    return KeyValue.pair(key.key(), record);
                })
                .to(RESULT_TOPIC, Produced.with(new Serdes.StringSerde(), avroSerde));

        return builder.build();
    }

    private static JoinResult joinProduct(GenericRecord purchase, GenericRecord product) {
        try {
            Schema schema = SchemaBuilder.record("PurchaseWithProduct").fields()
                    .requiredLong("purchase_id")
                    .requiredLong("purchase_quantity")
                    .requiredLong("product_id")
                    .requiredString("product_name")
                    .requiredDouble("product_price")
                    .endRecord();
            GenericRecord result = new GenericData.Record(schema);
            // копируем в наше сообщение нужные поля из сообщения о покупке
            result.put("purchase_id", purchase.get("id").toString());
            result.put("purchase_quantity", purchase.get("quantity"));
            result.put("product_id", purchase.get("productid"));
            // копируем в наше сообщение нужные поля из сообщения о товаре
            result.put("product_name", product.get("name"));
            result.put("product_price", product.get("price"));
            return new JoinResult(true, result, null);
        } catch (Exception e) {
            return new JoinResult(false, purchase, e.getMessage());
        }
    }

    private record JoinResult(boolean success, GenericRecord result, String errorMessage) {
    }
}
