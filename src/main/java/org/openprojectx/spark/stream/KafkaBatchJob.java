package org.openprojectx.spark.stream;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.avro.functions;

import static org.apache.spark.sql.functions.*;

import org.openprojectx.data.spark.bronze.SparkConfigLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class KafkaBatchJob {

    private static final Logger log = LoggerFactory.getLogger(KafkaBatchJob.class);


    public static void main(String[] args) {
        String startOffsets = "{\"orders\":{\"0\":100,\"1\":60,\"2\":66}}";
        String endOffsets = "{\"orders\":{\"0\":120,\"1\":120,\"2\":86}}";
        String topic = "orders";
        String bootstrapServers = "192.168.0.131:9092,192.168.0.132:9092,192.168.0.133:9092";

        KafkaBatchDataLossMonitor monitor =
                new KafkaBatchDataLossMonitor(bootstrapServers);

        List<KafkaBatchDataLossMonitor.DataLossEvent> losses =
                monitor.check(startOffsets, endOffsets);

        if (!losses.isEmpty()) {
            log.warn("Kafka data loss detected: {}", losses);
            losses.forEach(e ->
                    System.err.printf(
                            "DATA LOSS: %s-%d lost %d messages (%d → %d)%n",
                            e.topic(), e.partition(),
                            e.lostCount(), e.lostFrom(), e.lostTo()
                    )
            );
        }


        // decide policy:
        // throw new RuntimeException("Kafka data loss detected");
        // OR continue anyway

        String avroSchema = """
                {
                  "type": "record",
                  "name": "OrderEvent",
                  "namespace": "com.example",
                  "fields": [
                    { "name": "orderId", "type": "string" },
                    { "name": "amount",  "type": "double" },
                    { "name": "ts",      "type": "string" }
                  ]
                }
                """;

        SparkConf conf = SparkConfigLoader.loadFromResources("/spark-defaults.properties");
        SparkSession spark = SparkSession.builder()
                .appName("KafkaAvroToIceberg")
                .config(conf)
                .getOrCreate();

        Dataset<Row> kafkaDf = spark.read()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", topic)
                .option("startingOffsets", startOffsets)
                .option("endingOffsets", endOffsets)
//                    .option("failOnDataLoss", "false")
                .load();

        Dataset<Row> decoded = kafkaDf
                .select(
                        functions.from_avro(kafkaDf.col("value"), avroSchema).alias("data")
                )
                .select("data.*")
                .withColumn("ingested_at", current_timestamp())
                .withColumn("source", lit("kafka-batch"));

        // Optional sanity check
        long count = decoded.count();
        System.out.println("Rows to write into Iceberg: " + count);

        // ------------------------------------------------------------------
        // 7. Write to Iceberg Bronze table (atomic commit)
        // ------------------------------------------------------------------

        decoded.write()
                .format("iceberg")
                .mode(SaveMode.Append)
                .save("bronze.db.orders");

        // ------------------------------------------------------------------
        // 8. Finish
        // ------------------------------------------------------------------

        System.out.println("Kafka batch ingestion completed successfully.");

    }
}

