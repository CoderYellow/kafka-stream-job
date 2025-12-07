package org.openprojectx.data.spark.bronze

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.avro.functions.from_avro
import org.openprojectx.data.spark.bronze.SparkConfigLoader.loadFromResources
import org.openprojectx.spark.stream.DataLossMonitor

object KafkaAvroToIceberg {

  def main(args: Array[String]): Unit = {

    //    order-bronze-job
    val conf = loadFromResources()
    val spark = SparkSession.builder()
      .appName("KafkaAvroToIceberg")
      .config(conf)


      //      .config("spark.sql.catalog.iceberg.s3.region", "cn-guangzhou-a") // ✅ required
      //      .config("spark.sql.catalog.bronze.s3.endpoint", "https://s3.data.cn-guangzhou-a.k8s.openprojectx.org")
      //      // optional
      //      .config("spark.sql.catalog.bronze.s3.access-key-id", "spark-ingestion")
      //      .config("spark.sql.catalog.bronze.s3.secret-access-key", "xxxxxxxxxxxxxxxx")
      //      .config("spark.sql.catalog.bronze.s3.path-style-access", "true") // important for MinIO
      //      .config("spark.sql.catalog.bronze.s3.remote-signing-enabled", "false")
      .getOrCreate()


    // 1. Read Avro schema (from registry or local)
    val avroSchemaJson =
      """
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
      """

    // 2. Stream from Kafka
    val raw = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.0.129:9092")
      .option("subscribe", "orders")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      // If using Confluent SR:
      // .option("kafka.schema.registry.url", "http://schema-registry:8081")
      // .option(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false)
      .load()

    // 3. Decode Avro payload
    val decoded: DataFrame = raw
      .select(
        from_avro(col("value"), avroSchemaJson).as("data")
      )
      .select("data.*")
      .withColumn("ingested_at", current_timestamp())


    spark.streams.addListener(
      new DataLossMonitor("192.168.0.129:9092")
    )


    // 4. Write to Iceberg table
    val query = decoded.writeStream
      .format("iceberg")
      //      .option("path", "bronze.db.orders")      // catalog.db.table
      //      .option("checkpointLocation", "s3://data/ckpt/orders_stream/")
      .option("checkpointLocation", "file:///data/Git/kafka-stream-job/test/warehouse/ckpt/orders_stream/")
      .outputMode("append")
      //      .start()
      //      .toTable("bronze.db.orders")
      .start("bronze.db.orders")
    //      .awaitTermination()

    // Wait for first micro-batch
    query.processAllAvailable()
    Thread.sleep(2000)

    // Print progress
    println("---- PROGRESS ----")
    println("progress: "+query.lastProgress.prettyJson)

    // Explain plan
    println("---- PLAN ----")
    query.explain(true)

    // Block
    query.awaitTermination()

  }

}
