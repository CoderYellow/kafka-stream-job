package org.openprojectx.data.spark.bronze


import java.util.Properties
import java.time.Instant
import java.util.UUID

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.ByteArraySerializer

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}
import org.apache.avro.generic.GenericDatumWriter

object TestAvroProducer {

  val schemaJson: String =
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

  def main(args: Array[String]): Unit = {

    val schema = new Schema.Parser().parse(schemaJson)

    val props = new Properties()
    props.put("bootstrap.servers", " 192.168.0.131:9092,192.168.0.132:9092,192.168.0.133:9092")
    props.put("key.serializer", classOf[ByteArraySerializer].getName)
    props.put("value.serializer", classOf[ByteArraySerializer].getName)
    props.put("acks", "all")

    val producer = new KafkaProducer[Array[Byte], Array[Byte]](props)

    val topic = "orders"

    println("Producing 20 Avro messages to Kafka topic 'orders'...")

    (1 to 256).foreach { _ =>

      // Build Avro record
      val record: GenericRecord = new GenericData.Record(schema)
      record.put("orderId", UUID.randomUUID().toString)
      record.put("amount", Math.random() * 1000)
      record.put("ts", Instant.now().toString)

      // Serialize to bytes
      val writer = new GenericDatumWriter[GenericRecord](schema)
      val out = new java.io.ByteArrayOutputStream()
      val encoder: BinaryEncoder = EncoderFactory.get.binaryEncoder(out, null)
      writer.write(record, encoder)
      encoder.flush()
      out.close()
      val bytes = out.toByteArray

      // Produce
      val producerRecord = new ProducerRecord[Array[Byte], Array[Byte]](topic, null, bytes)
      producer.send(producerRecord)
    }

    producer.flush()
    producer.close()

    println("Done.")
  }
}

