package org.openprojectx.data.spark.bronze

import scala.io.Source
import io.circe._
import io.circe.parser._

import java.io.File

object Utils {


  def readCheckpointOffset(path: String, topic: String, partition: Int): Option[Long] = {
    val lines = Source.fromFile(path).getLines().toList

    if (lines.length < 3) return None

    val offsetsJson = lines(2)  // third line

    parse(offsetsJson) match {
      case Right(json) =>
        json.hcursor
          .downField(topic)
          .downField(partition.toString)
          .as[Long]
          .toOption

      case Left(err) =>
        println(s"Failed to parse offsets JSON: $err")
        None
    }
  }


   def checkDataLoss(): Unit = {
    val topic = "orders"
    val brokers = "192.168.0.129:9092"
    val checkpointOffsetsDir = "/data/Git/kafka-stream-job/test/warehouse/ckpt/orders_stream/offsets"
    val lossLogFile = "/data/Git/kafka-stream-job/test/data/loss-log.txt"

    // --------------------------------------------------------------------
    // LOSS DETECTION BEFORE STARTING STREAMING
    // --------------------------------------------------------------------
    val offsetFiles =
      new File(checkpointOffsetsDir)
        .listFiles()
        .filter(f => f.isFile && f.getName.matches("\\d+"))
        .sortBy(_.getName.toInt)

    if (offsetFiles.nonEmpty) {

      offsetFiles.foreach { file =>
        val filePath = file.getAbsolutePath
        val partitions = Seq(0, 1, 2) // or detect programmatically

        partitions.foreach { p =>
          SparkCheckpointReader.readCheckpointOffset(filePath, topic, p).foreach { cpOffset =>
            KafkaLossDetector.detectAndLogLoss(
              brokers = brokers,
              topic = topic,
              partition = p,
              checkpointOffset = cpOffset,
              logFile = lossLogFile
            )
          }
        }
      }


    }
  }

}
