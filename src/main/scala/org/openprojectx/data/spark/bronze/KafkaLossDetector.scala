package org.openprojectx.data.spark.bronze

import java.util.Properties
import org.apache.kafka.clients.admin.{AdminClient, OffsetSpec}
import org.apache.kafka.common.TopicPartition
import java.nio.file.{Files, Paths, StandardOpenOption}

object KafkaLossDetector {

  def detectAndLogLoss(
                        brokers: String,
                        topic: String,
                        partition: Int,
                        checkpointOffset: Long,
                        logFile: String
                      ): Unit = {

    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    val admin = AdminClient.create(props)

    val tp = new TopicPartition(topic, partition)

    val earliest =
      admin.listOffsets(java.util.Collections.singletonMap(tp, OffsetSpec.earliest()))
        .all()
        .get()
        .get(tp)
        .offset()

    if (checkpointOffset < earliest) {
      val lost = earliest - checkpointOffset
      val msg =
        s"[DATA-LOSS] topic=$topic partition=$partition lost=$lost chk=$checkpointOffset earliest=$earliest time=${System.currentTimeMillis()}\n"

      println(msg)
      Files.write(
        Paths.get(logFile),
        msg.getBytes("UTF-8"),
        StandardOpenOption.CREATE,
        StandardOpenOption.APPEND
      )
    }

    admin.close()
  }
}
