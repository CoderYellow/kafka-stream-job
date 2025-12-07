package org.openprojectx.data.spark.bronze

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import scala.io.Source

object SparkCheckpointReader {

  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  /**
   * Spark offset checkpoint file format:
   *   line 0: "v1"
   *   line 1: metadata JSON
   *   line 2: offsets JSON → we want this
   */
  def readCheckpointOffset(
                            path: String,
                            topic: String,
                            partition: Int
                          ): Option[Long] = {

    val lines = Source.fromFile(path).getLines().toList
    if (lines.size < 3) return None

    val offsetsJson = lines(2) // third line

    try {
      val json: JsonNode = mapper.readTree(offsetsJson)
      val topicNode = json.get(topic)
      if (topicNode == null) return None

      val pNode = topicNode.get(partition.toString)
      if (pNode == null) return None

      Some(pNode.asLong())
    } catch {
      case e: Exception =>
        println(s"[WARN] Failed to parse Spark checkpoint offset file $path: ${e.getMessage}")
        None
    }
  }
}
