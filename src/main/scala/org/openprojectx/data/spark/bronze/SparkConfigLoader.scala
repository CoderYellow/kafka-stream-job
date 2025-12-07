package org.openprojectx.data.spark.bronze

import org.apache.spark.SparkConf

import java.util.Properties

object SparkConfigLoader {

  def loadFromResources(resourcePath: String = "/spark-defaults.properties"): SparkConf = {
    val props = new Properties()
    val stream = Option(getClass.getResourceAsStream(resourcePath))
      .getOrElse(throw new RuntimeException(s"Cannot find $resourcePath in classpath"))

    props.load(stream)

    val sparkConf = new SparkConf(false)
    props.forEach {
      case (key: String, value: String) if key.startsWith("spark.") =>
        sparkConf.set(key, value)
      case _ => // ignore non-spark.* entries
    }

    sparkConf
  }
}
