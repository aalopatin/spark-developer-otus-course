package com.example.service

import com.example.domain.Message
import com.example.domain.Message.messageSchema
import com.example.util.ConfigManager.KafkaConfig
import com.example.util.Constants.KAFKA_SOURCE
import com.example.util.Logging
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.{Dataset, SparkSession}


trait KafkaService extends Logging {
  val brokers = KafkaConfig.getValue("broker")
  val sourceTopic = KafkaConfig.getValue("source.topic")
  val groupId = KafkaConfig.getValue("source.group.id")
  val fromBeginning = if (KafkaConfig.getBoolean("source.earliest")) "earliest" else "latest"

  def readFromKafka(spark: SparkSession): Dataset[Message] = {
    info(s"Reading from kafka with topic : ${sourceTopic}")
    import spark.implicits._
    spark
      .readStream
      .format(KAFKA_SOURCE)
      .options(kafkaSourceOptions)
      .load()
      .selectExpr("cast(value as string) as value")
      .select(from_json(col("value"), messageSchema).as[Message])
  }

  def kafkaSourceOptions: Map[String, String] = Map(
    ("kafka.bootstrap.servers", brokers),
    ("group.id", groupId),
    ("startingOffsets", fromBeginning),
    ("subscribe", sourceTopic)
  )
}
