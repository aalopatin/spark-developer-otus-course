package com.example

import com.example.domain.Message
import com.example.spark.Job
import org.apache.spark.sql.{Dataset, SparkSession}

object Main extends Job {

  override def run(spark: SparkSession): Unit = {

    val messagesDataset: Dataset[Message] = readFromKafka(spark)

    writeToPostgresql(messagesDataset)
  }
}
