package com.example.util

object Constants {
  val APP_NAME = "spark-kafka-postgresql"
  val SPARK_MASTER = "local[2]"
  val CHECKPOINT_PATH = "src/main/resources/checkpoints"
  val ERROR_LOG_LEVEL = "ERROR"

  val BROKER = "localhost:29092"
  val TOPIC = "fraud-numbers"
  val GROUP_ID = "kafka-group"
  val EARLIEST = true
  val KAFKA_SOURCE = "kafka"

  val POSTGRESQL_FORMAT = "jdbc"
  val POSTGRESQL_DRIVER = "org.postgresql.Driver"
  val POSTGRESQL_URL = "jdbc:postgresql://localhost:5432/kafka-postgres"
}
