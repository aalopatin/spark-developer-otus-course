package com.example.spark

import com.example.service.{KafkaService, PostgresService}
import com.example.util.ConfigManager.SparkConfig
import com.example.util.Constants.{APP_NAME, ERROR_LOG_LEVEL, SPARK_MASTER}
import com.example.util.Logging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait Job extends Logging with KafkaService with PostgresService {
  val master: String = SparkConfig.getValueOpt("master").getOrElse { noConfigFound("master"); SPARK_MASTER }
  val appName: String = SparkConfig.getValueOpt("app.name").getOrElse { noConfigFound("app.name"); APP_NAME }
  val logLevel: String = SparkConfig.getValueOpt("log.level").getOrElse { noConfigFound("log.level"); ERROR_LOG_LEVEL }

  def main(args: Array[String]): Unit = {
    run(createSparkSession())
  }

  def createSparkSession(config: Map[String, String] = Map.empty): SparkSession = {
    val spark = SparkSession.builder()
      .config(new SparkConf().setAll(config))
      .appName(appName)
      .master(master)
      .getOrCreate()

    spark.sparkContext.setLogLevel(logLevel)
    spark
  }

  def run(spark: SparkSession): Unit
}
