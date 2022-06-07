package com.example.service

import com.example.domain.Message
import com.example.util.ConfigManager.PostgreSqlConfig
import com.example.util.{Constants, Logging}
import org.apache.spark.sql.{Dataset, SaveMode}

trait PostgresService extends Logging {

  lazy val dataSource: String = Constants.POSTGRESQL_FORMAT

  def writeToPostgresql(dataset: Dataset[Message], mode: SaveMode = SaveMode.Append) = {
    info("Writing dataset to postgresql.")
    dataset.writeStream
      .foreachBatch { (batch: Dataset[Message], _: Long) =>
        batch.write
          .format(dataSource)
          .options(postgresqlSinkOptions)
          .mode(mode)
          .save()
      }
      .start()
      .awaitTermination()
  }

  def postgresqlSinkOptions: Map[String, String] = Map(
    "dbtable" -> PostgreSqlConfig.getValue("dbtable"), // table
    "user" -> PostgreSqlConfig.getValue("user"), // Database username
    "password" -> PostgreSqlConfig.getValue("password"), // Password
    "driver" -> PostgreSqlConfig.getValue("driver"),
    "url" -> PostgreSqlConfig.getValue("url")
  )
}
