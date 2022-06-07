package com.example

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkWriteRead {
  def readTableDF(dbTable: String, partitionColumn: String, numPartitions: String, lowerBound: String, upperBound: String)(implicit spark: SparkSession) = {
    spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/kafka-postgres")
      .option("driver", "org.postgresql.Driver")
      .option("user", "postgres")
      .option("password", "postgres")
      .option("partitionColumn", partitionColumn)
      .option("numPartitions", numPartitions)
      .option("lowerBound", lowerBound)
      .option("upperBound", upperBound)
      .option("dbtable", dbTable)
      .load()
  }

  def writeTableDF(DF: DataFrame, dbTable: String, saveMode: SaveMode, truncate: Boolean = false)(implicit spark: SparkSession) = {
    DF.write
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/kafka-postgres")
      .option("driver", "org.postgresql.Driver")
      .option("user", "postgres")
      .option("password", "postgres")
      .option("dbtable", dbTable)
      .option("truncate", truncate)
      .mode(saveMode)
      .save()
  }
}
