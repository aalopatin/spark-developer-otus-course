package com.example

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, count, expr, lit, round, to_timestamp, when}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import com.example.DataFrameSyntax._
import com.example.Formatters.formatterDTTM
import com.example.UdfUtils.normalizeNumber

object Main extends App {
  implicit val spark = SparkSession
    .builder()
    .appName("spark")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val currentDate = LocalDateTime.now()
  val currentDateStr = currentDate.format(formatterDTTM)
  val phonesDF = SparkWriteRead
    .readTableDF("messages",
      "date",
      "10",
      "2022-01-01 00:00:00",
      currentDateStr
    )
    .where(col("date") < to_timestamp(lit(currentDateStr)))
    .withColumn("number", normalizeNumber(col("number")))

  val countMessagesDFPerDay = phonesDF.countPer("number", "date", currentDate, "1")

  val countMessagesDFPer7days = phonesDF.countPer("number", "date", currentDate, "7")

  val countMessagesDFPer30days = phonesDF.countPer("number", "date", currentDate, "30")

  val countMessagesDFPer180days = phonesDF.countPer("number", "date", currentDate, "180")

  val dataframe =
    phonesDF.select("number").distinct()
      .join(countMessagesDFPerDay, Seq("number"), "left")
      .join(countMessagesDFPer7days, Seq("number"), "left")
      .join(countMessagesDFPer30days, Seq("number"), "left")
      .join(countMessagesDFPer180days, Seq("number"), "left")
      .withColumn("ratio", round($"count_per_1"/$"count_per_7", 2))
      .na.fill(0)
      .withColumn("score",
        when($"ratio".between(0.5, 1), "red")
          .when($"ratio".between(0.2, 0.49), "yellow")
          .when($"ratio".between(0, 0.19), "green")
      )

  dataframe.show()

  SparkWriteRead.writeTableDF(dataframe, "result", SaveMode.Overwrite, true)

}
