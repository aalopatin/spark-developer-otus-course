package com.example

import com.example.Formatters.formatterDTTM
import com.example.Main.currentDate
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count, expr, lit, to_timestamp, when}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


object DataFrameSyntax {
  implicit class RichDataFrame(df: DataFrame) {
    def countPer(groupColumn: String, dateColumn: String, startDate: LocalDateTime, days: String): DataFrame = {
      df.where(col(dateColumn) > (to_timestamp(lit(startDate.format(formatterDTTM))) - expr(s"INTERVAL $days DAY")))
        .groupBy(groupColumn)
        .agg(count("*").as(s"count_per_$days"))
    }
  }

}
