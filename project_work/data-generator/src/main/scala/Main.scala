import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, count, lit, rand, randn, round, to_timestamp}

import scalaj.http.{Http, HttpOptions}

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}
import scala.language.postfixOps

import io.circe._

import io.circe.generic.auto._
import io.circe.syntax._

object Main extends App {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Test data generator")
    .getOrCreate()

  import spark.implicits._

  val r = scala.util.Random

  val numbersDF = spark
    .read
    .csv("src/main/resources/numbers10.csv")

  val possibleStartDays = 180 * 86400
  val possibleCountDays = 180 * 86400
  val maxCountAppeal = 1000
  val statDate = 1640995200 // 2022.01.01 00:00:00

  val recordsDF = numbersDF.withColumn("startDate", (lit(statDate) + lit(possibleStartDays) * rand()).cast("Long"))
    .flatMap(
      number => {
        val days = r.nextInt(possibleCountDays)
        val count = r.nextInt(maxCountAppeal) + 1
        for {
          num <- 1 to count
        } yield (number.getString(0), number.getLong(1) + r.nextInt(days))
      }
    )
    .toDF("number", "date")
    .withColumn("date", to_timestamp($"date"))
    .as[Message]

  recordsDF.foreach(r => {
    val string = r.asJson.toString()
    Http("http://localhost:3000/kafka/publish")
      .header("Content-Type", "application/json")
      .postData(string)
      .method("POST")
      .asString

  }: Unit)

}
