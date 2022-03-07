package homework2

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.desc

object Task1 extends App {
  val factsPath = "src/main/resources/data/yellow_taxi_jan_25_2018"
  val zonesPath = "src/main/resources/data/taxi_zones.csv"

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/otus"
  val user = "docker"
  val password = "docker"

  val taxiFactsDF = spark
    .read
    .load(factsPath)

  taxiFactsDF.show()

  val taxiZonesDF = spark
    .read
    .format("csv")
    .option("header", "true")
    .load(zonesPath)

  val mostPopularBorough = taxiFactsDF
    .join(
      taxiZonesDF,
      $"PULocationID" === $"LocationID"
    )
    .groupBy("Borough")
    .count()
    .orderBy(desc("count"))

  mostPopularBorough.show()

  mostPopularBorough.write.mode(SaveMode.Overwrite).save("result/task1/most_popular_borough")

  spark.read.load("result/task1/most_popular_borough").show()

}
