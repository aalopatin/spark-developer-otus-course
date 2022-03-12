package homework2

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, desc}

object Task1 extends App {
  val factsPath = "src/main/resources/data/yellow_taxi_jan_25_2018"
  val zonesPath = "src/main/resources/data/taxi_zones.csv"
  val result = "result/task1/most_popular_borough"

  implicit val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val taxiFactsDF = readParquet(factsPath)

  taxiFactsDF.show()

  val taxiZonesDF = readCSV(zonesPath)

  val mostPopularBorough = processTaxiData(taxiFactsDF, taxiZonesDF)

  mostPopularBorough.show()

  mostPopularBorough.write.mode(SaveMode.Overwrite).save(result)

  readParquet(result).show()

  def readParquet(path: String)(implicit spark: SparkSession) =
    spark
      .read
      .load(path)

  def readCSV(path: String)(implicit spark: SparkSession) =
    spark
      .read
      .format("csv")
      .option("header", "true")
      .load(path)

  def processTaxiData(facts: DataFrame, zones: DataFrame) =
    facts
      .join(zones, col("PULocationID") === col("LocationID"))
      .groupBy("Borough")
      .count()
      .orderBy(desc("count"))

}
