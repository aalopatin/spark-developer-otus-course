package homework2

import homework2.model.{Distance, DistanceDivision, TaxiFact}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{avg, col, count, lit, max, min, round, stddev, stddev_pop, stddev_samp, when}

import java.util.Properties

object Task3 extends App {

  implicit val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val factsPath = "src/main/resources/data/yellow_taxi_jan_25_2018"

  val taxiFactsDF = readParquet(factsPath)

  val distanceDF = spark.createDataset(Seq((0, 5), (5, 10), (10, 15), (15, 20))).toDF("from", "to")

  val divisionDS = getInitialDS(taxiFactsDF, distanceDF).as[Distance]

  val distanceDivision = processData(divisionDS).as[DistanceDivision]

  distanceDivision.show()

  val connectionProperties = new Properties()
  connectionProperties.put("user", "docker")
  connectionProperties.put("password", "docker")

  distanceDivision
    .write
    .jdbc("jdbc:postgresql://localhost:5432/docker", "distance_division", connectionProperties)

  def readParquet(path: String)(implicit spark: SparkSession) =
    spark
      .read
      .load(path)

  def getInitialDS(facts: DataFrame, distance: DataFrame)(implicit spark: SparkSession) = {

    facts
      .join(
        distance,
        (col("trip_distance") >= col("from")).and(col("trip_distance") < col("to"))
      )
      .withColumnRenamed("trip_distance", "distance")
      .select(col("distance"), col("from"), col("to"))

  }

  def processData(ds: Dataset[Distance]) = {
    ds
      .groupBy(col("from"), col("to"))
      .agg(
        count("distance") as "count",
        round(avg("distance"), 2) as "average",
        round(stddev("distance"), 2) as "deviation",
        min("distance") as "min",
        max("distance") as "max"
      )
      .orderBy("from")
  }
}
