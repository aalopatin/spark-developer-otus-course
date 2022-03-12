package homework2

import homework2.Task2.{processTaxiData, readParquet}
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

class Task2Test extends AnyFlatSpec {

  implicit val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("Test for Task 2 - RDD")
    .getOrCreate()


  it should "upload and process data" in {
    val factsRDD = readParquet("src/main/resources/data/yellow_taxi_jan_25_2018")
    val mostPopularTimeRDD = processTaxiData(factsRDD)

    val firstLine = mostPopularTimeRDD.first()

    assert(firstLine._1 == 19)
    assert(firstLine._2 == 22121)

  }

}
