package homework2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

object Task2 extends App {

  implicit val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val factsPath = "src/main/resources/data/yellow_taxi_jan_25_2018"

  val factsRDD = readParquet(factsPath)

  val mostPopularTimeRDD = processTaxiData(factsRDD)

  //V1
  val mostPopularTimeDF = spark.createDataFrame(mostPopularTimeRDD).toDF("Hours", "Count")
  mostPopularTimeDF.show(24)

  //V2
  println("+-----+-----+")
  println("|Hours|Count|")
  println("+-----+-----+")
  mostPopularTimeRDD
    .foreach(f => {
      val hour = f._1.toString
      val count = f._2.toString

      val hourL = hour.length
      val countL = count.length

      println(s"|${" " * (5 - hourL)}$hour|${" " * (5 - countL)}$count|")
    })
  println("+-----+-----+")

  def readParquet(path: String)(implicit spark: SparkSession) =
    spark
      .read
      .load(path)
      .rdd

  def processTaxiData(facts: RDD[Row]) =
    facts
    .map(f => (f.getTimestamp(1).toLocalDateTime.getHour, 1))
    .reduceByKey(_ + _)
    .sortBy(f => f._2, ascending = false)

  mostPopularTimeRDD.map(f => f._1.toString + " " + f._2.toString).saveAsTextFile("result/task2/most_popular_time.txt")

}