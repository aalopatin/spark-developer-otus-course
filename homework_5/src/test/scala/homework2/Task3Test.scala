package homework2

import homework2.Task3.{getInitialDS, processData, readParquet}
import homework2.model.Distance
import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.SharedSparkSession

class Task3Test extends SharedSparkSession{

  import testImplicits._

  test("distance division") {
    val taxiFactsDF = readParquet("src/main/resources/data/yellow_taxi_jan_25_2018")
    val distanceDF = spark.createDataset(Seq((0, 5), (5, 10), (10, 15), (15, 20))).toDF("from", "to")
    val divisionDS = getInitialDS(taxiFactsDF, distanceDF).as[Distance]
    val distanceDivision = processData(divisionDS)

    checkAnswer(
      distanceDivision,
      Row(0, 5, 289221, 1.61, 1.03, 0.0, 4.99) ::
        Row(5, 10, 25554, 7.1, 1.5, 5.0, 9.99) ::
        Row(10, 15, 9292, 11.67, 1.32, 10.0, 14.99) ::
        Row(15, 20, 6200, 17.5, 1.23, 15.0, 19.99) :: Nil
    )

//    checkAnswer(
//      distanceDivision,
//      Row("Manhattan",304266,0.0,2.23,66.0)  ::
//        Row("Queens",17712,0.0,11.14,53.5) ::
//        Row("Unknown",6644,0.0,2.34,42.8) ::
//        Row("Brooklyn",3037,0.0,3.28,27.37) ::
//        Row("Bronx",211,0.0,2.99,20.09) ::
//        Row("EWR",19,0.0,3.46,17.3) ::
//        Row("Staten Island",4,0.0,0.2,0.5) :: Nil
//    )

  }

}
