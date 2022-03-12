package homework2

import homework2.Task1.{processTaxiData, readCSV, readParquet}
import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.SharedSparkSession

class Task1Test extends SharedSparkSession {

  test("join - processTaxiData") {
    val taxiFactsDF = readParquet("src/main/resources/data/yellow_taxi_jan_25_2018")
    val taxiZonesDF = readCSV("src/main/resources/data/taxi_zones.csv")

    val mostPopularBorough = processTaxiData(taxiFactsDF, taxiZonesDF)

    checkAnswer(
      mostPopularBorough,
      Row("Manhattan",304266)  ::
        Row("Queens",17712) ::
        Row("Unknown",6644) ::
        Row("Brooklyn",3037) ::
        Row("Bronx",211) ::
        Row("EWR",19) ::
        Row("Staten Island",4) :: Nil
    )

  }
}
