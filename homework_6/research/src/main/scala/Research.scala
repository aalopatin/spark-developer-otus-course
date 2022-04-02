import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lit, when}

object Research extends App {

  val irisCSV = "research/src/main/resources/IRIS.csv"
  val irisTestCSV = "research/src/main/resources/IRIS_test.csv"

  // Создаем Spark сессию
  val spark = SparkSession.builder()
    .appName("Research")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  // Читаем набор данных
  val irisDF = spark.read
    .option("header", true)
    .option("inferSchema", "true")
    .csv(irisCSV)

  irisDF.show()

  val stringIndexer = new StringIndexer()
    .setInputCols(Array("species"))
    .setOutputCols(Array("label"))
    .setHandleInvalid("skip")

  val species = irisDF.select("species").distinct()
  val species_indexes = stringIndexer.fit(species).transform(species)

  val irisDFLabeled = irisDF
    .join(species_indexes, "species")

  irisDFLabeled.show()

  val vecAssembler = new VectorAssembler()
    .setInputCols(Array("sepal_length", "sepal_width", "petal_length", "petal_width"))
    .setOutputCol("features")
  val irisDFVectored = vecAssembler.transform(irisDFLabeled)

  irisDFVectored.show()

  val data = irisDFVectored.select("features", "label")

  data.show()

  val lr = new LogisticRegression()

  val model = lr.fit(data)

  val pipeline = new Pipeline().setStages(Array(vecAssembler, model))

  val pipelineModel = pipeline.fit(irisDF)

  val irisTestDF = spark.read
    .option("header", true)
    .option("inferSchema", "true")
    .csv(irisTestCSV)

  val predictionPipeline = pipelineModel.transform(irisTestDF)

  predictionPipeline.show(150)

  val result = predictionPipeline
    .join(
      species_indexes,
      $"prediction" === $"label"
    ).select("sepal_length", "sepal_width", "petal_length", "petal_width", "species")

  result.show()

  pipelineModel.write.overwrite().save("result/pipelineModel")

}
