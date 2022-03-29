import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession

object Research extends App {

  val filePath = "src/main/resources/IRIS.csv"

  // Создаем Spark сессию
  val spark = SparkSession.builder()
    .appName("Research")
    .config("spark.master", "local")
    .getOrCreate()

  // Читаем набор данных
  val irisDF = spark.read
    .option("header", true)
    .option("inferSchema", "true")
    .csv(filePath)

  //  irisDF.show()

  val vecAssembler = new VectorAssembler()
    .setInputCols(Array("sepal_length", "sepal_width", "petal_length", "petal_width"))
    .setOutputCol("features")
  val vecIrisDF = vecAssembler.transform(irisDF)
  vecIrisDF.show()
  //  vecIrisDF.select("sepal_length", "sepal_width", "petal_length", "petal_width", "features", "species").show(10)

  val categoricalCols = vecIrisDF.dtypes.filter(_._2.equals("StringType")).map(_._1)
  val indexOutputCols = categoricalCols.map(_ + "_Index")
  val oheOutputCols = categoricalCols.map(_ + "_OHE")

  val stringIndexer = new StringIndexer()
    .setInputCols(categoricalCols)
    .setOutputCols(indexOutputCols)
    .setHandleInvalid("skip")

  val indexedIrisDF = stringIndexer.fit(vecIrisDF).transform(vecIrisDF)

//  val oheEncoder = new OneHotEncoder()
//    .setInputCols(indexOutputCols)
//    .setOutputCols(oheOutputCols)
//
//  val codedIrisDF = oheEncoder.fit(indexedIrisDF).transform(indexedIrisDF)

  //  indexedIrisDF.show()


//  val Array(train, test) = codedIrisDF.randomSplit(Array(.8, .2), seed = 42)
  val Array(train, test) = indexedIrisDF.randomSplit(Array(.8, .2), seed = 42)

  val lr = new LogisticRegression()
  val model = lr
    .setLabelCol("species_Index")
    .setFeaturesCol("features")
    .fit(train)

  val prediction = model.transform(test)
  prediction.show()

//  val pipeline = new Pipeline().setStages(Array(vecAssembler, stringIndexer, oheEncoder, lr))
    val pipeline = new Pipeline().setStages(Array(vecAssembler, stringIndexer, lr))

  val pipelineModel = pipeline.fit(irisDF)

  // Читаем набор тестовых данных
  val irisTestDF = spark.read
    .option("header", true)
    .option("inferSchema", "true")
    .csv("src/main/resources/IRIS_test.csv")

  val predictionPipeline = pipelineModel.transform(irisDF)
  predictionPipeline.show(150)

  //  val Array(trainDF, testDF) = irisDF.randomSplit(Array(.8, .2))
  //  val pipelineModel = pipeline.fit(trainDF)

  //  val predictionPipeline = pipelineModel.transform(testDF)
  //  predictionPipeline.show()

  pipelineModel.write.overwrite().save("result/pipelineModel")
}
