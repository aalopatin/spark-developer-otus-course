import org.apache.spark.SparkConf
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.concat_ws

object Main extends App {

  val path2model = "result/pipelineModel"
  val brokers = "localhost:29092"
  val groupId = "ml"
  val inputTopic = "input"
  val predictionTopic = "prediction"
  val speciesIndexesFile = "result/species_indexes"
  val checkpointLocation = "checkout"

  // Загружаем модель
  val model = PipelineModel.load(path2model)

  // Создаём SparkSession
  val spark = SparkSession.builder()
    .appName("MLStructuredStreaming")
    .getOrCreate()

  val speciesIndexesDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .load(speciesIndexesFile).distinct()

  import spark.implicits._

  // Читаем входной поток
  val input = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", brokers)
    .option("subscribe", inputTopic)
    .load()
    .selectExpr("CAST(value AS STRING)")
    .as[String]
    .map(_.split(","))
    .map(Data(_))

  // Применяем модель к входным данным
  val prediction = model.transform(input)

  // Выводим результат
  val query = prediction
    .join(speciesIndexesDF, $"prediction" === $"label")
    .select(concat_ws(",", $"sepal_length", $"sepal_width", $"petal_length", $"petal_width", $"species").as("value"))
    .writeStream
    .option("checkpointLocation", checkpointLocation)
    .outputMode("append")
    .format("kafka")
    .option("kafka.bootstrap.servers", brokers)
    .option("topic", predictionTopic)
    .start()

  query.awaitTermination()

}
