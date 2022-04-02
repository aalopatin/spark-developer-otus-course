import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._

import java.util.Properties

object Main extends App {

  val path2model = "result/pipelineModel"
  val brokers = "localhost:29092"
  val groupId = "ml"
  val inputTopic = "input"
  val predictionTopic = "prediction"
  val speciesIndexesFile = "result/species_indexes"

  // Создаём Streaming Context и получаем Spark Context
  val sparkConf = new SparkConf().setAppName("MLStreaming").setMaster("local")
  val streamingContext = new StreamingContext(sparkConf, Seconds(1))
  val sparkContext = streamingContext.sparkContext

  val spark = SparkSessionSingleton.getInstance(sparkContext.getConf)

  val speciesIndexesDF = spark.read
    .option("header", true)
    .option("inferSchema", "true")
    .load(speciesIndexesFile)

  val speciesIndexes = speciesIndexesDF.collect().map(row => (row(1), row(0))).toMap

  // Загружаем модель
  val model = PipelineModel.load(path2model)

  // Создаём свойства Producer'а для вывода в выходную тему Kafka (тема с расчётом)
  val props: Properties = new Properties()
  props.put("bootstrap.servers", brokers)

  // Создаём Kafka Sink (Producer)
  val kafkaSink = sparkContext.broadcast(KafkaSink(props))

  // Параметры подключения к Kafka для чтения
  val kafkaParams = Map[String, Object](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
    ConsumerConfig.GROUP_ID_CONFIG -> groupId,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
  )

  // Подписываемся на входную тему Kafka (тема с данными)
  val inputTopicSet = Set(inputTopic)
  val messages = KafkaUtils.createDirectStream[String, String](
    streamingContext,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](inputTopicSet, kafkaParams)
  )

  // Разбиваем входную строку на элементы
  val lines = messages
    .map(_.value)
    .map(_.replace("\"", "").split(","))

  // Обрабатываем каждый входной набор
  lines.foreachRDD { rdd =>
    // Get the singleton instance of SparkSession
    val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
    import spark.implicits._

    // Преобразовываем RDD в DataFrame
    val data = rdd
      .toDF("input")
      .withColumn("sepal_length", $"input"(0).cast(DoubleType))
      .withColumn("sepal_width", $"input"(1).cast(DoubleType))
      .withColumn("petal_length", $"input"(2).cast(DoubleType))
      .withColumn("petal_width", $"input"(3).cast(DoubleType))
      .drop("input")

    if (data.count > 0) {
      val prediction = model.transform(data)

      val result = prediction.select("sepal_length", "sepal_width", "petal_length", "petal_width", "prediction")

      result
        .foreach { row =>kafkaSink.value.send(
          predictionTopic,
          s"${row(0)},${row(1)}, ${row(2)}, ${row(3)}, ${speciesIndexes(row(4))}")
        }
    }
  }

  streamingContext.start()
  streamingContext.awaitTermination()

  object SparkSessionSingleton {
    @transient private var instance: SparkSession = _

    def getInstance(sparkConf: SparkConf): SparkSession = {
      if (instance == null) {
        instance = SparkSession.builder
          .config(sparkConf)
          .getOrCreate()
      }
      instance
    }
  }

}
