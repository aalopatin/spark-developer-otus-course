import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.Properties

object MLStreaming extends App {

  val path2model = "result/pipelineModel"
  val brokers = "localhost:29092"
  val groupId = "ml"
  val inputTopic = "input"
  val predictionTopic = "prediction"

  // Создаём Streaming Context и получаем Spark Context
  val sparkConf = new SparkConf().setAppName("MLStreaming")
  val streamingContext = new StreamingContext(sparkConf, Seconds(1))
  val sparkContext = streamingContext.sparkContext

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

    // Если получили непустой набор данных, передаем входные данные в модель, вычисляем и выводим ID клиента и результат
    if (data.count > 0) {
      val prediction = model.transform(data)
      prediction
        .foreach { row => kafkaSink.value.send(predictionTopic, s"${row(0)},${row(1)}, ${row(2)}, ${row(3)}, ${row(4)}") }
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


