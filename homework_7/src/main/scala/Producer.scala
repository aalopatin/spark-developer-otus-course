import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.jdk.CollectionConverters._
import java.io.File
import java.nio.charset.Charset
import java.util.Properties

object Producer extends App {

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:29092")

  val producer = new KafkaProducer(props, new StringSerializer, new StringSerializer)

  val csvFile = new File("src/main/resources/bestsellers.csv")
  val parser = CSVParser.parse(csvFile, Charset.defaultCharset(), CSVFormat.EXCEL)
  val (headerRecord :: records) = parser.getRecords.asScala.toList
  val header = headerRecord.toList.asScala.toList

  records.foreach(
    record => {
      val jsonBuilder = new StringBuilder
      jsonBuilder.append("{")
      for ((title, field) <- header zip record.toList.asScala.toList) {
        jsonBuilder.append(s""""$title": "$field";""")
      }
      jsonBuilder.replace(jsonBuilder.length() - 1 , jsonBuilder.length(), "}")
      val json = jsonBuilder.toString()
      producer.send(new ProducerRecord("books", json))
  })
  producer.close()
}
