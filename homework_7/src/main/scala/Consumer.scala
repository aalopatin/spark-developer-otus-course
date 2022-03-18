import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

import java.time.Duration
import java.util.Properties
import scala.jdk.CollectionConverters._

object Consumer extends App {
  type CRStrStr = ConsumerRecord[String, String]
  type TupleAccumulator = (List[CRStrStr], List[CRStrStr], List[CRStrStr])

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:29092")
  props.put("group.id", "consumer2")
  props.put("auto.offset.reset", "earliest")

  val consumer = new KafkaConsumer(props, new StringDeserializer, new StringDeserializer)

  try {
    consumer.subscribe(List("books").asJava)

    val (partition0, partition1, partition2) = readData(consumer, (Nil, Nil, Nil))

    partition0.foreach(record => printRecord(record))
    partition1.foreach(record => printRecord(record))
    partition2.foreach(record => printRecord(record))
  } finally {
    consumer.close()
  }

  def readData(consumer: Consumer[String, String], accumulator: TupleAccumulator): TupleAccumulator = {
    val records = consumer.poll(Duration.ofSeconds(1))
    if (records.count() == 0) accumulator
    else {
      readData(consumer, getAccumalator(records.iterator().asScala, accumulator))
    }
  }

  def getAccumalator(recordsIterator: Iterator[CRStrStr], accumulator: TupleAccumulator): TupleAccumulator = {
    if (recordsIterator.hasNext) {
      val record = recordsIterator.next()
      val partition = record.partition()
      partition match {
        case 0 => getAccumalator(
          recordsIterator,
          (
            getAccumulatorElement(record, accumulator._1),
            accumulator._2,
            accumulator._3
          )
        )
        case 1 => getAccumalator(
          recordsIterator,
          (
            accumulator._1,
            getAccumulatorElement(record, accumulator._2),
            accumulator._3
          )
        )
        case 2 => getAccumalator(
          recordsIterator,
          (
            accumulator._1,
            accumulator._2,
            getAccumulatorElement(record, accumulator._3)
          )
        )
        case _ => getAccumalator(recordsIterator, accumulator)
      }
    } else {
      accumulator
    }
  }

  def getAccumulatorElement(record: CRStrStr, element: List[CRStrStr]): List[CRStrStr] = {
    if (element.length < 5) record :: element
    else record :: element.dropRight(1)
  }

  def printRecord(record: CRStrStr) = {
    println(s"Partition: ${record.partition()}, offset: ${record.offset()}, value: ${record.value()}")
//    println(record.value())
  }
}
