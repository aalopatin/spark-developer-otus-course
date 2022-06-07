package com.example.domain

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.StructType

case class Message(
                    number: String,
                    date: String
                  )

object Message {
  val messageSchema: StructType = Encoders.product[Message].schema
}