package com.example

import java.time.format.DateTimeFormatter

object Formatters {
  val formatterDTTM: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  val formatterDT: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
}

