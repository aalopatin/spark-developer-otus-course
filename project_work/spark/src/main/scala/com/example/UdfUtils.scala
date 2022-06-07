package com.example

import com.google.i18n.phonenumbers.PhoneNumberUtil
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object UdfUtils {

  def normalizeNumber: UserDefinedFunction = udf[String, String] { s =>
    try {
      PhoneNumberUtil.getInstance().parse(s, "RU").getNationalNumber.toString
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        s
      }
    }

  }

}
