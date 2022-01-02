package org.example

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._

import java.io.{File, FileWriter}

object Main extends App {

  if (args.length == 0)
    println("You have to input at least one parameter.")
  else {
    val file = scala.io.Source.fromURL("https://raw.githubusercontent.com/mledoze/countries/master/countries.json")
    val text = file.mkString

    val json = parse(text)

    val countries = json.children
      .filter(
        country => {
          val JString(region) = country \ "region"
          region == "Africa"
        }
      ).sortWith(
      (x, y) => {
        val xValue = getAreaValue(x)
        val yValue = getAreaValue(y)
        xValue > yValue
      }
    )

    val newJson = countries.take(10).map(country => {
      val JString(name) = country \ "name" \ "official"
      val JString(capital) = (country \ "capital")(0)
      val area = getAreaValue(country)
      ("name" -> name) ~
        ("capital" -> capital) ~
        ("area" -> area)
    })

    val newText = compact(render(newJson))

    val fileWriter = new FileWriter(new File(args(0)))
    fileWriter.write(newText)
    fileWriter.close()
  }

  //File has a few countries with area that has type of Double
  def getAreaValue(value: JValue): Int = {
    value \ "area" match {
      case JDouble(value) => value.toInt
      case JInt(value) => value.toInt
      case _ => 0
    }
  }

}
