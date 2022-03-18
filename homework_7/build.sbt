ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "homework_7"
  )

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "3.1.0",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.2",
  "org.apache.commons" % "commons-csv" % "1.9.0"
)