name := "kafka-postgres"

version := "0.1"

scalaVersion := "2.13.8"

val postgresVersion = "42.3.5"
val sparkVersion = "3.2.1"
val  typeSafeConfigVersion = "1.4.0"

libraryDependencies ++= Seq(
  //spark
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  //kafka
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,

  //postgresql
  "org.postgresql" % "postgresql" % postgresVersion,

  "com.typesafe" % "config" % typeSafeConfigVersion
)