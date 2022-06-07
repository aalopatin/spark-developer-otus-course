name := "spark"

version := "0.1"

scalaVersion := "2.13.8"

val sparkVersion = "3.2.1"
val  typeSafeConfigVersion = "1.4.0"
val postgresVersion = "42.3.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.typesafe" % "config" % typeSafeConfigVersion,

  "com.googlecode.libphonenumber" % "libphonenumber" % "8.12.41",
  //postgresql
  "org.postgresql" % "postgresql" % postgresVersion
)