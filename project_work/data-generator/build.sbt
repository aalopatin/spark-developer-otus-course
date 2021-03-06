ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

val sparkVersion = "3.2.1"
val postgresVersion = "42.2.24"
val scalajHttpVersion = "2.4.2"
val circeVersion = "0.14.1"

lazy val root = (project in file("."))
  .settings(
    name := "test-data-generator",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion
      , "org.apache.spark" %% "spark-sql" % sparkVersion
      , "org.postgresql" % "postgresql" % postgresVersion
      , "org.scalaj" %% "scalaj-http" % scalajHttpVersion
    ),
    libraryDependencies ++= Seq(
      "io.circe" %% "circe-core",
      "io.circe" %% "circe-generic",
      "io.circe" %% "circe-parser"
    ).map(_ % circeVersion)
  )
