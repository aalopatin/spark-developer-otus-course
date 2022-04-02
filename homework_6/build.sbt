
name := "homework_6"

ThisBuild / version := "0.1"
ThisBuild / scalaVersion := "2.12.12"

val sparkVersion = "3.2.1"

lazy val commonSettings = Seq(
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion
    , "org.apache.spark" %% "spark-sql" % sparkVersion
    , "org.apache.spark" %% "spark-mllib" % sparkVersion
  )
)

lazy val research = (project in file("research"))
  .settings(
    name := "research",
    commonSettings
  )

lazy val streaming = (project in file("streaming"))
  .settings(
    name := "streaming",
    commonSettings,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-streaming" % sparkVersion,
      "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
      "org.apache.kafka" % "kafka-clients" % "3.1.0"
    )
  )

lazy val structured_streaming = (project in file("structured_streaming"))
  .settings(
    name := "structured-streaming",
    commonSettings,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
      "org.apache.kafka" % "kafka-clients" % "3.1.0"
    )
  )

