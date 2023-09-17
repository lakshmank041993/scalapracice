ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "scalapractice"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.1" exclude("org.apache.hadoop", "hadoop-client"),
  "org.apache.spark" %% "spark-sql" % "3.3.1",
  "org.apache.spark" %% "spark-streaming" % "3.3.1",
)
libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.3.1"
libraryDependencies += "org.apache.spark" %% "spark-avro" % "3.3.1"




