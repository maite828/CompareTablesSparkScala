name := "CompareTablesProject"

version := "0.1"

scalaVersion := "2.12.18"

ThisBuild / fork := true
ThisBuild / javaOptions ++= Seq(
  "-Dhadoop.home.dir=/tmp/hadoop-dummy",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0",
  "org.apache.spark" %% "spark-hive" % "3.5.0",
  "com.typesafe.play" %% "play-json" % "2.9.4",
  "org.apache.hadoop" % "hadoop-client" % "3.3.4"
)

Compile / run / mainClass := Some("Main")
