name := "CompareTablesProject"

version := "0.1"

scalaVersion := "2.12.18"

ThisBuild / fork := true



libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql"  % "3.5.0",
  "org.apache.spark" %% "spark-hive" % "3.5.0",
  "com.typesafe.play" %% "play-json" % "2.9.4" // Para parsear el JSON

)

Compile / run / mainClass := Some("Main")

