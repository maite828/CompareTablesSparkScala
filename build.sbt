name := "CompareTablesProject"

version := "0.1"

scalaVersion := "2.12.18"

ThisBuild / fork := true
ThisBuild / javaOptions ++= {
  val os = sys.props.getOrElse("os.name","").toLowerCase
  val hadoopHome =
    if (os.contains("win")) sys.env.getOrElse("HADOOP_HOME", "C:/hadoop-dummy")
    else "/tmp/hadoop-dummy"
  Seq(
    s"-Dhadoop.home.dir=$hadoopHome",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql"  % "3.5.0",
  "org.apache.spark" %% "spark-hive" % "3.5.0",
  "org.scalatest" %% "scalatest" % "3.2.15" % Test,
  "com.typesafe.play" %% "play-json" % "2.9.4",
  "org.apache.hadoop"   %  "hadoop-client" % "3.3.4",
  "com.norbitltd"     %% "spoiwo"        % "1.7.0"
    exclude("org.scala-lang.modules", "scala-xml_2.12"),
  "com.crealytics"    %% "spark-excel"   % "0.13.5"
)

// ------------------------------------------------------------------
// Evict conflicts between scala-xml versions:
// ------------------------------------------------------------------
import sbt.Keys.evictionErrorLevel
import sbt.librarymanagement.EvictionWarningOptions
evictionErrorLevel := Level.Warn

// Force scala-xml 2.1.0 (la que usa Spark 3.5.0)
dependencyOverrides += "org.scala-lang.modules" %% "scala-xml" % "2.1.0"

// ------------------------------------------------------------------
// Main class for `sbt run`
// ------------------------------------------------------------------
Compile / run / mainClass := Some("Main")
