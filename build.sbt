// build.sbt — proyecto ÚNICO, JAR “thin” (Spark/Hadoop/Hive en "provided")
import sbt._
import Keys._
import sbtassembly.AssemblyPlugin
import sbtassembly.AssemblyPlugin.autoImport._
import sbtassembly.MergeStrategy

ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "com.santander.cib.adhc.internal_aml_tools"
ThisBuild / fork := true
ThisBuild / javaOptions ++= Seq(
  "-Dhadoop.home.dir=/tmp/hadoop-dummy",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED"
)

lazy val root = (project in file("."))
  .enablePlugins(AssemblyPlugin)
  .settings(
    name := "CompareTablesProject",
    publish / skip := true,

    // Main class
    Compile / run / mainClass := Some("Main"),
    assembly / mainClass := (Compile / run / mainClass).value,

    // Nombre del jar que esperan tus scripts/Makefile
    assembly / assemblyJarName := "compare-assembly.jar",

    // Dependencias: Spark/Hadoop/Hive en provided (thin JAR)
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.0" % "provided",
      "org.apache.spark" %% "spark-sql"  % "3.5.0" % "provided",
      "org.apache.spark" %% "spark-hive" % "3.5.0" % "provided",
      "org.apache.hadoop" % "hadoop-client" % "3.3.4" % "provided",

      "com.typesafe.play" %% "play-json"   % "2.9.4",
      "com.norbitltd"     %% "spoiwo"      % "1.7.0" exclude("org.scala-lang.modules","scala-xml_2.12"),
      "com.crealytics"    %% "spark-excel" % "0.13.5",
      "org.scalatest"     %% "scalatest"   % "3.2.15" % Test
    ),

    // Forzar scala-xml de Spark para evitar conflictos
    dependencyOverrides += "org.scala-lang.modules" %% "scala-xml" % "2.1.0",

    // No abortar por evictions (solo warning)
    evictionErrorLevel := Level.Warn,

    // Merge strategy: eliminar firmas/MANIFEST problemáticos
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) =>
        xs.map(_.toLowerCase) match {
          case ("manifest.mf" :: Nil) => MergeStrategy.discard
          case ("index.list"  :: Nil) => MergeStrategy.discard
          case ("dependencies":: Nil) => MergeStrategy.discard
          case ("license"      :: Nil)=> MergeStrategy.discard
          case ("notice"       :: Nil)=> MergeStrategy.discard
          case ("services"     :: _ ) => MergeStrategy.filterDistinctLines
          case _                      => MergeStrategy.discard // descarta también firmas (.SF/.RSA/.DSA)
        }
      case PathList("module-info.class") => MergeStrategy.discard
      case "reference.conf"              => MergeStrategy.concat
      case "application.conf"            => MergeStrategy.concat
      case _                             => MergeStrategy.first
    }
  )
