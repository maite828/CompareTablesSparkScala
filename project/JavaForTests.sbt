// project/JavaForTests.sbt

import scala.sys.process._
import java.io.File
import scala.util.Try

def javaHome17OnMac: Option[File] = {
  val os = sys.props.getOrElse("os.name", "").toLowerCase
  if (os.contains("mac")) {
    Try(Seq("/usr/libexec/java_home","-v","17").!!.trim).toOption.map(new File(_))
  } else None
}

// Forzar fork en run/test para que usen la JVM indicada
Compile / run / fork := true
Test / fork := true

// En macOS, fija javaHome de tareas forkeadas a JDK 17 si existe
ThisBuild / javaHome := javaHome17OnMac.orElse((ThisBuild / javaHome).value)
Compile / run / javaHome := (ThisBuild / javaHome).value
Test   / javaHome := (ThisBuild / javaHome).value

// No añadimos --enable-native-access (no existe en Java 17)
// Si algún día saltas a Java 21+, puedes añadirlo puntualmente aquí si lo necesitas.

// HADOOP_HOME dummy en tests para silenciar warnings
Test / envVars += ("HADOOP_HOME" -> (sys.props.getOrElse("user.home", ".") + "/.hadoop-dummy"))
