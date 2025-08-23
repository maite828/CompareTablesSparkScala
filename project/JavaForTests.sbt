// project/JavaForTests.sbt
import scala.util.Try

ThisBuild / Test / javaOptions ++= {
  val isJdk21plus = Try(sys.props("java.specification.version").toInt).toOption.exists(_ >= 21)
  if (isJdk21plus) Seq("--add-opens=java.base/java.lang=ALL-UNNAMED", "--enable-native-access=ALL-UNNAMED")
  else Seq.empty
}
