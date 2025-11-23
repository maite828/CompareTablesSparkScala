
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.apache.spark.sql.SparkSession

/**
  * Trait de ayuda para compartir una única instancia de `SparkSession` entre
  * todos los tests unitarios e integración.
  *
  * Ventajas:
  *   • Reduce el tiempo de arranque en CI.
  *   • Evita consumo extra de memoria.
  *   • Garantiza configuración homogénea (master local[*]).
  *
  * Uso:
  * ```scala
  * class MiSpec extends AnyFlatSpec with Matchers with SparkSessionTestWrapper {
  * }
  * ```
  */
trait SparkSessionTestWrapper extends BeforeAndAfterAll { self: Suite =>

  /** Sesión Spark compartida (lazy). */
  protected lazy val spark: SparkSession = SparkSession.builder()
    .appName("unit-tests")
    .master("local[*]")
    .config("spark.ui.enabled", "false")   // desactiva UI para GitHub Actions
    .config("spark.driver.host", "localhost")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.hadoop.security.authentication", "simple")
    .config("spark.hadoop.security.authorization", "false")
    .getOrCreate()

  /** Cierre ordenado tras terminar todos los tests de la Suite. */
  override protected def afterAll(): Unit = {
    try {
      spark.stop()
    } finally {
      super.afterAll()
    }
  }
}
