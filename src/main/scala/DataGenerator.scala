import org.apache.spark.sql.{SaveMode, SparkSession}

object DataGenerator {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataGenerator")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    spark.sql("CREATE DATABASE IF NOT EXISTS db")

    // 1. Tabla Referencia
    // ID=1: balance="" (cadena vacía) para probar NULL vs ""
    // ID=2: Duplicado con diferentes prioridades (priority=1 y priority=2)
    // IDs 5-10: Datos adicionales para muestreo aleatorio
    val refData = Seq(
      (1, "Alice", Some(""), "2023-11-22", "ES", 1),           // balance = "" (vacío)
      (1, "Alice", Some(""), "2023-11-22", "ES", 1),           // balance = "" (vacío)
      (2, "Bob_v1", Some("200.0"), "2023-11-23", "ES", 4),     // Prioridad alta
      (2, "Bob_v2", Some("220.0"), "2023-11-23", "ES", 2),     // Prioridad alta - debería ganar
      (2, "Bob_v4", Some("255.0"), "2023-11-23", "ES", 2),     // Prioridad alta - debería ganar
      (2, "Bob", Some("180.0"), "2023-11-22", "ES", 0),
      (3, "Charlie", Some("300.0"), "2023-11-22", "FR", 1),
      (5, "Emma", Some("150.0"), "2023-11-22", "ES", 1),
      (6, "Frank", Some("275.0"), "2023-11-22", "FR", 1),
      (7, "Grace", Some("320.0"), "2023-11-22", "ES", 2),
      (8, "Henry", Some("190.0"), "2023-11-22", "FR", 1),
      (9, "Iris", Some("410.0"), "2023-11-22", "ES", 1),
      (10, "Jack", Some("230.0"), "2023-11-22", "FR", 2)
    ).toDF("id", "name", "balance", "data_date_part", "geo", "priority")

    refData.write
      .mode(SaveMode.Overwrite)
      .partitionBy("geo", "data_date_part")
      .saveAsTable("db.tabla_referencia")

    println("✅ Creada tabla db.tabla_referencia")

    // 2. Tabla Nueva (con nombres de columna distintos para probar mapeo)
    // ID=1: balance=null para probar NULL vs ""
    // ID=2: Duplicado con diferentes prioridades
    // IDs 4-10: Datos adicionales para muestreo aleatorio
    val newData = Seq(
      (1, "Alice", None, "2023-11-22", "ES", 1),                    // balance = null
      (2, "Bob_v1", Some("250.0"), "2023-11-23", "ES", 2),          // Prioridad baja
      (2, "Bob_v2", Some("260.0"), "2023-11-23", "ES", 4),          // Prioridad alta - debería ganar
      (2, "Bob_v4", Some("260.0"), "2023-11-23", "ES", 2),          // Prioridad alta - debería ganar
      (4, "David", Some("400.0"), "2023-11-22", "FR", 1),           // Nuevo
      (5, "Emma", Some("155.0"), "2023-11-22", "ES", 1),            // Cambio en balance
      (6, "Frank", Some("275.0"), "2023-11-22", "FR", 1),           // Igual
      (7, "Grace", Some("325.0"), "2023-11-22", "ES", 2),           // Cambio en balance
      (8, "Henry", Some("190.0"), "2023-11-22", "FR", 1),           // Igual
      (9, "Iris", Some("415.0"), "2023-11-22", "ES", 1),            // Cambio en balance
      (9, "Iris", Some("415.0"), "2023-11-22", "ES", 2),            // Cambio en balance
      (10, "Jack", Some("230.0"), "2023-11-22", "FR", 2)            // Igual
    ).toDF("id_v2", "nombre_cliente", "saldo", "fecha_proceso", "pais", "priority")

    newData.write
      .mode(SaveMode.Overwrite)
      .partitionBy("pais", "fecha_proceso")
      .saveAsTable("db.tabla_nueva")

    println("✅ Creada tabla db.tabla_nueva")

    spark.stop()
  }
}
