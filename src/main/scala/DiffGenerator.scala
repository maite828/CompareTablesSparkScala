import org.apache.spark.sql.{DataFrame, SparkSession, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object DiffGenerator {

  /**
   * Formatea valores para la salida:
   * - NULL o vacío → "-"
   * - Otros valores → string normalizado
   */
  private def formatString(c: Column): Column = {
    val s = c.cast("string")
    when(s.isNull || trim(s) === "", lit("-")).otherwise(s)
  }

  /**
   * Genera la tabla de diferencias entre dos DataFrames
   *
   * @param spark SparkSession
   * @param refDf DataFrame de referencia
   * @param newDf Nuevo DataFrame a comparar
   * @param compositeKeyCols Columnas que forman la clave compuesta
   * @param compareColsIn Columnas a comparar (ya filtradas por el controlador)
   * @param includeEquals Si incluir coincidencias exactas en el resultado
   * @return DataFrame con las diferencias encontradas
   */
  def generateDifferencesTable(
      spark: SparkSession,
      refDf: DataFrame,
      newDf: DataFrame,
      compositeKeyCols: Seq[String],
      compareColsIn: Seq[String],
      includeEquals: Boolean
  ): DataFrame = {

    import spark.implicits._

    // 1) Normalización de strings vacíos en las claves compuestas → null
    val normalizeEmptyToNull: Column => Column =
      c => when(trim(c.cast("string")) === "", lit(null)).otherwise(c)

    val normalizedRef = compositeKeyCols.foldLeft(refDf) { (df, colName) =>
      df.withColumn(colName, normalizeEmptyToNull(col(colName)))
    }
    val normalizedNew = compositeKeyCols.foldLeft(newDf) { (df, colName) =>
      df.withColumn(colName, normalizeEmptyToNull(col(colName)))
    }

    // 2) Filtrado de columnas constantes (tipo partición) para no comparar basura
    val baseCols   = compareColsIn.filterNot(compositeKeyCols.contains).distinct
    val commonCols = normalizedRef.columns.toSet.intersect(normalizedNew.columns.toSet)
    val autoPartitionLike = commonCols.filter { c =>
      baseCols.contains(c) &&
      isConstantColumn(normalizedRef, c) &&
      isConstantColumn(normalizedNew, c)
    }
    if (autoPartitionLike.nonEmpty) {
      println(s"[INFO] Excluyendo columnas constantes: ${autoPartitionLike.mkString(", ")}")
    }
    val compareCols = baseCols.filterNot(autoPartitionLike.contains)

    // 3) Agregación por clave compuesta (usar first(ignoreNulls = true) para TODOS los tipos)
    val refAgg = aggregateByCompositeKey(normalizedRef, compositeKeyCols, compareCols).alias("ref")
    val newAgg = aggregateByCompositeKey(normalizedNew, compositeKeyCols, compareCols).alias("new")

    // 4) Join full y banderas de presencia (null-safe en clave)
    val joinCondition = compositeKeyCols.map(c => col(s"ref.$c") <=> col(s"new.$c")).reduce(_ && _)
    val joined = refAgg.join(newAgg, joinCondition, "fullouter")
      .withColumn("exists_ref", col("ref._present").isNotNull)
      .withColumn("exists_new", col("new._present").isNotNull)

    // 5) Construcción de diffs por columna
    val diffs = compareCols.map { colName =>
      buildDiffStruct(compositeKeyCols, colName)
    }

    // 6) Explode y filtrado final
    val result = joined
      .select(array(diffs: _*).as("diffs"))
      .withColumn("diff", explode($"diffs"))
      .select("diff.*")

    if (includeEquals) result else result.filter($"results" =!= "MATCH")
  }

  /** Determina si una columna tiene valores constantes */
  private def isConstantColumn(df: DataFrame, colName: String): Boolean = {
    df.select(col(colName)).distinct().limit(2).count() <= 1
  }

  /** Agrega un DataFrame por sus claves compuestas: usar siempre first(ignoreNulls=true) */
  private def aggregateByCompositeKey(
      df: DataFrame,
      keyCols: Seq[String],
      compareCols: Seq[String]
  ): DataFrame = {
    val aggs = compareCols.map { c =>
      first(col(c), ignoreNulls = true).as(c)
    }
    df.groupBy(keyCols.map(col): _*)
      .agg(aggs.head, aggs.tail: _*)
      .withColumn("_present", lit(1))
  }

  /**
   * Construye la estructura de diferencias:
   * - id: coalesce(ref.key, new.key) por componente, y SOLO en visualización null → "NULL"
   * - value_ref / value_new formateados con "-"
   * - results: ONLY_IN_REF / ONLY_IN_NEW / MATCH / NO_MATCH
   */
  private def buildDiffStruct(
      keyCols: Seq[String],
      colName: String
  ): Column = {

    val refCol = col(s"ref.$colName")
    val newCol = col(s"new.$colName")

    val result = when(!col("exists_new"), lit("ONLY_IN_REF"))
      .when(!col("exists_ref"), lit("ONLY_IN_NEW"))
      .when(refCol <=> newCol, lit("MATCH"))
      .otherwise(lit("NO_MATCH"))

    // 1) Combinar valor de clave (ref/new) a nivel de componente SIN usar "NULL" string
    val mergedKeyParts: Seq[Column] = keyCols.map { k =>
      coalesce(col(s"ref.$k"), col(s"new.$k"))
    }

    // 2) Solo para mostrar: null → "NULL"
    val printableKeyParts: Seq[Column] = mergedKeyParts.map { p =>
      when(p.isNull, lit("NULL")).otherwise(p.cast("string"))
    }

    val compositeId = concat_ws("_", printableKeyParts: _*)

    struct(
      compositeId.as("id"),
      lit(colName).as("column"),
      formatString(refCol).as("value_ref"),
      formatString(newCol).as("value_new"),
      result.as("results")
    )
  }
}
