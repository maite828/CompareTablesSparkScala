import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Locale

object PartitionFormatTool {

  private val ISO  = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ROOT)
  private val DMY  = DateTimeFormatter.ofPattern("dd/MM/yyyy", Locale.ROOT)
  private val BASIC= DateTimeFormatter.BASIC_ISO_DATE // yyyyMMdd

  /** Normaliza un partitionSpec recibido en KV-mode.
   * Reemplaza tokens de fecha con executionDate (yyyy-MM-dd) y limpia barras extra.
   * Ejemplos de tokens soportados: YYYY-MM-dd, yyyy-MM-dd, $EXEC_DATE, ${EXEC_DATE}, {{ds}}
   */
  def normalizeKvPartitionSpec(rawSpec: Option[String], executionDateISO: String): Option[String] = {
    if (rawSpec.isEmpty) return None
    val exec = executionDateISO
    val s0   = rawSpec.get.trim
    if (s0.isEmpty) return None

    val s1 = s0
      .replace("YYYY-MM-dd", exec)
      .replace("yyyy-MM-dd", exec)
      .replace("$EXEC_DATE", exec)
      .replace("${EXEC_DATE}", exec)
      .replace("{{ds}}", exec)

    // quitar comillas superfluas alrededor de valores y normalizar dobles barras
    val cleaned = s1.split("/").toSeq
      .map(_.trim)
      .filter(_.nonEmpty)
      .map { kv =>
        val parts = kv.split("=", 2)
        if (parts.length == 2) {
          val k = parts(0).trim
          val v = parts(1).trim.stripPrefix("\"").stripPrefix("'").stripSuffix("\"").stripSuffix("'")
          s"$k=$v"
        } else kv
      }
      .mkString("/")

    if (cleaned.isEmpty) None else Some(cleaned + (if (cleaned.endsWith("/") ) "" else "/"))
  }

  /** Extrae una fecha ISO (yyyy-MM-dd) desde el partitionSpec si existe; si no, devuelve defaultISO. */
  def extractDateOr(partitionSpec: Option[String], defaultISO: String): String = {
    val spec = partitionSpec.getOrElse("")
    if (!spec.exists(_.isDigit)) return defaultISO // sin dígitos, seguro que no hay fecha útil
    val extracted = extractDateFromPartitionSpec(partitionSpec)
    if (extracted == null || extracted.isEmpty) defaultISO else extracted
  }

  /** Intenta extraer una fecha ISO desde un spec. Si no encuentra nada, devuelve cadena vacía. */
  def extractDateFromPartitionSpec(partitionSpec: Option[String]): String = {
    if (partitionSpec.isEmpty) return ""

    val spec = partitionSpec.get

    // 1) ISO explícito  yyyy-MM-dd  (entre comillas o sin comillas)
    val isoQuoted = """[A-Za-z0-9_]+\s*=\s*"([0-9]{4}-[0-9]{2}-[0-9]{2})"""".r
      .findFirstMatchIn(spec).map(_.group(1))
    val isoBare   = """\b([0-9]{4}-[0-9]{2}-[0-9]{2})\b""".r
      .findFirstMatchIn(spec).map(_.group(1))

    // 2) DMY  dd/MM/yyyy  → convertir a ISO
    val dmyQuoted = """[A-Za-z0-9_]+\s*=\s*"([0-9]{2}/[0-9]{2}/[0-9]{4})"""".r
      .findFirstMatchIn(spec).map(m => safeParse(m.group(1), DMY).getOrElse(""))
    val dmyBare   = """\b([0-9]{2}/[0-9]{2}/[0-9]{4})\b""".r
      .findFirstMatchIn(spec).map(m => safeParse(m.group(1), DMY).getOrElse(""))

    // 3) Heurística triple (tres números con año y dos de 2 dígitos)
    val quotedNums = """=\s*"([0-9]{2,4})"""".r.findAllMatchIn(spec).map(_.group(1)).toList
    val bareNums   = """\b([0-9]{2,4})\b""".r.findAllMatchIn(spec).map(_.group(1)).toList
    val merged     = (quotedNums ++ bareNums).distinct.take(3)
    val triple     = tripleToIso(merged)

    isoQuoted.orElse(isoBare)
      .orElse(dmyQuoted.filter(_.nonEmpty))
      .orElse(dmyBare.filter(_.nonEmpty))
      .orElse(triple)
      .getOrElse("")
  }

  // ---------- helpers ----------

  private def safeParse(s: String, fmt: DateTimeFormatter): Option[String] =
    try Some(LocalDate.parse(s, fmt).format(ISO)) catch { case _: Throwable => None }

  private def toInt(s: String): Option[Int] =
    try Some(s.toInt) catch { case _: Throwable => None }

  private def tripleToIso(tokens: List[String]): Option[String] = {
    // buscamos un año razonable (4 dígitos), y dos números de 2 dígitos (mes/día)
    val nums = tokens.flatMap(toInt)
    if (nums.length < 3) return None
    val years = nums.filter(n => n >= 1900 && n <= 2100)
    val year  = if (years.nonEmpty) years.head else return None
    val twos  = nums.filter(n => n >= 0 && n <= 99).take(2)
    if (twos.length < 2) return None
    val m1 = twos(0); val d1 = twos(1)

    def fmt(y: Int, m: Int, d: Int): Option[String] =
      if (m >= 1 && m <= 12 && d >= 1 && d <= 31) Some(f"$y-$m%02d-$d%02d") else None

    fmt(year, m1, d1).orElse(fmt(year, d1, m1))
  }
}
