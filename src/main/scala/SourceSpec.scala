
import org.apache.spark.sql.types.StructType

sealed trait SourceSpec {
  def options: Map[String, String]
  def schema: Option[StructType]
}

final case class HiveTable(
  tableName: String,
  override val options: Map[String, String] = Map.empty
) extends SourceSpec {
  override val schema: Option[StructType] = None
}

final case class FileSource(
  path: String,
  format: String, // "csv" | "parquet"
  override val options: Map[String, String] = Map.empty,
  override val schema: Option[StructType] = None
) extends SourceSpec