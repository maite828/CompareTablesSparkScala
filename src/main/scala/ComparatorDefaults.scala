
/** Centraliza valores “mágicos” para legibilidad y control. */
object ComparatorDefaults {
  // Spark / IO
  final val ShufflePartitionsDefault    = 100
  final val FileOutputCommitterAlgV2    = 2
  final val Sha256Bits                  = 256
  final val MaxPartitionBytes           = 134217728L // 128MB in bytes (controls output file size by actual data size)

  // Límites de logging / muestreo
  final val MaxOrderMismatchesToLog     = 50
  final val MaxTypeDiffsToLog           = 100
  final val SampleIdsForSummary         = 10

  // Duplicados / diffs
  final val MinOccurrencesToBeDuplicate = 2          // >1 → duplicado
  final val DistinctCheckLimit          = 2          // para “columna constante”
  final val SingleDistinctCount         = 1          // para “columna constante”
  final val HashNullToken               = "__NULL__"
  final val HashSeparator               = "§"

  // Fechas “tripleToIso”
  final val YearMin                     = 1900
  final val YearMax                     = 2100
  final val MonthMin                    = 1
  final val MonthMax                    = 12
  final val DayMin                      = 1
  final val DayMax                      = 31
}