spark.sql("""
CREATE TABLE IF NOT EXISTS customer_differences (
  id STRING,
  Columna STRING,
  Valor_ref STRING,
  Valor_new STRING,
  Resultado STRING
)
PARTITIONED BY (partition_hour STRING)
STORED AS PARQUET
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS customer_summary (
  Resultado STRING,
  count BIGINT,
  `% Ref` STRING
)
PARTITIONED BY (partition_hour STRING)
STORED AS PARQUET
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS customer_duplicates (
  origin STRING,
  id STRING,
  exact_duplicates BOOLEAN,
  total INT,
  variations ARRAY<STRUCT<row: STRUCT<country: STRING, amount: DOUBLE, status: STRING>>>
)
PARTITIONED BY (partition_hour STRING)
STORED AS PARQUET
""")


