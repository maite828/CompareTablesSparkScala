-- Tabla 1: ref_customers (con partición)
CREATE TABLE IF NOT EXISTS default.ref_customers (
  id INT,
  country STRING,
  amount DOUBLE,
  status STRING
)
PARTITIONED BY (partition_date STRING)
STORED AS PARQUET;

-- Tabla 2: new_customers (con partición)
CREATE TABLE IF NOT EXISTS default.new_customers (
  id INT,
  country STRING,
  amount DOUBLE,
  status STRING
)
PARTITIONED BY (partition_date STRING)
STORED AS PARQUET;

-- Tabla 3: customer_differences
CREATE TABLE IF NOT EXISTS default.customer_differences (
  id INT,
  Columna STRING,
  Valor_ref STRING,
  Valor_new STRING,
  Resultado STRING
)
PARTITIONED BY (partition_hour STRING)
STORED AS PARQUET;

-- Tabla 4: customer_summary
CREATE TABLE IF NOT EXISTS default.customer_summary (
  Resultado STRING,
  count BIGINT,
  `% Ref` STRING
)
PARTITIONED BY (partition_hour STRING)
STORED AS PARQUET;

-- Tabla 5: customer_duplicates
CREATE TABLE IF NOT EXISTS default.customer_duplicates (
  origin STRING,
  id INT,
  exact_duplicates BOOLEAN,
  total INT,
  variations ARRAY<STRUCT<row: STRUCT<id: INT, country: STRING, amount: DOUBLE, status: STRING>>>
)
PARTITIONED BY (partition_hour STRING)
STORED AS PARQUET;

