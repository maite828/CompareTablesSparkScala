#!/usr/bin/env bash
set -euo pipefail

echo "🚀 Ejecutando comparación de tablas (local, Spark 3.5.x + Hive)"

# Java 17 o 11 en macOS
if [[ "$(uname -s)" == "Darwin" ]]; then
  if JAVA_17_HOME="$(/usr/libexec/java_home -v 17 2>/dev/null)"; then
    export JAVA_HOME="$JAVA_17_HOME"
  elif JAVA_11_HOME="$(/usr/libexec/java_home -v 11 2>/dev/null)"; then
    export JAVA_HOME="$JAVA_11_HOME"
  fi
  [[ -n "${JAVA_HOME:-}" ]] && export PATH="$JAVA_HOME/bin:$PATH"
  java -version 2>&1 | head -n1 || true
fi

# 1) Construir thin JAR
echo "🎯 Construyendo thin jar (assembly)…"
sbt -v clean assembly
JAR="target/scala-2.12/compare-assembly.jar"
[[ -f "$JAR" ]] || { echo "🛑 No existe $JAR"; exit 1; }

# 2) Asegurar Spark 3.5.2 local si no hay 3.5.x en PATH
ensure_spark35() {
  local need=true
  if command -v spark-submit >/dev/null 2>&1; then
    local v
    v="$(spark-submit --version 2>&1 | grep -Eo 'version [0-9]+\.[0-9]+\.[0-9]+' | head -n1 | awk '{print $2}')" || true
    [[ "$v" =~ ^3\.5\.[0-9]+$ ]] && need=false
  fi
  if $need; then
    echo "⬇️  Descargando Spark 3.5.2…"
    mkdir -p .spark
    local tgz=".spark/spark-3.5.2-bin-hadoop3.tgz"
    local dir=".spark/spark-3.5.2-bin-hadoop3"
    [[ -f "$tgz" ]] || curl -fL --progress-bar \
      "https://archive.apache.org/dist/spark/spark-3.5.2/spark-3.5.2-bin-hadoop3.tgz" \
      -o "$tgz"
    [[ -d "$dir" ]] || tar -xzf "$tgz" -C .spark
    export SPARK_HOME="$(cd "$dir" && pwd -P)"
    export PATH="$SPARK_HOME/bin:$PATH"
  fi
  echo "✨ Spark: $(spark-submit --version 2>&1 | head -n1)"
}
ensure_spark35

# 3) Metastore local Derby
echo "🧹 Limpiando metastore/warehouse…"
rm -rf metastore_db/ derby.log
mkdir -p spark-warehouse

# 4) Ejecutar
echo "📦 Ejecutando spark-submit con $JAR"
exec spark-submit \
  --class Main \
  --master local[*] \
  --conf spark.sql.warehouse.dir="$PWD/spark-warehouse" \
  --conf spark.hadoop.hive.metastore.warehouse.dir="$PWD/spark-warehouse" \
  --conf javax.jdo.option.ConnectionURL="jdbc:derby:;databaseName=$PWD/metastore_db;create=true" \
  --conf javax.jdo.option.ConnectionDriverName=org.apache.derby.jdbc.EmbeddedDriver \
  --conf datanucleus.autoCreateSchema=true \
  --conf datanucleus.fixedDatastore=false \
  --conf datanucleus.readOnlyDatastore=false \
  --conf hive.metastore.schema.verification=false \
  --conf hive.metastore.schema.verification.record.version=false \
  "$JAR"
