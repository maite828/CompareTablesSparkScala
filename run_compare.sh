#!/usr/bin/env bash
set -euo pipefail

echo "🚀 Ejecutando comparación de tablas (local, Spark 3.5.x + Hive)"

# -------- Spark & Java config --------
SPARK_VERSION="${SPARK_VERSION:-3.5.2}"
SPARK_DIST="spark-${SPARK_VERSION}-bin-hadoop3"
SPARK_DIR="$PWD/.spark/${SPARK_DIST}"
SPARK_TGZ_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_DIST}.tgz"
COMMON_JAVA_OPTS=(
  "--add-opens=java.base/java.lang=ALL-UNNAMED"
  "--add-opens=java.base/java.io=ALL-UNNAMED"
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
)

# Selecciona Java 11 (recomendado para Spark 3.5.x)
if [[ "$(uname -s)" == "Darwin" ]]; then
  if JAVA_11_HOME="$(/usr/libexec/java_home -v 11 2>/dev/null)"; then
    export JAVA_HOME="$JAVA_11_HOME"
  else
    echo "🛑 No JDK 11 encontrado. Instala: brew install --cask temurin@11"
    exit 1
  fi
  export PATH="$JAVA_HOME/bin:$PATH"
fi
java -version 2>&1 | head -n1
export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:-} ${COMMON_JAVA_OPTS[*]}"
export SPARK_SUBMIT_OPTS="${SPARK_SUBMIT_OPTS:-} ${COMMON_JAVA_OPTS[*]}"

# -------- Build thin jar --------
echo "🎯 Construyendo thin jar (assembly)…"
sbt clean assembly

JAR_PATH="target/scala-2.12/compare-assembly.jar"
[[ -f "$JAR_PATH" ]] || { echo "🛑 No existe $JAR_PATH"; exit 1; }

# -------- Ensure local Spark distribution --------
if [[ ! -d "$SPARK_DIR/jars" ]]; then
  echo "⬇️  Descargando Spark ${SPARK_VERSION}…"
  mkdir -p "$PWD/.spark"
  curl -fL "$SPARK_TGZ_URL" | tar -xz -C "$PWD/.spark"
fi
if [[ ! -d "$SPARK_DIR/jars" ]]; then
  echo "🛑 Spark mal descomprimido (no hay ${SPARK_DIR}/jars)."
  exit 1
fi

echo "🧹 Limpiando metastore/warehouse…"
rm -rf metastore_db/ derby.log spark-warehouse/* || true
mkdir -p spark-warehouse

# -------- Run with spark-submit --------
export SPARK_HOME="$SPARK_DIR"
echo "📦 Ejecutando spark-submit con $JAR_PATH"
"$SPARK_HOME/bin/spark-submit" \
  --master local[*] \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.ui.enabled=false \
  "$JAR_PATH"
