#!/usr/bin/env bash
set -euo pipefail

echo "🚀 Ejecutando comparación de tablas (local, Spark 3.5.x + Hive)"

# -------- Spark & Java config --------
SPARK_VERSION="${SPARK_VERSION:-3.5.2}"
SPARK_DIST="spark-${SPARK_VERSION}-bin-hadoop3"
SPARK_DIR="$PWD/.spark/${SPARK_DIST}"
SPARK_TGZ_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_DIST}.tgz"

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

# -------- Build thin jar --------
echo "🎯 Construyendo thin jar (assembly)…"
"/c/Program Files (x86)/sbt/bin/sbt.bat" clean assembly

JAR_PATH="target/scala-2.12/compare-assembly.jar"
[[ -f "$JAR_PATH" ]] || { echo "🛑 No existe $JAR_PATH"; exit 1; }

# -------- Ensure local Spark distribution --------
if [[ ! -d "$SPARK_DIR/jars" ]]; then
  echo "⬇️  Descargando Spark ${SPARK_VERSION}…"
  mkdir -p "$PWD/.spark"

  # Detectar si estamos en Windows (Git Bash/MINGW)
  if [[ "$(uname -s)" == MINGW* ]] || [[ "$(uname -s)" == MSYS* ]]; then
    echo "🪟 Detectado Windows, usando PowerShell para descargar..."
    powershell.exe -Command "
      try {
        Write-Host 'Descargando Spark...'
        [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
        Invoke-WebRequest -Uri '$SPARK_TGZ_URL' -OutFile '.spark/spark.tgz' -UseBasicParsing
        Write-Host 'Descarga completada'
      } catch {
        Write-Host 'Error en descarga: ' \$_.Exception.Message
        exit 1
      }
    "
    if [[ -f ".spark/spark.tgz" ]]; then
      tar -xzf ".spark/spark.tgz" -C "$PWD/.spark"
      rm ".spark/spark.tgz"
    else
      echo "🛑 Error: No se pudo descargar Spark"
      exit 1
    fi
  else
    curl -fL "$SPARK_TGZ_URL" | tar -xz -C "$PWD/.spark"
  fi
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
  