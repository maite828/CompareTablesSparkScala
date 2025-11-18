#!/usr/bin/env bash
set -euo pipefail

echo "🚀 Ejecutando comparación de tablas (local, Spark 3.5.x + Hive)"

# -------- Spark & Java config --------
SPARK_VERSION="${SPARK_VERSION:-3.5.2}"
SPARK_DIST="spark-${SPARK_VERSION}-bin-hadoop3"
SPARK_DIR="$PWD/.spark/${SPARK_DIST}"
SPARK_TGZ_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_DIST}.tgz"
SBT_CMD="${SBT_CMD:-sbt}"

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

# Resolver sbt (PATH o rutas habituales de Windows). Mantén comillas para rutas con espacios.
if ! command -v "$SBT_CMD" >/dev/null 2>&1; then
  for sbt_path in "/c/Program Files (x86)/sbt/bin/sbt.bat" "/c/Program Files/sbt/bin/sbt.bat"; do
    if [[ -x "$sbt_path" ]]; then
      SBT_CMD="$sbt_path"
      break
    fi
  done
fi
if ! command -v "$SBT_CMD" >/dev/null 2>&1 && [[ ! -x "$SBT_CMD" ]]; then
  echo "🛑 No se encontró sbt. Instálalo (winget/choco) o exporta SBT_CMD apuntando a sbt.bat."
  exit 1
fi

"$SBT_CMD" clean assembly

JAR_PATH="target/scala-2.12/compare-assembly.jar"
[[ -f "$JAR_PATH" ]] || { echo "🛑 No existe $JAR_PATH"; exit 1; }

# -------- Ensure local Spark distribution --------
# En esta rama NO descargamos Spark automáticamente para no romper Windows.
# Reutiliza tu SPARK_HOME o el bundle bajo .spark si ya existe.
if [[ -x "${SPARK_HOME:-}/bin/spark-submit" && -d "${SPARK_HOME:-}/jars" ]]; then
  echo "✅ Usando SPARK_HOME existente: $SPARK_HOME"
elif [[ -d "$SPARK_DIR/jars" ]]; then
  echo "✅ Usando Spark ya descomprimido en $SPARK_DIR"
  export SPARK_HOME="$SPARK_DIR"
else
  echo "🛑 No se encontró Spark. Define SPARK_HOME apuntando a una distro con /bin y /jars."
  exit 1
fi

if [[ -z "${SPARK_HOME:-}" || ! -d "$SPARK_HOME/jars" ]]; then
  echo "🛑 Spark mal descomprimido (no hay ${SPARK_HOME:-<unset>}/jars)."
  exit 1
fi

echo "🧹 Limpiando metastore/warehouse…"
rm -rf metastore_db/ derby.log spark-warehouse/* || true
mkdir -p spark-warehouse

# -------- Run with spark-submit --------
echo "📦 Ejecutando spark-submit con $JAR_PATH"
"$SPARK_HOME/bin/spark-submit" \
  --master local[*] \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.ui.enabled=false \
  "$JAR_PATH"
  
