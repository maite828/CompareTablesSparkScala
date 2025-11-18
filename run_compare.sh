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
# Ruta Spark ya presente en Windows (solicitud usuario)
DEFAULT_WIN_SPARK="C:/Users/x732182/IdeaProjects/mio/CompareTablesSparkScala/.spark/spark-3.5.2-bin-hadoop3"

# Selecciona Java 11 (recomendado para Spark 3.5.x)
if [[ "$(uname -s)" == "Darwin" ]]; then
  if JAVA_11_HOME="$(/usr/libexec/java_home -v 11 2>/dev/null)"; then
    export JAVA_HOME="$JAVA_11_HOME"
  else
    echo "🛑 No JDK 11 encontrado. Instala: brew install --cask temurin@11"
    exit 1
  fi
  export PATH="$JAVA_HOME/bin:$PATH"
elif [[ "$OSTYPE" == msys* || "$OSTYPE" == cygwin* || "$OSTYPE" == mingw* ]]; then
  resolve_java_home_windows() {
    # Respeta JAVA_HOME si apunta a un JDK real
    if [[ -n "${JAVA_HOME:-}" && -x "$JAVA_HOME/bin/java" ]]; then
      printf '%s\n' "$JAVA_HOME"
      return 0
    fi

    # Ignora javapath y deriva desde java en PATH
    if JAVA_BIN="$(command -v java 2>/dev/null)"; then
      JAVA_BIN_WIN="$(cygpath -m "$JAVA_BIN")"
      JAVA_HOME_WIN="${JAVA_BIN_WIN%/bin/java.exe}"
      JAVA_HOME_WIN="${JAVA_HOME_WIN%/bin/java}"
      JAVA_HOME_UNIX="$(cygpath -u "$JAVA_HOME_WIN")"
      if [[ -x "$JAVA_HOME_UNIX/bin/java" ]]; then
        printf '%s\n' "$JAVA_HOME_UNIX"
        return 0
      fi
    fi

    # Busca el último JDK instalado en rutas estándar
    DEFAULT_WIN_JAVA="/c/Program Files/Java/jdk-11"
    if [[ -x "$DEFAULT_WIN_JAVA/bin/java" ]]; then
      printf '%s\n' "$DEFAULT_WIN_JAVA"
      return 0
    fi

    for base in "/c/Program Files/Java" "/c/Program Files (x86)/Java" "/c/Program Files/Eclipse Adoptium"; do
      # shellcheck disable=SC2012
      if candidate=$(ls -1d "$base"/jdk-* "$base"/temurin-* "$base"/zulu-* 2>/dev/null | sort -V | tail -n1); then
        if [[ -x "$candidate/bin/java" ]]; then
          printf '%s\n' "$candidate"
          return 0
        fi
      fi
    done
    return 1
  }

  if resolved_java_home="$(resolve_java_home_windows)"; then
    export JAVA_HOME="$resolved_java_home"
    export PATH="$JAVA_HOME/bin:$PATH"
    echo "🪟 Windows → usando JAVA_HOME=$JAVA_HOME"
  else
    echo "🛑 No se encontró JDK en Windows; define JAVA_HOME (ej. C:/Program Files/Java/jdk-17.x)."
    exit 1
  fi
fi
java -version 2>&1 | head -n1
export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:-} ${COMMON_JAVA_OPTS[*]}"
export SPARK_SUBMIT_OPTS="${SPARK_SUBMIT_OPTS:-} ${COMMON_JAVA_OPTS[*]}"

# Forzar SPARK_HOME al bundle del repo en Windows
if [[ "$OSTYPE" == msys* || "$OSTYPE" == cygwin* || "$OSTYPE" == mingw* ]]; then
  if default_win_unix="$(cygpath -u "$DEFAULT_WIN_SPARK" 2>/dev/null || true)"; then
    export SPARK_HOME="$default_win_unix"
  fi
  # Evita chequeo de proceso padre (ps -o no soportado en algunas shells)
  export SPARK_NO_PARENT_CHECK=1
  # Hadoop dummy para Windows (evita winutils)
  HADOOP_DUMMY_WIN="$PWD/.hadoop-dummy"
  mkdir -p "$HADOOP_DUMMY_WIN"/bin
  export HADOOP_HOME="$HADOOP_DUMMY_WIN"
  export hadoop_home_dir="$HADOOP_DUMMY_WIN"
fi

# -------- Build thin jar --------
echo "🎯 Construyendo thin jar (assembly)…"
sbt clean assembly

JAR_PATH="target/scala-2.12/compare-assembly.jar"
[[ -f "$JAR_PATH" ]] || { echo "🛑 No existe $JAR_PATH"; exit 1; }

# -------- Detect/ensure Spark distribution --------
detect_spark() {
  # 1) Honra SPARK_HOME si ya está instalado
  if [[ -n "${SPARK_HOME:-}" && -x "$SPARK_HOME/bin/spark-submit" ]]; then
    return 0
  fi

  # 1b) En Windows, usar la ruta conocida si existe
  if [[ "$OSTYPE" == msys* || "$OSTYPE" == cygwin* || "$OSTYPE" == mingw* ]]; then
    if default_win_unix="$(cygpath -u "$DEFAULT_WIN_SPARK" 2>/dev/null || true)"; then
      if [[ -x "$default_win_unix/bin/spark-submit" ]]; then
        export SPARK_HOME="$default_win_unix"
        return 0
      fi
    fi
  fi

  # 2) Usa caché local ".spark/<dist>"
  if [[ -x "$SPARK_DIR/bin/spark-submit" ]]; then
    export SPARK_HOME="$SPARK_DIR"
    return 0
  fi

  # 3) Intentar descargar si no existe
  echo "⬇️  Descargando Spark ${SPARK_VERSION}…"
  mkdir -p "$PWD/.spark"
  CURL_FLAGS=(-fL --connect-timeout 15 --max-time 900)
  if [[ "${SPARK_ALLOW_INSECURE_DOWNLOAD:-}" == "1" ]]; then
    CURL_FLAGS+=(-k)
    echo "⚠️  SPARK_ALLOW_INSECURE_DOWNLOAD=1 → usando curl -k (sin revocación CRL)"
  fi

  if curl "${CURL_FLAGS[@]}" "$SPARK_TGZ_URL" | tar -xz -C "$PWD/.spark"; then
    export SPARK_HOME="$SPARK_DIR"
    return 0
  fi

  echo "⚠️  Descarga falló. Reintenta con SPARK_ALLOW_INSECURE_DOWNLOAD=1 o define SPARK_HOME a tu instalación existente."
  echo "   URL manual: $SPARK_TGZ_URL"
  return 1
}

if ! detect_spark; then
  exit 1
fi
if [[ ! -d "$SPARK_HOME/jars" ]]; then
  echo "🛑 Spark mal descomprimido (no hay ${SPARK_HOME}/jars)."
  exit 1
fi

SPARK_SUBMIT_BIN="$SPARK_HOME/bin/spark-submit"
if [[ "$OSTYPE" == msys* || "$OSTYPE" == cygwin* || "$OSTYPE" == mingw* ]]; then
  # Usa el script .cmd para evitar dependencias de ps/cygpath en bash
  SPARK_HOME_WIN="$(cygpath -m "$SPARK_HOME")"
  export SPARK_HOME="$SPARK_HOME_WIN"
  SPARK_SUBMIT_BIN="$SPARK_HOME/bin/spark-submit.cmd"
fi

echo "🧹 Limpiando metastore/warehouse…"
rm -rf metastore_db/ derby.log spark-warehouse/* || true
mkdir -p spark-warehouse

# -------- Run with spark-submit --------
echo "📦 Ejecutando spark-submit con $JAR_PATH"
"$SPARK_SUBMIT_BIN" \
  --master local[*] \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.ui.enabled=false \
  "$JAR_PATH"
