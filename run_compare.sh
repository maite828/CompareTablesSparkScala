#!/usr/bin/env bash
set -euo pipefail

echo "🚀 Ejecutando comparación de tablas (local, Spark 3.5.x + Hive)"

# -------- Spark & Java config --------
SPARK_VERSION="${SPARK_VERSION:-3.5.2}"
SPARK_DIST="spark-${SPARK_VERSION}-bin-hadoop3"
SPARK_DIR="$PWD/.spark/${SPARK_DIST}"
SPARK_TGZ_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_DIST}.tgz"
SPARK_TGZ_PATH="$PWD/.spark/${SPARK_DIST}.tgz"
# Java recomendado depende de la versión de Spark (4.x → Java 17)
if [[ "$SPARK_VERSION" == 4.* ]]; then
  REQUESTED_JAVA_VERSION="${SPARK_JAVA_VERSION:-17}"
else
  REQUESTED_JAVA_VERSION="${SPARK_JAVA_VERSION:-11}"
fi
COMMON_JAVA_OPTS=(
  "--add-opens=java.base/java.lang=ALL-UNNAMED"
  "--add-opens=java.base/java.io=ALL-UNNAMED"
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
)
# Ruta Spark ya presente en Windows (solicitud usuario)
DEFAULT_WIN_SPARK="C:/Users/x732182/IdeaProjects/mio/CompareTablesSparkScala/.spark/spark-3.5.2-bin-hadoop3"

# Selecciona Java adecuado (por defecto 11, Spark 4.x → 17)
if [[ "$(uname -s)" == "Darwin" ]]; then
  if JAVA_HOME_DETECTED="$(/usr/libexec/java_home -v "$REQUESTED_JAVA_VERSION" 2>/dev/null)"; then
    export JAVA_HOME="$JAVA_HOME_DETECTED"
  else
    echo "🛑 No JDK $REQUESTED_JAVA_VERSION encontrado. Instala: brew install --cask temurin@$REQUESTED_JAVA_VERSION"
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
    DEFAULT_WIN_JAVA="/c/Program Files/Java/jdk-$REQUESTED_JAVA_VERSION"
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
  # Asegura winutils.exe si está disponible en el bundle
  WINUTILS_TARGET="$HADOOP_DUMMY_WIN/bin/winutils.exe"
  if [[ ! -f "$WINUTILS_TARGET" ]]; then
    for candidate in \
      "$SPARK_HOME/bin/winutils.exe" \
      "$PWD/.spark/winutils.exe" \
      "$PWD/winutils.exe"; do
      if [[ -f "$candidate" ]]; then
        cp "$candidate" "$WINUTILS_TARGET"
        break
      fi
    done
  fi
  export PATH="$HADOOP_HOME/bin:$PATH"
fi

# -------- Build thin jar --------
echo "🎯 Construyendo thin jar (assembly)…"
sbt clean assembly

JAR_PATH="target/scala-2.12/compare-assembly.jar"
[[ -f "$JAR_PATH" ]] || { echo "🛑 No existe $JAR_PATH"; exit 1; }

# -------- Detect/ensure Spark distribution --------
spark_home_version() {
  local home="$1"
  local core_jar
  if core_jar=$(ls "$home"/jars/spark-core_*.jar 2>/dev/null | head -n1); then
    basename "$core_jar" | sed -E 's/^spark-core_[^-]+-([0-9.]+).*$/\1/' | head -n1
    return 0
  fi
  if [[ -f "$home/RELEASE" ]]; then
    grep -Eo 'Spark[[:space:]]+[0-9]+\.[0-9]+\.[0-9]+' "$home/RELEASE" | awk '{print $2}' | head -n1
    return 0
  fi
  return 1
}

detect_spark() {
  # 1) Honra SPARK_HOME si ya está instalado
  if [[ -n "${SPARK_HOME:-}" && -x "$SPARK_HOME/bin/spark-submit" ]]; then
    if current_version=$(spark_home_version "$SPARK_HOME" 2>/dev/null); then
      if [[ "$current_version" == "$SPARK_VERSION" ]]; then
        return 0
      else
        echo "ℹ️  SPARK_HOME apunta a Spark $current_version; se necesita Spark $SPARK_VERSION, ignorando SPARK_HOME."
        unset SPARK_HOME
      fi
    else
      echo "ℹ️  SPARK_HOME definido pero no se pudo determinar la versión; se descargará Spark $SPARK_VERSION."
      unset SPARK_HOME
    fi
  fi

  # 1b) En Windows, usar la ruta conocida si existe
  if [[ "$OSTYPE" == msys* || "$OSTYPE" == cygwin* || "$OSTYPE" == mingw* ]]; then
    if default_win_unix="$(cygpath -u "$DEFAULT_WIN_SPARK" 2>/dev/null || true)"; then
      if [[ -x "$default_win_unix/bin/spark-submit" ]]; then
        if current_version=$(spark_home_version "$default_win_unix" 2>/dev/null); then
          if [[ "$current_version" == "$SPARK_VERSION" ]]; then
            export SPARK_HOME="$default_win_unix"
            return 0
          fi
        fi
      fi
    fi
  fi

  # 2) Usa caché local ".spark/<dist>"
  if [[ -d "$SPARK_DIR" && ! -d "$SPARK_DIR/jars" ]]; then
    echo "ℹ️  Spark en caché sin directorio jars; se re-descargará ($SPARK_DIR)."
    rm -rf "$SPARK_DIR"
  fi
  if [[ -x "$SPARK_DIR/bin/spark-submit" && -d "$SPARK_DIR/jars" ]]; then
    export SPARK_HOME="$SPARK_DIR"
    return 0
  fi

  # 3) Intentar descargar si no existe
  echo "⬇️  Descargando Spark ${SPARK_VERSION}…"
  mkdir -p "$PWD/.spark"
  CURL_FLAGS=(-fL --connect-timeout 20 --max-time 0)
  if [[ "${SPARK_ALLOW_INSECURE_DOWNLOAD:-}" == "1" ]]; then
    CURL_FLAGS+=(-k)
    echo "⚠️  SPARK_ALLOW_INSECURE_DOWNLOAD=1 → usando curl -k (sin revocación CRL)"
  fi

  # Reintenta/continúa descargas interrumpidas; valida y limpia archivos corruptos
  for attempt in 1 2; do
    [[ $attempt -gt 1 ]] && echo "↻ Reintentando descarga (intento $attempt)..."
    rm -rf "$SPARK_DIR"
    if curl "${CURL_FLAGS[@]}" -C - -o "$SPARK_TGZ_PATH" "$SPARK_TGZ_URL"; then
      if tar -tzf "$SPARK_TGZ_PATH" >/dev/null 2>&1; then
        if tar -xzf "$SPARK_TGZ_PATH" -C "$PWD/.spark"; then
          export SPARK_HOME="$SPARK_DIR"
          return 0
        fi
      else
        echo "ℹ️  Paquete corrupto, limpiando y reintentando..."
        rm -f "$SPARK_TGZ_PATH"
      fi
    fi
  done

  echo "⚠️  Descarga falló. Reintenta con SPARK_ALLOW_INSECURE_DOWNLOAD=1 o define SPARK_HOME a tu instalación existente."
  echo "   URL manual: $SPARK_TGZ_URL"
  echo "   Archivo parcial (puede eliminarse): $SPARK_TGZ_PATH"
  return 1
}

if ! detect_spark; then
  exit 1
fi
ensure_spark_integrity() {
  local home="$1"
  local missing=0
  [[ -d "$home/jars" ]] || missing=1
  [[ -f "$home/jars/spark-core_2.12-${SPARK_VERSION}.jar" ]] || missing=1
  ls "$home"/jars/spark-launcher_*.jar >/dev/null 2>&1 || missing=1
  [[ $missing -eq 0 ]] && return 0
  echo "ℹ️  Spark en $home parece incompleto; se forzará nueva descarga."
  rm -rf "$home"
  rm -f "$SPARK_TGZ_PATH"
  return 1
}

until ensure_spark_integrity "$SPARK_HOME"; do
  SPARK_HOME=""
  if ! detect_spark; then
    exit 1
  fi
done
if chosen_version=$(spark_home_version "$SPARK_HOME" 2>/dev/null); then
  echo "✅ Usando SPARK_HOME=$SPARK_HOME (Spark $chosen_version)"
else
  echo "✅ Usando SPARK_HOME=$SPARK_HOME"
fi

SPARK_SUBMIT_BIN="$SPARK_HOME/bin/spark-submit"
if [[ "$OSTYPE" == msys* || "$OSTYPE" == cygwin* || "$OSTYPE" == mingw* ]]; then
  # Usa el script .cmd y convierte rutas a formato Windows
  SPARK_HOME_WIN="$(cygpath -m "$SPARK_HOME")"
  HADOOP_HOME_WIN="$(cygpath -m "${HADOOP_HOME:-$HADOOP_DUMMY_WIN}")"
  export SPARK_HOME="$SPARK_HOME_WIN"
  export HADOOP_HOME="$HADOOP_HOME_WIN"
  export hadoop_home_dir="$HADOOP_HOME_WIN"
  SPARK_SUBMIT_BIN="$SPARK_HOME/bin/spark-submit.cmd"
fi


# -------- Run with spark-submit --------
# -------- Run with spark-submit --------
echo "📦 Ejecutando spark-submit con $JAR_PATH"

# Detectar si se pasa --class (ej: para DataGenerator)
MAIN_CLASS_ARGS=""
APP_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --class)
      MAIN_CLASS_ARGS="--class $2"
      shift 2
      ;;
    *)
      APP_ARGS+=("$1")
      shift
      ;;
  esac
done

# Nota: $MAIN_CLASS_ARGS debe ir ANTES del JAR. Los argumentos de la app van DESPUÉS.
# shellcheck disable=SC2086
"$SPARK_SUBMIT_BIN" \
  --master local[*] \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.ui.enabled=false \
  $MAIN_CLASS_ARGS \
  "$JAR_PATH" \
  "${APP_ARGS[@]+"${APP_ARGS[@]}"}"
