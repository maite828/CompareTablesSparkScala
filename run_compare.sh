#!/usr/bin/env bash
set -euo pipefail

echo "🚀 Ejecutando comparación de tablas con Spark + Hive"

# ───────────────────────────────────────────────────────────────────────────────
# 0) Resolución de JAVA_HOME en macOS (Spark 3.5.0 requiere JDK 8/11/17; evita Java 24)
# ───────────────────────────────────────────────────────────────────────────────
SBT_JAVA_HOME_ARGS=()

if [[ "$(uname -s)" == "Darwin" ]]; then
  # Intenta localizar un JDK 17 instalado en macOS
  if JAVA_17_HOME="$(/usr/libexec/java_home -v 17 2>/dev/null)"; then
    export JAVA_HOME="$JAVA_17_HOME"
    export PATH="$JAVA_HOME/bin:$PATH"
    echo "🔧 macOS: usando JDK 17 en JAVA_HOME: $JAVA_HOME"
    SBT_JAVA_HOME_ARGS=( -java-home "$JAVA_HOME" )
  else
    # Ruta típica si instalaste Temurin 17 con Homebrew (cask)
    CASK_HOME="/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home"
    if [[ -d "$CASK_HOME" ]]; then
      export JAVA_HOME="$CASK_HOME"
      export PATH="$JAVA_HOME/bin:$PATH"
      echo "🔧 macOS: usando JDK 17 (Temurin) en JAVA_HOME: $JAVA_HOME"
      SBT_JAVA_HOME_ARGS=( -java-home "$JAVA_HOME" )
    else
      echo "🛑 macOS: No se encontró JDK 17."
      echo "    Instálalo con Homebrew:  brew install --cask temurin@17"
      echo "    O instala Temurin 17 desde Adoptium y reintenta."
      exit 1
    fi
  fi

  # (Opcional) Definir HADOOP_HOME dummy para silenciar warnings inofensivos
  export HADOOP_HOME="${HADOOP_HOME:-$HOME/.hadoop-dummy}"
  mkdir -p "$HADOOP_HOME"
fi

# ───────────────────────────────────────────────────────────────────────────────
# 1) Limpieza de artefactos temporales y datos Hive/Spark locales
# ───────────────────────────────────────────────────────────────────────────────
echo "🧹 Limpiando metastore local, logs y warehouse de Spark..."
rm -rf metastore_db/ derby.log
rm -rf spark-warehouse/*

# Limpia también los directorios de compilación de sbt
echo "🧹 Limpiando artefactos de compilación (target/ y project/target/)..."
rm -rf target/ project/target/

# (Opcional) Si quisieras limpiar TODO lo no versionado en Git, descomenta:
# echo "🧹 Limpieza profunda con Git (¡cuidado!)..."
# git clean -fdx

# ───────────────────────────────────────────────────────────────────────────────
# 2) Compilación y ejecución
# ───────────────────────────────────────────────────────────────────────────────
echo "🎯 Compilando y ejecutando el proyecto con sbt..."
# En macOS pasamos -java-home con el JDK 17 localizado; en otros SO no hace falta
sbt "${SBT_JAVA_HOME_ARGS[@]}" clean compile run

echo "✅ Ejecución completada"
