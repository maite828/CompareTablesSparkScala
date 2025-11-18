#!/usr/bin/env bash
set -euo pipefail

echo "🔎 Detectando plataforma..."
UNAME_OUT="$(uname -s || echo unknown)"

is_macos=false
is_windows_gitbash=false

case "$UNAME_OUT" in
  Darwin) is_macos=true ;;
  MINGW*|MSYS*|CYGWIN*) is_windows_gitbash=true ;; # Git Bash / MSYS / Cygwin
  *) echo "⚠️  Plataforma no reconocida: $UNAME_OUT. Intentaré ejecutar tests igualmente." ;;
esac

# ───────────────────────────────────────────────────────────────────────────────
# 1) Selección de JAVA_HOME (arreglado para Windows)
# ───────────────────────────────────────────────────────────────────────────────
if $is_macos; then
  # Forzar JDK 17 en macOS (Spark/Hadoop friendly)
  if ! JAVA_17_HOME="$(/usr/libexec/java_home -v 17 2>/dev/null)"; then
    echo "🛑 No se encontró JDK 17 en macOS."
    echo "   Instálalo:  brew install --cask temurin@17"
    exit 1
  fi
  export JAVA_HOME="$JAVA_17_HOME"
  export PATH="$JAVA_HOME/bin:$PATH"
  echo "🍎 macOS → usando JAVA_HOME=$JAVA_HOME"
elif $is_windows_gitbash; then
  # Buscar una instalación válida de Java en PATH.
  JAVA_HOME=""
  JAVA_EXEC_LIST=($(where java 2>/dev/null || type -p java))
  JAVA_FOUND=false
  for JAVA_EXEC in "${JAVA_EXEC_LIST[@]}"; do
    JAVA_DIR="$(dirname "$JAVA_EXEC")"
    JAVA_BASE="$(dirname "$JAVA_DIR")"
    if [[ -x "$JAVA_BASE/bin/java.exe" || -x "$JAVA_BASE/bin/java" ]]; then
      JAVA_HOME="$JAVA_BASE"
      JAVA_FOUND=true
      break
    fi
  done

  # Si no hay una JAVA válida en PATH, intentar con la ruta de IntelliJ (como mencionaste)
  if ! $JAVA_FOUND && [[ -x "/c/Program Files/Java/jdk-11/bin/java.exe" ]]; then
    JAVA_HOME="/c/Program Files/Java/jdk-11"
    JAVA_FOUND=true
    echo "🪟 Usando JAVA_HOME de IntelliJ: $JAVA_HOME"
  fi

  if $JAVA_FOUND; then
    export JAVA_HOME
    export PATH="$JAVA_HOME/bin:$PATH"
    echo "🪟 Windows (Git Bash) → usando JAVA_HOME=$JAVA_HOME"
  else
    echo "🛑 No se encontró JDK válido en PATH ni en IntelliJ."
    echo "Instala un JDK y asegúrate que su carpeta bin está en el PATH."
    exit 1
  fi
else
  # Otras plataformas: no forzamos nada
  echo "🌐 Plataforma $UNAME_OUT → no se fuerza JAVA_HOME."
fi

# ───────────────────────────────────────────────────────────────────────────────
# 2) HADOOP_HOME “dummy” (silencia warnings)
# ───────────────────────────────────────────────────────────────────────────────
export HADOOP_HOME="${HADOOP_HOME:-$HOME/.hadoop-dummy}"
mkdir -p "$HADOOP_HOME"

# ───────────────────────────────────────────────────────────────────────────────
# 3) Apagar servidor sbt previo para no reutilizar otra JVM
# ───────────────────────────────────────────────────────────────────────────────
if command -v sbtn >/dev/null 2>&1; then
  sbtn --shutdown || true
fi
rm -rf "$HOME/.sbt/1.0/server" || true

# ───────────────────────────────────────────────────────────────────────────────
# 4) Ejecutar tests
# ───────────────────────────────────────────────────────────────────────────────
echo "🎯 Ejecutando tests: sbt clean test"
if [[ -n "${JAVA_HOME:-}" ]]; then
  exec sbt -java-home "$JAVA_HOME" clean test
else
  exec sbt clean test
fi