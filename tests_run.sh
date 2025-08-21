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
# 1) Selección de JAVA_HOME
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
  # En Windows (Git Bash) no tocamos JAVA_HOME; usas el que tengas configurado
  if [[ -z "${JAVA_HOME:-}" ]]; then
    echo "🪟 Windows (Git Bash) → JAVA_HOME no definido, usará el que resuelva sbt."
  else
    echo "🪟 Windows (Git Bash) → usando JAVA_HOME=$JAVA_HOME"
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
