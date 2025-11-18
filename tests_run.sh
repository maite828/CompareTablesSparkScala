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
  # Resolver JAVA_HOME válido en Windows (evita javapath roto)
  resolve_java_home_windows() {
    # 1) Respeta JAVA_HOME si apunta a un JDK real
    if [[ -n "${JAVA_HOME:-}" && -x "$JAVA_HOME/bin/java" ]]; then
      printf '%s\n' "$JAVA_HOME"
      return 0
    fi

    # 2) Si JAVA_HOME apunta a javapath (shims de Oracle), ignóralo y busca real
    if [[ "${JAVA_HOME:-}" =~ javapath ]]; then
      echo "ℹ️  JAVA_HOME apunta a javapath; buscando JDK real en PATH..." >&2
    fi

    # 3) Intenta derivar desde el java en PATH
    if JAVA_BIN="$(command -v java 2>/dev/null)"; then
      JAVA_BIN_WIN="$(cygpath -m "$JAVA_BIN")"
      JAVA_HOME_WIN="${JAVA_BIN_WIN%/bin/java.exe}"
      JAVA_HOME_WIN="${JAVA_HOME_WIN%/bin/java}" # por si viene sin .exe
      JAVA_HOME_UNIX="$(cygpath -u "$JAVA_HOME_WIN")"
      if [[ -x "$JAVA_HOME_UNIX/bin/java" ]]; then
        printf '%s\n' "$JAVA_HOME_UNIX"
        return 0
      fi
    fi

    # 4) Busca el último JDK instalado en rutas estándar
    for base in "/c/Program Files/Java" "/c/Program Files (x86)/Java"; do
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
    echo "🪟 Windows (Git Bash) → usando JAVA_HOME=$JAVA_HOME"
  else
    echo "🛑 No se encontró JDK en Windows; define JAVA_HOME (ej. C:/Program Files/Java/jdk-17.x)."
    exit 1
  fi
else
  # Otras plataformas: no forzamos nada
  echo "🌐 Plataforma $UNAME_OUT → no se fuerza JAVA_HOME."
fi

# ───────────────────────────────────────────────────────────────────────────────
# 1b) Opciones JVM comunes (evitan IllegalAccess con Java 17 en Spark)
# ───────────────────────────────────────────────────────────────────────────────
COMMON_JAVA_OPTS=(
  "--add-opens=java.base/java.lang=ALL-UNNAMED"
  "--add-opens=java.base/java.io=ALL-UNNAMED"
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
)
export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:-} ${COMMON_JAVA_OPTS[*]}"
export SBT_OPTS="${SBT_OPTS:-} ${COMMON_JAVA_OPTS[*]}"

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
