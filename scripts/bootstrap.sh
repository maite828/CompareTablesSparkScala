#!/usr/bin/env bash
set -euo pipefail

echo "🔧 Bootstrap (macOS/Linux)"

command -v curl >/dev/null 2>&1 || { echo "❌ Necesitas 'curl'"; exit 1; }

# 1) Instala Coursier si no existe
if ! command -v cs >/dev/null 2>&1; then
  if command -v brew >/dev/null 2>&1; then
    echo "📦 Instalando coursier con Homebrew..."
    brew install coursier/formulas/coursier
  else
    echo "⬇️  Descargando coursier (cs)..."
    curl -fLo cs https://git.io/coursier-cli
    chmod +x cs
    sudo mv cs /usr/local/bin/cs || {
      mkdir -p "${HOME}/.local/bin"
      mv cs "${HOME}/.local/bin/cs"
      export PATH="${HOME}/.local/bin:${PATH}"
    }
  fi
fi

# 2) JDK 11 + sbt
echo "☕ Instalando JDK 11 + sbt con coursier..."
cs setup -y --jvm temurin:11
cs install sbt || true

# 3) Hadoop dummy
mkdir -p /tmp/hadoop-dummy

echo "✅ Versiones:"
java -version
sbt -version

echo "🎉 Bootstrap OK. Ya puedes ejecutar: ./run_compare.sh"
