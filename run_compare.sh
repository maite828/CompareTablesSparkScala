#!/usr/bin/env bash
set -euo pipefail

echo "🚀 Ejecutando comparación de tablas con Spark + Hive"

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
sbt clean compile run

echo "✅ Ejecución completada"
