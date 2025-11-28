#!/usr/bin/env bash
set -euo pipefail

echo "🚀 Iniciando DEMO de Comparación con Mapeo de Columnas..."

# 0. Limpiar datos de ejecuciones anteriores
echo "----------------------------------------------------------------"
echo "🧹 Paso 0: Limpiando datos de ejecuciones anteriores..."
echo "----------------------------------------------------------------"
rm -rf metastore_db/ derby.log spark-warehouse/* /tmp/demo_out/* 2>/dev/null || true
mkdir -p spark-warehouse /tmp/demo_out
echo "✅ Limpieza completada"

# 1. Generar datos de prueba (Tablas Hive)
echo "----------------------------------------------------------------"
echo "🛠️  Paso 1: Generando tablas de prueba (db.tabla_referencia, db.tabla_nueva)..."
echo "----------------------------------------------------------------"
./run_compare.sh --class DataGenerator

# 2. Ejecutar la comparación
echo "----------------------------------------------------------------"
echo "🔍 Paso 2: Ejecutando comparación con Mapeo de Columnas..."
echo "   (Mapeando: id_v2 -> id, nombre_cliente -> name, saldo -> balance, fecha_proceso -> data_date_part, pais -> geo)"
echo "----------------------------------------------------------------"

./run_compare.sh \
  refTable=db.tabla_referencia \
  newTable=db.tabla_nueva \
  initiativeName=demo_mapeo \
  tablePrefix=default.res_demo_ \
  outputBucket=/tmp/demo_out \
  executionDate=2023-11-22 \
  refPartitionSpec=geo=*/data_date_part=[2023-11-22,2023-11-23] \
  newPartitionSpec=geo=*/fecha_proceso=[2023-11-22,2023-11-23] \
  compositeKeyCols=id \
  checkDuplicates=true \
  includeEqualsInDiff=true \
  colMap.id=id_v2 \
  colMap.name=nombre_cliente \
  colMap.balance=saldo \
  colMap.data_date_part=fecha_proceso \
  colMap.geo=pais \
  #priorityCols=name \
  #refFilter="id = 1" \
  #newFilter="id_v2 = 1"

echo "----------------------------------------------------------------"
echo "✅ Demo finalizada con éxito."
echo "----------------------------------------------------------------"
