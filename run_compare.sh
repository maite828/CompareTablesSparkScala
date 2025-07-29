#!/bin/bash

# Activar entorno
echo "🚀 Ejecutando comparación de tablas con Spark + Hive"

# Opcional: limpiar el metastore local si ha habido errores previos (usa con precaución)
# rm -rf metastore_db/ derby.log spark-warehouse/

# Ejecutar el proyecto con sbt
sbt clean compile run

