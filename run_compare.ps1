#Requires -Version 5
$ErrorActionPreference = "Stop"
Write-Host "🚀 Ejecutando comparación de tablas con Spark + Hive (Windows)"

# Limpieza
Write-Host "🧹 Limpiando metastore local, logs y warehouse de Spark..."
Remove-Item -Recurse -Force -ErrorAction SilentlyContinue metastore_db, derby.log
if (Test-Path "spark-warehouse") { Get-ChildItem "spark-warehouse" | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue }
Write-Host "🧹 Limpiando artefactos de compilación..."
Remove-Item -Recurse -Force -ErrorAction SilentlyContinue target, project\target

# Compilación y ejecución
Write-Host "🎯 Compilando y ejecutando el proyecto con sbt..."
sbt clean compile run

Write-Host "✅ Ejecución completada"
