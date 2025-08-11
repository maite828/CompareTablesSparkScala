#!/bin/bash

# Script para configurar Java 11 para el proyecto CompareTablesSparkScala

echo "Configurando Java 11 para CompareTablesSparkScala..."

# Configurar JAVA_HOME para Java 11
export JAVA_HOME=/opt/homebrew/Cellar/openjdk@11/11.0.28/libexec/openjdk.jdk/Contents/Home
export PATH=$JAVA_HOME/bin:$PATH

# Verificar la versión
echo "Java version activa:"
java -version

echo ""
echo "JAVA_HOME configurado en: $JAVA_HOME"
echo ""
echo "Para usar esta configuración en tu shell actual, ejecuta:"
echo "source scripts/setup-java11.sh"
echo ""
echo "O para hacerlo permanente, agrega estas líneas a tu ~/.zshrc:"
echo "export JAVA_HOME=/opt/homebrew/Cellar/openjdk@11/11.0.28/libexec/openjdk.jdk/Contents/Home"
echo "export PATH=\$JAVA_HOME/bin:\$PATH"
