# 🔍 CompareTables - Software de Comparación y Análisis de Datos

## 📋 Resumen Ejecutivo

**CompareTables** es una solución empresarial desarrollada en **Scala + Apache Spark** que permite comparar dos conjuntos de datos (referencia vs. nuevo) y generar análisis detallados de calidad, diferencias y duplicados. Ideal para auditorías de datos, migraciones de sistemas y control de calidad.

## 🎯 Caso de Uso Principal

**Escenario**: Una empresa necesita verificar la calidad de datos después de una migración de sistema o actualización de base de datos.

**Entrada**: 
- **Tabla REF**: Datos originales del sistema anterior
- **Tabla NEW**: Datos del nuevo sistema

**Salida**: Análisis completo con 3 reportes:
1. **Diferencias** - Cambios específicos por campo
2. **Duplicados** - Registros duplicados y variaciones
3. **Resumen** - Métricas de calidad y KPIs

---

## 📊 Ejemplo Práctico: Comparación de Clientes

### Datos de Entrada

**Tabla REF (Sistema Anterior):**
```
| id | country | amount           | status |
|----|---------|------------------|---------|
| 1  | US      | 100.40          | active  |
| 1  | US      | 100.40          | active  | ← Duplicado exacto
| 2  | ES      | 1.000000000000000001 | expired |
| 3  | MX      | 150.00          | active  |
| 4  | FR      | 200.00          | new     |
| 4  | BR      | 201.00          | new     | ← Mismo ID, valores diferentes
| 5  | FR      | 300.00          | active  |
| 5  | FR      | 300.50          | active  | ← Mismo ID, amount diferente
| 7  | PT      | 300.50          | active  |
| 8  | BR      | 100.50          | pending |
| 10 | GR      | 60.00           | new     |
| NULL| GR      | 61.00           | new     |
| NULL| GR      | 60.00           | new     |
```

**Tabla NEW (Sistema Nuevo):**
```
| id | country | amount           | status |
|----|---------|------------------|---------|
| 1  | US      | 100.40          | active  |
| 2  | ES      | 1.000000000000000001 | expired |
| 4  | BR      | 201.00          | new     |
| 4  | BR      | 200.00          | new     |
| 4  | BR      | 200.00          | new     | ← Duplicados exactos
| 4  | BR      | 200.00          | new     |
| 6  | DE      | 400.00          | new     |
| 6  | DE      | 400.00          | new     | ← Duplicados exactos
| 6  | DE      | 400.10          | new     | ← Variación en amount
| 7  |         | 300.50          | active  | ← Country vacío
| 8  | BR      | NULL            | pending | ← Amount NULL
| 9  | AN      | 80.00           | NULL    | ← Status NULL
| NULL| GR      | 60.00           | new     |
| NULL| GR      | 60.00           | new     | ← Duplicados exactos
| NULL| GR      | 60.00           | new     |
| NULL| GR      | 61.00           | new     |
```

---

## 📈 Tabla 1: Differences (Diferencias)

**Propósito**: Muestra las diferencias específicas entre REF y NEW por campo.

**Ejemplo de Salida:**
```
| id | column  | value_ref        | value_new        | results     |
|----|---------|------------------|------------------|-------------|
| 2  | country | ES               | ES               | NO_MATCH    | ← Espacios diferentes
| 3  | country | MX               | -                | ONLY_IN_REF | ← Solo en REF
| 4  | country | FR               | BR               | NO_MATCH    | ← Valores diferentes
| 6  | country | -                | DE               | ONLY_IN_NEW | ← Solo en NEW
| 7  | country | PT               | -                | NO_MATCH    | ← PT vs vacío
| 8  | amount  | 100.50           | -                | NO_MATCH    | ← 100.50 vs NULL
| 9  | status  | new              | -                | NO_MATCH    | ← new vs NULL
```

**Interpretación**:
- **MATCH**: Valores idénticos
- **NO_MATCH**: Mismo ID, valores diferentes
- **ONLY_IN_REF**: ID solo en tabla de referencia
- **ONLY_IN_NEW**: ID solo en tabla nueva

---

## 🔍 Tabla 2: Duplicates (Duplicados)

**Propósito**: Identifica registros duplicados y sus variaciones dentro de cada tabla.

**Ejemplo de Salida:**
```
| origin               | id  | exact_duplicates | dups_w_variations | occurrences | variations                    |
|----------------------|-----|------------------|-------------------|-------------|-------------------------------|
| default.ref_customers| 1   | 1                | 0                 | 2           | -                            | ← 2 filas idénticas
| default.ref_customers| 4   | 0                | 1                 | 2           | country: [BR,FR] | amount: [200.00,201.00] |
| default.new_customers| 4   | 2                | 1                 | 4           | amount: [200.00,201.00]      |
| default.new_customers| 6   | 1                | 1                 | 3           | amount: [400.00,400.10]      |
```

**Interpretación**:
- **exact_duplicates**: Número de filas idénticas
- **dups_w_variations**: Número de grupos con valores diferentes
- **occurrences**: Total de filas con ese ID
- **variations**: Detalle de las diferencias encontradas

---

## 📊 Tabla 3: Summary (Resumen)

**Propósito**: Proporciona métricas de calidad y KPIs consolidados.

**Ejemplo de Salida:**
```
| bloque  | metrica                  | universo | numerador | denominador | pct    | ejemplos |
|---------|--------------------------|----------|-----------|-------------|--------|----------|
| KPIS    | IDs Uniques              | REF      | 10        | -           | -      | -        |
| KPIS    | IDs Uniques              | NEW      | 8         | -           | -      | -        |
| KPIS    | Total REF                | ROWS     | 13        | -           | -      | -        |
| KPIS    | Total NEW                | ROWS     | 16        | -           | -      | -        |
| KPIS    | Total (NEW-REF)          | ROWS     | 3         | 13          | 23.1%  | -        |
| KPIS    | Quality global           | REF      | 1         | 10          | 10.0%  | -        |
| MATCH   | 1:1 (exact matches)      | BOTH     | 2         | 7           | 28.6%  | 1,NULL   |
| NO MATCH| 1:1 (match not identical)| BOTH     | 5         | 7           | 71.4%  | 2,4,7,8,9|
| GAP     | 1:0 (only in reference)  | REF      | 3         | 10          | 30.0%  | 10,3,5   |
| GAP     | 0:1 (only in new)        | NEW      | 1         | 8           | 12.5%  | 6        |
| DUPS    | duplicates (both)        | BOTH     | 2         | 7           | 28.6%  | 4,NULL   |
| DUPS    | duplicates (ref)         | REF      | 1         | 10          | 10.0%  | 5        |
| DUPS    | duplicates (new)         | NEW      | 1         | 8           | 12.5%  | 6        |
```

---

## 🎯 Interpretación de Métricas del Summary

### **Bloque KPIS (Indicadores Clave)**
- **IDs Uniques**: Número de identificadores únicos por dataset
- **Total ROWS**: Número total de filas por dataset
- **Total (NEW-REF)**: Diferencia en número de filas (crecimiento/reducción)
- **Quality global**: Porcentaje de IDs con matches exactos y sin duplicados

### **Bloque MATCH (Coincidencias)**
- **1:1 (exact matches)**: IDs que existen en ambos datasets con valores idénticos
- **1:1 (match not identical)**: IDs que existen en ambos pero con valores diferentes

### **Bloque GAP (Brechas)**
- **1:0 (only in reference)**: IDs que solo existen en el dataset de referencia
- **0:1 (only in new)**: IDs que solo existen en el nuevo dataset

### **Bloque DUPS (Duplicados)**
- **duplicates (both)**: IDs que tienen duplicados en ambos datasets
- **duplicates (ref)**: IDs que solo tienen duplicados en referencia
- **duplicates (new)**: IDs que solo tienen duplicados en nuevo

---

## 🚀 Casos de Uso Empresariales

### **1. Migración de Sistemas**
- **Antes**: Datos del sistema legacy
- **Después**: Datos del nuevo sistema
- **Objetivo**: Verificar integridad de la migración

### **2. Auditoría de Calidad**
- **Antes**: Datos de producción
- **Después**: Datos después de limpieza
- **Objetivo**: Medir impacto de la limpieza

### **3. Control de Cambios**
- **Antes**: Versión anterior de datos
- **Después**: Versión actualizada
- **Objetivo**: Identificar modificaciones no autorizadas

### **4. Reconciliación de Datos**
- **Antes**: Datos de sistema A
- **Después**: Datos de sistema B
- **Objetivo**: Alinear información entre sistemas

---

## 💡 Beneficios Clave

✅ **Visibilidad Total**: Análisis detallado de diferencias por campo
✅ **Detección Automática**: Identificación automática de duplicados y variaciones
✅ **Métricas de Calidad**: KPIs cuantificables de la integridad de datos
✅ **Escalabilidad**: Procesa millones de registros con Apache Spark
✅ **Flexibilidad**: Soporta múltiples fuentes de datos (Hive, archivos)
✅ **Exportación**: Resultados en Excel para análisis posterior

---

## 🔧 Configuración Técnica

**Lenguaje**: Scala 2.12.18
**Framework**: Apache Spark 3.5.0
**Almacenamiento**: Hive con particionamiento
**Exportación**: Excel (spark-excel)
**Configuración**: Java 11 compatible

---

## 📞 Uso

```bash
# Ejecutar comparación completa
./run_compare.sh

# O ejecutar desde SBT
sbt "runMain Main"
```

**Resultados**: Se generan 3 tablas en Hive con prefijo configurable y se exporta resumen a Excel.

