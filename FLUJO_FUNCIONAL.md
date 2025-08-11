# 🔄 Flujo Funcional de CompareTables

## 📋 Diagrama General del Proceso

```
┌─────────────────┐    ┌─────────────────┐
│   DATOS REF     │    │   DATOS NEW     │
│ (Sistema Anterior)│    │ (Sistema Nuevo) │
└─────────┬───────┘    └─────────┬───────┘
          │                       │
          └───────────┬───────────┘
                      │
                      ▼
            ┌─────────────────────┐
            │  COMPARACIÓN        │
            │  AUTOMÁTICA        │
            └─────────┬───────────┘
                      │
                      ▼
    ┌─────────────────────────────────────┐
    │        3 REPORTES GENERADOS         │
    └─┬─────────────┬─────────────┬───────┘
      │             │             │
      ▼             ▼             ▼
┌──────────┐  ┌──────────┐  ┌──────────┐
│DIFFERENCES│  │DUPLICATES│  │ SUMMARY  │
│(Diferencias)│  │(Duplicados)│  │ (Resumen) │
└──────────┘  └──────────┘  └──────────┘
```

---

## 🔍 Flujo Detallado por Componente

### **1. 📊 DIFFGENERATOR - Proceso de Comparación**

```
┌─────────────────────────────────────────────────────────────┐
│                    DATOS DE ENTRADA                        │
├─────────────────────────────────────────────────────────────┤
│ REF: 13 filas, 10 IDs únicos                              │
│ NEW: 16 filas, 8 IDs únicos                               │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    NORMALIZACIÓN                           │
├─────────────────────────────────────────────────────────────┤
│ • Claves vacías → NULL                                     │
│ • Columnas constantes → Excluidas                          │
│ • Pre-ordenamiento por priorityCol (si existe)             │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    AGRUPACIÓN                              │
├─────────────────────────────────────────────────────────────┤
│ • Agrupa por claves compuestas                             │
│ • Aplica estrategias de agregación:                        │
│   - Numéricos: MAX                                         │
│   - Texto: MAX (canonicalizado)                            │
│   - Fechas: MAX                                            │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    FULL OUTER JOIN                         │
├─────────────────────────────────────────────────────────────┤
│ • Une REF y NEW por claves                                 │
│ • Política de NULLs configurable                           │
│ • Marca existencia en cada dataset                         │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    EXPLOSIÓN DE DIFERENCIAS                │
├─────────────────────────────────────────────────────────────┤
│ • 1 fila por columna por ID                                │
│ • Resultados: MATCH/NO_MATCH/ONLY_IN_REF/ONLY_IN_NEW      │
│ • Formateo fiel de valores (DecimalType, etc.)             │
└─────────────────────────────────────────────────────────────┘
```

**Ejemplo de Salida:**
```
| id | column  | value_ref | value_new | results     |
|----|---------|-----------|-----------|-------------|
| 1  | country | US        | US        | MATCH       | ← ✅ Coincide
| 4  | country | FR        | BR        | NO_MATCH    | ← ⚠️ Diferente  
| 3  | country | MX        | -         | ONLY_IN_REF | ← ❌ Solo en REF
| 6  | country | -         | DE        | ONLY_IN_NEW | ← ❌ Solo en NEW
```

---

### **2. 🔍 DUPLICATEDETECTOR - Proceso de Detección**

```
┌─────────────────────────────────────────────────────────────┐
│                    UNIÓN Y ETIQUETADO                      │
├─────────────────────────────────────────────────────────────┤
│ • Combina REF + NEW                                        │
│ • Etiqueta origen: "ref" / "new"                          │
│ • Aplica priorityCol si existe                             │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    CÁLCULO DE HASH                         │
├─────────────────────────────────────────────────────────────┤
│ • SHA-256 por fila (excluyendo origen)                     │
│ • Canonicaliza valores complejos                           │
│ • Normaliza NULLs y valores vacíos                         │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    AGRUPACIÓN Y ANÁLISIS                   │
├─────────────────────────────────────────────────────────────┤
│ • Agrupa por origen + claves compuestas                    │
│ • Calcula:                                                 │
│   - exact_duplicates = total - countDistinct(hash)         │
│   - dups_w_variations = max(countDistinct(hash) - 1, 0)   │
│   - occurrences = total del grupo                          │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    CONSTRUCCIÓN DE VARIACIONES             │
├─────────────────────────────────────────────────────────────┤
│ • Colecta sets de valores por columna                      │
│ • Filtra valores NULL y vacíos                             │
│ • Formatea: "campo: [v1,v2] | campo2: [v3,v4]"            │
└─────────────────────────────────────────────────────────────┘
```

**Ejemplo de Salida:**
```
| origin               | id  | exact_duplicates | dups_w_variations | occurrences | variations                    |
|----------------------|-----|------------------|-------------------|-------------|-------------------------------|
| default.ref_customers| 1   | 1                | 0                 | 2           | -                            | ← 2 filas idénticas
| default.ref_customers| 4   | 0                | 1                 | 2           | country: [BR,FR] | amount: [200.00,201.00] |
| default.new_customers| 4   | 2                | 1                 | 4           | amount: [200.00,201.00]      |
```

---

### **3. 📊 SUMMARYGENERATOR - Proceso de Consolidación**

```
┌─────────────────────────────────────────────────────────────┐
│                    ANÁLISIS DE PRESENCIA                   │
├─────────────────────────────────────────────────────────────┤
│ • Construye IDs compuestos                                 │
│ • Calcula:                                                  │
│   - IDs únicos por dataset                                 │
│   - IDs en ambos datasets                                  │
│   - IDs solo en REF                                        │
│   - IDs solo en NEW                                        │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    ANÁLISIS DE DUPLICADOS                  │
├─────────────────────────────────────────────────────────────┤
│ • Identifica duplicados por dataset                        │
│ • Calcula intersecciones y diferencias                     │
│ • Colecta ejemplos de IDs                                  │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    ANÁLISIS DE DIFERENCIAS                 │
├─────────────────────────────────────────────────────────────┤
│ • Usa diffDf para identificar:                             │
│   - Matches exactos (MATCH)                                │
│   - Variaciones (NO_MATCH)                                 │
│   - Gaps (ONLY_IN_REF/ONLY_IN_NEW)                        │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    CÁLCULO DE CALIDAD                      │
├─────────────────────────────────────────────────────────────┤
│ • Quality global = IDs exactos sin duplicados              │
│ • Porcentajes por métrica                                  │
│ • Ejemplos limitados a 6 IDs                               │
└─────────────────────────────────────────────────────────────┘
```

**Ejemplo de Salida:**
```
| bloque  | metrica                  | universo | numerador | denominador | pct    | ejemplos |
|---------|--------------------------|----------|-----------|-------------|--------|----------|
| KPIS    | IDs Uniques              | REF      | 10        | -           | -      | -        |
| KPIS    | Total (NEW-REF)          | ROWS     | 3         | 13          | 23.1%  | -        |
| MATCH   | 1:1 (exact matches)      | BOTH     | 2         | 7           | 28.6%  | 1,NULL   |
| NO MATCH| 1:1 (match not identical)| BOTH     | 5         | 7           | 71.4%  | 2,4,7,8,9|
| GAP     | 1:0 (only in reference)  | REF      | 3         | 10          | 30.0%  | 10,3,5   |
| DUPS    | duplicates (both)        | BOTH     | 2         | 7           | 28.6%  | 4,NULL   |
```

---

## 🎯 Interpretación de Métricas del Summary

### **Bloque KPIS (Indicadores Clave)**
```
┌─────────────────────────────────────────────────────────────┐
│                    MÉTRICAS PRINCIPALES                    │
├─────────────────────────────────────────────────────────────┤
│ • IDs Uniques: Número de identificadores únicos           │
│ • Total ROWS: Número total de filas                       │
│ • Total (NEW-REF): Crecimiento/reducción de datos         │
│ • Quality global: % de IDs perfectos sin duplicados       │
└─────────────────────────────────────────────────────────────┘
```

### **Bloque MATCH (Coincidencias)**
```
┌─────────────────────────────────────────────────────────────┐
│                    ANÁLISIS DE COINCIDENCIAS              │
├─────────────────────────────────────────────────────────────┤
│ • 1:1 (exact matches): IDs idénticos en ambos datasets    │
│ • 1:1 (match not identical): IDs con valores diferentes   │
│ • Denominador: Total de IDs en ambos datasets              │
└─────────────────────────────────────────────────────────────┘
```

### **Bloque GAP (Brechas)**
```
┌─────────────────────────────────────────────────────────────┐
│                    ANÁLISIS DE BRECHAS                     │
├─────────────────────────────────────────────────────────────┤
│ • 1:0 (only in reference): IDs perdidos en migración      │
│ • 0:1 (only in new): IDs nuevos o duplicados              │
│ • Denominador: Total de IDs únicos del dataset respectivo  │
└─────────────────────────────────────────────────────────────┘
```

### **Bloque DUPS (Duplicados)**
```
┌─────────────────────────────────────────────────────────────┐
│                    ANÁLISIS DE DUPLICADOS                  │
├─────────────────────────────────────────────────────────────┤
│ • duplicates (both): Duplicados en ambos datasets          │
│ • duplicates (ref): Solo duplicados en referencia          │
│ • duplicates (new): Solo duplicados en nuevo               │
│ • Denominador: Total de IDs únicos del dataset respectivo  │
└─────────────────────────────────────────────────────────────┘
```

---

## 🔄 Flujo de Datos Completo

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Main      │───▶│TableComparison│───▶│DiffGenerator│
│ (Configura) │    │Controller   │    │(Diferencias)│
└─────────────┘    └─────────────┘    └─────────────┘
                           │                   │
                           │                   ▼
                           │            ┌─────────────┐
                           │            │   diffDf   │
                           │            │(Diferencias)│
                           │            └─────────────┘
                           │                   │
                           ▼                   │
                   ┌─────────────┐             │
                   │DuplicateDetector│         │
                   │(Duplicados)│             │
                   └─────────────┘             │
                           │                   │
                           ▼                   ▼
                   ┌─────────────┐    ┌─────────────┐
                   │   dupDf    │    │   diffDf    │
                   │(Duplicados)│    │(Diferencias)│
                   └─────────────┘    └─────────────┘
                           │                   │
                           └─────────┬─────────┘
                                     │
                                     ▼
                            ┌─────────────┐
                            │SummaryGenerator│
                            │ (Resumen)   │
                            └─────────────┘
                                     │
                                     ▼
                            ┌─────────────┐
                            │  summaryDf  │
                            │ (Resumen)   │
                            └─────────────┘
                                     │
                                     ▼
                            ┌─────────────┐
                            │   Hive      │
                            │ + Excel    │
                            └─────────────┘
```

---

## 💡 Puntos Clave del Proceso

1. **Normalización**: Todos los componentes normalizan datos de entrada
2. **Agregación**: Se agrupan filas por claves compuestas antes de comparar
3. **Canonicalización**: Valores complejos se convierten a formato estándar
4. **Priorización**: Si existe priorityCol, se eliminan duplicados automáticamente
5. **Formateo Fiel**: Se respetan tipos de datos originales (especialmente DecimalType)
6. **Ejemplos**: Se proporcionan IDs de ejemplo para facilitar revisión manual
7. **Exportación**: Resultados en Hive (análisis) + Excel (presentación)
