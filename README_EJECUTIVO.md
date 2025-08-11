# 🚀 CompareTables - Resumen Ejecutivo

## 💼 ¿Qué es?

**CompareTables** es una herramienta empresarial que **compara dos conjuntos de datos** y genera **reportes automáticos de calidad**, diferencias y duplicados.

## 🎯 ¿Para qué sirve?

- ✅ **Migraciones de sistemas** - Verificar integridad de datos
- ✅ **Auditorías de calidad** - Medir impacto de limpieza de datos  
- ✅ **Control de cambios** - Detectar modificaciones no autorizadas
- ✅ **Reconciliación** - Alinear información entre sistemas

## 📊 ¿Cómo funciona?

### **Entrada**: 2 Tablas
- **REF**: Datos del sistema anterior/original
- **NEW**: Datos del sistema nuevo/actualizado

### **Salida**: 3 Reportes
1. **Diferencias** - Cambios específicos por campo
2. **Duplicados** - Registros duplicados y variaciones  
3. **Resumen** - KPIs de calidad consolidados

---

## 🔍 Ejemplo Práctico: Clientes

### **Datos de Entrada**
```
REF (Sistema Anterior)          NEW (Sistema Nuevo)
| id | country | amount |       | id | country | amount |
|----|---------|--------|       |----|---------|--------|
| 1  | US      | 100.40 |       | 1  | US      | 100.40 | ← ✅ IDENTICO
| 4  | FR      | 200.00 |       | 4  | BR      | 200.00 | ← ⚠️ COUNTRY DIFERENTE
| 3  | MX      | 150.00 |       | -  | -       | -      | ← ❌ SOLO EN REF
| -  | -       | -      |       | 6  | DE      | 400.00 | ← ❌ SOLO EN NEW
```

### **Reporte de Diferencias**
```
| id | column  | value_ref | value_new | results     |
|----|---------|-----------|-----------|-------------|
| 1  | country | US        | US        | MATCH       | ← ✅ Coincide
| 4  | country | FR        | BR        | NO_MATCH    | ← ⚠️ Diferente
| 3  | country | MX        | -         | ONLY_IN_REF | ← ❌ Solo en REF
| 6  | country | -         | DE        | ONLY_IN_NEW | ← ❌ Solo en NEW
```

### **Reporte de Duplicados**
```
| origin | id | exact_dups | variations | total |
|--------|----|------------|------------|-------|
| REF    | 1  | 1          | 0          | 2     | ← 2 filas idénticas
| NEW    | 4  | 2          | 1          | 4     | ← 2 duplicados + 1 variación
```

### **Reporte de Resumen (KPIs)**
```
| Métrica                    | REF | NEW | %    |
|----------------------------|-----|-----|------|
| IDs Únicos                | 10  | 8   | -    |
| Total Filas               | 13  | 16  | +23% |
| Matches Exactos           | -   | -   | 29%  |
| Solo en REF               | -   | -   | 30%  |
| Solo en NEW               | -   | -   | 13%  |
| Calidad Global            | -   | -   | 10%  |
```

---

## 🎯 Interpretación de Resultados

### **✅ MATCH (29%)**
- IDs que existen en ambos sistemas con **valores idénticos**
- **Bueno**: Datos consistentes entre sistemas

### **⚠️ NO_MATCH (71%)**  
- IDs que existen en ambos pero con **valores diferentes**
- **Revisar**: Posibles errores de migración o cambios intencionales

### **❌ ONLY_IN_REF (30%)**
- IDs que **solo existen** en el sistema anterior
- **Peligroso**: Datos perdidos en la migración

### **❌ ONLY_IN_NEW (13%)**
- IDs que **solo existen** en el nuevo sistema
- **Revisar**: Datos nuevos o duplicados

### **🔍 DUPLICADOS**
- **Exactos**: Filas idénticas (pueden eliminarse)
- **Con variaciones**: Mismo ID, valores diferentes (requieren revisión)

---

## 💡 Beneficios Empresariales

| Área | Beneficio |
|------|-----------|
| **Riesgo** | ✅ Detección temprana de problemas de datos |
| **Calidad** | ✅ Métricas cuantificables de integridad |
| **Eficiencia** | ✅ Automatización de auditorías manuales |
| **Compliance** | ✅ Evidencia documentada de migraciones |
| **Costos** | ✅ Reducción de errores y re-trabajos |

---

## 🚀 Casos de Uso Reales

### **🏦 Sector Bancario**
- Migración de core banking
- Reconciliación de cuentas
- Auditoría de transacciones

### **🏥 Sector Salud**
- Migración de historiales médicos
- Validación de datos de pacientes
- Control de medicamentos

### **🏭 Sector Industrial**
- Migración de sistemas ERP
- Control de inventarios
- Validación de facturación

---

## 🔧 Tecnología

- **Lenguaje**: Scala (JVM)
- **Procesamiento**: Apache Spark (Big Data)
- **Almacenamiento**: Hive + Parquet
- **Exportación**: Excel automático
- **Escalabilidad**: Millones de registros

---

## 📞 Uso

```bash
# Ejecutar comparación completa
./run_compare.sh

# Resultados en Hive + Excel
```

**Tiempo de procesamiento**: Minutos para millones de registros
**Formato de salida**: 3 tablas Hive + 1 archivo Excel
**Configuración**: Archivo de configuración simple
