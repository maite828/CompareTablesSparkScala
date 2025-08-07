## **Sistema de Comparación( 2 tablas ) - Ejemplo Completo**

# CompareTablesProject
![CI](https://github.com/maite828/CompareTablesSparkScala/actions/workflows/ci.yml/badge.svg)

---

### **1. Configuración del Sistema**

```json
{
  "refTable": "db.ref_customers",
  "newTable": "db.new_customers",
  "partitionSpec": "[partition_date=2025-07-25]",
  "compositeKeyCols": ["id"],
  "ignoreCols": ["last_update"],
  "reportTable": "customer_summary",
  "diffTable": "customer_differences",
  "duplicatesTable": "customer_duplicates",
  "checkDuplicates": true,
  "includeEqualsInDiff": false
}
```

> **Explicación JSON**
>
> - `refTable`: Tabla de referencia para comparación
> - `newTable`: Tabla nueva a comparar
> - `partitionSpec`: Filtra datos por partición específica (formato `[col=valor]`)
> - `ignoreCols`: Columnas a ignorar en la comparación
> - `compositeKeyCols`: Define cómo identificar registros clave o eliminar duplicados
> - `checkDuplicates`: Detecta tanto duplicados exactos como con variaciones
> - `includeEqualsInDiff`: Controla si se incluyen coincidencias en el reporte de diferencias

---

### **2. Datos de Entrada**

**Tabla de Referencia (`ref_customers`)**

```
| id | country | amount | status  |
|----|---------|--------|---------|
| 1  | US      | 100.50 | active  |
| 2  | ES      | 75.20  | pending |
| 3  | MX      | 150.00 | active  |
| 4  | BR      | 200.00 | new     |
| 4  | BR      | 200.00 | new     | ← Duplicado idéntico
| 5  | FR      | 300.00 | active  |
| 5  | FR      | 300.50 | active  | ← Duplicado con variaciones
```

**Tabla Nueva (`new_customers`)**

```
| id | country | amount | status  |
|----|---------|--------|---------|
| 1  | US      | 100.49 | active  | ← Diferencia numérica
| 2  | ES      | 75.20  | expired | ← Diferencia textual
| 4  | BR      | 200.00 | new     |
| 4  | BR      | 200.00 | new     | ← Duplicado idéntico
| 6  | DE      | 400.00 | new     | ← Registro nuevo
| 6  | DE      | 400.00 | new     | ← Duplicado idéntico
| 6  | DE      | 400.10 | new     | ← Variación en duplicado
```

---

### **3. Resultados de la Comparación**

##### **Tabla 1: Resumen Ejecutivo (`customer_summary`)**

```
| Métrica                         | count_Ref | count_New | % Ref   | Status       | Ejemplos (IDs)                   |
|---------------------------------|-----------|-----------|---------|--------------|----------------------------------|
| Total registros                 | 8         | 9         | 100.0%  | +1           | -                                | ← La tabla nueva tiene 1 registro adicional (ID 6). Total de filas( incluye duplicados), no IDs únicos.
| Registros 1:1 exactos (Match)   | 1         | 1         | 12.5%   | Match        | ID 4                             | ← Solo ID 4 hace match completo
| Registros 1:1 con diferencias   | 2         | 2         | 25.0%   | No Match     | ID 1, ID 2                       | ← ID 1, ID 2 no coinciden en los valores
| Registros 1:0                   | 2         | -         | 25.0%   | Falta        | ID 3, ID 5                       | ← Están solo en referencia
| Registros 0:1                   | -         | 1         | -       | Sobra        | ID 6                             | ← Solo en tabla nueva
| Duplicados exactos              | 1         | 1         | 12.5%   | Exactos      | ID 4                             | ← Registros 100% iguales
| Duplicados con variaciones (ref)| 1         | -         | 12.5%   | Diferencias  | ID 5                             | ← ref tiene 2 versiones distintas
| Duplicados con variaciones (new)| -         | 1         | -       | Diferencias  | ID 6                             | ← new tiene 2 iguales + 1 variación
```

##### **Y la Tabla con 30k registros debería verse así: 200 columns and 30000 rows**

```
+---------+------------------------------+----------+-----------+-------------+--------+----------+
| bloque  | metrica                      | universo | numerador | denominador |  pct   | ejemplos |
+---------+------------------------------+----------+-----------+-------------+--------+----------+
| KPIS    | IDs Uniques                  | REF      | 11        | -           |   -    | -        |
| KPIS    | IDs Uniques                  | NEW      | 11        | -           |   -    | -        |
| KPIS    | Total REF                    | ROWS     | 11        | -           |   -    | -        |
| KPIS    | Total NEW                    | ROWS     | 11        | -           |   -    | -        |
| KPIS    | Total (NEW-REF)              | REF      | 0         | 11          |  0.0%  | -        |
| KPIS    | Quality global               | ALL_IDS  | 11        | 11          | 100.0% | -        |
| MATCH   | 1:1 (exact matches)          | REF      | 11        | 11          | 100.0% | -        |
| NO MATCH| 1:1 (match not identical)    | REF      | 0         | 11          |  0.0%  | -        |
| GAP     | 1:0 (only in reference)      | REF      | 0         | 11          |  0.0%  | -        |
| GAP     | 0:1 (only in new)            | REF      | 0         | 11          |  0.0%  | -        |
| DUPS    | Duplicates (both)            | REF      | 0         | 11          |  0.0%  | -        |
| DUPS    | duplicates (ref)             | REF      | 0         | 11          |  0.0%  | -        |
| DUPS    | Duplicates (new)             | NEW      | 0         | 11          |  0.0%  | -        |
+---------+------------------------------+----------+-----------+-------------+--------+----------+
```

> **Notas sobre la interpretación de esta tabla:**
>
> - El foco del análisis se centra en la **tabla de referencia (`ref_customers`)**, ya que el objetivo es validar si la nueva tabla (`new`) puede **reemplazar** completamente a la actual.
> - El seguimiento se realiza durante un período de prueba, ejecutando comparaciones diarias hasta que ambas tablas sean **idénticas**.
> - La columna `% Ref` se calcula como `(count_Ref / Total registros Ref) * 100`.
> - Para filas donde `count_Ref` está vacío (`-`), el valor de `% Ref` también se deja vacío para evitar confusión.

##### **Tabla 2: Diferencias Detalladas (`customer_differences`)**

```
| ID | Campo   | Valor Ref | Valor New | Resultado       |
|----|---------|-----------|-----------|-----------------|
| 1  | amount  | 100.50    | 100.49    | DIFERENCIA      | ← Diferencia mínima relevante
| 2  | status  | pending   | expired   | DIFERENCIA      | ← Cambio de estado
| 3  | country | MX        | -         | ONLY_IN_REF     | ← Registro solo en referencia pongo solo la primera columna
| 5  | country | FR        | -         | ONLY_IN_REF     | ← Registro solo en referencia pongo solo la primera columna
| 6  | country | -         | DE        | ONLY_IN_NEW     | ← Registro nuevo pongo solo la primera columna
```

##### **Y la Tabla en modo Extendido debería verse así: (`customer_differences`)**

```
| ID  | Campo   | Valor Ref | Valor New | Resultado       |
|-----|---------|-----------|-----------|-----------------|
| 1   | amount  | 100.50    | 100.49    | DIFERENCIA      |
| 1   | country | US        | US        | COINCIDE        |
| 1   | status  | active    | active    | COINCIDE        |
| 2   | amount  | 75.20     | 75.20     | COINCIDE        |
| 2   | status  | pending   | expired   | DIFERENCIA      |
| 3   | country | MX        | -         | ONLY_IN_REF     |
| 3   | amount  | 150.00    | -         | ONLY_IN_REF     |
| 3   | status  | active    | -         | ONLY_IN_REF     |
| 4   | country | BR        | BR        | COINCIDE        |
| 4   | amount  | 200.00    | 200.00    | COINCIDE        |
| 4   | status  | new       | new       | COINCIDE        |
| 5   | country | FR        | -         | ONLY_IN_REF     |
| 5   | amount  | 300.00    | -         | ONLY_IN_REF     |
| 5   | status  | active    | -         | ONLY_IN_REF     |
| 6   | country | -         | DE        | ONLY_IN_NEW     |
| 6   | amount  | -         | 400.00    | ONLY_IN_NEW     |
| 6   | status  | -         | new       | ONLY_IN_NEW     |
```

##### **Tabla 3: Detalle de Duplicados (`customer_duplicates`)**

```
| Origen     | ID | Exactos | Con variaciones | Total | Variaciones              |
|------------|----|---------|------------------|--------|---------------------------|
| Referencia | 4  | 2       | 0                | 2      | -                         | ← Registros iguales en tabla de referencia
| Nuevos     | 4  | 2       | 0                | 2      | -                         | ← Registros iguales también en tabla nueva
| Referencia | 5  | 0       | 2                | 2      | amount: [300.00, 300.50]  | ← Variación amount respecto al resto
| Nuevos     | 6  | 2       | 1                | 3      | amount: [400.00, 400.10]  | ← Variación amount respecto al resto
```

---

### **Notas Clave**

1. **Precisión absoluta**:

   - `100.50 ≠ 100.49` → Diferencia.
   - Strings comparados exactamente (`pending ≠ expired`).

2. **Duplicados**:

   - **Idénticos**: Todos los campos iguales (ID 4).
   - **Con variaciones**: Mismo ID, valores distintos (IDs 5 y 6).

3. **Registros únicos**:

   - `ONLY_IN_REF`: ID 3 (MX), ID 5 (FR).
   - `ONLY_IN_NEW`: ID 6 (DE).

4. **Consistencia**:

   - Términos uniformes: `COINCIDE`, `DIFERENCIA`, `ONLY_IN_*`.

5. **Modo extendido**:

   - Muestra coincidencias y diferencias para análisis exhaustivos.

> **Notas técnicas**
>
> - Comparaciones `case-sensitive`.
> - Comparación numérica exacta.
> - `NULL` se considera diferencia.

---

### **¿Cómo leer las tablas?**

- **Resumen Ejecutivo**: Panorama general.
- **Diferencias Detalladas**: Discrepancias relevantes.
- **Modo Extendido**: Todos los campos comparados.
- **Duplicados**: Repeticiones y diferencias entre ellas.
