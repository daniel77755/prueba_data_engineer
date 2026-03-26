# Reporte de Calidad de Datos
**Generado:** 2026-03-25 23:58:09

## CLIENTES
- Registros totales: 300
- Duplicados (registro completo): 0
- **Nulos por columna:**
  - `email`: 12 (4.0%)
  - `ciudad`: 7 (2.33%)
- **Cardinalidad de columnas:**

| columna | valores únicos |
|---|---|
| `cliente_id` | 300 |
| `nombre` | 163 |
| `apellido` | 162 |
| `email` | 288 |
| `telefono` | 276 |
| `ciudad` | 245 |
| `pais` | 6 |
| `segmento` | 4 |
| `fecha_registro` | 248 |
| `fecha_consentimiento` | 243 |
| `activo` | 2 |
| `data_owner` | 2 |
| `clasificacion_dato` | 1 |


## PRODUCTOS
- Registros totales: 80
- Duplicados (registro completo): 0
- **Nulos por columna:**
  - `stock_disponible`: 5 (6.25%)
- **Estadísticas numéricas:**

| métrica | precio_venta | costo | stock_disponible |
|---|---|---|---|
| count | 80.0 | 80.0 | 75.0 |
| mean | 681643.39 | 367033.85 | 227.76 |
| std | 493426.21 | 249694.86 | 142.59 |
| min | 26062.48 | 15038.53 | 6.0 |
| 25% | 243543.63 | 146397.72 | 102.5 |
| 50% | 622668.4 | 327720.52 | 209.0 |
| 75% | 1036182.56 | 566464.46 | 354.0 |
| max | 1897679.9 | 796598.31 | 495.0 |

- **Cardinalidad de columnas:**

| columna | valores únicos |
|---|---|
| `producto_id` | 80 |
| `nombre_producto` | 80 |
| `categoria` | 5 |
| `subcategoria` | 20 |
| `precio_venta` | 80 |
| `costo` | 80 |
| `stock_disponible` | 69 |
| `proveedor_id` | 15 |
| `nombre_proveedor` | 80 |
| `fecha_creacion` | 68 |
| `activo` | 2 |
| `data_owner` | 1 |
| `clasificacion_dato` | 1 |


## PEDIDOS
- Registros totales: 1200
- Duplicados (registro completo): 0
- **Nulos por columna:**
  - `fecha_entrega`: 367 (30.58%)
- **Estadísticas numéricas:**

| métrica | total_bruto | descuento_pct | total_neto |
|---|---|---|---|
| count | 1200.0 | 1200.0 | 1200.0 |
| mean | 1761830.55 | 0.06 | 1657728.14 |
| std | 1000344.15 | 0.09 | 955159.94 |
| min | 27501.4 | 0.0 | 23527.19 |
| 25% | 901166.45 | 0.0 | 826973.82 |
| 50% | 1784356.7 | 0.0 | 1648489.18 |
| 75% | 2602896.24 | 0.11 | 2413204.84 |
| max | 3495289.74 | 0.3 | 3490626.32 |

- **Cardinalidad de columnas:**

| columna | valores únicos |
|---|---|
| `pedido_id` | 1179 |
| `cliente_id` | 262 |
| `fecha_pedido` | 604 |
| `fecha_entrega` | 508 |
| `estado` | 5 |
| `canal` | 4 |
| `metodo_pago` | 5 |
| `pais_envio` | 6 |
| `total_bruto` | 1200 |
| `descuento_pct` | 31 |
| `total_neto` | 1200 |
| `data_owner` | 1 |
| `clasificacion_dato` | 1 |


## DETALLE_PEDIDOS
- Registros totales: 4177
- Duplicados (registro completo): 0
- **Estadísticas numéricas:**

| métrica | cantidad | precio_unitario | descuento_pct | subtotal |
|---|---|---|---|---|
| count | 4177.0 | 4177.0 | 4177.0 | 4177.0 |
| mean | 3.0 | 683104.26 | 0.03 | 2013878.84 |
| std | 1.44 | 488884.21 | 0.06 | 1883664.92 |
| min | 1.0 | 26062.48 | 0.0 | 21371.23 |
| 25% | 2.0 | 248256.24 | 0.0 | 549051.6 |
| 50% | 3.0 | 623582.81 | 0.0 | 1417991.4 |
| 75% | 4.0 | 1029632.63 | 0.03 | 2936312.3 |
| max | 5.0 | 1897679.9 | 0.2 | 9488399.5 |

- **Cardinalidad de columnas:**

| columna | valores únicos |
|---|---|
| `item_id` | 4177 |
| `pedido_id` | 1179 |
| `producto_id` | 80 |
| `cantidad` | 5 |
| `precio_unitario` | 80 |
| `descuento_pct` | 21 |
| `subtotal` | 1515 |
| `data_owner` | 1 |
| `clasificacion_dato` | 1 |


## EVENTOS
- Registros totales: 4000
- Duplicados (registro completo): 0
- **Nulos por columna:**
  - `cliente_id`: 1159 (28.98%)
  - `producto_id`: 2379 (59.48%)
  - `duracion_seg`: 184 (4.6%)
- **Estadísticas numéricas:**

| métrica | duracion_seg |
|---|---|
| count | 3816.0 |
| mean | 304.16 |
| std | 173.36 |
| min | 1.0 |
| 25% | 153.75 |
| 50% | 303.0 |
| 75% | 454.0 |
| max | 600.0 |

- **Cardinalidad de columnas:**

| columna | valores únicos |
|---|---|
| `evento_id` | 4000 |
| `cliente_id` | 300 |
| `session_id` | 1724 |
| `tipo_evento` | 6 |
| `timestamp` | 4000 |
| `producto_id` | 80 |
| `dispositivo` | 3 |
| `pais` | 6 |
| `duracion_seg` | 599 |
| `data_owner` | 1 |
| `clasificacion_dato` | 1 |


## Resumen de Reglas de Calidad

| Tabla | Campo | Regla | Afectados | Acción |
|---|---|---|---|---|
| clientes | cliente_id | R1_pk_no_nula | 0 | marcar en regla_calidad |
| pedidos | pedido_id | R1_pk_no_nula | 0 | marcar en regla_calidad |
| detalle_pedidos | item_id | R1_pk_no_nula | 0 | marcar en regla_calidad |
| productos | producto_id | R1_pk_no_nula | 0 | marcar en regla_calidad |
| eventos | evento_id | R1_pk_no_nula | 0 | marcar en regla_calidad |
| clientes | cliente_id | R2_pk_duplicada | 0 | marcar en regla_calidad |
| pedidos | pedido_id | R2_pk_duplicada | 42 | marcar en regla_calidad |
| detalle_pedidos | item_id | R2_pk_duplicada | 0 | marcar en regla_calidad |
| clientes | email | R3_email_formato | 0 | nulificar email inválido; marcar en regla_calidad |
| pedidos | total_neto | R4_total_neto_positivo | 0 | marcar en regla_calidad |
| detalle_pedidos | cantidad | R5_cantidad_positiva | 0 | marcar en regla_calidad |
| clientes | telefono | R6_telefono_sin_letras | 25 | nulificar telefono con letras; marcar en regla_calidad |
| pedidos | fecha_pedido | R7_fecha_no_futura | 0 | marcar en regla_calidad |
| productos | precio_venta | R8_precio_positivo | 0 | marcar en regla_calidad |

## Clasificación de Sensibilidad por Tabla

| Tabla | Clasificación |
|---|---|
| clientes | confidencial |
| productos | interno |
| pedidos | interno |
| detalle_pedidos | interno |
| eventos | público |

## Columnas PII Identificadas

- **clientes:** nombre, apellido, email, telefono

## Linaje de Datos

| Campo destino | Origen | Transformación |
|---|---|---|
| clientes.cliente_id | clientes.csv > cliente_id | marcado en regla_calidad si nulo (R1) o duplicado (R2) |
| clientes.nombre | clientes.csv > nombre | hash SHA-256 (enmascaramiento PII) |
| clientes.apellido | clientes.csv > apellido | hash SHA-256 (enmascaramiento PII) |
| clientes.email | clientes.csv > email | nulificación si formato inválido (R3); marcado en regla_calidad; hash SHA-256 (enmascaramiento PII) |
| clientes.telefono | clientes.csv > telefono | nulificación si contiene letras a-z (R6, case-insensitive); marcado en regla_calidad; enmascaramiento: últimos 4 dígitos (PII) |
| clientes.ciudad | clientes.csv > ciudad | sin transformación; carga directa |
| clientes.pais | clientes.csv > pais | sin transformación; carga directa |
| clientes.segmento | clientes.csv > segmento | sin transformación; carga directa |
| clientes.fecha_registro | clientes.csv > fecha_registro | marcado en regla_calidad si supera retención de 3650 días (diccionario_datos.csv) |
| clientes.fecha_consentimiento | clientes.csv > fecha_consentimiento | sin transformación; carga directa |
| clientes.activo | clientes.csv > activo | sin transformación; carga directa |
| clientes.data_owner | clientes.csv > data_owner | sin transformación; carga directa |
| clientes.clasificacion_dato | clientes.csv > clasificacion_dato | sin transformación; carga directa |
| productos.producto_id | productos.csv > producto_id | marcado en regla_calidad si nulo (R1) |
| productos.nombre_producto | productos.csv > nombre_producto | sin transformación; carga directa |
| productos.categoria | productos.csv > categoria | sin transformación; carga directa |
| productos.subcategoria | productos.csv > subcategoria | sin transformación; carga directa |
| productos.precio_venta | productos.csv > precio_venta | conversión numérica; marcado en regla_calidad si <= 0 (R8) |
| productos.costo | productos.csv > costo | conversión numérica; carga directa |
| productos.stock_disponible | productos.csv > stock_disponible | conversión numérica; carga directa |
| productos.proveedor_id | productos.csv > proveedor_id | sin transformación; carga directa |
| productos.nombre_proveedor | productos.csv > nombre_proveedor | sin transformación; carga directa |
| productos.fecha_creacion | productos.csv > fecha_creacion | sin transformación; carga directa |
| productos.activo | productos.csv > activo | sin transformación; carga directa |
| productos.data_owner | productos.csv > data_owner | sin transformación; carga directa |
| productos.clasificacion_dato | productos.csv > clasificacion_dato | sin transformación; carga directa |
| pedidos.pedido_id | pedidos.csv > pedido_id | marcado en regla_calidad si nulo (R1) o duplicado (R2) |
| pedidos.cliente_id | pedidos.csv > cliente_id | sin transformación; carga directa |
| pedidos.fecha_pedido | pedidos.csv > fecha_pedido | marcado en regla_calidad si fecha futura (R7) o supera retención de 1825 días |
| pedidos.fecha_entrega | pedidos.csv > fecha_entrega | sin transformación; carga directa (NULL permitido) |
| pedidos.estado | pedidos.csv > estado | sin transformación; carga directa |
| pedidos.canal | pedidos.csv > canal | sin transformación; carga directa |
| pedidos.metodo_pago | pedidos.csv > metodo_pago | sin transformación; carga directa |
| pedidos.pais_envio | pedidos.csv > pais_envio | sin transformación; carga directa |
| pedidos.total_bruto | pedidos.csv > total_bruto | conversión numérica; carga directa |
| pedidos.descuento_pct | pedidos.csv > descuento_pct | conversión numérica; carga directa |
| pedidos.total_neto | pedidos.csv > total_neto | conversión numérica; marcado en regla_calidad si <= 0 (R4) |
| pedidos.data_owner | pedidos.csv > data_owner | sin transformación; carga directa |
| pedidos.clasificacion_dato | pedidos.csv > clasificacion_dato | sin transformación; carga directa |
| detalle_pedidos.item_id | detalle_pedidos.csv > item_id | marcado en regla_calidad si nulo (R1) o duplicado (R2) |
| detalle_pedidos.pedido_id | detalle_pedidos.csv > pedido_id | sin transformación; carga directa |
| detalle_pedidos.producto_id | detalle_pedidos.csv > producto_id | sin transformación; carga directa |
| detalle_pedidos.cantidad | detalle_pedidos.csv > cantidad | conversión numérica; marcado en regla_calidad si < 1 (R5) |
| detalle_pedidos.precio_unitario | detalle_pedidos.csv > precio_unitario | conversión numérica; carga directa |
| detalle_pedidos.descuento_pct | detalle_pedidos.csv > descuento_pct | conversión numérica; carga directa |
| detalle_pedidos.subtotal | detalle_pedidos.csv > subtotal | conversión numérica; carga directa |
| detalle_pedidos.data_owner | detalle_pedidos.csv > data_owner | sin transformación; carga directa |
| detalle_pedidos.clasificacion_dato | detalle_pedidos.csv > clasificacion_dato | sin transformación; carga directa |
| eventos.evento_id | eventos.csv > evento_id | marcado en regla_calidad si nulo (R1) |
| eventos.cliente_id | eventos.csv > cliente_id | sin transformación; carga directa (NULL si anónimo) |
| eventos.session_id | eventos.csv > session_id | sin transformación; carga directa |
| eventos.tipo_evento | eventos.csv > tipo_evento | sin transformación; carga directa |
| eventos.timestamp | eventos.csv > timestamp | marcado en regla_calidad si supera retención de 365 días (diccionario_datos.csv) |
| eventos.producto_id | eventos.csv > producto_id | sin transformación; carga directa (NULL si no aplica) |
| eventos.dispositivo | eventos.csv > dispositivo | sin transformación; carga directa |
| eventos.pais | eventos.csv > pais | sin transformación; carga directa |
| eventos.duracion_seg | eventos.csv > duracion_seg | conversión numérica; carga directa |
| eventos.data_owner | eventos.csv > data_owner | sin transformación; carga directa |
| eventos.clasificacion_dato | eventos.csv > clasificacion_dato | sin transformación; carga directa |
| clientes.regla_calidad | pipeline — _flag_violations() + apply_retencion() | columna calculada: concatena códigos de reglas incumplidas (R1–R6, RETENCION_*) separados por ' | '; vacío si cumple todo |
| productos.regla_calidad | pipeline — _flag_violations() | columna calculada: concatena códigos R1, R8; vacío si cumple todo |
| pedidos.regla_calidad | pipeline — _flag_violations() + apply_retencion() | columna calculada: concatena códigos R1, R2, R4, R7, RETENCION_*; vacío si cumple todo |
| detalle_pedidos.regla_calidad | pipeline — _flag_violations() | columna calculada: concatena códigos R1, R2, R5; vacío si cumple todo |
| eventos.regla_calidad | pipeline — _flag_violations() + apply_retencion() | columna calculada: concatena códigos R1, RETENCION_*; vacío si cumple todo |
