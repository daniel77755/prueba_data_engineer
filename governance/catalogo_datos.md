# Catálogo de Datos — RetailTech S.A.S

**Versión:** 1.2
**Fecha:** 2026-03-25
**Responsable governance:** governance@retailtech.co
**Base de datos:** `retailtech` (MySQL)

---

## Índice de tablas

| Tabla | Dominio | Clasificación máxima | Data Owner |
|---|---|---|---|
| [clientes](#clientes) | CRM / Clientes | confidencial | crm@retailtech.co |
| [productos](#productos) | Catálogo / Inventario | interno | catalogo@retailtech.co |
| [pedidos](#pedidos) | Ventas / Finanzas | confidencial | ventas@retailtech.co |
| [detalle_pedidos](#detalle_pedidos) | Ventas / Finanzas | confidencial | ventas@retailtech.co |
| [eventos](#eventos) | Analytics Digital | público | analytics@retailtech.co |

---

## Niveles de clasificación

| Nivel | Descripción | Acceso |
|---|---|---|
| **público** | Datos sin restricción, visibles al público general | Sin restricción |
| **interno** | Uso exclusivo del equipo RetailTech | Empleados autorizados |
| **confidencial** | Datos sensibles de negocio o personales | Acceso controlado por rol |
| **restringido** | Información crítica o regulada (PII sensible) | Solo data owners + legal |

---

## clientes

**Descripción:** Registro maestro de clientes de RetailTech. Contiene información personal (PII) y de segmentación comercial. Los campos PII son enmascarados mediante SHA-256 antes de cualquier uso analítico.
**Data Owner:** crm@retailtech.co
**Clasificación:** confidencial
**Retención base:** 3650 días (10 años) para fecha_registro · 1825 días (5 años) para PII · 365 días para datos de ubicación
**Filas aprox.:** 300

| Columna | Tipo | Descripción | PII | Clasificación | Retención (días) | Constraint |
|---|---|---|:---:|---|---|---|
| `cliente_id` | VARCHAR(10) | Identificador único del cliente | No | confidencial | 1825 | PK |
| `nombre` | VARCHAR(100) | Nombre de pila del cliente | **Sí** | confidencial | 1825 | — |
| `apellido` | VARCHAR(100) | Apellido del cliente | **Sí** | confidencial | 1825 | — |
| `email` | VARCHAR(200) | Correo electrónico de contacto | **Sí** | confidencial | 1825 | — |
| `telefono` | VARCHAR(30) | Teléfono de contacto | **Sí** | confidencial | 1825 | — |
| `ciudad` | VARCHAR(100) | Ciudad de residencia | No | interno | 365 | — |
| `pais` | VARCHAR(50) | País de residencia | No | interno | 365 | — |
| `segmento` | VARCHAR(20) | Segmento comercial (B2B / B2C) | No | interno | 365 | — |
| `fecha_registro` | DATE | Fecha de alta del cliente | No | interno | 3650 | — |
| `fecha_consentimiento` | DATE | Fecha de aceptación política datos (GDPR / Ley 1581) | No | confidencial | 3650 | — |
| `activo` | BOOLEAN | Indica si el cliente está activo | No | interno | 365 | — |
| `data_owner` | VARCHAR(100) | Correo del responsable del dato | No | interno | 3650 | — |
| `clasificacion_dato` | VARCHAR(20) | Nivel de clasificación de seguridad | No | interno | 3650 | — |
| `regla_calidad` | VARCHAR(200) | Códigos de reglas de calidad incumplidas separados por ` \| ` (NULL si cumple todo) | No | interno | 3650 | Generada por pipeline |
| `aplica_retencion_dias` | VARCHAR(2) | Indica si el registro supera la política de retención: `si` / `no` | No | interno | 3650 | Generada por pipeline |

**Notas de privacidad:**
- `nombre`, `apellido`, `email` son enmascarados con SHA-256 en la capa `clean`.
- `telefono` se trunca a los últimos 4 dígitos. Si contiene letras (a-z, case-insensitive) es nulificado (R6).
- Ningún campo PII es expuesto por el agente conversacional.
- La retención se evalúa sobre `fecha_registro`: registros fuera del límite son **marcados** en `aplica_retencion_dias = 'si'`; no se eliminan.

---

## productos

**Descripción:** Catálogo maestro de productos disponibles en la plataforma. Incluye información de precios, costos, inventario y proveedor. Los datos de costo y proveedor son confidenciales para uso interno de compras y finanzas.
**Data Owner:** catalogo@retailtech.co
**Clasificación:** interno (confidencial para campos de costo y proveedor)
**Retención base:** 3650 días (10 años) para maestro · 365 días para precios · 90 días para stock
**Filas aprox.:** 80

| Columna | Tipo | Descripción | PII | Clasificación | Retención (días) | Constraint |
|---|---|---|:---:|---|---|---|
| `producto_id` | VARCHAR(10) | Identificador único del producto | No | interno | 3650 | PK |
| `nombre_producto` | VARCHAR(200) | Nombre comercial del producto | No | público | 3650 | — |
| `categoria` | VARCHAR(50) | Categoría principal del producto | No | público | 3650 | — |
| `subcategoria` | VARCHAR(50) | Subcategoría del producto | No | público | 3650 | — |
| `precio_venta` | DECIMAL(12,2) | Precio de venta al público (COP) | No | interno | 365 | — |
| `costo` | DECIMAL(12,2) | Costo de adquisición (COP) | No | confidencial | 365 | — |
| `stock_disponible` | INTEGER | Unidades disponibles en inventario | No | interno | 90 | — |
| `proveedor_id` | VARCHAR(10) | ID del proveedor | No | interno | 1825 | FK |
| `nombre_proveedor` | VARCHAR(200) | Nombre del proveedor | No | confidencial | 1825 | — |
| `fecha_creacion` | DATE | Fecha de alta del producto | No | interno | 3650 | — |
| `activo` | BOOLEAN | Indica si el producto está disponible | No | interno | 365 | — |
| `data_owner` | VARCHAR(100) | Correo del responsable del dato | No | interno | 3650 | — |
| `clasificacion_dato` | VARCHAR(20) | Nivel de clasificación de seguridad | No | interno | 3650 | — |
| `regla_calidad` | VARCHAR(200) | Códigos de reglas de calidad incumplidas separados por ` \| ` (NULL si cumple todo) | No | interno | 3650 | Generada por pipeline |
| `aplica_retencion_dias` | VARCHAR(2) | Indica si el registro supera la política de retención: `si` / `no` | No | interno | 3650 | Generada por pipeline |

**Notas:**
- `costo` y `nombre_proveedor` son accesibles solo para roles de finanzas y compras.
- `precio_venta` puede diferir del `precio_unitario` en `detalle_pedidos` (captura precio histórico en el momento de la compra).
- Regla de calidad R8: `precio_venta > 0`; registros inválidos son **marcados** en `regla_calidad`.
- `aplica_retencion_dias` siempre es `no` para esta tabla (sin política de retención activa).

---

## pedidos

**Descripción:** Registro transaccional de todos los pedidos realizados en la plataforma. Contiene datos financieros (totales, descuentos), logísticos (canal, país, estado) y temporales. Es la tabla central del modelo analítico de ventas.
**Data Owner:** ventas@retailtech.co
**Clasificación:** confidencial (por campos financieros)
**Retención base:** 1825 días (5 años)
**Filas aprox.:** 1,200

| Columna | Tipo | Descripción | PII | Clasificación | Retención (días) | Constraint |
|---|---|---|:---:|---|---|---|
| `pedido_id` | VARCHAR(12) | Identificador único del pedido | No | interno | 1825 | PK |
| `cliente_id` | VARCHAR(10) | FK al cliente que realizó el pedido | No | interno | 1825 | FK → clientes |
| `fecha_pedido` | DATE | Fecha en que se realizó el pedido | No | interno | 1825 | — |
| `fecha_entrega` | DATE | Fecha de entrega efectiva (NULL si no entregado) | No | interno | 1825 | — |
| `estado` | VARCHAR(20) | Estado del pedido (pendiente / enviado / entregado / devuelto / cancelado) | No | interno | 365 | — |
| `canal` | VARCHAR(20) | Canal de venta (web / mobile / tienda_fisica / marketplace) | No | interno | 1825 | — |
| `metodo_pago` | VARCHAR(30) | Método de pago utilizado | No | confidencial | 1825 | — |
| `pais_envio` | VARCHAR(50) | País destino del envío | No | interno | 1825 | — |
| `total_bruto` | DECIMAL(14,2) | Total antes de descuentos (COP) | No | confidencial | 1825 | — |
| `descuento_pct` | DECIMAL(5,2) | Porcentaje de descuento aplicado | No | interno | 1825 | — |
| `total_neto` | DECIMAL(14,2) | Total final pagado (COP) | No | confidencial | 1825 | — |
| `data_owner` | VARCHAR(100) | Correo del responsable del dato | No | interno | 1825 | — |
| `clasificacion_dato` | VARCHAR(20) | Nivel de clasificación de seguridad | No | interno | 1825 | — |
| `regla_calidad` | VARCHAR(200) | Códigos de reglas de calidad incumplidas separados por ` \| ` (NULL si cumple todo) | No | interno | 1825 | Generada por pipeline |
| `aplica_retencion_dias` | VARCHAR(2) | Indica si el registro supera la política de retención: `si` / `no` | No | interno | 1825 | Generada por pipeline |

**Notas:**
- `total_bruto`, `total_neto` y `metodo_pago` son accesibles solo para roles de finanzas.
- `fecha_entrega` puede ser NULL para pedidos en estado pendiente o enviado.
- Regla de calidad R4: `total_neto > 0`; registros inválidos son **marcados** en `regla_calidad`.
- Regla de calidad R7: `fecha_pedido` no puede ser futura; registros inválidos son **marcados** en `regla_calidad`.
- La retención se evalúa sobre `fecha_pedido`: registros fuera del límite son **marcados** en `aplica_retencion_dias = 'si'`; no se eliminan.

---

## detalle_pedidos

**Descripción:** Tabla de líneas de detalle por pedido (modelo estrella: fact table). Cada fila representa un producto dentro de un pedido, con precio unitario histórico y subtotal. Es la fuente principal para análisis de productos más vendidos.
**Data Owner:** ventas@retailtech.co
**Clasificación:** confidencial (por campos de precio y subtotal)
**Retención base:** 1825 días (5 años)
**Filas aprox.:** 4,177

| Columna | Tipo | Descripción | PII | Clasificación | Retención (días) | Constraint |
|---|---|---|:---:|---|---|---|
| `item_id` | VARCHAR(13) | Identificador único de línea de pedido | No | interno | 1825 | PK |
| `pedido_id` | VARCHAR(12) | FK al pedido | No | interno | 1825 | FK → pedidos |
| `producto_id` | VARCHAR(10) | FK al producto | No | interno | 1825 | FK → productos |
| `cantidad` | INTEGER | Unidades pedidas | No | interno | 1825 | — |
| `precio_unitario` | DECIMAL(12,2) | Precio unitario al momento de la compra (COP) | No | confidencial | 1825 | — |
| `descuento_pct` | DECIMAL(5,2) | Descuento por ítem | No | interno | 1825 | — |
| `subtotal` | DECIMAL(14,2) | Subtotal de la línea (COP) | No | confidencial | 1825 | — |
| `data_owner` | VARCHAR(100) | Correo del responsable del dato | No | interno | 1825 | — |
| `clasificacion_dato` | VARCHAR(20) | Nivel de clasificación de seguridad | No | interno | 1825 | — |
| `regla_calidad` | VARCHAR(200) | Códigos de reglas de calidad incumplidas separados por ` \| ` (NULL si cumple todo) | No | interno | 1825 | Generada por pipeline |
| `aplica_retencion_dias` | VARCHAR(2) | Indica si el registro supera la política de retención: `si` / `no` | No | interno | 1825 | Generada por pipeline |

**Notas:**
- `precio_unitario` captura el precio histórico al momento de la compra, puede diferir del precio vigente en `productos`.
- Regla de calidad R5: `cantidad >= 1`; registros con cantidad inválida son **marcados** en `regla_calidad`.
- `aplica_retencion_dias` siempre es `no` para esta tabla (sin política de retención activa).

---

## eventos

**Descripción:** Log de eventos digitales del comportamiento de usuarios en la plataforma web y app. Incluye clics, vistas de producto, carritos, compras y abandonos. `cliente_id` puede ser NULL para sesiones anónimas.
**Data Owner:** analytics@retailtech.co
**Clasificación:** público (confidencial para el campo `cliente_id`)
**Retención base:** 365 días para eventos y timestamp · 90 días para session_id y duracion_seg
**Filas aprox.:** 4,000

| Columna | Tipo | Descripción | PII | Clasificación | Retención (días) | Constraint |
|---|---|---|:---:|---|---|---|
| `evento_id` | VARCHAR(12) | Identificador único del evento digital | No | público | 365 | PK |
| `cliente_id` | VARCHAR(10) | FK al cliente (NULL si sesión anónima) | No | confidencial | 365 | FK → clientes |
| `session_id` | VARCHAR(14) | Identificador de sesión web/app | No | interno | 90 | — |
| `tipo_evento` | VARCHAR(30) | Tipo de interacción (vista / clic / carrito / compra / abandono) | No | público | 365 | — |
| `timestamp` | TIMESTAMP | Fecha y hora exacta del evento | No | interno | 365 | — |
| `producto_id` | VARCHAR(10) | FK al producto (NULL si no aplica) | No | público | 365 | FK → productos |
| `dispositivo` | VARCHAR(20) | Tipo de dispositivo (mobile / desktop / tablet) | No | público | 365 | — |
| `pais` | VARCHAR(50) | País del evento | No | público | 365 | — |
| `duracion_seg` | INTEGER | Duración de la interacción en segundos | No | público | 90 | — |
| `data_owner` | VARCHAR(100) | Correo del responsable del dato | No | interno | 365 | — |
| `clasificacion_dato` | VARCHAR(20) | Nivel de clasificación de seguridad | No | interno | 365 | — |
| `regla_calidad` | VARCHAR(200) | Códigos de reglas de calidad incumplidas separados por ` \| ` (NULL si cumple todo) | No | interno | 365 | Generada por pipeline |
| `aplica_retencion_dias` | VARCHAR(2) | Indica si el registro supera la política de retención: `si` / `no` | No | interno | 365 | Generada por pipeline |

**Notas:**
- `cliente_id` es confidencial aunque no sea PII directa, ya que vincula al cliente con su comportamiento de navegación.
- `session_id` y `duracion_seg` tienen retención corta (90 días) por política de analytics.
- La retención se evalúa sobre `timestamp`: registros fuera del límite (hoy − 365 días) son **marcados** en `aplica_retencion_dias = 'si'`; no se eliminan.

---

## Relaciones entre tablas

```
clientes (cliente_id) ──────< pedidos (cliente_id)
pedidos  (pedido_id)  ──────< detalle_pedidos (pedido_id)
productos(producto_id)──────< detalle_pedidos (producto_id)
productos(producto_id)──────< eventos (producto_id)
clientes (cliente_id) ──────< eventos (cliente_id)
```

---

## Columnas de gobernanza generadas por el pipeline

Todas las tablas incluyen dos columnas adicionales calculadas durante la etapa de limpieza del pipeline. No provienen de los CSVs fuente.

| Columna | Tipo | Descripción | Valores posibles |
|---|---|---|---|
| `regla_calidad` | VARCHAR(200) | Códigos de reglas de calidad incumplidas por el registro | Código(s) separados por ` \| ` · NULL si cumple todo |
| `aplica_retencion_dias` | VARCHAR(2) | Indica si el registro supera la política de retención de su tabla | `si` · `no` |

**Valores posibles en `regla_calidad`:**

| Código | Regla | Tabla(s) |
|---|---|---|
| `R1_pk_no_nula` | PK es nula | Todas |
| `R2_pk_duplicada` | PK aparece más de una vez | clientes, pedidos, detalle_pedidos |
| `R3_email_formato` | Email no contiene `@` y dominio válido | clientes |
| `R4_total_neto_positivo` | `total_neto <= 0` | pedidos |
| `R5_cantidad_positiva` | `cantidad < 1` | detalle_pedidos |
| `R6_telefono_sin_letras` | `telefono` contiene letras a-z | clientes |
| `R7_fecha_no_futura` | `fecha_pedido` es fecha futura | pedidos |
| `R8_precio_positivo` | `precio_venta <= 0` | productos |

---

## Reglas de calidad aplicadas en pipeline

| ID | Regla | Tabla(s) | Acción sobre el valor | Acción sobre el registro |
|---|---|---|---|---|
| R1 | PK no nula | Todas | — | Marcado en `regla_calidad`; no se elimina |
| R2 | PK duplicada | clientes, pedidos, detalle_pedidos | — | Marcado en `regla_calidad`; no se elimina |
| R3 | Formato email válido (`@` y dominio) | clientes | Email nulificado | Marcado en `regla_calidad` |
| R4 | `total_neto > 0` | pedidos | — | Marcado en `regla_calidad`; no se elimina |
| R5 | `cantidad >= 1` | detalle_pedidos | — | Marcado en `regla_calidad`; no se elimina |
| R6 | `telefono` no debe contener letras (a-z, case-insensitive) | clientes | Teléfono nulificado | Marcado en `regla_calidad` |
| R7 | `fecha_pedido` no futura | pedidos | — | Marcado en `regla_calidad`; no se elimina |
| R8 | `precio_venta > 0` | productos | — | Marcado en `regla_calidad`; no se elimina |

---

## Política de retención

La retención se evalúa en la etapa de limpieza del pipeline (`apply_retencion`). Los registros cuya fecha de referencia es anterior a `hoy − retencion_dias` son **marcados** con `aplica_retencion_dias = 'si'`. Ningún registro es eliminado. Los registros dentro del período reciben `aplica_retencion_dias = 'no'`. Las tablas sin campo de retención definido (`productos`, `detalle_pedidos`) siempre reciben `aplica_retencion_dias = 'no'`.

| Tabla | Campo de referencia | Retención | Registros marcados (`si`) |
|---|---|---|---|
| clientes | fecha_registro | 3650 días (10 años) | 0 (datos dentro del período) |
| pedidos | fecha_pedido | 1825 días (5 años) | 0 (datos dentro del período) |
| eventos | timestamp | 365 días (1 año) | 4,000 (datos de 2023-2024, fuera del límite) |
| productos | — | Sin política activa | 0 (siempre `no`) |
| detalle_pedidos | — | Sin política activa | 0 (siempre `no`) |

---

*Documento actualizado a partir de los cambios del pipeline v1.2 — RetailTech Data Governance v1.2*
