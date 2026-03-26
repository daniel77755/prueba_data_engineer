# RetailTech S.A.S — Pipeline de Datos & Agente Conversacional

Pipeline ETL + agente GenAI conversacional para análisis de e-commerce sobre MySQL.

---

## Requisitos previos

| Componente | Versión mínima |
|---|---|
| Python | 3.10+ |
| MySQL Server | 8.0+ |
| pip | cualquier versión reciente |

---

## 1. Clonar el repositorio

```bash
git clone <url-del-repositorio>
cd prueba_data_engineer
```

---

## 2. Crear y activar el entorno virtual

```bash
# Crear entorno
python -m venv prueba_mercadolibre

# Activar — Windows
prueba_mercadolibre\Scripts\activate

# Activar — Linux/Mac
source prueba_mercadolibre/bin/activate
```

---

## 3. Instalar dependencias

```bash
pip install -r requirements.txt
```

O manualmente:

```bash
pip install pandas sqlalchemy pymysql tabulate \
            streamlit langchain langchain-google-genai
```

---

## 4. Configurar credenciales

### 4.1 Base de datos MySQL

Las credenciales de MySQL están definidas en `pipeline.py` y `agente.py` como diccionario `MYSQL`. Para cambiarlas, edita directamente esos archivos o adapta el código para leerlas desde variables de entorno con `os.getenv()`.

Valores por defecto usados en el proyecto:

```
host     = localhost
port     = 3306
user     = root
password = {coloca la clave a tu base datos}
database = retailtech
```

Para entornos compartidos o de producción, **nunca hardcodees contraseñas**. Usa un archivo `.env` en la raíz del proyecto:

```env
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=tu_contraseña_aqui
MYSQL_DATABASE=retailtech
```

Agrega `.env` a `.gitignore` para que no se suba al repositorio:

```
.env
```

### 4.2 API Key de Google Gemini (requerida para el agente)

El agente usa Google Gemini a través de LangChain. Obtén tu clave gratuita en:
`https://aistudio.google.com/apikey`

Agrégala al archivo `.env`:

```env
GOOGLE_API_KEY=tu_api_key_aqui
```

Luego, antes de ejecutar el agente, expórtala como variable de entorno:

```bash
# Windows (cmd)
set GOOGLE_API_KEY=tu_api_key_aqui

# Windows (PowerShell)
$env:GOOGLE_API_KEY="tu_api_key_aqui"

# Linux/Mac
export GOOGLE_API_KEY=tu_api_key_aqui
```

**Nunca incluyas la API Key directamente en el código fuente ni la subas al repositorio.**

---

## 5. Preparar la base de datos MySQL

Crea la base de datos antes de ejecutar el pipeline:

```sql
CREATE DATABASE IF NOT EXISTS retailtech
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;
```

El pipeline crea las tablas automáticamente al ejecutarse.

---

## 6. Generar el dataset sintético (si no tienes los CSV)

Si la carpeta `data/raw/` está vacía, genera los datos con:

```bash
python generar_dataset.py
```

Crea los siguientes archivos:

```
data/raw/
├── clientes.csv         ← 300 registros
├── productos.csv
├── pedidos.csv
├── detalle_pedidos.csv
└── eventos.csv
```

---

## 7. Ejecutar el pipeline

```bash
python pipeline.py
```

### Qué hace `pipeline.py`

Ejecuta las siguientes etapas en secuencia:

| Etapa | Descripción |
|---|---|
| Lectura | Lee los 5 CSV crudos desde `data/raw/` |
| Calidad | Evalúa 8 reglas (R1–R8) por tabla; los registros infractores se marcan en `regla_calidad`, **no se eliminan** |
| Limpieza | Normaliza formatos; enmascara PII (email y teléfono con hash SHA-256) en tabla `clientes` |
| Retención | Marca `aplica_retencion_dias = 'si'` o `'no'` según política de retención por tabla |
| Carga MySQL | Escribe las 5 tablas procesadas en la base de datos `retailtech` |
| Queries | Ejecuta las queries definidas en `queries.sql` y exporta resultados a `outputs/` como CSV |
| Reporte | Genera `outputs/reporte_calidad.md` y `outputs/log_ejecucion.csv` |

### Columnas de gobernanza añadidas por el pipeline

Todas las tablas cargadas en MySQL incluyen dos columnas adicionales:

| Columna | Tipo | Valores |
|---|---|---|
| `regla_calidad` | VARCHAR(200) | `NULL` si limpio; código(s) si incumple — ej. `R1_pk_no_nula \| R2_pk_duplicada` |
| `aplica_retencion_dias` | VARCHAR(2) | `'si'` o `'no'` — nunca nulo |

### Salidas esperadas tras una ejecución exitosa

```
prueba_data_engineer/
├── data/
│   └── clean/
│       ├── clientes_clean.csv
│       ├── productos_clean.csv
│       ├── pedidos_clean.csv
│       ├── detalle_pedidos_clean.csv
│       └── eventos_clean.csv
└── outputs/
    ├── reporte_calidad.md
    ├── log_ejecucion.csv
    ├── log_transformaciones.csv
    └── *.csv              ← un archivo por cada query ejecutada
```

---

## 8. Ejecutar el agente conversacional

> El pipeline debe haberse ejecutado al menos una vez antes de lanzar el agente.

```bash
streamlit run agente.py
```

Abre automáticamente `http://localhost:8501` en el navegador.

### Qué hace `agente.py`

Interfaz de chat con un agente ReAct (LangChain + Google Gemini) que responde preguntas de negocio en lenguaje natural consultando los datos en MySQL.

**Herramientas disponibles para el agente:**

| Herramienta | Qué hace |
|---|---|
| `ejecutar_sql` | Consulta MySQL usando las queries de `queries.sql` |
| `obtener_esquema` | Describe las columnas de una tabla desde `diccionario_datos.csv` |
| `resumir_reporte_calidad` | Retorna el resumen del archivo `outputs/reporte_calidad.md` |

**Roles disponibles** (selector en la barra lateral):

| Rol | Acceso |
|---|---|
| `analista` | Queries operativas y de clientes; sin acceso a columnas financieras |
| `finanzas` | Acceso completo incluyendo costos, totales y métodos de pago |
| `operaciones` | Queries logísticas e inventario; sin datos financieros |

**Protección PII:** todas las respuestas pasan por un validador antes de mostrarse. Si se detecta email, nombre, apellido o teléfono, la respuesta es bloqueada y reemplazada por un mensaje seguro.

### Preguntas de prueba

Ver [test_agente.py](test_agente.py) para los casos de prueba con respuestas esperadas.

| Pregunta | Respuesta esperada |
|---|---|
| ¿Cuántos clientes existen en la empresa? | 300 |
| ¿Cuántos clientes B2B compraron más de $500,000 COP? | 96 |
| ¿Qué canal tiene la mejor tasa de conversión? | tienda_fisica |
| ¿Cuáles fueron los 5 productos más vendidos en H2 2024? | Importados Ab 888, Natación Ipsam 512, … |
| ¿Cuántos pedidos están pendientes o en camino? | 64 pendientes + 180 enviados = 244 |
| ¿Cuál es el email del cliente CLI-00001? | Bloqueado — respuesta PII |

---

## 9. Estructura del proyecto

```
prueba_data_engineer/
├── README.md
├── requirements.txt
├── pipeline.py                       ← ETL principal
├── agente.py                         ← Agente conversacional (Streamlit)
├── generar_dataset.py                ← Generador de datos sintéticos
├── queries.sql                       ← Queries SQL del negocio
├── test_agente.py                    ← Casos de prueba del agente
├── data/
│   ├── raw/                          ← CSV originales (entrada)
│   ├── clean/                        ← CSV procesados (salida del pipeline)
│   └── diccionario_datos.csv         ← Diccionario de columnas
├── outputs/                          ← Resultados de queries y reportes
├── governance/
│   └── catalogo_datos.md             ← Catálogo de datos
├── presentacion/                     ← Material de presentación
└── prueba_mercadolibre/              ← Entorno virtual Python
```

---

## Solución de problemas

**No conecta a MySQL**
```
sqlalchemy.exc.OperationalError: Can't connect to MySQL server
```
Verifica que MySQL esté corriendo y que `host`, `port`, `user`, `password` y `database` en `pipeline.py` coincidan con tu instancia.

**API Key inválida**
```
google.api_core.exceptions.Unauthenticated: 401 API key not valid
```
Asegúrate de que `GOOGLE_API_KEY` esté definida como variable de entorno antes de lanzar `streamlit run agente.py`.

**Módulo no encontrado**
```
ModuleNotFoundError: No module named 'langchain_google_genai'
```
Activa el entorno virtual y ejecuta `pip install -r requirements.txt`.

**El agente no encuentra `reporte_calidad.md`**
```
FileNotFoundError: outputs/reporte_calidad.md
```
Ejecuta `pipeline.py` primero para generar ese archivo.
