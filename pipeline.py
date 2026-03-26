"""
pipeline.py — RetailTech S.A.S | Data Engineering Pipeline
===========================================================
Flujo completo:
  1. Lectura de CSVs crudos
  2. Validaciones de calidad (8 reglas)
  3. Limpieza, enmascaramiento PII y retención
  4. Carga en MySQL
  5. Ejecución de queries SQL y exportación a CSV
  6. Log de ejecución con tiempos por etapa
  7. Clasificación de sensibilidad y linaje de datos
"""

import os
import hashlib
import time
import logging
from datetime import datetime, timedelta

import pandas as pd
import sqlalchemy

# ---------------------------------------------
# CONFIGURACIÓN
# ---------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

PATHS = {
    "raw":        os.path.join(BASE_DIR, "data", "raw"),
    "clean":      os.path.join(BASE_DIR, "data", "clean"),
    "diccionario": os.path.join(BASE_DIR, "data", "diccionario_datos.csv"),
    "outputs":    os.path.join(BASE_DIR, "outputs"),
}

MYSQL = {
    "host":     "localhost",
    "port":     3306,
    "user":     "root",
    "password": "",
    "database": "retailtech",
}

TABLAS = ["clientes", "productos", "pedidos", "detalle_pedidos", "eventos"]

# Columnas PII identificadas del diccionario de datos
PII_COLUMNS = {
    "clientes": ["nombre", "apellido", "email", "telefono"],
}

# Clasificación de sensibilidad por tabla (del diccionario de datos)
TABLA_CLASIFICACION = {
    "clientes":        "confidencial",
    "productos":       "interno",
    "pedidos":         "interno",
    "detalle_pedidos": "interno",
    "eventos":         "público",
}

# Linaje: campo_destino -> {origen, transformacion}
LINAJE = {
    # ── clientes ──────────────────────────────────────────────────────────────
    "clientes.cliente_id":          {"origen": "clientes.csv > cliente_id",          "transformacion": "marcado en regla_calidad si nulo (R1) o duplicado (R2)"},
    "clientes.nombre":              {"origen": "clientes.csv > nombre",               "transformacion": "hash SHA-256 (enmascaramiento PII)"},
    "clientes.apellido":            {"origen": "clientes.csv > apellido",             "transformacion": "hash SHA-256 (enmascaramiento PII)"},
    "clientes.email":               {"origen": "clientes.csv > email",                "transformacion": "nulificación si formato inválido (R3); marcado en regla_calidad; hash SHA-256 (enmascaramiento PII)"},
    "clientes.telefono":            {"origen": "clientes.csv > telefono",             "transformacion": "nulificación si contiene letras a-z (R6, case-insensitive); marcado en regla_calidad; enmascaramiento: últimos 4 dígitos (PII)"},
    "clientes.ciudad":              {"origen": "clientes.csv > ciudad",               "transformacion": "sin transformación; carga directa"},
    "clientes.pais":                {"origen": "clientes.csv > pais",                 "transformacion": "sin transformación; carga directa"},
    "clientes.segmento":            {"origen": "clientes.csv > segmento",             "transformacion": "sin transformación; carga directa"},
    "clientes.fecha_registro":      {"origen": "clientes.csv > fecha_registro",       "transformacion": "marcado en regla_calidad si supera retención de 3650 días (diccionario_datos.csv)"},
    "clientes.fecha_consentimiento":{"origen": "clientes.csv > fecha_consentimiento", "transformacion": "sin transformación; carga directa"},
    "clientes.activo":              {"origen": "clientes.csv > activo",               "transformacion": "sin transformación; carga directa"},
    "clientes.data_owner":          {"origen": "clientes.csv > data_owner",           "transformacion": "sin transformación; carga directa"},
    "clientes.clasificacion_dato":  {"origen": "clientes.csv > clasificacion_dato",   "transformacion": "sin transformación; carga directa"},
    # ── productos ─────────────────────────────────────────────────────────────
    "productos.producto_id":        {"origen": "productos.csv > producto_id",         "transformacion": "marcado en regla_calidad si nulo (R1)"},
    "productos.nombre_producto":    {"origen": "productos.csv > nombre_producto",     "transformacion": "sin transformación; carga directa"},
    "productos.categoria":          {"origen": "productos.csv > categoria",           "transformacion": "sin transformación; carga directa"},
    "productos.subcategoria":       {"origen": "productos.csv > subcategoria",        "transformacion": "sin transformación; carga directa"},
    "productos.precio_venta":       {"origen": "productos.csv > precio_venta",        "transformacion": "conversión numérica; marcado en regla_calidad si <= 0 (R8)"},
    "productos.costo":              {"origen": "productos.csv > costo",               "transformacion": "conversión numérica; carga directa"},
    "productos.stock_disponible":   {"origen": "productos.csv > stock_disponible",    "transformacion": "conversión numérica; carga directa"},
    "productos.proveedor_id":       {"origen": "productos.csv > proveedor_id",        "transformacion": "sin transformación; carga directa"},
    "productos.nombre_proveedor":   {"origen": "productos.csv > nombre_proveedor",    "transformacion": "sin transformación; carga directa"},
    "productos.fecha_creacion":     {"origen": "productos.csv > fecha_creacion",      "transformacion": "sin transformación; carga directa"},
    "productos.activo":             {"origen": "productos.csv > activo",              "transformacion": "sin transformación; carga directa"},
    "productos.data_owner":         {"origen": "productos.csv > data_owner",          "transformacion": "sin transformación; carga directa"},
    "productos.clasificacion_dato": {"origen": "productos.csv > clasificacion_dato",  "transformacion": "sin transformación; carga directa"},
    # ── pedidos ───────────────────────────────────────────────────────────────
    "pedidos.pedido_id":            {"origen": "pedidos.csv > pedido_id",             "transformacion": "marcado en regla_calidad si nulo (R1) o duplicado (R2)"},
    "pedidos.cliente_id":           {"origen": "pedidos.csv > cliente_id",            "transformacion": "sin transformación; carga directa"},
    "pedidos.fecha_pedido":         {"origen": "pedidos.csv > fecha_pedido",          "transformacion": "marcado en regla_calidad si fecha futura (R7) o supera retención de 1825 días"},
    "pedidos.fecha_entrega":        {"origen": "pedidos.csv > fecha_entrega",         "transformacion": "sin transformación; carga directa (NULL permitido)"},
    "pedidos.estado":               {"origen": "pedidos.csv > estado",                "transformacion": "sin transformación; carga directa"},
    "pedidos.canal":                {"origen": "pedidos.csv > canal",                 "transformacion": "sin transformación; carga directa"},
    "pedidos.metodo_pago":          {"origen": "pedidos.csv > metodo_pago",           "transformacion": "sin transformación; carga directa"},
    "pedidos.pais_envio":           {"origen": "pedidos.csv > pais_envio",            "transformacion": "sin transformación; carga directa"},
    "pedidos.total_bruto":          {"origen": "pedidos.csv > total_bruto",           "transformacion": "conversión numérica; carga directa"},
    "pedidos.descuento_pct":        {"origen": "pedidos.csv > descuento_pct",         "transformacion": "conversión numérica; carga directa"},
    "pedidos.total_neto":           {"origen": "pedidos.csv > total_neto",            "transformacion": "conversión numérica; marcado en regla_calidad si <= 0 (R4)"},
    "pedidos.data_owner":           {"origen": "pedidos.csv > data_owner",            "transformacion": "sin transformación; carga directa"},
    "pedidos.clasificacion_dato":   {"origen": "pedidos.csv > clasificacion_dato",    "transformacion": "sin transformación; carga directa"},
    # ── detalle_pedidos ───────────────────────────────────────────────────────
    "detalle_pedidos.item_id":           {"origen": "detalle_pedidos.csv > item_id",           "transformacion": "marcado en regla_calidad si nulo (R1) o duplicado (R2)"},
    "detalle_pedidos.pedido_id":         {"origen": "detalle_pedidos.csv > pedido_id",         "transformacion": "sin transformación; carga directa"},
    "detalle_pedidos.producto_id":       {"origen": "detalle_pedidos.csv > producto_id",       "transformacion": "sin transformación; carga directa"},
    "detalle_pedidos.cantidad":          {"origen": "detalle_pedidos.csv > cantidad",          "transformacion": "conversión numérica; marcado en regla_calidad si < 1 (R5)"},
    "detalle_pedidos.precio_unitario":   {"origen": "detalle_pedidos.csv > precio_unitario",   "transformacion": "conversión numérica; carga directa"},
    "detalle_pedidos.descuento_pct":     {"origen": "detalle_pedidos.csv > descuento_pct",     "transformacion": "conversión numérica; carga directa"},
    "detalle_pedidos.subtotal":          {"origen": "detalle_pedidos.csv > subtotal",          "transformacion": "conversión numérica; carga directa"},
    "detalle_pedidos.data_owner":        {"origen": "detalle_pedidos.csv > data_owner",        "transformacion": "sin transformación; carga directa"},
    "detalle_pedidos.clasificacion_dato":{"origen": "detalle_pedidos.csv > clasificacion_dato","transformacion": "sin transformación; carga directa"},
    # ── eventos ───────────────────────────────────────────────────────────────
    "eventos.evento_id":            {"origen": "eventos.csv > evento_id",             "transformacion": "marcado en regla_calidad si nulo (R1)"},
    "eventos.cliente_id":           {"origen": "eventos.csv > cliente_id",            "transformacion": "sin transformación; carga directa (NULL si anónimo)"},
    "eventos.session_id":           {"origen": "eventos.csv > session_id",            "transformacion": "sin transformación; carga directa"},
    "eventos.tipo_evento":          {"origen": "eventos.csv > tipo_evento",           "transformacion": "sin transformación; carga directa"},
    "eventos.timestamp":            {"origen": "eventos.csv > timestamp",             "transformacion": "marcado en regla_calidad si supera retención de 365 días (diccionario_datos.csv)"},
    "eventos.producto_id":          {"origen": "eventos.csv > producto_id",           "transformacion": "sin transformación; carga directa (NULL si no aplica)"},
    "eventos.dispositivo":          {"origen": "eventos.csv > dispositivo",           "transformacion": "sin transformación; carga directa"},
    "eventos.pais":                 {"origen": "eventos.csv > pais",                  "transformacion": "sin transformación; carga directa"},
    "eventos.duracion_seg":         {"origen": "eventos.csv > duracion_seg",          "transformacion": "conversión numérica; carga directa"},
    "eventos.data_owner":           {"origen": "eventos.csv > data_owner",            "transformacion": "sin transformación; carga directa"},
    "eventos.clasificacion_dato":   {"origen": "eventos.csv > clasificacion_dato",    "transformacion": "sin transformación; carga directa"},
    # ── regla_calidad (columna generada por el pipeline) ──────────────────────
    "clientes.regla_calidad":          {"origen": "pipeline — _flag_violations() + apply_retencion()", "transformacion": "columna calculada: concatena códigos de reglas incumplidas (R1–R6, RETENCION_*) separados por ' | '; vacío si cumple todo"},
    "productos.regla_calidad":         {"origen": "pipeline — _flag_violations()",                     "transformacion": "columna calculada: concatena códigos R1, R8; vacío si cumple todo"},
    "pedidos.regla_calidad":           {"origen": "pipeline — _flag_violations() + apply_retencion()", "transformacion": "columna calculada: concatena códigos R1, R2, R4, R7, RETENCION_*; vacío si cumple todo"},
    "detalle_pedidos.regla_calidad":   {"origen": "pipeline — _flag_violations()",                     "transformacion": "columna calculada: concatena códigos R1, R2, R5; vacío si cumple todo"},
    "eventos.regla_calidad":           {"origen": "pipeline — _flag_violations() + apply_retencion()", "transformacion": "columna calculada: concatena códigos R1, RETENCION_*; vacío si cumple todo"},
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------
# 1. INGESTA — Lectura de CSVs crudos
# ---------------------------------------------

def load_raw_csv(tabla: str) -> pd.DataFrame:
    """Lee el CSV crudo de una tabla desde PATHS['raw'] y lo retorna como DataFrame.
    Todas las columnas se cargan como string para preservar el valor original.
    Los valores '', 'NULL', 'null' y 'NA' son interpretados como NaN.
    """
    path = os.path.join(PATHS["raw"], f"{tabla}.csv")
    return pd.read_csv(path, dtype=str, keep_default_na=False,
                       na_values=["", "NULL", "null", "NA"])


def load_all_raw() -> dict:
    """Carga los CSVs crudos de todas las tablas definidas en TABLAS.
    Retorna un dict {nombre_tabla: DataFrame}.
    """
    return {tabla: load_raw_csv(tabla) for tabla in TABLAS}


def load_diccionario() -> pd.DataFrame:
    """Carga el diccionario de datos desde PATHS['diccionario'].
    Contiene metadata de cada columna: tipo, PII, data_owner, retencion_dias y clasificación.
    """
    return pd.read_csv(PATHS["diccionario"])


def get_retencion_dias(diccionario: pd.DataFrame, tabla: str, columna: str):
    """Consulta el diccionario de datos y retorna el valor de retencion_dias
    para la combinación tabla+columna indicada. Retorna None si no se encuentra.
    """
    mask = (diccionario["tabla"] == tabla) & (diccionario["columna"] == columna)
    row = diccionario[mask]
    return int(row.iloc[0]["retencion_dias"]) if not row.empty else None


# ---------------------------------------------
# 2. CALIDAD — Perfilado y 8 reglas de validación
# ---------------------------------------------

def profile_table(df: pd.DataFrame, tabla: str) -> dict:
    """Genera el perfil de calidad de una tabla: total de registros, nulos por columna
    (absoluto y porcentaje), duplicados de registro completo, estadísticas descriptivas
    de columnas numéricas (count, mean, std, min, percentiles, max) y cardinalidad
    de todas las columnas. Solo calcula estadísticas numéricas para columnas donde
    al menos el 50% de los valores son convertibles a número.
    Retorna un dict con todas las métricas.
    """
    total = len(df)
    nulls = df.isnull().sum()

    # Convertir columnas convertibles a numérico para estadísticas descriptivas
    num_cols = {}
    for col in df.columns:
        converted = pd.to_numeric(df[col], errors="coerce")
        if converted.notna().sum() > 0 and converted.notna().sum() >= len(df) * 0.5:
            num_cols[col] = converted
    if num_cols:
        num_df = pd.DataFrame(num_cols)
        stats = num_df.describe().round(2).to_dict()
    else:
        stats = "sin datos completamente numéricos"

    return {
        "tabla": tabla,
        "total_registros": total,
        "nulos": {
            col: {"absoluto": int(nulls[col]), "pct": round(nulls[col] / total * 100, 2)}
            for col in df.columns
        },
        "duplicados_registro_completo": int(df.duplicated().sum()),
        "estadisticas_numericas": stats,
        "cardinalidad_categoricas": {
            col: int(df[col].nunique())
            for col in df.columns
        },
    }


def _log_regla(tabla: str, campo: str, regla: str, afectados: int, accion: str) -> dict:
    """Construye un dict de hallazgo para el log de reglas de calidad.
    Incluye timestamp, tabla, campo, código de regla, cantidad de registros
    afectados y la acción correctiva aplicada.
    """
    return {
        "timestamp": datetime.now().isoformat(),
        "tabla": tabla,
        "campo": campo,
        "regla": regla,
        "registros_afectados": afectados,
        "accion": accion,
    }


def run_quality_rules(dfs: dict) -> list:
    """Evalúa las 8 reglas de calidad sobre los DataFrames crudos y retorna
    la lista de hallazgos para el log. Cada hallazgo indica cuántos registros
    incumplen la regla pero NO aplica correcciones (solo mide).
    Las correcciones de valor (nulificación) se aplican en la etapa de limpieza (clean_*).
    Ninguna regla elimina registros: los incumplimientos se marcan en la columna
    'regla_calidad' que se añade en clean_all().
      R1 — PK no nula en todas las tablas
      R2 — PK duplicada en clientes, pedidos y detalle_pedidos
      R3 — email con formato válido (debe contener '@' y dominio)
      R4 — total_neto > 0 en pedidos
      R5 — cantidad >= 1 en detalle_pedidos
      R6 — telefono no contiene letras a-z (case-insensitive)
      R7 — fecha_pedido no es futura
      R8 — precio_venta > 0 en productos
    """
    logs = []

    # R1 — PK no nula en todas las tablas
    for tabla, pk in [("clientes", "cliente_id"), ("pedidos", "pedido_id"),
                      ("detalle_pedidos", "item_id"), ("productos", "producto_id"),
                      ("eventos", "evento_id")]:
        n = int(dfs[tabla][pk].isnull().sum())
        logs.append(_log_regla(tabla, pk, "R1_pk_no_nula", n, "marcar en regla_calidad"))

    # R2 — PK duplicada (todas las ocurrencias de la PK que aparece más de una vez)
    for tabla, pk in [("clientes", "cliente_id"), ("pedidos", "pedido_id"),
                      ("detalle_pedidos", "item_id")]:
        n = int(dfs[tabla].duplicated(subset=[pk], keep=False).sum())
        logs.append(_log_regla(tabla, pk, "R2_pk_duplicada", n, "marcar en regla_calidad"))

    # R3 — Formato de email válido
    mask = dfs["clientes"]["email"].notna() & \
           ~dfs["clientes"]["email"].str.contains(r"@.*\.", na=False, regex=True)
    logs.append(_log_regla("clientes", "email", "R3_email_formato",
                            int(mask.sum()), "nulificar email inválido; marcar en regla_calidad"))

    # R4 — total_neto > 0
    tn = pd.to_numeric(dfs["pedidos"]["total_neto"], errors="coerce")
    logs.append(_log_regla("pedidos", "total_neto", "R4_total_neto_positivo",
                            int((tn <= 0).sum()), "marcar en regla_calidad"))

    # R5 — cantidad >= 1
    cant = pd.to_numeric(dfs["detalle_pedidos"]["cantidad"], errors="coerce")
    logs.append(_log_regla("detalle_pedidos", "cantidad", "R5_cantidad_positiva",
                            int((cant < 1).sum()), "marcar en regla_calidad"))

    # R6 — telefono no debe contener letras (a-z, case-insensitive)
    con_letra = dfs["clientes"]["telefono"].str.contains(r"[a-z]", case=False, na=False, regex=True)
    logs.append(_log_regla("clientes", "telefono", "R6_telefono_sin_letras", int(con_letra.sum()),
                            "nulificar telefono con letras; marcar en regla_calidad"))

    # R7 — fecha_pedido no futura
    fechas = pd.to_datetime(dfs["pedidos"]["fecha_pedido"], errors="coerce")
    n = int((fechas > pd.Timestamp.today()).sum())
    logs.append(_log_regla("pedidos", "fecha_pedido", "R7_fecha_no_futura", n,
                            "marcar en regla_calidad"))

    # R8 — precio_venta > 0
    pv = pd.to_numeric(dfs["productos"]["precio_venta"], errors="coerce")
    logs.append(_log_regla("productos", "precio_venta", "R8_precio_positivo",
                            int((pv <= 0).sum()), "marcar en regla_calidad"))

    return logs


def _flag_violations(df: pd.DataFrame, tabla: str) -> pd.Series:
    """Evalúa cada fila del DataFrame contra todas las reglas aplicables a la tabla
    y retorna una Series con las reglas incumplidas separadas por ' | '.
    Si una fila no incumple ninguna regla, el valor es cadena vacía ''.
    Esta Series se asigna como columna 'regla_calidad' antes de guardar y cargar a MySQL.
    Reglas evaluadas por tabla:
      clientes       — R1 (cliente_id nulo), R2 (cliente_id duplicado), R3 (email inválido), R6 (telefono con letras)
      productos      — R1 (producto_id nulo), R8 (precio_venta <= 0)
      pedidos        — R1 (pedido_id nulo), R2 (pedido_id duplicado), R4 (total_neto <= 0), R7 (fecha_pedido futura)
      detalle_pedidos— R1 (item_id nulo), R2 (item_id duplicado), R5 (cantidad < 1)
      eventos        — R1 (evento_id nulo)
    """
    flags = pd.Series([""] * len(df), index=df.index, dtype=str)

    def _add(mask: pd.Series, codigo: str) -> None:
        flags[mask] = flags[mask].apply(
            lambda v: f"{v} | {codigo}" if v else codigo
        )

    if tabla == "clientes":
        _add(df["cliente_id"].isnull(), "R1_pk_no_nula")
        _add(df.duplicated(subset=["cliente_id"], keep=False), "R2_pk_duplicada")
        invalid_email = df["email"].notna() & \
                        ~df["email"].str.contains(r"@.*\.", na=False, regex=True)
        _add(invalid_email, "R3_email_formato")
        _add(df["telefono"].str.contains(r"[a-z]", case=False, na=False, regex=True),
             "R6_telefono_sin_letras")

    elif tabla == "productos":
        _add(df["producto_id"].isnull(), "R1_pk_no_nula")
        pv = pd.to_numeric(df["precio_venta"], errors="coerce")
        _add(pv.isna() | (pv <= 0), "R8_precio_positivo")

    elif tabla == "pedidos":
        _add(df["pedido_id"].isnull(), "R1_pk_no_nula")
        _add(df.duplicated(subset=["pedido_id"], keep=False), "R2_pk_duplicada")
        tn = pd.to_numeric(df["total_neto"], errors="coerce")
        _add(tn.isna() | (tn <= 0), "R4_total_neto_positivo")
        fechas = pd.to_datetime(df["fecha_pedido"], errors="coerce")
        _add(fechas > pd.Timestamp.today(), "R7_fecha_no_futura")

    elif tabla == "detalle_pedidos":
        _add(df["item_id"].isnull(), "R1_pk_no_nula")
        _add(df.duplicated(subset=["item_id"], keep=False), "R2_pk_duplicada")
        cant = pd.to_numeric(df["cantidad"], errors="coerce")
        _add(cant.isna() | (cant < 1), "R5_cantidad_positiva")

    elif tabla == "eventos":
        _add(df["evento_id"].isnull(), "R1_pk_no_nula")

    return flags


# ---------------------------------------------
# 3. LIMPIEZA, ENMASCARAMIENTO PII Y RETENCIÓN
# ---------------------------------------------

def _sha256(valor: str) -> str:
    """Retorna el hash SHA-256 en hexadecimal del valor recibido.
    Usado para enmascarar campos PII (nombre, apellido, email).
    """
    return hashlib.sha256(str(valor).encode()).hexdigest()


def _mask_telefono(valor: str) -> str:
    """Enmascara un número de teléfono conservando solo los últimos 4 dígitos
    con el prefijo '****'. Si tiene menos de 4 dígitos retorna '****'.
    """
    digits = "".join(c for c in str(valor) if c.isdigit())
    return f"****{digits[-4:]}" if len(digits) >= 4 else "****"


def mask_pii(df: pd.DataFrame, tabla: str) -> pd.DataFrame:
    """Aplica enmascaramiento PII a las columnas definidas en PII_COLUMNS para la tabla dada.
    - nombre, apellido, email → hash SHA-256.
    - telefono → últimos 4 dígitos con prefijo '****'.
    Los valores nulos se conservan sin transformar. Retorna una copia del DataFrame.
    """
    df = df.copy()
    for col in PII_COLUMNS.get(tabla, []):
        if col not in df.columns:
            continue
        if col == "telefono":
            df[col] = df[col].apply(lambda v: _mask_telefono(v) if pd.notna(v) else v)
        else:
            df[col] = df[col].apply(lambda v: _sha256(v) if pd.notna(v) else v)
    return df


def apply_retencion(df: pd.DataFrame, tabla: str, col_fecha: str,
                    diccionario: pd.DataFrame) -> pd.DataFrame:
    """Evalúa la política de retención para la tabla y marca la columna 'aplica_retencion_dias'
    con 'si' para los registros cuya col_fecha sea anterior al límite (hoy − retencion_dias),
    y con 'no' para los que están dentro del período.
    La columna debe existir en el DataFrame antes de llamar esta función (inicializada en clean_all).
    No elimina ningún registro ni modifica 'regla_calidad'.
    Si no se encuentra retencion_dias para la tabla+columna, todos los registros quedan en 'no'.
    """
    dias = get_retencion_dias(diccionario, tabla, col_fecha)
    if dias is None:
        return df
    df = df.copy()
    fechas = pd.to_datetime(df[col_fecha], errors="coerce")
    limite = pd.Timestamp.today() - timedelta(days=dias)
    fuera = fechas < limite
    n_fuera = int(fuera.sum())
    df["aplica_retencion_dias"] = "no"
    df.loc[fuera, "aplica_retencion_dias"] = "si"
    if n_fuera > 0:
        log.info(
            f"  [Governance] {tabla}.{col_fecha}: {n_fuera} registros "
            f"fuera de retención ({dias} días, límite: {limite.date()}). "
            f"Marcados aplica_retencion_dias='si'."
        )
    return df


def clean_clientes(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica correcciones de valor a la tabla clientes. No elimina ninguna fila.
    - Nulifica email si no contiene '@' y dominio válido (R3).
    - Nulifica telefono si contiene letras a-z (R6, case-insensitive).
    - Enmascara campos PII: nombre, apellido, email con SHA-256; telefono con últimos 4 dígitos.
    La columna 'regla_calidad' es asignada por clean_all() después de esta función.
    """
    df = df.copy()
    df["email"] = df["email"].where(
        df["email"].str.contains(r"@.*\.", na=False, regex=True), other=pd.NA
    )
    df["telefono"] = df["telefono"].where(
        ~df["telefono"].str.contains(r"[a-z]", case=False, na=False, regex=True), other=pd.NA
    )
    return mask_pii(df, "clientes")


def clean_productos(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica correcciones de valor a la tabla productos. No elimina ninguna fila.
    - Convierte precio_venta a numérico (valores no convertibles quedan como NaN).
    La columna 'regla_calidad' es asignada por clean_all() después de esta función.
    """
    df = df.copy()
    df["precio_venta"] = pd.to_numeric(df["precio_venta"], errors="coerce")
    return df


def clean_pedidos(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica correcciones de valor a la tabla pedidos. No elimina ninguna fila.
    - Rellena estado nulo con 'desconocido'.
    - Convierte total_neto a numérico (valores no convertibles quedan como NaN).
    La columna 'regla_calidad' es asignada por clean_all() después de esta función.
    """
    df = df.copy()
    df["estado"] = df["estado"].fillna("desconocido")
    df["total_neto"] = pd.to_numeric(df["total_neto"], errors="coerce")
    return df


def clean_detalle_pedidos(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica correcciones de valor a la tabla detalle_pedidos. No elimina ninguna fila.
    - Convierte cantidad a numérico (valores no convertibles quedan como NaN).
    La columna 'regla_calidad' es asignada por clean_all() después de esta función.
    """
    df = df.copy()
    df["cantidad"] = pd.to_numeric(df["cantidad"], errors="coerce")
    return df


def clean_eventos(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica correcciones de valor a la tabla eventos. No elimina ninguna fila.
    No hay correcciones de valor aplicables a esta tabla; la función retorna una copia limpia.
    La columna 'regla_calidad' es asignada por clean_all() después de esta función.
    """
    return df.copy()


def clean_all(dfs: dict, diccionario: pd.DataFrame) -> dict:
    """Ejecuta la limpieza de todas las tablas y retorna un dict {nombre_tabla: DataFrame_limpio}.
    Proceso por tabla:
      1. Aplica correcciones de valor (nulificaciones, conversiones) via clean_*().
      2. Evalúa todas las reglas de calidad y construye la columna 'regla_calidad'
         con los códigos de reglas incumplidas separados por ' | ' (vacío si cumple todo).
      3. Aplica la política de retención añadiendo el código correspondiente en 'regla_calidad'.
    Ningún registro es eliminado en ningún paso.
    """
    cleaned = {
        "clientes":        clean_clientes(dfs["clientes"]),
        "productos":       clean_productos(dfs["productos"]),
        "pedidos":         clean_pedidos(dfs["pedidos"]),
        "detalle_pedidos": clean_detalle_pedidos(dfs["detalle_pedidos"]),
        "eventos":         clean_eventos(dfs["eventos"]),
    }

    # Asignar columna regla_calidad con flags de reglas incumplidas
    # e inicializar aplica_retencion_dias en 'no' para todas las tablas
    for tabla, df in cleaned.items():
        df["regla_calidad"] = _flag_violations(df, tabla)
        df["aplica_retencion_dias"] = "no"
        cleaned[tabla] = df

    # Aplicar política de retención: marca 'si'/'no' en aplica_retencion_dias
    cleaned["clientes"] = apply_retencion(cleaned["clientes"], "clientes", "fecha_registro", diccionario)
    cleaned["pedidos"]  = apply_retencion(cleaned["pedidos"],  "pedidos",  "fecha_pedido",   diccionario)
    cleaned["eventos"]  = apply_retencion(cleaned["eventos"],  "eventos",  "timestamp",       diccionario)

    # Convertir cadena vacía en regla_calidad a NULL (NaN) para CSV y MySQL
    # aplica_retencion_dias siempre tiene valor ('si' o 'no'), no se nulifica
    for df in cleaned.values():
        df["regla_calidad"] = df["regla_calidad"].replace("", pd.NA)

    return cleaned


def save_clean_csvs(dfs_clean: dict) -> None:
    """Guarda cada DataFrame limpio como CSV en PATHS['clean'] con el nombre {tabla}_clean.csv."""
    os.makedirs(PATHS["clean"], exist_ok=True)
    for tabla, df in dfs_clean.items():
        df.to_csv(os.path.join(PATHS["clean"], f"{tabla}_clean.csv"), index=False)


# ---------------------------------------------
# 4. CARGA EN MYSQL
# ---------------------------------------------

def get_engine() -> sqlalchemy.engine.Engine:
    url = (
        f"mysql+pymysql://{MYSQL['user']}:{MYSQL['password']}"
        f"@{MYSQL['host']}:{MYSQL['port']}/{MYSQL['database']}"
    )
    return sqlalchemy.create_engine(url)


def load_to_mysql(dfs_clean: dict, engine: sqlalchemy.engine.Engine) -> None:
    for tabla, df in dfs_clean.items():
        df.to_sql(tabla, con=engine, if_exists="replace", index=False, chunksize=500)
        log.info(f"  Tabla '{tabla}' cargada en MySQL ({len(df)} filas)")


# ---------------------------------------------
# 5. QUERIES SQL Y EXPORTACIÓN
# ---------------------------------------------

def _load_queries_from_file(sql_path: str) -> list:
    """
    Lee queries.sql y retorna lista de (nombre, sql).
    Cada query ocupa un bloque separado por línea en blanco.
    El nombre del CSV se extrae del comentario -- entre 'permite' y ', el impacto'.
    Si no se encuentra ese patrón, se usa el índice como fallback.
    """
    import re

    with open(sql_path, encoding="utf-8") as f:
        content = f.read()

    queries = []
    for i, block in enumerate(content.split("\n\n"), start=1):
        block = block.strip()
        if not block:
            continue

        # Extraer el comentario inline completo (-- ...)
        comment = ""
        for line in block.splitlines():
            idx = line.find("--")
            if idx != -1:
                comment = line[idx + 2:].strip()
                break

        # Nombre: desde 'permite' hasta ', el impacto' (case-insensitive)
        match = re.search(r"permite(.+?),\s*el impacto", comment, re.IGNORECASE)
        if match:
            nombre = f"permite {match.group(1).strip()}"
        else:
            nombre = f"q{i:02d}"

        # Eliminar comentarios inline antes de ejecutar
        sql_clean = " ".join(
            ln.split("--")[0].rstrip() for ln in block.splitlines()
        ).strip()
        queries.append((nombre, sql_clean))
    return queries


def run_queries_and_export(engine: sqlalchemy.engine.Engine) -> None:
    sql_path = os.path.join(BASE_DIR, "queries.sql")
    queries = _load_queries_from_file(sql_path)
    os.makedirs(PATHS["outputs"], exist_ok=True)
    with engine.connect() as conn:
        for nombre, sql in queries:
            df_result = pd.read_sql(sqlalchemy.text(sql), conn)
            df_result.to_csv(os.path.join(PATHS["outputs"], f"{nombre}.csv"), index=False)
            log.info(f"  Query '{nombre}' exportada ({len(df_result)} filas)")


# ---------------------------------------------
# 6. LOGS Y REPORTE DE CALIDAD
# ---------------------------------------------

def save_quality_log(quality_logs: list) -> None:
    os.makedirs(PATHS["outputs"], exist_ok=True)
    pd.DataFrame(quality_logs).to_csv(
        os.path.join(PATHS["outputs"], "log_transformaciones.csv"), index=False
    )


def save_quality_report(profiles: dict, quality_logs: list) -> None:
    os.makedirs(PATHS["outputs"], exist_ok=True)
    lines = [
        "# Reporte de Calidad de Datos\n",
        f"**Generado:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n",
    ]

    # Índice de duplicados PK por tabla extraído de quality_logs (R2)
    dup_pk = {}
    for r in quality_logs:
        if r["regla"] == "R2_pk_unica":
            dup_pk[r["tabla"]] = {"campo": r["campo"], "cantidad": r["registros_afectados"]}

    for tabla, p in profiles.items():
        lines.append(f"## {tabla.upper()}\n")
        lines.append(f"- Registros totales: {p['total_registros']}\n")

        # Duplicados a nivel de registro completo
        lines.append(f"- Duplicados (registro completo): {p['duplicados_registro_completo']}\n")

        # Duplicados a nivel de PK
        if tabla in dup_pk:
            pk_info = dup_pk[tabla]
            lines.append(
                f"- Duplicados (PK `{pk_info['campo']}`): {pk_info['cantidad']}\n"
            )

        # Nulos por columna (absoluto y porcentaje)
        nulos = {c: v for c, v in p["nulos"].items() if v["absoluto"] > 0}
        if nulos:
            lines.append("- **Nulos por columna:**\n")
            for col, v in nulos.items():
                lines.append(f"  - `{col}`: {v['absoluto']} ({v['pct']}%)\n")

        # Estadísticas descriptivas de columnas numéricas
        stats = p.get("estadisticas_numericas")
        if isinstance(stats, dict):
            lines.append("- **Estadísticas numéricas:**\n\n")
            cols_num = list(stats.keys())
            metricas = ["count", "mean", "std", "min", "25%", "50%", "75%", "max"]
            header = "| métrica | " + " | ".join(cols_num) + " |\n"
            sep    = "|---|" + "---|" * len(cols_num) + "\n"
            lines.append(header)
            lines.append(sep)
            for m in metricas:
                fila = f"| {m} | "
                fila += " | ".join(str(stats[c].get(m, "—")) for c in cols_num)
                fila += " |\n"
                lines.append(fila)
            lines.append("\n")

        # Cardinalidad de columnas
        card = p.get("cardinalidad_categoricas")
        if card:
            lines.append("- **Cardinalidad de columnas:**\n\n")
            lines.append("| columna | valores únicos |\n|---|---|\n")
            for col, n in card.items():
                lines.append(f"| `{col}` | {n} |\n")
            lines.append("\n")

        lines.append("\n")

    lines += [
        "## Resumen de Reglas de Calidad\n\n",
        "| Tabla | Campo | Regla | Afectados | Acción |\n",
        "|---|---|---|---|---|\n",
    ]
    for r in quality_logs:
        lines.append(
            f"| {r['tabla']} | {r['campo']} | {r['regla']} "
            f"| {r['registros_afectados']} | {r['accion']} |\n"
        )

    lines += [
        "\n## Clasificación de Sensibilidad por Tabla\n\n",
        "| Tabla | Clasificación |\n|---|---|\n",
    ]
    for tabla, clasif in TABLA_CLASIFICACION.items():
        lines.append(f"| {tabla} | {clasif} |\n")

    lines += ["\n## Columnas PII Identificadas\n\n"]
    for tabla, cols in PII_COLUMNS.items():
        lines.append(f"- **{tabla}:** {', '.join(cols)}\n")

    lines += [
        "\n## Linaje de Datos\n\n",
        "| Campo destino | Origen | Transformación |\n|---|---|---|\n",
    ]
    for campo, info in LINAJE.items():
        lines.append(f"| {campo} | {info['origen']} | {info['transformacion']} |\n")

    content = "".join(lines)
    with open(os.path.join(PATHS["outputs"], "reporte_calidad.md"), "w", encoding="utf-8") as f:
        f.write(content)


def save_execution_log(stage_times: list) -> None:
    os.makedirs(PATHS["outputs"], exist_ok=True)
    pd.DataFrame(stage_times).to_csv(
        os.path.join(PATHS["outputs"], "log_ejecucion.csv"), index=False
    )

# ---------------------------------------------
# ORQUESTADOR PRINCIPAL
# ---------------------------------------------

def run_pipeline() -> None:
    stage_times = []

    def run_stage(name: str, fn, *args, **kwargs):
        log.info(f"Iniciando: {name}")
        t0 = time.time()
        result = fn(*args, **kwargs)
        elapsed = round(time.time() - t0, 3)
        stage_times.append({
            "etapa": name,
            "duracion_seg": elapsed,
            "timestamp": datetime.now().isoformat(),
        })
        log.info(f"  OK: {name} ({elapsed}s)")
        return result

    # Etapa 1 — Lectura
    dfs_raw    = run_stage("1_lectura_csvs_crudos", load_all_raw)
    diccionario = load_diccionario()
    
    # Etapa 2 — Calidad
    profiles     = run_stage("2a_perfilado", lambda: {t: profile_table(dfs_raw[t], t) for t in TABLAS})
    quality_logs = run_stage("2b_reglas_calidad", run_quality_rules, dfs_raw)

    # Etapa 3 — Limpieza y PII
    dfs_clean = run_stage("3a_limpieza_y_pii", clean_all, dfs_raw, diccionario)
    run_stage("3b_guardar_clean_csvs", save_clean_csvs, dfs_clean)
    
    # Etapa 4 — MySQL
    engine = get_engine()
    run_stage("4_carga_mysql", load_to_mysql, dfs_clean, engine)

    # Etapa 5 — Queries
    run_stage("5_queries_y_exportacion", run_queries_and_export, engine)

    # Etapa 6 — Logs y reporte
    run_stage("6a_log_transformaciones", save_quality_log, quality_logs)
    run_stage("6b_reporte_calidad", save_quality_report, profiles, quality_logs)
    run_stage("6c_log_ejecucion", save_execution_log, stage_times)

    log.info("Pipeline finalizado correctamente.")


if __name__ == "__main__":
    run_pipeline()
