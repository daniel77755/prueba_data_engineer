"""
agente.py — RetailTech S.A.S | Agente Conversacional GenAI
===========================================================
Arquitectura:
  - Framework : LangChain + Google Gemini (gemini-flash-latest, free tier)
  - Patrón    : ReAct (Reasoning + Acting) — razonamiento paso a paso visible
  - Interfaz  : Streamlit (UI web ligera, gratuita)
  - BD        : MySQL (misma instancia que pipeline.py)

Herramientas disponibles para el agente:
  1. ejecutar_sql            — consulta MySQL usando queries de queries.sql
  2. obtener_esquema         — describe columnas de una tabla desde diccionario_datos.csv
  3. resumir_reporte_calidad — retorna resumen del reporte_calidad.md

Control de acceso por rol (simulado):
  - analista   : queries operativas y de clientes; sin acceso a columnas financieras
  - finanzas   : acceso completo incluyendo costos, totales y métodos de pago
  - operaciones: queries logísticas y de inventario; sin acceso a datos financieros

Privacidad:
  - Toda respuesta pasa por validador PII antes de mostrarse al usuario.
  - Si se detecta PII, la respuesta es bloqueada y reemplazada por mensaje seguro.
"""

import os
import re
import sqlalchemy
import pandas as pd
import streamlit as st

from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.tools import tool
from langchain.agents import create_agent

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURACIÓN
# ══════════════════════════════════════════════════════════════════════════════

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

MYSQL = {
    "host":     "localhost",
    "port":     3306,
    "user":     "root",
    "password": "datatest2025",
    "database": "retailtech",
}

PATHS = {
    "diccionario":     os.path.join(BASE_DIR, "data", "diccionario_datos.csv"),
    "reporte_calidad": os.path.join(BASE_DIR, "outputs", "reporte_calidad.md"),
    "queries_sql":     os.path.join(BASE_DIR, "queries.sql"),
}

# Columnas PII que nunca deben aparecer en respuestas
PII_FIELDS = ["email", "nombre", "apellido", "telefono", "teléfono"]

# Patrón regex para detectar PII en texto libre (email y teléfono)
PII_PATTERN = re.compile(
    r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+"   # email
    r"|\+?\d[\d\s\-().]{7,}\d",                            # teléfono
    re.IGNORECASE,
)

# ══════════════════════════════════════════════════════════════════════════════
# CONTROL DE ACCESO POR ROL
# ══════════════════════════════════════════════════════════════════════════════

# Columnas financieras/confidenciales bloqueadas por rol
COLUMNAS_BLOQUEADAS_POR_ROL: dict[str, list[str]] = {
    "analista": [
        "costo", "total_neto", "total_bruto", "precio_unitario",
        "subtotal", "metodo_pago",
    ],
    "finanzas": [],   # sin restricciones adicionales
    "operaciones": [
        "costo", "total_neto", "total_bruto", "precio_unitario",
        "subtotal", "metodo_pago",
    ],
}

# Queries permitidas por rol (índices del catálogo q0..q9)
# q0  — total clientes
# q1  — clientes B2B > $500k
# q2  — canal mejor conversión
# q3  — top 5 productos sem2 2024
# q4  — clientes por país
# q5  — ventas Colombia vs México  ← solo finanzas
# q6  — pedidos pendientes/enviados
# q7  — producto más devuelto 2023
# q8  — tiempo promedio entrega por país
# q9  — productos activos/inactivos
QUERIES_POR_ROL: dict[str, list[str]] = {
    "analista":    ["q0", "q1", "q2", "q3", "q4", "q6", "q7", "q8", "q9"],
    "finanzas":    ["q0", "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9"],
    "operaciones": ["q2", "q6", "q7", "q8", "q9"],
}

# Descripción visible del rol para la UI
ROL_DESCRIPCION: dict[str, str] = {
    "analista":    "Acceso a métricas de clientes, productos y operaciones. Sin datos financieros.",
    "finanzas":    "Acceso completo incluyendo ventas, costos y métodos de pago.",
    "operaciones": "Acceso a logística, inventario y tiempos de entrega. Sin datos financieros.",
}


# Variable de módulo — única fuente de verdad para las tools @tool.
# Se sincroniza con session_state al inicio de cada render en run_app().
_rol_activo: str = "analista"


def set_rol(rol: str) -> None:
    """Actualiza el rol en la variable de módulo y en session_state."""
    global _rol_activo
    _rol_activo = rol
    if hasattr(st, "session_state"):
        st.session_state["rol"] = rol


def sync_rol() -> None:
    """Sincroniza _rol_activo desde session_state al inicio de cada render.
    Necesario porque st.rerun() re-ejecuta el módulo y resetea variables de módulo.
    """
    global _rol_activo
    _rol_activo = st.session_state.get("rol", "analista")


def get_rol() -> str:
    """Retorna el rol activo desde la variable de módulo."""
    return _rol_activo


def queries_permitidas() -> list[str]:
    return QUERIES_POR_ROL.get(get_rol(), [])


def columnas_bloqueadas() -> list[str]:
    return COLUMNAS_BLOQUEADAS_POR_ROL.get(get_rol(), [])


# ══════════════════════════════════════════════════════════════════════════════
# UTILIDADES
# ══════════════════════════════════════════════════════════════════════════════

def get_engine() -> sqlalchemy.engine.Engine:
    url = (
        f"mysql+pymysql://{MYSQL['user']}:{MYSQL['password']}"
        f"@{MYSQL['host']}:{MYSQL['port']}/{MYSQL['database']}"
    )
    return sqlalchemy.create_engine(url)


def load_queries_sql() -> dict[str, dict]:
    """
    Lee queries.sql y retorna dict {q0..qN: {sql, descripcion}}.
    Soporta queries multilínea: acumula líneas hasta encontrar ';'.
    El comentario -- al final de la línea con ';' se usa como descripción.
    """
    path = PATHS["queries_sql"]
    if not os.path.exists(path):
        return {}

    resultado = {}
    i = 0
    buffer = []
    descripcion = None

    with open(path, encoding="utf-8") as f:
        for linea in f:
            linea_strip = linea.strip()
            if not linea_strip or linea_strip.startswith("--"):
                continue

            if "--" in linea_strip:
                partes = linea_strip.split("--", 1)
                linea_strip = partes[0].strip()
                descripcion = partes[1].strip()

            buffer.append(linea_strip.rstrip(";").strip())

            if ";" in linea.rstrip():
                sql = " ".join(p for p in buffer if p).strip()
                if sql:
                    resultado[f"q{i}"] = {
                        "sql": sql,
                        "descripcion": descripcion or f"query {i}",
                    }
                    i += 1
                buffer = []
                descripcion = None

    return resultado


def validate_pii(text: str) -> str:
    """Bloquea la respuesta si contiene datos PII detectables."""
    if PII_PATTERN.search(text):
        return (
            "⚠️ Respuesta bloqueada: se detectaron datos PII (email o teléfono). "
            "El agente no puede exponer información personal identificable."
        )
    return text


# ══════════════════════════════════════════════════════════════════════════════
# TOOLS DEL AGENTE
# ══════════════════════════════════════════════════════════════════════════════

@tool
def ejecutar_sql(clave: str) -> str:
    """
    Ejecuta una query predefinida de queries.sql usando su clave (q0, q1, q2...).
    Recibe la clave del catálogo, no SQL directo. Ejemplo: ejecutar_sql('q0').
    """
    catalogo = load_queries_sql()
    permitidas = queries_permitidas()
    bloqueadas = columnas_bloqueadas() + PII_FIELDS

    if clave not in catalogo:
        claves_disponibles = ", ".join(k for k in catalogo if k in permitidas)
        return f"Clave '{clave}' no existe. Claves disponibles para tu rol: {claves_disponibles}"

    # Verificar que la query está permitida para el rol activo
    if clave not in permitidas:
        return (
            f"Acceso denegado: la query '{clave}' no está disponible para el rol '{get_rol()}'. "
            f"Consultas permitidas: {', '.join(permitidas)}"
        )

    sql = catalogo[clave]["sql"]

    # Bloquear columnas PII y columnas restringidas por rol
    for campo in bloqueadas:
        if re.search(rf"\bselect\b.*\b{campo}\b", sql.lower()):
            return (
                f"Acceso denegado: la consulta incluye el campo '{campo}', "
                f"que no está disponible para el rol '{get_rol()}'."
            )

    try:
        engine = get_engine()
        with engine.connect() as conn:
            df = pd.read_sql(sqlalchemy.text(sql), conn)
        if df.empty:
            return "La consulta no retornó resultados."
        return df.to_markdown(index=False)
    except Exception as e:
        return f"Error al ejecutar la consulta: {e}"


@tool
def obtener_esquema(tabla: str) -> str:
    """
    Retorna el esquema y descripción de las columnas de una tabla específica
    a partir del diccionario de datos oficial. Incluye tipo, descripción,
    clasificación y si es PII. Las columnas bloqueadas para el rol activo
    se marcan como restringidas.
    """
    path = PATHS["diccionario"]
    if not os.path.exists(path):
        return "No se encontró el diccionario de datos."

    dic = pd.read_csv(path)
    resultado = dic[dic["tabla"] == tabla.lower()].copy()
    if resultado.empty:
        return f"No se encontró información para la tabla '{tabla}'."

    bloqueadas = columnas_bloqueadas() + PII_FIELDS
    resultado["acceso_rol"] = resultado["columna"].apply(
        lambda c: f"RESTRINGIDO ({get_rol()})" if c.lower() in bloqueadas else "permitido"
    )

    cols = ["columna", "tipo_dato", "descripcion", "es_pii", "clasificacion", "acceso_rol"]
    return resultado[cols].to_markdown(index=False)


@tool
def resumir_reporte_calidad() -> str:
    """
    Retorna el resumen ejecutivo del reporte de calidad de datos generado
    por el pipeline. Incluye hallazgos, reglas aplicadas y clasificación
    de sensibilidad por tabla.
    """
    path = PATHS["reporte_calidad"]
    if not os.path.exists(path):
        return "No se encontró el reporte de calidad. Ejecuta primero pipeline.py."

    with open(path, encoding="utf-8") as f:
        contenido = f.read()

    secciones = contenido.split("---")
    resumen = "---".join(secciones[:3]) if len(secciones) >= 3 else contenido
    return resumen[:2000]


# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURACIÓN DEL AGENTE REACT
# ══════════════════════════════════════════════════════════════════════════════

def build_system_prompt() -> str:
    """Construye el system prompt con el catálogo filtrado por rol activo."""
    rol = get_rol()
    permitidas = queries_permitidas()
    bloqueadas_col = columnas_bloqueadas()

    queries = load_queries_sql()
    catalogo = "\n".join(
        f"  {k}: {v['descripcion']}"
        for k, v in queries.items()
        if k in permitidas
    ) if queries else "  (sin queries disponibles)"

    restriccion_col = (
        f"- Columnas bloqueadas para tu rol ({rol}): {', '.join(bloqueadas_col)}. "
        "No uses ni menciones estas columnas en ninguna respuesta."
        if bloqueadas_col else
        "- Tienes acceso completo a todas las columnas."
    )

    return f"""
        Eres un asistente de datos para RetailTech S.A.S, empresa de e-commerce en Latinoamérica.
        Ayudas al equipo de negocio a consultar datos de ventas, clientes, productos y eventos digitales.

        ROL ACTIVO: {rol.upper()}
        {ROL_DESCRIPCION[rol]}

        REGLAS ESTRICTAS:
        - Nunca inventes datos ni generes SQL propio — usa ÚNICAMENTE las queries del catálogo de abajo.
        - Para preguntas sobre datos usa la herramienta 'ejecutar_sql' con la clave correspondiente.
        - Para preguntas sobre estructura de tablas usa 'obtener_esquema'.
        - Para preguntas sobre calidad de datos usa 'resumir_reporte_calidad'.
        - Si ninguna query del catálogo responde la pregunta, responde exactamente:
          "No tengo información relacionada para responder esa pregunta."
        - Nunca expongas datos PII: emails, teléfonos, nombres completos, apellidos.
        {restriccion_col}
        - Responde siempre en español, de forma clara y concisa.

        QUERIES DISPONIBLES PARA TU ROL (solo puedes usar estas):
    {catalogo}
    """


GEMINI_MODELS = [
    "gemini-3.1-flash-lite-preview",
    "gemini-2.5-flash",
    "gemini-2.5-flash-lite",
]
GOOGLE_API_KEY = "AIzaSyDeVqR63Dvz5PtQJ6xKkiNLqEYhfjS0LxE"


def build_agent():
    last_error = None
    for model_name in GEMINI_MODELS:
        try:
            llm = ChatGoogleGenerativeAI(
                model=model_name,
                google_api_key=GOOGLE_API_KEY,
                temperature=0,
            )
            tools = [ejecutar_sql, obtener_esquema, resumir_reporte_calidad]
            agent = create_agent(model=llm, tools=tools, system_prompt=build_system_prompt())
            st.session_state["modelo_activo"] = model_name
            return agent
        except Exception as e:
            last_error = e
            continue
    raise RuntimeError(f"Ningún modelo disponible. Último error: {last_error}")


# ══════════════════════════════════════════════════════════════════════════════
# INTERFAZ STREAMLIT
# ══════════════════════════════════════════════════════════════════════════════

ROL_BADGE: dict[str, str] = {
    "analista":    "🔵 Analista",
    "finanzas":    "🟢 Finanzas",
    "operaciones": "🟠 Operaciones",
}


def render_sidebar() -> None:
    """Sidebar con selector de rol y tabla de permisos."""
    with st.sidebar:
        st.header("Control de acceso")

        rol_anterior = get_rol()
        rol_nuevo = st.selectbox(
            "Rol activo",
            options=["analista", "finanzas", "operaciones"],
            index=["analista", "finanzas", "operaciones"].index(rol_anterior),
            format_func=lambda r: ROL_BADGE[r],
        )

        # Si el rol cambia, actualizar variable de módulo, resetear agente e historial
        if rol_nuevo != rol_anterior:
            set_rol(rol_nuevo)
            st.session_state.agent = build_agent()
            st.session_state.messages = []
            st.rerun()

        set_rol(rol_nuevo)
        st.caption(ROL_DESCRIPCION[rol_nuevo])

        modelo = st.session_state.get("modelo_activo", "—")
        st.caption(f"Modelo: `{modelo}`")

        st.divider()
        st.subheader("Permisos del rol")

        permitidas = QUERIES_POR_ROL[rol_nuevo]
        bloqueadas_col = COLUMNAS_BLOQUEADAS_POR_ROL[rol_nuevo]

        st.markdown(
            "**Columnas bloqueadas:** "
            + (", ".join(f"`{c}`" for c in bloqueadas_col) if bloqueadas_col else "ninguna")
        )



def run_app() -> None:
    st.set_page_config(
        page_title="RetailTech — Agente de Datos",
        page_icon="📊",
        layout="wide",
    )

    # Restaurar _rol_activo desde session_state tras cada st.rerun()
    sync_rol()

    render_sidebar()

    rol = get_rol()
    st.title(f"📊 RetailTech — Agente Conversacional de Datos")
    st.caption(f"Rol activo: {ROL_BADGE[rol]} · {ROL_DESCRIPCION[rol]}")

    # Inicializar estado
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "agent" not in st.session_state:
        st.session_state.agent = build_agent()

    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    pregunta = st.chat_input("¿Cuáles fueron los 5 productos más vendidos?")
    if not pregunta:
        return

    st.session_state.messages.append({"role": "user", "content": pregunta})
    with st.chat_message("user"):
        st.markdown(pregunta)

    with st.chat_message("assistant"):
        razonamiento = st.expander("🧠 Razonamiento ReAct (paso a paso)", expanded=True)
        try:
            texto = ""
            for chunk in st.session_state.agent.stream(
                {"messages": [{"role": "user", "content": pregunta}]},
                stream_mode="updates",
            ):
                for _, estado in chunk.items():
                    for msg in estado.get("messages", []):
                        tool_calls = getattr(msg, "tool_calls", None) or []
                        msg_name   = getattr(msg, "name", None)
                        content    = getattr(msg, "content", None)

                        if tool_calls:
                            # Thought — el modelo decide qué herramienta usar
                            for tc in tool_calls:
                                razonamiento.markdown(
                                    f"**🔍 Thought** → usando tool `{tc['name']}`  \n"
                                    f"```\n{tc['args']}\n```"
                                )
                        elif msg_name:
                            # Observation — resultado de la herramienta
                            razonamiento.markdown(
                                f"**📋 Observation** (`{msg_name}`)  \n"
                                f"```\n{str(content)[:800]}\n```"
                            )
                        elif content and not tool_calls:
                            # Final Answer — respuesta final del modelo
                            if isinstance(content, str):
                                candidato = content.strip()
                            elif isinstance(content, list):
                                partes = []
                                for p in content:
                                    if isinstance(p, dict):
                                        partes.append(p.get("text") or p.get("content") or "")
                                    else:
                                        partes.append(str(p))
                                candidato = " ".join(partes).strip()
                            else:
                                candidato = str(content).strip()

                            if candidato:
                                texto = candidato

            texto = validate_pii(texto) if texto else "No se obtuvo respuesta."
        except Exception as e:
            texto = f"Error del agente: {e}"

        st.markdown(texto)

    st.session_state.messages.append({"role": "assistant", "content": texto})


if __name__ == "__main__":
    run_app()
