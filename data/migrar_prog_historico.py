"""
migrar_prog_historico.py
========================
Lee data/prog_historico_consolidado.json.gz desde GitHub y carga
a la tabla `programacion` en Azure SQL.

Mapea snake_case (JSON) → PascalCase (SQL).
Combina fecha + hora en datetime2 unificado.
Ignora campos que vienen de API o se descartan por convención.

Variables de entorno requeridas:
  TOKEN_REPO    — Personal Access Token GitHub
  SQL_SERVER    — autransp-server.database.windows.net
  SQL_DATABASE  — autransp-db
  SQL_USER      — autransp_admin
  SQL_PASSWORD  — contraseña de la BD

Ejecutar UNA SOLA VEZ tras correr la Pieza 1 (DDL).
"""

import os
import json
import gzip
import math
import requests
from datetime import datetime, time
from collections import Counter

# ============================================================
# CONFIGURACIÓN
# ============================================================

GITHUB_TOKEN   = os.environ.get("TOKEN_REPO", "")
GITHUB_USUARIO = "alexanderortega2000-glitch"
GITHUB_REPO    = "AuTransp"

SQL_SERVER   = os.environ.get("SQL_SERVER",   "autransp-server.database.windows.net")
SQL_DATABASE = os.environ.get("SQL_DATABASE", "autransp-db")
SQL_USER     = os.environ.get("SQL_USER",     "autransp_admin")
SQL_PASSWORD = os.environ.get("SQL_PASSWORD", "")


def get_conn():
    import pyodbc
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USER};"
        f"PWD={SQL_PASSWORD};"
        f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str)


def descargar_json_gz(ruta_repo: str) -> list:
    url = f"https://raw.githubusercontent.com/{GITHUB_USUARIO}/{GITHUB_REPO}/main/{ruta_repo}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}
    resp = requests.get(url, headers=headers, timeout=120)
    resp.raise_for_status()
    return json.loads(gzip.decompress(resp.content).decode("utf-8"))


# ============================================================
# UTILIDADES DE LIMPIEZA
# ============================================================

def limpiar(v):
    """Convierte ruido (None, NaN, '0', '', strings vacíos) a None
    para campos de TEXTO. Para texto, '0' suele ser placeholder."""
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    s = str(v).strip()
    if s in ("", "nan", "NaN", "None", "NaT", "0"):
        return None
    return s


def texto_libre(v):
    """Como limpiar pero conserva '0' por si era un comentario real
    que decía '0'. Usar solo en campos de texto libre."""
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    s = str(v).strip()
    if s in ("", "nan", "NaN", "None", "NaT"):
        return None
    return s


def num(v):
    """A número decimal. Si no se puede, None."""
    if v is None:
        return None
    s = str(v).strip()
    if s in ("", "nan", "NaN", "None", "NaT"):
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def entero(v):
    f = num(v)
    return int(f) if f is not None else None


def parse_fecha_sola(v):
    """Parsea YYYY-MM-DD u otros formatos a datetime (a 00:00:00)."""
    if not v:
        return None
    s = str(v).strip()
    if s in ("", "nan", "NaN", "None", "NaT", "0"):
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S",
                "%d/%m/%Y", "%d/%m/%Y %H:%M:%S"):
        try:
            return datetime.strptime(s[:19], fmt)
        except ValueError:
            continue
    return None


def combinar_fecha_hora(fecha_str, hora_str):
    """Combina 'YYYY-MM-DD' + 'HH:MM:SS' en datetime.
    Si solo viene fecha, hora=00:00:00. Si solo viene hora sin fecha, None."""
    fecha = parse_fecha_sola(fecha_str)
    if not fecha:
        return None
    if not hora_str:
        return fecha
    s = str(hora_str).strip()
    if s in ("", "nan", "NaN", "None", "NaT", "0"):
        return fecha
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            t = datetime.strptime(s, fmt).time()
            return fecha.replace(hour=t.hour, minute=t.minute, second=t.second)
        except ValueError:
            continue
    return fecha  # si no se puede parsear hora, mantenemos la fecha sola


def fundir_comentarios(*partes):
    """Funde 'comentarios' + 'comentario' en un solo string con separador.
    Evita duplicados literales."""
    vistos = []
    for p in partes:
        v = texto_libre(p)
        if v and v not in vistos and v not in ("0", "N/A"):
            vistos.append(v)
    return " | ".join(vistos) if vistos else None


# ============================================================
# UPSERT — programacion
# ============================================================

# Lista de columnas en orden (sin las que ya existen como "originales"
# que podrían tener valores: ST, FechaActualizacion, ActualizadoPor)
COLS = [
    "ST",
    # originales
    "Coordinador", "ProveedorTransp", "Subflota", "FechaEjecucion",
    "TipoProg", "NombreOrigen", "NombreDestino", "LoteDestino",
    "Zona", "Area", "Turno", "ActualizadoPor",
    # nuevas (Pieza 1)
    "SemanaPrograma", "FechaSolicitud",
    "Jefe", "RespProd",
    "CodActividad", "Actividad", "Proveedor", "Sociedad", "OS",
    "CodRecursoCarga", "NombreRecursoCarga", "UMRecursoCarga", "Implemento",
    "TipoAplicacion", "RecursoEjecucion",
    "CantidadProgramada", "AreaProgramada", "DosisProgramada",
    "Sacos", "CargadoresProg",
    "CodRecursoServ", "NombreRecursoServ", "CodProveedorTransp",
    "CodOrigen", "CodDestino",
    "CodRecepCosto", "NombreRecepCosto", "LoteRecepCosto",
    "Comentarios",
    "FechaHoraRecepcion", "FechaHoraPlanInicio", "FechaHoraPlanLlegada",
    "TipoGrupo", "Grupo", "EncargadoGrupo", "Contacto", "CodMaquina",
    "Producto", "UM",
    "Controlador", "MotivoRetrasoInicio", "MotivoRetrasoEntrega",
    "MotivoEstatusViaje", "DocumentoTraslado",
    "MotivoDevolucion", "CantidadDevolucion", "BultosDevolucion",
    "NotaDevolucion", "ComentarioCabina", "ComentarioIntegracion",
]

# Construye SQL dinámicamente para no errar manualmente
_set_clause = ", ".join(f"{c}=?" for c in COLS if c != "ST") + ", FechaActualizacion=GETUTCDATE()"
_insert_cols = ", ".join(COLS) + ", FechaActualizacion"
_insert_vals = ", ".join("?" for _ in COLS) + ", GETUTCDATE()"

UPSERT_SQL = f"""
MERGE programacion AS target
USING (SELECT ? AS ST) AS source ON target.ST = source.ST
WHEN MATCHED THEN UPDATE SET {_set_clause}
WHEN NOT MATCHED THEN INSERT ({_insert_cols}) VALUES ({_insert_vals});
"""


def mapear(r: dict) -> tuple:
    """Mapea un registro JSON a una tupla de valores en el orden de COLS,
    duplicado para el MERGE (USING + INSERT)."""

    st = limpiar(r.get("st"))
    if not st:
        return None  # registros sin ST se descartan

    # Coordinador: prefiere `coordinador` sobre `coordinador_archivo`
    coordinador = limpiar(r.get("coordinador")) or limpiar(r.get("coordinador_archivo"))

    # NombreDestino: el JSON usa `nom_destino`
    nombre_destino = limpiar(r.get("nom_destino"))
    nombre_origen  = limpiar(r.get("nomb_origen"))

    # Comentarios: fusiona `comentarios` + `comentario`
    comentarios = fundir_comentarios(r.get("comentarios"), r.get("comentario"))

    valores = [
        st,                                              # ST
        coordinador,                                     # Coordinador
        limpiar(r.get("proveedor_transp")),              # ProveedorTransp
        limpiar(r.get("subflota")),                      # Subflota
        parse_fecha_sola(r.get("fecha_ejecucion")),      # FechaEjecucion
        limpiar(r.get("tipo_prog")),                     # TipoProg
        nombre_origen,                                   # NombreOrigen
        nombre_destino,                                  # NombreDestino
        limpiar(r.get("lote_destino")),                  # LoteDestino
        limpiar(r.get("zona")),                          # Zona
        limpiar(r.get("area_programada")),               # Area (campo original; texto)
        limpiar(r.get("turno")),                         # Turno
        "migracion_inicial",                             # ActualizadoPor
        # === nuevas ===
        parse_fecha_sola(r.get("semana_programa")),      # SemanaPrograma
        parse_fecha_sola(r.get("fecha_solicitud")),      # FechaSolicitud
        limpiar(r.get("jefe")),                          # Jefe
        limpiar(r.get("resp_prod")),                     # RespProd
        limpiar(r.get("cod_actividad")),                 # CodActividad
        limpiar(r.get("actividad")),                     # Actividad
        limpiar(r.get("proveedor")),                     # Proveedor
        limpiar(r.get("sociedad")),                      # Sociedad
        limpiar(r.get("os")),                            # OS
        limpiar(r.get("cod_recurso_carga")),             # CodRecursoCarga
        limpiar(r.get("nomb_recurso_carga")),            # NombreRecursoCarga
        limpiar(r.get("um_recurso_carga")),              # UMRecursoCarga
        limpiar(r.get("implemento")),                    # Implemento
        limpiar(r.get("tipo_aplicacion")),               # TipoAplicacion
        limpiar(r.get("recurso_ejecucion")),             # RecursoEjecucion
        num(r.get("cantidad_programada")),               # CantidadProgramada
        num(r.get("area_programada")),                   # AreaProgramada (numérico)
        num(r.get("dosis_programada")),                  # DosisProgramada
        num(r.get("sacos")),                             # Sacos
        entero(r.get("cargadores")),                     # CargadoresProg
        limpiar(r.get("cod_recurso_serv")),              # CodRecursoServ
        limpiar(r.get("nomb_recurso_serv")),             # NombreRecursoServ
        limpiar(r.get("cod_proveedor_transp")),          # CodProveedorTransp
        limpiar(r.get("cod_origen")),                    # CodOrigen
        limpiar(r.get("cod_destino")),                   # CodDestino
        limpiar(r.get("cod_recep_costo")),               # CodRecepCosto
        limpiar(r.get("nomb_recep_costo")),              # NombreRecepCosto
        limpiar(r.get("lote_recep_costo")),              # LoteRecepCosto
        comentarios,                                     # Comentarios (fundido)
        combinar_fecha_hora(r.get("fecha_solicitud"), r.get("hora_recepcion")),  # FechaHoraRecepcion
        combinar_fecha_hora(r.get("fecha_ejecucion"), r.get("hora_plan_inicio")), # FechaHoraPlanInicio
        combinar_fecha_hora(r.get("fecha_ejecucion"), r.get("hora_plan_llegada")),# FechaHoraPlanLlegada
        limpiar(r.get("tipo_grupo")),                    # TipoGrupo
        limpiar(r.get("grupo")),                         # Grupo
        limpiar(r.get("encargado_grupo")),               # EncargadoGrupo
        limpiar(r.get("contacto")),                      # Contacto
        limpiar(r.get("cod_maquina")),                   # CodMaquina
        limpiar(r.get("nomb_recurso_carga")),            # Producto (alias para producto cosecha — algunos JSONs lo llaman así)
        limpiar(r.get("um_recurso_carga")),              # UM (alias)
        limpiar(r.get("controlador_seg")),               # Controlador
        texto_libre(r.get("motivo_retraso_inicio")),     # MotivoRetrasoInicio
        texto_libre(r.get("motivo_retraso_entrega")),    # MotivoRetrasoEntrega
        texto_libre(r.get("motivo_estatus_viaje")),      # MotivoEstatusViaje
        limpiar(r.get("documento_traslado")),            # DocumentoTraslado
        texto_libre(r.get("motivo_devolucion")),         # MotivoDevolucion
        num(r.get("cantidad_devolucion")),               # CantidadDevolucion
        num(r.get("bultos_devolucion")),                 # BultosDevolucion
        texto_libre(r.get("nota_devolucion")),           # NotaDevolucion
        texto_libre(r.get("comentario_cabina")),         # ComentarioCabina
        texto_libre(r.get("comentario_integracion")),    # ComentarioIntegracion
    ]

    # Para MERGE necesitamos los valores duplicados (USING ST + valores SET/INSERT)
    # USING toma solo el ST, después siguen los demás (sin ST ya que ST es la PK que
    # se usa solo en USING). Pero el INSERT sí necesita ST de nuevo.
    # En este UPSERT_SQL: USING(?) → SET sin ST → INSERT con ST.
    # Total params = 1 (USING) + (len(COLS)-1) (SET) + len(COLS) (INSERT)
    sin_st = valores[1:]
    return tuple([st] + sin_st + valores)


# ============================================================
# MIGRACIÓN POR LOTES
# ============================================================

def migrar(conn, registros: list, batch_size: int = 500):
    cursor = conn.cursor()
    ok = 0
    sin_st = 0
    err = 0
    primeros_errores = []

    for i, r in enumerate(registros):
        try:
            vals = mapear(r)
            if vals is None:
                sin_st += 1
                continue
            cursor.execute(UPSERT_SQL, vals)
            ok += 1
            if ok % batch_size == 0:
                conn.commit()
                print(f"    → {ok:,} procesados...", flush=True)
        except Exception as e:
            err += 1
            if len(primeros_errores) < 3:
                primeros_errores.append((r.get("st"), str(e)))

    conn.commit()
    return ok, sin_st, err, primeros_errores


# ============================================================
# RECONCILIACIÓN
# ============================================================

def reconciliacion(conn, registros: list):
    """Imprime conteos para comparar con Excel local."""
    print("\n" + "=" * 60)
    print("  RECONCILIACIÓN — programacion")
    print("=" * 60)

    # 1) Conteo total
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM programacion")
    total_sql = cursor.fetchone()[0]
    print(f"\n  Total registros JSON : {len(registros):,}")
    print(f"  Total registros SQL  : {total_sql:,}")

    # 2) Distribución por año (SemanaPrograma)
    print(f"\n  Distribución por año (SemanaPrograma) — SQL:")
    cursor.execute("""
        SELECT YEAR(SemanaPrograma) AS anio, COUNT(*) AS n
        FROM programacion
        WHERE SemanaPrograma IS NOT NULL
        GROUP BY YEAR(SemanaPrograma)
        ORDER BY anio
    """)
    for row in cursor.fetchall():
        print(f"    {row[0]}: {row[1]:,}")

    # 3) Distribución por TipoProg
    print(f"\n  Distribución por TipoProg — SQL:")
    cursor.execute("""
        SELECT TipoProg, COUNT(*) AS n
        FROM programacion
        GROUP BY TipoProg
        ORDER BY n DESC
    """)
    for row in cursor.fetchall():
        print(f"    {row[0] or '(NULL)'}: {row[1]:,}")

    # 4) Conteo de NULLs en campos críticos
    cursor.execute("""
        SELECT
            SUM(CASE WHEN ST IS NULL OR ST='' THEN 1 ELSE 0 END) AS sin_st,
            SUM(CASE WHEN FechaEjecucion IS NULL THEN 1 ELSE 0 END) AS sin_fecha_ejec,
            SUM(CASE WHEN Coordinador IS NULL THEN 1 ELSE 0 END) AS sin_coord,
            SUM(CASE WHEN TipoProg IS NULL THEN 1 ELSE 0 END) AS sin_tipo
        FROM programacion
    """)
    sin_st, sin_fe, sin_co, sin_tp = cursor.fetchone()
    print(f"\n  Campos críticos vacíos en SQL:")
    print(f"    Sin ST            : {sin_st:,}")
    print(f"    Sin FechaEjecucion: {sin_fe:,}")
    print(f"    Sin Coordinador   : {sin_co:,}")
    print(f"    Sin TipoProg      : {sin_tp:,}")

    # 5) Distribución del JSON por año (para que compares)
    print(f"\n  Distribución por año (semana_programa) — JSON original:")
    contador = Counter()
    for r in registros:
        f = parse_fecha_sola(r.get("semana_programa"))
        if f:
            contador[f.year] += 1
    for anio in sorted(contador):
        print(f"    {anio}: {contador[anio]:,}")

    print(f"\n  → Compara estos conteos contra tus Excel locales.")
    print(f"  → Si los números no cuadran, avísame antes de seguir.")


# ============================================================
# MAIN
# ============================================================

def main():
    print("\n" + "=" * 60)
    print("  MIGRACIÓN A AZURE SQL — programacion")
    print("=" * 60)

    if not SQL_PASSWORD:
        print("[ERROR] SQL_PASSWORD no configurado")
        raise SystemExit(1)

    print("\n[1/3] Descargando prog_historico_consolidado.json.gz...")
    registros = descargar_json_gz("data/prog_historico_consolidado.json.gz")
    print(f"  {len(registros):,} registros leídos")

    print("\n[2/3] Conectando a Azure SQL e insertando...")
    conn = get_conn()
    print("  ✓ Conectado")

    ok, sin_st, err, primeros_errores = migrar(conn, registros)
    print(f"\n  Insertados/actualizados: {ok:,}")
    print(f"  Descartados (sin ST)  : {sin_st:,}")
    print(f"  Errores               : {err:,}")
    if primeros_errores:
        print(f"  Primeros 3 errores:")
        for st, e in primeros_errores:
            print(f"    ST {st}: {e[:120]}")

    print("\n[3/3] Reconciliación...")
    reconciliacion(conn, registros)

    conn.close()
    print("\n✓ Migración programacion completada.\n")


if __name__ == "__main__":
    main()
