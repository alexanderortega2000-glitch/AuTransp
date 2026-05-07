"""
actualizar_api_actual.py
========================
Consulta la API de transportes y hace UPSERT en la tabla viajes_api de Azure SQL.

Ventana de consulta:
  - Últimos 7 días + 3 días adelante (viajes activos y recientes)
  - EsHistorico = 0 (viajes actuales)

Se ejecuta:
  - Via cron cada 15 minutos (workflow actualizar_api.yml)
  - Via workflow_dispatch (botón manual)

Variables de entorno requeridas:
  API_USUARIO  — usuario de la API (default: arivas)
  SQL_SERVER, SQL_DATABASE, SQL_USER, SQL_PASSWORD
"""

import os
import math
import pyodbc
import requests
import pandas as pd
from datetime import date, timedelta, datetime

# ============================================================
# CONFIGURACIÓN
# ============================================================

API_URL     = "https://logistico.grupocassa.com/api-transportes-varios-web/api/SolicitudesTransporte/GetSolicitudesTransporte"
API_USUARIO = os.environ.get("API_USUARIO", "arivas")
API_PARAMS  = {
    "Movil":                "0",
    "Usuario":              API_USUARIO,
    "Integrado":            "0",
    "CorporativoAlmacenes": "1",
    "CorporativoHaciendas": "0",
    "FueraPlan":            "0",
}

SQL_SERVER   = os.environ.get("SQL_SERVER",   "autransp-server.database.windows.net")
SQL_DATABASE = os.environ.get("SQL_DATABASE", "autransp-db")
SQL_USER     = os.environ.get("SQL_USER",     "autransp_admin")
SQL_PASSWORD = os.environ.get("SQL_PASSWORD", "")

# ============================================================
# SQL
# ============================================================

def get_conn():
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USER};"
        f"PWD={SQL_PASSWORD};"
        f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str)

# Columnas de viajes_api (excluye EsHistorico y FechaActualizacion — se manejan aparte)
COLS_SQL = [
    "ST", "TipoViaje", "OS", "PuntoPartida", "DsPuntoPartida",
    "PuntoEntrega", "DsPuntoEntrega", "FechaEntrega", "ID_EstatusST",
    "Estado", "Asignado", "Km", "KmReal", "Estimacion", "CostoFinal",
    "Diferencia", "Comentario", "FechaFinalizacion", "Integrado",
    "ObsValidaciones", "CodEquipo", "FueraPlan", "Nom_Motorista",
    "FechaInicioViaje", "ComentInicioViaje", "FechaFinViaje", "ComentFinViaje",
    "FechaEntregaST", "ComentEntrega", "CantidadCargadores",
    "Permanencia", "Permanencia_Aplica", "InicioPermanencia", "FinPermanencia",
    "HorasPermanencia", "HorasPermanenciaEst", "OficialCosecha", "Frente",
]

# Mapeo API field → SQL column (cuando difieren)
API_TO_SQL = {
    "ID_ST": "ST",
}

_set_clause  = ", ".join(f"{c}=?" for c in COLS_SQL if c != "ST") + ", EsHistorico=?, FechaActualizacion=GETUTCDATE()"
_insert_cols = ", ".join(COLS_SQL) + ", EsHistorico, FechaActualizacion"
_insert_vals = ", ".join("?" for _ in COLS_SQL) + ", ?, GETUTCDATE()"

UPSERT_SQL = f"""
MERGE viajes_api AS target
USING (SELECT ? AS ST) AS source ON target.ST = source.ST
WHEN MATCHED THEN UPDATE SET {_set_clause}
WHEN NOT MATCHED THEN INSERT ({_insert_cols}) VALUES ({_insert_vals});
"""

# ============================================================
# UTILIDADES
# ============================================================

def limpiar(v):
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    s = str(v).strip()
    if s in ("", "nan", "NaN", "None", "NaT", "inf", "-inf"):
        return None
    return s


def parse_fecha(v):
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    s = str(v).strip()
    if s in ("", "nan", "NaN", "None", "NaT"):
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d",
                "%d/%m/%Y %H:%M:%S", "%d/%m/%Y"):
        try:
            return datetime.strptime(s[:19], fmt)
        except ValueError:
            continue
    return None


def num(v):
    if v is None:
        return None
    if isinstance(v, (int, float)) and not (isinstance(v, float) and math.isnan(v)):
        return float(v)
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

# ============================================================
# CONSULTA API
# ============================================================

def consultar_api() -> list:
    hoy    = date.today()
    inicio = hoy - timedelta(days=7)
    fin    = hoy + timedelta(days=3)
    print(f"  Período: {inicio.strftime('%d/%m/%Y')} → {fin.strftime('%d/%m/%Y')}")

    todos  = []
    cursor = inicio
    while cursor <= fin:
        fin_lote = min(cursor + timedelta(days=1), fin)
        params   = {
            **API_PARAMS,
            "FechaInicio": cursor.strftime("%d-%m-%Y"),
            "FechaFin":    fin_lote.strftime("%d-%m-%Y"),
        }
        try:
            resp = requests.get(API_URL, params=params, timeout=120)
            resp.raise_for_status()
            data = resp.json()
            if data:
                todos.extend(data)
                print(f"    {cursor.strftime('%d/%m')}: {len(data):,} registros")
        except Exception as e:
            print(f"    ⚠️  Error {cursor}: {e}")
        cursor = fin_lote + timedelta(days=1)

    if not todos:
        return []

    df = pd.DataFrame(todos)
    df["ID_ST"] = df["ID_ST"].astype(str)

    # Deduplicar — prioridad a registros con más información
    def prioridad(row):
        if pd.notna(row.get("FechaFinalizacion")) and row.get("FechaFinalizacion") != "":
            return 0
        if pd.notna(row.get("FechaInicioViaje")) and row.get("FechaInicioViaje") != "":
            return 1
        return 2

    df["_prio"] = df.apply(prioridad, axis=1)
    df = (df.sort_values("_prio")
            .drop_duplicates(subset=["ID_ST"], keep="first")
            .drop(columns=["_prio"])
            .reset_index(drop=True))

    print(f"  Total únicos: {len(df):,} registros")
    return df.to_dict(orient="records")


# ============================================================
# UPSERT EN SQL
# ============================================================

FECHA_CORTE_HISTORICO = datetime(2025, 11, 1)

def es_historico(r: dict) -> int:
    """1 si el viaje es anterior al corte histórico, 0 si es actual."""
    fecha_ref = (parse_fecha(r.get("FechaEntrega"))
                 or parse_fecha(r.get("FechaInicioViaje"))
                 or parse_fecha(r.get("FechaEntregaST")))
    if fecha_ref and fecha_ref < FECHA_CORTE_HISTORICO:
        return 1
    return 0
    """Convierte un registro de la API a tupla para el UPSERT."""
    st = limpiar(str(r.get("ID_ST", "") or ""))
    if not st:
        return None

    valores = [
        st,
        limpiar(r.get("TipoViaje")),
        limpiar(r.get("OS")),
        limpiar(r.get("PuntoPartida")),
        limpiar(r.get("DsPuntoPartida")),
        limpiar(r.get("PuntoEntrega")),
        limpiar(r.get("DsPuntoEntrega")),
        parse_fecha(r.get("FechaEntrega")),
        entero(r.get("ID_EstatusST")),
        limpiar(r.get("Estado")),
        limpiar(r.get("Asignado")),
        num(r.get("Km")),
        num(r.get("KmReal")),
        num(r.get("Estimacion")),
        num(r.get("CostoFinal")),
        num(r.get("Diferencia")),
        limpiar(r.get("Comentario")),
        parse_fecha(r.get("FechaFinalizacion")),
        limpiar(r.get("Integrado")),
        limpiar(r.get("ObsValidaciones")),
        limpiar(r.get("CodEquipo")),
        limpiar(r.get("FueraPlan")),
        limpiar(r.get("Nom_Motorista")),
        parse_fecha(r.get("FechaInicioViaje")),
        limpiar(r.get("ComentInicioViaje")),
        parse_fecha(r.get("FechaFinViaje")),
        limpiar(r.get("ComentFinViaje")),
        parse_fecha(r.get("FechaEntregaST")),
        limpiar(r.get("ComentEntrega")),
        entero(r.get("CantidadCargadores")),
        num(r.get("Permanencia")),
        limpiar(r.get("Permanencia_Aplica")),
        parse_fecha(r.get("InicioPermanencia")),
        parse_fecha(r.get("FinPermanencia")),
        num(r.get("HorasPermanencia")),
        num(r.get("HorasPermanenciaEst")),
        limpiar(r.get("OficialCosecha")),
        limpiar(r.get("Frente")),
    ]

    historico = es_historico(r)

    # Para MERGE:
    # - USING: ST
    # - UPDATE SET: campos sin ST + EsHistorico
    # - INSERT: todos los campos + EsHistorico
    sin_st = valores[1:]
    return tuple([st] + sin_st + [historico] + valores + [historico])


def upsert_en_sql(conn, registros: list):
    cursor = conn.cursor()
    cursor.fast_executemany = True

    ok  = 0
    err = 0
    primeros_errores = []
    lote = []

    def flush():
        nonlocal ok, err
        if not lote:
            return
        try:
            cursor.executemany(UPSERT_SQL, lote)
            conn.commit()
            ok += len(lote)
        except Exception:
            conn.rollback()
            for vals in lote:
                try:
                    cursor.execute(UPSERT_SQL, vals)
                    conn.commit()
                    ok += 1
                except Exception as e2:
                    err += 1
                    if len(primeros_errores) < 3:
                        primeros_errores.append((vals[0], str(e2)[:120]))
        lote.clear()

    for r in registros:
        vals = mapear(r)
        if vals is None:
            continue
        lote.append(vals)
        if len(lote) >= 500:
            flush()

    flush()
    return ok, err, primeros_errores

# ============================================================
# MAIN
# ============================================================

def main():
    print("\n" + "=" * 60)
    print("  ACTUALIZAR API ACTUAL → Azure SQL")
    print("=" * 60)

    if not SQL_PASSWORD:
        print("[ERROR] SQL_PASSWORD no configurado")
        raise SystemExit(1)

    print("\n[1/2] Consultando API de transportes...")
    registros = consultar_api()

    if not registros:
        print("⚠️  Sin datos de la API. No se actualiza SQL.")
        return

    print(f"\n[2/2] Actualizando viajes_api en Azure SQL...")
    conn = get_conn()
    print("  ✓ Conectado")

    ok, err, errores = upsert_en_sql(conn, registros)
    conn.close()

    print(f"\n  Insertados/actualizados: {ok:,}")
    print(f"  Errores               : {err:,}")
    if errores:
        for st, e in errores:
            print(f"    ST {st}: {e}")

    print(f"\n✓ viajes_api actualizada — {date.today().strftime('%Y-%m-%d')}\n")


if __name__ == "__main__":
    main()
