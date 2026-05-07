"""
backfill_api.py
===============
Consulta la API mes a mes para los últimos N meses y hace UPSERT
en viajes_api. Recupera FechaEntrega y otros campos que el cron
de 15 minutos perdió porque los viajes cayeron fuera de la ventana.

Uso:
    python data/backfill_api.py --meses 3

Variables de entorno:
    API_USUARIO, SQL_SERVER, SQL_DATABASE, SQL_USER, SQL_PASSWORD
"""

import os
import sys
import math
import argparse
import pyodbc
import requests
import pandas as pd
from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta

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

FECHA_CORTE_HISTORICO = datetime(2025, 11, 1)

# ============================================================
# UTILIDADES
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


def limpiar(v):
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    s = str(v).strip()
    if s in ("", "nan", "NaN", "None", "NaT"):
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
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y"):
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


def es_historico(r: dict) -> int:
    fecha_ref = (parse_fecha(r.get("FechaEntrega"))
                 or parse_fecha(r.get("FechaInicioViaje"))
                 or parse_fecha(r.get("FechaEntregaST")))
    if fecha_ref and fecha_ref < FECHA_CORTE_HISTORICO:
        return 1
    return 0

# ============================================================
# SQL
# ============================================================

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

_set_clause  = ", ".join(f"{c}=?" for c in COLS_SQL if c != "ST") + ", EsHistorico=?, FechaActualizacion=GETUTCDATE()"
_insert_cols = ", ".join(COLS_SQL) + ", EsHistorico, FechaActualizacion"
_insert_vals = ", ".join("?" for _ in COLS_SQL) + ", ?, GETUTCDATE()"

UPSERT_SQL = f"""
MERGE viajes_api AS target
USING (SELECT ? AS ST) AS source ON target.ST = source.ST
WHEN MATCHED THEN UPDATE SET {_set_clause}
WHEN NOT MATCHED THEN INSERT ({_insert_cols}) VALUES ({_insert_vals});
"""


def mapear(r: dict) -> tuple:
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
    sin_st = valores[1:]
    return tuple([st] + sin_st + [historico] + valores + [historico])

# ============================================================
# CONSULTA API — día a día dentro de un rango
# ============================================================

def consultar_rango(inicio: date, fin: date) -> list:
    todos  = []
    cursor = inicio
    while cursor <= fin:
        fin_lote = min(cursor + timedelta(days=1), fin)
        params = {
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
        except Exception as e:
            print(f"    ⚠️  Error {cursor}: {e}")
        cursor = fin_lote + timedelta(days=1)

    if not todos:
        return []

    df = pd.DataFrame(todos)
    df["ID_ST"] = df["ID_ST"].astype(str)

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

    return df.to_dict(orient="records")

# ============================================================
# UPSERT EN SQL
# ============================================================

def upsert_lote(conn, registros: list):
    cursor = conn.cursor()
    cursor.fast_executemany = True
    ok = err = 0
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
                except Exception:
                    err += 1
        lote.clear()

    for r in registros:
        vals = mapear(r)
        if vals is None:
            continue
        lote.append(vals)
        if len(lote) >= 500:
            flush()

    flush()
    return ok, err

# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Backfill viajes_api mes a mes.")
    parser.add_argument("--meses", type=int, default=3,
                        help="Número de meses hacia atrás a recuperar (default: 3)")
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("  BACKFILL API → Azure SQL")
    print(f"  Meses a recuperar: {args.meses}")
    print("=" * 60)

    if not SQL_PASSWORD:
        print("[ERROR] SQL_PASSWORD no configurado")
        sys.exit(1)

    conn = get_conn()
    print("  ✓ Conectado a SQL\n")

    hoy = date.today()
    total_ok = total_err = 0

    for i in range(args.meses, 0, -1):
        # Primer día del mes i meses atrás
        inicio_mes = (hoy.replace(day=1) - relativedelta(months=i-1))
        if i == args.meses:
            # Primer mes: desde el día 1 del mes más antiguo
            inicio = hoy.replace(day=1) - relativedelta(months=i-1)
        else:
            inicio = hoy.replace(day=1) - relativedelta(months=i-1)

        # Último día del mes (o hoy si es el mes actual)
        fin_mes = inicio.replace(day=1) + relativedelta(months=1) - timedelta(days=1)
        fin = min(fin_mes, hoy)

        print(f"[Mes {i}] {inicio.strftime('%d/%m/%Y')} → {fin.strftime('%d/%m/%Y')}")
        registros = consultar_rango(inicio, fin)
        print(f"  API devolvió: {len(registros):,} registros únicos")

        if registros:
            ok, err = upsert_lote(conn, registros)
            total_ok  += ok
            total_err += err
            print(f"  SQL: {ok:,} actualizados, {err:,} errores")
        else:
            print(f"  Sin datos")
        print()

    conn.close()
    print("=" * 60)
    print(f"  TOTAL actualizados : {total_ok:,}")
    print(f"  TOTAL errores      : {total_err:,}")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
