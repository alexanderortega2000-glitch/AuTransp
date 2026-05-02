"""
migrar_a_sql.py
===============
Script de migración única — lee api_historico.json.gz y api_actual.json.gz
desde GitHub y los inserta en Azure SQL (tabla viajes_api).

Ejecutar UNA SOLA VEZ desde GitHub Actions para poblar la BD inicial.

Variables de entorno requeridas:
  TOKEN_REPO      — Personal Access Token GitHub
  SQL_SERVER      — autransp-server.database.windows.net
  SQL_DATABASE    — autransp-db
  SQL_USER        — autransp_admin
  SQL_PASSWORD    — contraseña de la BD
"""

import os
import json
import gzip
import math
import requests
import struct
from datetime import datetime

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

# ============================================================
# CONEXIÓN SQL — usando pyodbc
# ============================================================

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

# ============================================================
# GITHUB — descargar json.gz
# ============================================================

def descargar_json_gz(ruta_repo: str) -> list:
    url  = f"https://raw.githubusercontent.com/{GITHUB_USUARIO}/{GITHUB_REPO}/main/{ruta_repo}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    resp = requests.get(url, headers=headers, timeout=120)
    if resp.status_code == 404:
        print(f"  ⚠️  No existe: {ruta_repo}")
        return []
    resp.raise_for_status()
    return json.loads(gzip.decompress(resp.content).decode("utf-8"))

# ============================================================
# NORMALIZACIÓN
# ============================================================

def limpiar(v):
    if v is None: return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)): return None
    if str(v) in ("nan", "NaN", "None", "NaT", ""): return None
    return v

def parse_fecha(v):
    if not v or str(v) in ("nan", "NaN", "None", "NaT", ""): return None
    try:
        s = str(v).strip()
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f",
                    "%d/%m/%Y %H:%M:%S", "%Y-%m-%d"):
            try: return datetime.strptime(s[:19], fmt)
            except: continue
    except: pass
    return None

def parse_num(v):
    if v is None or str(v) in ("nan", "NaN", "None", ""): return None
    try: return float(v)
    except: return None

def parse_int(v):
    if v is None or str(v) in ("nan", "NaN", "None", ""): return None
    try: return int(float(v))
    except: return None

# ============================================================
# INSERCIÓN EN LOTES
# ============================================================

UPSERT_SQL = """
MERGE viajes_api AS target
USING (SELECT ? AS ST) AS source ON target.ST = source.ST
WHEN MATCHED THEN UPDATE SET
    TipoViaje=?, OS=?, PuntoPartida=?,
    DsPuntoPartida=?, PuntoEntrega=?,
    DsPuntoEntrega=?, FechaEntrega=?,
    ID_EstatusST=?, Estado=?, Asignado=?,
    Km=?, KmReal=?, Estimacion=?, CostoFinal=?,
    Diferencia=?, Comentario=?,
    FechaFinalizacion=?, Integrado=?,
    ObsValidaciones=?, CodEquipo=?,
    FueraPlan=?, Nom_Motorista=?,
    FechaInicioViaje=?, ComentInicioViaje=?,
    FechaFinViaje=?, ComentFinViaje=?,
    FechaEntregaST=?, ComentEntrega=?,
    CantidadCargadores=?, Permanencia=?,
    Permanencia_Aplica=?, InicioPermanencia=?,
    FinPermanencia=?, HorasPermanencia=?,
    HorasPermanenciaEst=?, OficialCosecha=?,
    Frente=?, EsHistorico=?,
    FechaActualizacion=GETUTCDATE()
WHEN NOT MATCHED THEN INSERT (
    ST, TipoViaje, OS, PuntoPartida, DsPuntoPartida, PuntoEntrega,
    DsPuntoEntrega, FechaEntrega, ID_EstatusST, Estado, Asignado,
    Km, KmReal, Estimacion, CostoFinal, Diferencia, Comentario,
    FechaFinalizacion, Integrado, ObsValidaciones, CodEquipo, FueraPlan,
    Nom_Motorista, FechaInicioViaje, ComentInicioViaje, FechaFinViaje,
    ComentFinViaje, FechaEntregaST, ComentEntrega, CantidadCargadores,
    Permanencia, Permanencia_Aplica, InicioPermanencia, FinPermanencia,
    HorasPermanencia, HorasPermanenciaEst, OficialCosecha, Frente, EsHistorico
) VALUES (
    ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
);
"""

def insertar_lote(conn, registros: list, es_historico: bool):
    cursor = conn.cursor()
    ok = 0
    err = 0
    for r in registros:
        try:
            vals = (
                str(r.get("ID_ST", "")),           # ST (USING clause)
                limpiar(r.get("TipoViaje")),
                limpiar(r.get("OS")),
                limpiar(r.get("PuntoPartida")),
                limpiar(r.get("DsPuntoPartida")),
                limpiar(r.get("PuntoEntrega")),
                limpiar(r.get("DsPuntoEntrega")),
                parse_fecha(r.get("FechaEntrega")),
                parse_int(r.get("ID_EstatusST")),
                limpiar(r.get("Estado")),
                limpiar(r.get("Asignado")),
                parse_num(r.get("Km")),
                parse_num(r.get("KmReal")),
                parse_num(r.get("Estimacion")),
                parse_num(r.get("CostoFinal")),
                parse_num(r.get("Diferencia")),
                limpiar(str(r.get("Comentario") or "")[:500]),
                parse_fecha(r.get("FechaFinalizacion")),
                limpiar(r.get("Integrado")),
                limpiar(str(r.get("ObsValidaciones") or "")[:500]),
                limpiar(r.get("CodEquipo")),
                limpiar(r.get("FueraPlan")),
                limpiar(r.get("Nom_Motorista")),
                parse_fecha(r.get("FechaInicioViaje")),
                limpiar(str(r.get("ComentInicioViaje") or "")[:500]),
                parse_fecha(r.get("FechaFinViaje")),
                limpiar(str(r.get("ComentFinViaje") or "")[:500]),
                parse_fecha(r.get("FechaEntregaST")),
                limpiar(str(r.get("ComentEntrega") or "")[:500]),
                parse_int(r.get("CantidadCargadores")),
                parse_num(r.get("Permanencia")),
                limpiar(r.get("Permanencia_Aplica")),
                parse_fecha(r.get("InicioPermanencia")),
                parse_fecha(r.get("FinPermanencia")),
                parse_num(r.get("HorasPermanencia")),
                parse_num(r.get("HorasPermanenciaEst")),
                limpiar(r.get("OficialCosecha")),
                limpiar(r.get("Frente")),
                1 if es_historico else 0,
                # Repetir para el INSERT VALUES
                str(r.get("ID_ST", "")),
                limpiar(r.get("TipoViaje")),
                limpiar(r.get("OS")),
                limpiar(r.get("PuntoPartida")),
                limpiar(r.get("DsPuntoPartida")),
                limpiar(r.get("PuntoEntrega")),
                limpiar(r.get("DsPuntoEntrega")),
                parse_fecha(r.get("FechaEntrega")),
                parse_int(r.get("ID_EstatusST")),
                limpiar(r.get("Estado")),
                limpiar(r.get("Asignado")),
                parse_num(r.get("Km")),
                parse_num(r.get("KmReal")),
                parse_num(r.get("Estimacion")),
                parse_num(r.get("CostoFinal")),
                parse_num(r.get("Diferencia")),
                limpiar(str(r.get("Comentario") or "")[:500]),
                parse_fecha(r.get("FechaFinalizacion")),
                limpiar(r.get("Integrado")),
                limpiar(str(r.get("ObsValidaciones") or "")[:500]),
                limpiar(r.get("CodEquipo")),
                limpiar(r.get("FueraPlan")),
                limpiar(r.get("Nom_Motorista")),
                parse_fecha(r.get("FechaInicioViaje")),
                limpiar(str(r.get("ComentInicioViaje") or "")[:500]),
                parse_fecha(r.get("FechaFinViaje")),
                limpiar(str(r.get("ComentFinViaje") or "")[:500]),
                parse_fecha(r.get("FechaEntregaST")),
                limpiar(str(r.get("ComentEntrega") or "")[:500]),
                parse_int(r.get("CantidadCargadores")),
                parse_num(r.get("Permanencia")),
                limpiar(r.get("Permanencia_Aplica")),
                parse_fecha(r.get("InicioPermanencia")),
                parse_fecha(r.get("FinPermanencia")),
                parse_num(r.get("HorasPermanencia")),
                parse_num(r.get("HorasPermanenciaEst")),
                limpiar(r.get("OficialCosecha")),
                limpiar(r.get("Frente")),
                1 if es_historico else 0,
            )
            cursor.execute(UPSERT_SQL, vals)
            ok += 1
            if ok % 500 == 0:
                conn.commit()
                print(f"    → {ok:,} registros procesados...")
        except Exception as e:
            err += 1
            if err <= 3:
                print(f"    ⚠️  Error ST {r.get('ID_ST')}: {e}")
    conn.commit()
    return ok, err

# ============================================================
# MAIN
# ============================================================

def main():
    print("\n" + "=" * 60)
    print("  MIGRACIÓN A AZURE SQL — viajes_api")
    print("=" * 60)

    if not SQL_PASSWORD:
        print("[ERROR] SQL_PASSWORD no configurado")
        raise SystemExit(1)

    print("\n[1/4] Conectando a Azure SQL...")
    conn = get_conn()
    print("  ✓ Conectado")

    print("\n[2/4] Descargando api_historico.json.gz...")
    historico = descargar_json_gz("data/api_historico.json.gz")
    print(f"  {len(historico):,} registros históricos")

    print("\n[3/4] Descargando api_actual.json.gz...")
    actual = descargar_json_gz("data/api_actual.json.gz")
    print(f"  {len(actual):,} registros actuales")

    print("\n[4/4] Insertando en Azure SQL...")
    if historico:
        ok, err = insertar_lote(conn, historico, es_historico=True)
        print(f"  Histórico: {ok:,} insertados / {err} errores")

    if actual:
        ok, err = insertar_lote(conn, actual, es_historico=False)
        print(f"  Actual:    {ok:,} insertados / {err} errores")

    conn.close()
    print("\n✓ Migración completada.\n")

if __name__ == "__main__":
    main()
