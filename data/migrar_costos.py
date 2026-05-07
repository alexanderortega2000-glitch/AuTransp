"""
migrar_costos_actual.py
=======================
Hace UPSERT de CostosActuales.csv en la tabla costos de Azure SQL.
- ST es la llave única
- Si hay ST duplicados en el CSV, toma el más reciente por FechaApunte
- Actualiza si existe, inserta si no
- No toca registros históricos

Uso:
    python data/migrar_costos_actual.py --archivo "data/CostosActuales.csv"

Variables de entorno:
    SQL_SERVER, SQL_DATABASE, SQL_USER, SQL_PASSWORD
"""

import os, sys, math, argparse
import pandas as pd
import pyodbc
from datetime import datetime

SQL_SERVER   = os.environ.get("SQL_SERVER",   "autransp-server.database.windows.net")
SQL_DATABASE = os.environ.get("SQL_DATABASE", "autransp-db")
SQL_USER     = os.environ.get("SQL_USER",     "autransp_admin")
SQL_PASSWORD = os.environ.get("SQL_PASSWORD", "")

HACIENDAS_COSECHA = {2996, 1984, 2983, 1994, 2984, 1983, 1987, 2987}

# ============================================================
# CONEXIÓN
# ============================================================

def get_conn():
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};UID={SQL_USER};PWD={SQL_PASSWORD};"
        f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")

# ============================================================
# UTILIDADES
# ============================================================

def limpiar(v):
    if v is None: return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)): return None
    s = str(v).strip()
    return None if s in ("","nan","NaN","None","NaT","<NA>") else s

def parse_fecha(v):
    if v is None: return None
    s = str(v).strip()
    if s in ("","nan","NaN","None","NaT"): return None
    for fmt in ("%Y-%m-%d %H:%M:%S","%Y-%m-%d","%d/%m/%Y %H:%M:%S","%d/%m/%Y"):
        try: return datetime.strptime(s[:19], fmt)
        except: continue
    return None

def num(v):
    if v is None: return None
    if isinstance(v, float) and math.isnan(v): return None
    try: return float(str(v).strip().replace(",",""))
    except: return None

def entero(v):
    f = num(v); return int(f) if f is not None else None

# ============================================================
# CLASIFICACIÓN DE ÁREA
# ============================================================

def extraer_flota(equipo):
    if not equipo: return None
    e = str(equipo).upper()
    for f in ["VOLQUETA","CABEZAL","LOWBOY","PICK UP","BUS","CAMION","CISTERNA","MOTO","TRAILER"]:
        if f in e: return f
    return None

def clasificar_area(row):
    bk  = entero(row.get("BkRecurso"))
    bkh = entero(row.get("BkHacienda1"))
    nc  = str(row.get("NombreCuadrilla","") or "").upper()
    fl  = extraer_flota(row.get("Equipo","") or row.get("Flota",""))

    if bk in {42012,83008} and bkh in HACIENDAS_COSECHA: return "Cosecha"
    if bk in {42004,40003,40002,42007,40001,83003}: return "Red Vial"
    if "RED VIAL" in nc: return "Red Vial"
    if fl in {"VOLQUETA","CABEZAL","LOWBOY"}: return "Red Vial"
    if bk in {42012,83008}: return "Varios"
    if fl in {"PICK UP","BUS","CAMION","CISTERNA"}: return "Varios"
    if bk in {9013,83007,83005,83006,41002,42008,42001}: return "Varios"
    return "No Definido"

# ============================================================
# UPSERT SQL
# ============================================================

COLS = [
    "ST","BkRecurso","Recurso","BkProveedor","Proveedor","Equipo","Flota","Subflota",
    "NombreCuadrilla","BkHacienda1","OS","Area","CostoReal","CostoEstimado",
    "Integrado","Pagado","FechaApunte","FechaIntegracion","A_Pagar","A_Cobrar",
    "ValorTotalViaje","IntegracionPago","IntegracionCobro","EsHistorico",
]

_set   = ", ".join(f"{c}=?" for c in COLS if c!="ST") + ", FechaActualizacion=GETUTCDATE()"
_icols = ", ".join(COLS) + ", FechaActualizacion"
_ivals = ", ".join("?" for _ in COLS) + ", GETUTCDATE()"

UPSERT_SQL = (
    f"MERGE costos AS t USING (SELECT ? AS ST) AS s ON t.ST=s.ST "
    f"WHEN MATCHED THEN UPDATE SET {_set} "
    f"WHEN NOT MATCHED THEN INSERT ({_icols}) VALUES ({_ivals});"
)

def mapear(row) -> tuple:
    st = limpiar(str(row.get("ST","") or ""))
    if not st: return None

    area = clasificar_area(row)

    v = [
        st,
        entero(row.get("BkRecurso")),
        limpiar(row.get("Recurso")),
        entero(row.get("BkProveedor")),
        limpiar(row.get("Proveedor")),
        limpiar(row.get("Equipo")),
        limpiar(row.get("Flota") or extraer_flota(row.get("Equipo"))),
        limpiar(row.get("Subflota")),
        limpiar(row.get("NombreCuadrilla")),
        entero(row.get("BkHacienda1")),
        limpiar(row.get("OS")),
        area,
        num(row.get("CostoReal")),
        num(row.get("CostoEstimado")),
        limpiar(row.get("Integrado")),
        limpiar(row.get("Pagado")),
        parse_fecha(row.get("FechaApunte")),
        parse_fecha(row.get("FechaIntegracion")),
        num(row.get("A_Pagar")),
        num(row.get("A_Cobrar")),
        num(row.get("ValorTotalViaje")),
        parse_fecha(row.get("IntegracionPago")),
        parse_fecha(row.get("IntegracionCobro")),
        0,  # EsHistorico = 0 (actual)
    ]

    sin_st = v[1:]
    return tuple([st] + sin_st + v)

def upsert(conn, registros):
    cur = conn.cursor(); cur.fast_executemany = True
    ok = err = 0; errores = []; lote = []

    def flush():
        nonlocal ok, err
        if not lote: return
        try:
            cur.executemany(UPSERT_SQL, lote); conn.commit(); ok += len(lote)
        except:
            conn.rollback()
            for vals in lote:
                try: cur.execute(UPSERT_SQL, vals); conn.commit(); ok += 1
                except Exception as e:
                    err += 1
                    if len(errores)<3: errores.append((vals[0], str(e)[:120]))
        lote.clear()

    for r in registros:
        vals = mapear(r)
        if vals is None: continue
        lote.append(vals)
        if len(lote) >= 500: flush()
    flush()
    return ok, err, errores

# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--archivo", required=True, help="Ruta al CSV de costos actuales")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"  MIGRAR COSTOS ACTUALES → Azure SQL")
    print(f"  Archivo: {args.archivo}")
    print(f"{'='*60}\n")

    if not SQL_PASSWORD:
        print("[ERROR] SQL_PASSWORD no configurado"); sys.exit(1)

    # Leer CSV
    print("[1/3] Leyendo CSV...")
    df = pd.read_csv(args.archivo, dtype=str, encoding="utf-8-sig")
    print(f"  Filas leídas: {len(df):,}")

    # Normalizar columna ST
    df["ST"] = df["ST"].astype(str).str.strip()

    # Deduplicar por ST — tomar el más reciente por FechaApunte
    if "FechaApunte" in df.columns:
        df["_fecha"] = pd.to_datetime(df["FechaApunte"], errors="coerce")
        df = df.sort_values("_fecha", ascending=False).drop_duplicates("ST", keep="first").drop(columns=["_fecha"])
    else:
        df = df.drop_duplicates("ST", keep="last")

    print(f"  STs únicos: {len(df):,}")

    # Conectar SQL
    print("\n[2/3] Conectando a Azure SQL...")
    conn = get_conn()
    print("  ✓ Conectado")

    # UPSERT
    print("\n[3/3] Actualizando costos...")
    registros = df.to_dict(orient="records")
    ok, err, errores = upsert(conn, registros)
    conn.close()

    print(f"\n  Insertados/actualizados: {ok:,}")
    print(f"  Errores               : {err:,}")
    for st, e in errores:
        print(f"    ST {st}: {e}")
    print(f"\n✓ Completado — {datetime.today().strftime('%Y-%m-%d')}\n")

if __name__ == "__main__":
    main()
