"""
migrar_costos.py
================
Hace UPSERT de costos desde CSV.GZ a la tabla costos de Azure SQL.
Usado por dos workflows:
  - Costos actual    → data/costos_actual.csv.gz
  - Costos histórico → data/CostosAgricampoHist.csv.gz

Uso:
    python data/migrar_costos.py --archivo data/costos_actual.csv.gz
    python data/migrar_costos.py --archivo data/CostosAgricampoHist.csv.gz

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

def get_conn():
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};UID={SQL_USER};PWD={SQL_PASSWORD};"
        f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")

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
    v = [
        st, entero(row.get("BkRecurso")), limpiar(row.get("Recurso")),
        entero(row.get("BkProveedor")), limpiar(row.get("Proveedor")),
        limpiar(row.get("Equipo")), limpiar(row.get("Flota")),
        limpiar(row.get("Subflota")), limpiar(row.get("NombreCuadrilla")),
        entero(row.get("BkHacienda1")), limpiar(row.get("OS")),
        limpiar(row.get("Area")), num(row.get("CostoReal")),
        num(row.get("CostoEstimado")), limpiar(row.get("Integrado")),
        limpiar(row.get("Pagado")), parse_fecha(row.get("FechaApunte")),
        parse_fecha(row.get("FechaIntegracion")), num(row.get("A_Pagar")),
        num(row.get("A_Cobrar")), num(row.get("ValorTotalViaje")),
        parse_fecha(row.get("IntegracionPago")),
        parse_fecha(row.get("IntegracionCobro")),
        entero(row.get("EsHistorico", 0)),
    ]
    sin_st = v[1:]
    return tuple([st] + sin_st + v)

def upsert_en_sql(conn, registros):
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

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--archivo", required=True)
    args = parser.parse_args()

    print(f"\n{'='*60}\n  MIGRAR COSTOS → Azure SQL\n  Archivo: {args.archivo}\n{'='*60}\n")
    if not SQL_PASSWORD: print("[ERROR] SQL_PASSWORD no configurado"); sys.exit(1)

    print("[1/3] Leyendo CSV.GZ...")
    df = pd.read_csv(args.archivo, dtype=str, encoding="utf-8-sig", compression="gzip")
    print(f"  Filas   : {len(df):,}")
    print(f"  ST únicos: {df['ST'].nunique():,}")

    print("\n[2/3] Conectando a Azure SQL...")
    conn = get_conn(); print("  ✓ Conectado")

    print("\n[3/3] UPSERT costos...")
    ok, err, errores = upsert_en_sql(conn, df.to_dict(orient="records"))
    conn.close()

    print(f"\n  Insertados/actualizados: {ok:,}")
    print(f"  Errores               : {err:,}")
    for st, e in errores: print(f"    ST {st}: {e}")
    print(f"\n✓ Completado — {datetime.today().strftime('%Y-%m-%d')}\n")

if __name__ == "__main__":
    main()
