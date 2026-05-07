"""
migrar_costos.py
================
Hace UPSERT desde CSV.GZ a la tabla costos de Azure SQL.
Mapea dinámicamente todas las columnas presentes en el CSV
que existan en la tabla SQL.

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

def get_columnas_sql(conn) -> set:
    """Obtiene las columnas de la tabla costos en SQL."""
    cur = conn.cursor()
    cur.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'costos'")
    return {row[0] for row in cur.fetchall()}

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

# Columnas que son fechas
COLS_FECHA = {
    "FechaApunte","FechaIntegracion","FechaPago","FechaHoraInicio","FechaHoraFin",
    "FechaIntegracionSAPPago","FechaIntegracionSAPCobro","FechaActualizacion","FechaDigita",
}
# Columnas que son enteros
COLS_ENTERO = {
    "CantidadCargadores","Pasos","BkZafra","Agrupador","CantidadBloqueos","IdAgrupaST",
    "EsHistorico",
}
# Columnas que son decimales
COLS_DECIMAL = {
    "CostoReal","CostoEstimado","OdometroInicio","OdometroFin","Distancia","DuracionHoras",
    "HorasPermanencia","Paquetes","CantidadPago1","A_Cobrar","A_Pagar","ValorPagoComplemento",
    "ValorPagoPermanencia","A_COB_UNI","A_PAG_UNI","ValorTotalViaje","AreaAct","PesoVario",
}
# Columnas que son bigint
COLS_BIGINT = {"IdApMaquinaria","IdEnvioVario"}

def convertir(col, v):
    if col in COLS_FECHA:   return parse_fecha(v)
    if col in COLS_ENTERO:  return entero(v)
    if col in COLS_DECIMAL: return num(v)
    if col in COLS_BIGINT:  return entero(v)
    return limpiar(v)

def upsert_en_sql(conn, df: pd.DataFrame, cols_comunes: list):
    """UPSERT dinámico usando solo las columnas presentes en CSV y SQL."""
    cols_sin_st = [c for c in cols_comunes if c != "ST"]

    set_clause   = ", ".join(f"{c}=?" for c in cols_sin_st) + ", FechaActualizacion=GETUTCDATE()"
    insert_cols  = ", ".join(cols_comunes) + ", FechaActualizacion"
    insert_vals  = ", ".join("?" for _ in cols_comunes) + ", GETUTCDATE()"

    upsert_sql = (
        f"MERGE costos AS t USING (SELECT ? AS ST) AS s ON t.ST=s.ST "
        f"WHEN MATCHED THEN UPDATE SET {set_clause} "
        f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});"
    )

    cur = conn.cursor(); cur.fast_executemany = True
    ok = err = 0; errores = []; lote = []

    def flush():
        nonlocal ok, err
        if not lote: return
        try:
            cur.executemany(upsert_sql, lote); conn.commit(); ok += len(lote)
        except:
            conn.rollback()
            for vals in lote:
                try: cur.execute(upsert_sql, vals); conn.commit(); ok += 1
                except Exception as e:
                    err += 1
                    if len(errores)<3: errores.append((vals[0], str(e)[:120]))
        lote.clear()

    for _, row in df.iterrows():
        st = limpiar(str(row.get("ST","") or ""))
        if not st: continue

        # MERGE: ST (USING) + valores SET (sin ST) + valores INSERT (con ST)
        vals_set    = [convertir(c, row.get(c)) for c in cols_sin_st]
        vals_insert = [convertir(c, row.get(c)) for c in cols_comunes]
        lote.append(tuple([st] + vals_set + vals_insert))

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
    print(f"  Columnas: {len(df.columns):,}")

    print("\n[2/3] Conectando a Azure SQL...")
    conn = get_conn()
    print("  ✓ Conectado")

    # Columnas comunes entre CSV y SQL
    cols_sql = get_columnas_sql(conn)
    cols_csv = set(df.columns)
    cols_comunes = ["ST"] + sorted([c for c in cols_csv & cols_sql if c != "ST"])
    print(f"  Columnas mapeadas: {len(cols_comunes)}")
    cols_ignoradas = cols_csv - cols_sql - {"ST"}
    if cols_ignoradas:
        print(f"  Columnas ignoradas (no en SQL): {sorted(cols_ignoradas)}")

    print("\n[3/3] UPSERT costos...")
    ok, err, errores = upsert_en_sql(conn, df, cols_comunes)
    conn.close()

    print(f"\n  Insertados/actualizados: {ok:,}")
    print(f"  Errores               : {err:,}")
    for st, e in errores: print(f"    ST {st}: {e}")
    print(f"\n✓ Completado — {datetime.today().strftime('%Y-%m-%d')}\n")

if __name__ == "__main__":
    main()
