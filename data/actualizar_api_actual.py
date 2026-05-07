"""
actualizar_api_actual.py
========================
Consulta la API de transportes y hace UPSERT en viajes_api de Azure SQL.
Acepta parámetros de ventana para usarse en múltiples frecuencias.

Uso:
    # Cron cada 5 min (últimos 3 días + 1 adelante)
    python data/actualizar_api_actual.py --dias-atras 3 --dias-adelante 1

    # Semanal (último mes)
    python data/actualizar_api_actual.py --dias-atras 30

    # Quincenal (últimos 3 meses)
    python data/actualizar_api_actual.py --dias-atras 90

Variables de entorno requeridas:
    API_USUARIO, SQL_SERVER, SQL_DATABASE, SQL_USER, SQL_PASSWORD
"""

import os, sys, math, argparse, pyodbc, requests, pandas as pd
from datetime import date, timedelta, datetime

API_URL     = "https://logistico.grupocassa.com/api-transportes-varios-web/api/SolicitudesTransporte/GetSolicitudesTransporte"
API_USUARIO = os.environ.get("API_USUARIO", "arivas")
API_PARAMS  = {"Movil":"0","Usuario":API_USUARIO,"Integrado":"0",
               "CorporativoAlmacenes":"1","CorporativoHaciendas":"0","FueraPlan":"0"}

SQL_SERVER   = os.environ.get("SQL_SERVER",   "autransp-server.database.windows.net")
SQL_DATABASE = os.environ.get("SQL_DATABASE", "autransp-db")
SQL_USER     = os.environ.get("SQL_USER",     "autransp_admin")
SQL_PASSWORD = os.environ.get("SQL_PASSWORD", "")
FECHA_CORTE  = datetime(2025, 11, 1)

def get_conn():
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};UID={SQL_USER};PWD={SQL_PASSWORD};"
        f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")

def limpiar(v):
    if v is None: return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)): return None
    s = str(v).strip()
    return None if s in ("","nan","NaN","None","NaT") else s

def parse_fecha(v):
    if v is None: return None
    if isinstance(v, datetime): return v
    s = str(v).strip()
    if s in ("","nan","NaN","None","NaT"): return None
    for fmt in ("%Y-%m-%dT%H:%M:%S","%Y-%m-%d %H:%M:%S","%Y-%m-%d","%d/%m/%Y %H:%M:%S","%d/%m/%Y"):
        try: return datetime.strptime(s[:19], fmt)
        except ValueError: continue
    return None

def num(v):
    if v is None: return None
    if isinstance(v,(int,float)) and not (isinstance(v,float) and math.isnan(v)): return float(v)
    s = str(v).strip()
    if s in ("","nan","NaN","None","NaT"): return None
    try: return float(s)
    except: return None

def entero(v):
    f = num(v); return int(f) if f is not None else None

def es_historico(r):
    f = parse_fecha(r.get("FechaEntrega")) or parse_fecha(r.get("FechaInicioViaje")) or parse_fecha(r.get("FechaEntregaST"))
    return 1 if (f and f < FECHA_CORTE) else 0

COLS_SQL = [
    "ST","TipoViaje","OS","PuntoPartida","DsPuntoPartida","PuntoEntrega","DsPuntoEntrega",
    "FechaEntrega","ID_EstatusST","Estado","Asignado","Km","KmReal","Estimacion","CostoFinal",
    "Diferencia","Comentario","FechaFinalizacion","Integrado","ObsValidaciones","CodEquipo",
    "FueraPlan","Nom_Motorista","FechaInicioViaje","ComentInicioViaje","FechaFinViaje",
    "ComentFinViaje","FechaEntregaST","ComentEntrega","CantidadCargadores","Permanencia",
    "Permanencia_Aplica","InicioPermanencia","FinPermanencia","HorasPermanencia",
    "HorasPermanenciaEst","OficialCosecha","Frente",
]
_set   = ", ".join(f"{c}=?" for c in COLS_SQL if c!="ST") + ", EsHistorico=?, FechaActualizacion=GETUTCDATE()"
_icols = ", ".join(COLS_SQL) + ", EsHistorico, FechaActualizacion"
_ivals = ", ".join("?" for _ in COLS_SQL) + ", ?, GETUTCDATE()"
UPSERT_SQL = f"MERGE viajes_api AS t USING (SELECT ? AS ST) AS s ON t.ST=s.ST WHEN MATCHED THEN UPDATE SET {_set} WHEN NOT MATCHED THEN INSERT ({_icols}) VALUES ({_ivals});"

def mapear(r):
    st = limpiar(str(r.get("ID_ST","") or ""))
    if not st: return None
    v = [st, limpiar(r.get("TipoViaje")), limpiar(r.get("OS")),
         limpiar(r.get("PuntoPartida")), limpiar(r.get("DsPuntoPartida")),
         limpiar(r.get("PuntoEntrega")), limpiar(r.get("DsPuntoEntrega")),
         parse_fecha(r.get("FechaEntrega")), entero(r.get("ID_EstatusST")),
         limpiar(r.get("Estado")), limpiar(r.get("Asignado")),
         num(r.get("Km")), num(r.get("KmReal")), num(r.get("Estimacion")),
         num(r.get("CostoFinal")), num(r.get("Diferencia")), limpiar(r.get("Comentario")),
         parse_fecha(r.get("FechaFinalizacion")), limpiar(r.get("Integrado")),
         limpiar(r.get("ObsValidaciones")), limpiar(r.get("CodEquipo")),
         limpiar(r.get("FueraPlan")), limpiar(r.get("Nom_Motorista")),
         parse_fecha(r.get("FechaInicioViaje")), limpiar(r.get("ComentInicioViaje")),
         parse_fecha(r.get("FechaFinViaje")), limpiar(r.get("ComentFinViaje")),
         parse_fecha(r.get("FechaEntregaST")), limpiar(r.get("ComentEntrega")),
         entero(r.get("CantidadCargadores")), num(r.get("Permanencia")),
         limpiar(r.get("Permanencia_Aplica")), parse_fecha(r.get("InicioPermanencia")),
         parse_fecha(r.get("FinPermanencia")), num(r.get("HorasPermanencia")),
         num(r.get("HorasPermanenciaEst")), limpiar(r.get("OficialCosecha")),
         limpiar(r.get("Frente"))]
    h = es_historico(r)
    return tuple([st] + v[1:] + [h] + v + [h])

def consultar_api(inicio, fin):
    print(f"  Período: {inicio.strftime('%d/%m/%Y')} → {fin.strftime('%d/%m/%Y')}", flush=True)
    todos, cursor = [], inicio
    while cursor <= fin:
        fin_lote = min(cursor + timedelta(days=1), fin)
        try:
            resp = requests.get(API_URL, params={**API_PARAMS,
                "FechaInicio": cursor.strftime("%d-%m-%Y"),
                "FechaFin":    fin_lote.strftime("%d-%m-%Y")}, timeout=120)
            resp.raise_for_status()
            data = resp.json()
            if data:
                todos.extend(data)
                print(f"    {cursor.strftime('%d/%m')}: {len(data):,}", flush=True)
        except Exception as e:
            print(f"    ⚠️  {cursor}: {e}", flush=True)
        cursor = fin_lote + timedelta(days=1)
    if not todos: return []
    df = pd.DataFrame(todos)
    df["ID_ST"] = df["ID_ST"].astype(str)
    df["_p"] = df.apply(lambda r: 0 if (pd.notna(r.get("FechaFinalizacion")) and r.get("FechaFinalizacion")!="")
                        else 1 if (pd.notna(r.get("FechaInicioViaje")) and r.get("FechaInicioViaje")!="") else 2, axis=1)
    df = df.sort_values("_p").drop_duplicates("ID_ST",keep="first").drop(columns=["_p"]).reset_index(drop=True)
    print(f"  Total únicos: {len(df):,}", flush=True)
    return df.to_dict(orient="records")

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
                except Exception as e2:
                    err += 1
                    if len(errores)<3: errores.append((vals[0], str(e2)[:120]))
        lote.clear()
    for r in registros:
        vals = mapear(r)
        if vals is None: continue
        lote.append(vals)
        if len(lote)>=500: flush()
    flush()
    return ok, err, errores

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dias-atras",    type=int, default=3)
    parser.add_argument("--dias-adelante", type=int, default=1)
    args = parser.parse_args()

    hoy    = date.today()
    inicio = hoy - timedelta(days=args.dias_atras)
    fin    = hoy + timedelta(days=args.dias_adelante)

    print(f"\n{'='*60}\n  ACTUALIZAR API → SQL | -{args.dias_atras}d / +{args.dias_adelante}d\n{'='*60}")
    if not SQL_PASSWORD: print("[ERROR] SQL_PASSWORD no configurado"); sys.exit(1)

    print("\n[1/2] Consultando API...")
    registros = consultar_api(inicio, fin)
    if not registros: print("⚠️  Sin datos."); return

    print("\n[2/2] Actualizando SQL...")
    conn = get_conn(); print("  ✓ Conectado")
    ok, err, errores = upsert_en_sql(conn, registros)
    conn.close()

    print(f"\n  Insertados/actualizados: {ok:,}")
    print(f"  Errores               : {err:,}")
    for st, e in errores: print(f"    ST {st}: {e}")
    print(f"\n✓ Completado — {hoy}\n")

if __name__ == "__main__":
    main()
