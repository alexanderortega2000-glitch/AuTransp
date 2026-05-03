"""
migrar_costos_historico.py
==========================
Lee data/costos_historico.json.gz y data/costos_actual.json.gz desde GitHub
y carga a la tabla `costos` en Azure SQL.

  costos_historico → EsHistorico = 1
  costos_actual    → EsHistorico = 0

El JSON ya viene transformado por consolidar_costos.py:
  - Area calculada (Red Vial / Cosecha / Varios)
  - ST resuelto desde ID_ST u Observacion
  - FechaApunte parseada
  - Solo registros de transporte

Variables de entorno requeridas:
  TOKEN_REPO    — Personal Access Token GitHub
  SQL_SERVER    — autransp-server.database.windows.net
  SQL_DATABASE  — autransp-db
  SQL_USER      — autransp_admin
  SQL_PASSWORD  — contraseña de la BD

Ejecutar UNA SOLA VEZ tras correr Pieza 1 + Pieza 1B.
"""

import os
import json
import gzip
import math
import requests
from datetime import datetime
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
    if resp.status_code == 404:
        print(f"  ⚠️  No existe: {ruta_repo}")
        return []
    resp.raise_for_status()
    return json.loads(gzip.decompress(resp.content).decode("utf-8"))


# ============================================================
# UTILIDADES
# ============================================================

def limpiar(v):
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    s = str(v).strip()
    if s in ("", "nan", "NaN", "None", "NaT"):
        return None
    return s


def num(v):
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


def parse_fecha(v):
    if not v:
        return None
    s = str(v).strip()
    if s in ("", "nan", "NaN", "None", "NaT"):
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d",
                "%d/%m/%Y %H:%M:%S", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            return datetime.strptime(s[:19], fmt)
        except ValueError:
            continue
    return None


# ============================================================
# UPSERT — costos
# ============================================================

# Orden de columnas para el MERGE
COLS = [
    "ST",
    # originales (las dejamos NULL en migración inicial — vienen del flujo en vivo)
    "CostoReal", "CostoEstimado", "Integrado", "Pagado",
    "FechaIntegracion", "FechaPago", "TipoServicio",
    # bandera
    "EsHistorico",
    # JSON directo
    "FechaApunte", "OS1", "DsEstadoOS", "ID_ST", "IdAgrupaST", "IdProforma",
    "Area", "BkActividad", "DsActividad", "BkHacienda1", "Hacienda1",
    "BkLote", "BkRecurso", "Recurso", "BkProveedor", "Proveedor",
    "CodEquipo", "Equipo",
    "OdometroInicio", "OdometroFin", "Distancia",
    "FechaHoraInicio", "FechaHoraFin", "DuracionHoras", "HorasPermanencia",
    "CantidadCargadores", "Paquetes", "CantidadPago1",
    "A_Cobrar", "A_Pagar", "ValorPagoComplemento", "ValorPagoPermanencia",
    "Observacion",
    "IntegracionPago", "IntegracionCobro",
    "FechaIntegracionSAPPago", "FechaIntegracionSAPCobro",
    "ReferenciaPagoSAP", "ReferenciaCobroSAP",
    "SociedadGestora", "UsuarioDigita",
]

_set_clause   = ", ".join(f"{c}=?" for c in COLS if c != "ST") + ", FechaActualizacion=GETUTCDATE()"
_insert_cols  = ", ".join(COLS) + ", FechaActualizacion"
_insert_vals  = ", ".join("?" for _ in COLS) + ", GETUTCDATE()"

UPSERT_SQL = f"""
MERGE costos AS target
USING (SELECT ? AS ST) AS source ON target.ST = source.ST
WHEN MATCHED THEN UPDATE SET {_set_clause}
WHEN NOT MATCHED THEN INSERT ({_insert_cols}) VALUES ({_insert_vals});
"""


def mapear(r: dict, es_historico: bool):
    st = limpiar(r.get("ST"))
    if not st:
        return None

    valores = [
        st,                                              # ST
        # Campos del flujo en vivo — los dejamos NULL en la migración
        # (no vienen del JSON; se llenarán cuando llegue el flujo de Power Automate)
        None,                                            # CostoReal
        None,                                            # CostoEstimado
        None,                                            # Integrado
        None,                                            # Pagado
        None,                                            # FechaIntegracion
        None,                                            # FechaPago
        None,                                            # TipoServicio
        # Bandera histórico
        1 if es_historico else 0,                        # EsHistorico
        # JSON directo
        parse_fecha(r.get("FechaApunte")),               # FechaApunte
        limpiar(r.get("OS1")),                           # OS1
        limpiar(r.get("DsEstadoOS")),                    # DsEstadoOS
        limpiar(r.get("ID_ST")),                         # ID_ST
        limpiar(r.get("IdAgrupaST")),                    # IdAgrupaST
        limpiar(r.get("IdProforma")),                    # IdProforma
        limpiar(r.get("Area")),                          # Area (ya calculada)
        limpiar(r.get("BkActividad")),                   # BkActividad
        limpiar(r.get("DsActividad")),                   # DsActividad
        limpiar(r.get("BkHacienda1")),                   # BkHacienda1
        limpiar(r.get("Hacienda1")),                     # Hacienda1
        limpiar(r.get("BkLote")),                        # BkLote
        limpiar(r.get("BkRecurso")),                     # BkRecurso
        limpiar(r.get("Recurso")),                       # Recurso
        limpiar(r.get("BkProveedor")),                   # BkProveedor
        limpiar(r.get("Proveedor")),                     # Proveedor
        limpiar(r.get("CodEquipo")),                     # CodEquipo
        limpiar(r.get("Equipo")),                        # Equipo
        num(r.get("OdometroInicio")),                    # OdometroInicio
        num(r.get("OdometroFin")),                       # OdometroFin
        num(r.get("Distancia")),                         # Distancia
        # FechaHora* — en histórico siempre NULL (HoraInicio/Fin vienen como '0')
        None,                                            # FechaHoraInicio
        None,                                            # FechaHoraFin
        num(r.get("DuracionHoras")),                     # DuracionHoras
        num(r.get("HorasPermanencia")),                  # HorasPermanencia
        entero(r.get("CantidadCargadores")),             # CantidadCargadores
        num(r.get("Paquetes")),                          # Paquetes
        num(r.get("CantidadPago1")),                     # CantidadPago1
        num(r.get("A_COBRAR")),                          # A_Cobrar (mapeo case)
        num(r.get("A_PAGAR")),                           # A_Pagar
        num(r.get("ValorPagoComplemento")),              # ValorPagoComplemento
        num(r.get("ValorPagoPermanencia")),              # ValorPagoPermanencia
        limpiar(r.get("Observacion")),                   # Observacion
        limpiar(r.get("IntegracionPago")),               # IntegracionPago (texto literal)
        limpiar(r.get("IntegracionCobro")),              # IntegracionCobro
        parse_fecha(r.get("FechaIntegracionSAPPago")),   # FechaIntegracionSAPPago
        parse_fecha(r.get("FechaIntegracionSAPCobro")),  # FechaIntegracionSAPCobro
        limpiar(r.get("ReferenciaPagoSAP")),             # ReferenciaPagoSAP
        limpiar(r.get("ReferenciaCobroSAP")),            # ReferenciaCobroSAP
        limpiar(r.get("SociedadGestora")),               # SociedadGestora
        limpiar(r.get("UsuarioDigita")),                 # UsuarioDigita
    ]

    sin_st = valores[1:]
    return tuple([st] + sin_st + valores)


# ============================================================
# MIGRACIÓN POR LOTES
# ============================================================

def migrar(conn, registros: list, es_historico: bool, batch_size: int = 500):
    cursor = conn.cursor()
    ok = 0
    sin_st = 0
    err = 0
    primeros_errores = []

    for r in registros:
        try:
            vals = mapear(r, es_historico)
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
                primeros_errores.append((r.get("ST"), str(e)))

    conn.commit()
    return ok, sin_st, err, primeros_errores


# ============================================================
# RECONCILIACIÓN
# ============================================================

def reconciliacion(conn, registros_hist: list, registros_act: list):
    print("\n" + "=" * 60)
    print("  RECONCILIACIÓN — costos")
    print("=" * 60)

    cursor = conn.cursor()

    # 1) Conteo total
    cursor.execute("SELECT COUNT(*) FROM costos")
    total_sql = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM costos WHERE EsHistorico = 1")
    hist_sql = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM costos WHERE EsHistorico = 0")
    act_sql = cursor.fetchone()[0]

    print(f"\n  JSON histórico       : {len(registros_hist):,}")
    print(f"  JSON actual          : {len(registros_act):,}")
    print(f"  JSON total           : {len(registros_hist) + len(registros_act):,}")
    print(f"  SQL histórico (Es=1) : {hist_sql:,}")
    print(f"  SQL actual    (Es=0) : {act_sql:,}")
    print(f"  SQL total            : {total_sql:,}")

    # 2) Distribución por Area (SQL)
    print(f"\n  Distribución por Area — SQL:")
    cursor.execute("""
        SELECT Area, EsHistorico, COUNT(*) AS n
        FROM costos
        GROUP BY Area, EsHistorico
        ORDER BY Area, EsHistorico
    """)
    for row in cursor.fetchall():
        flag = "Hist" if row[1] else "Act"
        print(f"    {row[0] or '(NULL)':12s} {flag}: {row[2]:,}")

    # 3) Distribución por año (FechaApunte) — SQL
    print(f"\n  Distribución por año (FechaApunte) — SQL:")
    cursor.execute("""
        SELECT YEAR(FechaApunte) AS anio, COUNT(*) AS n
        FROM costos
        WHERE FechaApunte IS NOT NULL
        GROUP BY YEAR(FechaApunte)
        ORDER BY anio
    """)
    for row in cursor.fetchall():
        print(f"    {row[0]}: {row[1]:,}")

    # 4) NULLs críticos
    cursor.execute("""
        SELECT
            SUM(CASE WHEN ST IS NULL OR ST='' THEN 1 ELSE 0 END) AS sin_st,
            SUM(CASE WHEN FechaApunte IS NULL THEN 1 ELSE 0 END) AS sin_fa,
            SUM(CASE WHEN Area IS NULL THEN 1 ELSE 0 END) AS sin_area
        FROM costos
    """)
    sin_st, sin_fa, sin_area = cursor.fetchone()
    print(f"\n  Campos críticos vacíos en SQL:")
    print(f"    Sin ST          : {sin_st:,}")
    print(f"    Sin FechaApunte : {sin_fa:,}")
    print(f"    Sin Area        : {sin_area:,}")

    # 5) Suma de A_Pagar y A_Cobrar (para comparar contra Excel)
    cursor.execute("""
        SELECT
            CAST(SUM(A_Pagar)  AS decimal(18,2)) AS total_pagar,
            CAST(SUM(A_Cobrar) AS decimal(18,2)) AS total_cobrar
        FROM costos
    """)
    tp, tc = cursor.fetchone()
    print(f"\n  Sumas globales (para checksum contra Excel):")
    print(f"    Total A_Pagar  : {tp:,}")
    print(f"    Total A_Cobrar : {tc:,}")

    # 6) JSON vs SQL — distribución por año
    print(f"\n  Distribución por año (FechaApunte) — JSON original:")
    contador = Counter()
    for r in registros_hist + registros_act:
        f = parse_fecha(r.get("FechaApunte"))
        if f:
            contador[f.year] += 1
    for anio in sorted(contador):
        print(f"    {anio}: {contador[anio]:,}")

    print(f"\n  → Compara estos conteos contra tus Excel locales.")


# ============================================================
# MAIN
# ============================================================

def main():
    print("\n" + "=" * 60)
    print("  MIGRACIÓN A AZURE SQL — costos")
    print("=" * 60)

    if not SQL_PASSWORD:
        print("[ERROR] SQL_PASSWORD no configurado")
        raise SystemExit(1)

    print("\n[1/4] Descargando costos_historico.json.gz...")
    historico = descargar_json_gz("data/costos_historico.json.gz")
    print(f"  {len(historico):,} registros históricos")

    print("\n[2/4] Descargando costos_actual.json.gz...")
    actual = descargar_json_gz("data/costos_actual.json.gz")
    print(f"  {len(actual):,} registros actuales")

    print("\n[3/4] Conectando a Azure SQL e insertando...")
    conn = get_conn()
    print("  ✓ Conectado")

    if historico:
        print("\n  → Histórico (EsHistorico=1):")
        ok, sin, err, errs = migrar(conn, historico, es_historico=True)
        print(f"    Insertados: {ok:,}  |  Sin ST: {sin:,}  |  Errores: {err:,}")
        for st, e in errs:
            print(f"      ST {st}: {e[:120]}")

    if actual:
        print("\n  → Actual (EsHistorico=0):")
        ok, sin, err, errs = migrar(conn, actual, es_historico=False)
        print(f"    Insertados: {ok:,}  |  Sin ST: {sin:,}  |  Errores: {err:,}")
        for st, e in errs:
            print(f"      ST {st}: {e[:120]}")

    print("\n[4/4] Reconciliación...")
    reconciliacion(conn, historico, actual)

    conn.close()
    print("\n✓ Migración costos completada.\n")


if __name__ == "__main__":
    main()
