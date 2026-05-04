"""
migrar_costos.py
================
Migra el contenido de un archivo Excel local de Costos a la tabla `costos`
en Azure SQL.

Uso:
    # Histórico (EsHistorico = 1):
    python migrar_costos.py --archivo "C:/ruta/CostosAgricampoHist.xlsx" --es-historico

    # Actual (EsHistorico = 0):
    python migrar_costos.py --archivo "C:/ruta/CostosAgricampoActual.xlsx"

Variables de entorno:
    SQL_SERVER, SQL_DATABASE, SQL_USER, SQL_PASSWORD

Reglas de clasificación de Area (recalculadas en cada migración, ignorando
cualquier valor previo en SQL):

    1. BkRecurso ∈ {42012, 83008} con BkHacienda1 ∈ HACIENDAS_COSECHA → Cosecha
    2. BkRecurso ∈ {42004, 40003, 40002, 42007, 40001, 83003}         → Red Vial
       (Lowboy, Cachaza, Ceniza, Bagazo, Basura, Chip)
    3. NombreCuadrilla contiene "RED VIAL"                             → Red Vial
    4. Flota (extraída de Equipo) ∈ {VOLQUETA, CABEZAL, LOWBOY}        → Red Vial
    5. BkRecurso ∈ {42012, 83008} y no se cumple regla 1               → Varios
    6. Flota ∈ {PICK UP, BUS, CAMION, CISTERNA}                        → Varios
    7. BkRecurso ∈ {9013, 83007, 83005, 83006, 41002, 42008, 42001}    → Varios
    8. Caso contrario                                                  → No Definido
"""

import os
import sys
import re
import argparse
import math
from datetime import datetime
from collections import Counter


# ============================================================
# CONFIGURACIÓN
# ============================================================

SQL_SERVER   = os.environ.get("SQL_SERVER",   "autransp-server.database.windows.net")
SQL_DATABASE = os.environ.get("SQL_DATABASE", "autransp-db")
SQL_USER     = os.environ.get("SQL_USER",     "autransp_admin")
SQL_PASSWORD = os.environ.get("SQL_PASSWORD", "")

HACIENDAS_COSECHA = {2996, 1984, 2983, 1994, 2984, 1983, 1987, 2987}

# Recursos que SIEMPRE son Red Vial (regla 2)
RECURSOS_RED_VIAL_FIJO = {"42004", "40003", "40002", "42007", "40001", "83003"}

# Recursos cuyo destino depende de la hacienda (regla 1)
RECURSOS_COSECHA_CONDICIONAL = {"42012", "83008"}

# Recursos que SIEMPRE son Varios cuando no se cumplió regla 3 ni 4
RECURSOS_VARIOS_FIJO = {"9013", "83007", "83005", "83006", "41002", "42008", "42001"}

FLOTAS_RED_VIAL = {"VOLQUETA", "CABEZAL", "LOWBOY"}


# ============================================================
# UTILIDADES
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


def limpiar(v):
    """Texto: None, NaN, '', 'nan' → None."""
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    s = str(v).strip()
    if s in ("", "nan", "NaN", "None", "NaT"):
        return None
    return s


def num(v):
    """Decimal: None / no parseable → None."""
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


def parse_fecha(v):
    """Cualquier fecha → datetime."""
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    s = str(v).strip()
    if s in ("", "nan", "NaN", "None", "NaT"):
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y"):
        try:
            return datetime.strptime(s[:19], fmt)
        except ValueError:
            continue
    return None


# ============================================================
# RECUPERAR ST DESDE OBSERVACION
# ============================================================

REGEX_ST = re.compile(r"ST:\s*(\d+)", re.IGNORECASE)


def resolver_st(row: dict):
    """
    Intenta resolver el ST en este orden:
      1. Campo ST (si tiene valor)
      2. Campo ID_ST (si tiene valor)
      3. Regex 'ST: NNNN' en Observacion
    Retorna string o None.
    """
    st = limpiar(row.get("ST"))
    if st:
        try:
            return str(int(float(st)))  # normaliza '61562.0' → '61562'
        except (ValueError, TypeError):
            return st

    id_st = limpiar(row.get("ID_ST"))
    if id_st:
        try:
            return str(int(float(id_st)))
        except (ValueError, TypeError):
            return id_st

    obs = row.get("Observacion")
    if isinstance(obs, str):
        m = REGEX_ST.search(obs)
        if m:
            return m.group(1)

    return None


# ============================================================
# EXTRAER FLOTA / SUBFLOTA DEL CAMPO Equipo
# ============================================================

def extraer_flota(equipo_txt):
    """Devuelve (flota, subflota). flota = None si no se reconoce."""
    if not equipo_txt:
        return None, None
    s = equipo_txt.strip().upper()

    # Patrones en orden de longitud (más específico primero)
    if s.startswith("CAMION CISTERNA") or s.startswith("CAMIÓN CISTERNA"):
        resto = s.split(maxsplit=2)[2] if len(s.split()) >= 3 else ""
        if resto.startswith("2 EJES"):
            return "CISTERNA", "2 EJES"
        if resto.startswith("3 EJES"):
            return "CISTERNA", "3 EJES"
        return "CISTERNA", "CISTERNA"

    if s.startswith("CAMION LIVIANO") or s.startswith("CAMIÓN LIVIANO"):
        return "CAMION", "LIVIANO"
    if s.startswith("CAMION PESADO") or s.startswith("CAMIÓN PESADO"):
        return "CAMION", "PESADO"
    if s.startswith("CISTERNA 2 EJES"):
        return "CISTERNA", "2 EJES"
    if s.startswith("CISTERNA 3 EJES"):
        return "CISTERNA", "3 EJES"
    if s.startswith("CISTERNA"):
        return "CISTERNA", "CISTERNA"
    if s.startswith("PICK UP") or s.startswith("PICKUP") or s.startswith("PICK-UP"):
        return "PICK UP", "PICK UP"
    if s.startswith("BUS"):
        return "BUS", "BUS"
    if s.startswith("VOLQUETA"):
        return "VOLQUETA", "VOLQUETA"
    if s.startswith("CABEZAL"):
        return "CABEZAL", "CABEZAL"
    if s.startswith("LOWBOY"):
        return "LOWBOY", "LOWBOY"
    if s.startswith("PIPA"):
        return "CISTERNA", "CISTERNA"
    if s.startswith("CAMION") or s.startswith("CAMIÓN"):
        return "CAMION", "CAMION"   # genérico cuando no especifica liviano/pesado

    return None, None


# ============================================================
# CLASIFICAR AREA
# ============================================================

def clasificar_area(row: dict, flota: str, nombre_cuadrilla: str):
    """Aplica reglas en orden, devuelve string."""
    bk_recurso = limpiar(row.get("BkRecurso"))

    # 1. Hacienda Cosecha + recurso condicional → Cosecha
    if bk_recurso in RECURSOS_COSECHA_CONDICIONAL:
        try:
            hda = int(float(str(row.get("BkHacienda1") or "")))
            if hda in HACIENDAS_COSECHA:
                return "Cosecha"
        except (ValueError, TypeError):
            pass

    # 2. Recursos Red Vial fijos
    if bk_recurso in RECURSOS_RED_VIAL_FIJO:
        return "Red Vial"

    # 3. Cuadrilla menciona "RED VIAL"
    if nombre_cuadrilla and "RED VIAL" in nombre_cuadrilla.upper():
        return "Red Vial"

    # 4. Flota es Volqueta / Cabezal / Lowboy
    if flota in FLOTAS_RED_VIAL:
        return "Red Vial"

    # 5. Recurso condicional pero sin hacienda Cosecha
    if bk_recurso in RECURSOS_COSECHA_CONDICIONAL:
        return "Varios"

    # 6. Flota ligera/pesada/cisterna sin cuadrilla Red Vial
    if flota in {"PICK UP", "BUS", "CAMION", "CISTERNA"}:
        return "Varios"

    # 7. Recursos varios fijos
    if bk_recurso in RECURSOS_VARIOS_FIJO:
        return "Varios"

    return "No Definido"


# ============================================================
# LECTURA DEL EXCEL
# ============================================================

def leer_archivo(ruta_archivo: str):
    """Generator que devuelve dicts con headers como keys.
    Acepta: .xlsx, .xlsm, .csv, .csv.gz"""
    import csv as _csv
    _csv.field_size_limit(10_000_000)
    print(f"  Abriendo {ruta_archivo}...", flush=True)

    nombre_lower = ruta_archivo.lower()

    # CSV gzipeado
    if nombre_lower.endswith(".csv.gz"):
        import gzip
        import io
        with gzip.open(ruta_archivo, "rb") as gz:
            text = io.TextIOWrapper(gz, encoding="utf-8-sig", newline="")
            reader = _csv.reader(text, delimiter=";")
            headers = list(next(reader))
            print(f"  CSV.gz con {len(headers)} columnas", flush=True)
            for row in reader:
                if len(row) < len(headers):
                    row = row + [""] * (len(headers) - len(row))
                yield dict(zip(headers, row))
        return

    ext = os.path.splitext(ruta_archivo)[1].lower()

    if ext == ".csv":
        with open(ruta_archivo, "r", encoding="utf-8-sig", newline="") as f:
            reader = _csv.reader(f, delimiter=";")
            headers = list(next(reader))
            print(f"  CSV con {len(headers)} columnas", flush=True)
            for row in reader:
                if len(row) < len(headers):
                    row = row + [""] * (len(headers) - len(row))
                yield dict(zip(headers, row))
        return

    # Excel (.xlsx / .xlsm)
    import openpyxl
    wb = openpyxl.load_workbook(ruta_archivo, data_only=True, read_only=True)
    ws = wb["Apuntamientos"]
    rows = ws.iter_rows(values_only=True)
    headers = list(next(rows))
    print(f"  Excel con {len(headers)} columnas", flush=True)

    for row in rows:
        yield dict(zip(headers, row))

    wb.close()


# ============================================================
# UPSERT
# ============================================================

COLS = [
    "ST",
    # bandera
    "EsHistorico",
    # JSON / Excel directo
    "FechaApunte", "OS1", "DsEstadoOS", "ID_ST", "IdAgrupaST", "IdProforma",
    "Area",                 # CALCULADA
    "BkActividad", "DsActividad", "BkHacienda1", "Hacienda1",
    "BkLote", "BkRecurso", "Recurso", "BkProveedor", "Proveedor",
    "CodEquipo", "Equipo",
    "Flota", "Subflota",    # CALCULADAS desde Equipo
    "OdometroInicio", "OdometroFin", "Distancia",
    "DuracionHoras", "HorasPermanencia",
    "CantidadCargadores", "Paquetes", "CantidadPago1",
    "A_Cobrar", "A_Pagar", "ValorPagoComplemento", "ValorPagoPermanencia",
    "Observacion",
    "IntegracionPago", "IntegracionCobro",
    "FechaIntegracionSAPPago", "FechaIntegracionSAPCobro",
    "ReferenciaPagoSAP", "ReferenciaCobroSAP",
    "SociedadGestora", "UsuarioDigita",
    # Pieza 2A — extras del Excel crudo
    "IdApMaquinaria", "DsProgramacion", "BkZafra", "BkUniMed",
    "Pasos", "AreaAct", "EquipoArrastre",
    "CodigoTrabajador", "NombreTrabajador",
    "CodAyudante", "NomAyudante", "CCNomAyudante",
    "Cuadrilla", "NombreCuadrilla",
    "UnidadDeRecurso", "A_COB_UNI", "A_PAG_UNI", "ValorTotalViaje",
    "MATERIALSAP", "DsDocumento", "NumDocumento",
    "EnvioPago", "EnvioCobro", "FacturaPago", "FacturaCobro",
    "TipoDocInternoDS", "NumDocInterno", "CCNomina",
    "Agrupador", "Financiado", "CantidadBloqueos", "Activo",
    "IdEnvioVario", "PesoVario", "FechaDigita",
]

_insert_cols  = ", ".join(COLS) + ", FechaActualizacion"
_insert_vals  = ", ".join("?" for _ in COLS) + ", GETUTCDATE()"

# INSERT directo. La tabla se vacía con TRUNCATE al inicio del migrador.
# Si el Excel trae STs duplicados, el migrador los detecta y warn (Python-side,
# antes de mandar a SQL) — no llegan al INSERT.
INSERT_SQL = f"""
INSERT INTO costos ({_insert_cols}) VALUES ({_insert_vals});
"""


def mapear(row: dict, es_historico: bool, respetar_area: bool = False):
    """Construye la tupla de valores para el INSERT.

    Args:
        respetar_area: si True, usa el campo `Area` del Excel tal cual.
                       Si False (default), recalcula con las reglas.
    """
    st = resolver_st(row)
    if not st:
        return None

    flota, subflota = extraer_flota(limpiar(row.get("Equipo")))
    nombre_cuadrilla = limpiar(row.get("NombreCuadrilla"))

    if respetar_area:
        # Histórico: usar lo que viene del Excel (ya clasificado por el equipo)
        area_final = limpiar(row.get("Area"))
    else:
        # Actual / fuente sin clasificar: aplicar las reglas
        area_final = clasificar_area(row, flota, nombre_cuadrilla)

    valores = [
        st,
        1 if es_historico else 0,
        parse_fecha(row.get("FechaApunte")),
        limpiar(row.get("OS1")),
        limpiar(row.get("DsEstadoOS")),
        limpiar(row.get("ID_ST")),
        limpiar(row.get("IdAgrupaST")),
        limpiar(row.get("IdProforma")),
        area_final,
        limpiar(row.get("BkActividad")),
        limpiar(row.get("DsActividad")),
        limpiar(row.get("BkHacienda1")),
        limpiar(row.get("Hacienda1")),
        limpiar(row.get("BkLote")),
        limpiar(row.get("BkRecurso")),
        limpiar(row.get("Recurso")),
        limpiar(row.get("BkProveedor")),
        limpiar(row.get("Proveedor")),
        limpiar(row.get("CodEquipo")),
        limpiar(row.get("Equipo")),
        flota,
        subflota,
        num(row.get("OdometroInicio")),
        num(row.get("OdometroFin")),
        num(row.get("Distancia")),
        num(row.get("DuracionHoras")),
        num(row.get("HorasPermanencia")),
        entero(row.get("CantidadCargadores")),
        num(row.get("Paquetes")),
        num(row.get("CantidadPago1")),
        num(row.get("A_COBRAR")),
        num(row.get("A_PAGAR")),
        num(row.get("ValorPagoComplemento")),
        num(row.get("ValorPagoPermanencia")),
        limpiar(row.get("Observacion")),
        limpiar(row.get("IntegracionPago")),
        limpiar(row.get("IntegracionCobro")),
        parse_fecha(row.get("FechaIntegracionSAPPago")),
        parse_fecha(row.get("FechaIntegracionSAPCobro")),
        limpiar(row.get("ReferenciaPagoSAP")),
        limpiar(row.get("ReferenciaCobroSAP")),
        limpiar(row.get("SociedadGestora")),
        limpiar(row.get("UsuarioDigita")),
        # Pieza 2A
        entero(row.get("IdApMaquinaria")),
        limpiar(row.get("DsProgramacion")),
        entero(row.get("BkZafra")),
        limpiar(row.get("BkUniMed")),
        entero(row.get("Pasos")),
        num(row.get("AreaAct")),
        limpiar(row.get("EquipoArrastre")),
        limpiar(row.get("CodigoTrabajador")),
        limpiar(row.get("NombreTrabajador")),
        limpiar(row.get("CodAyudante")),
        limpiar(row.get("NomAyudante")),
        limpiar(row.get("CCNomAyudante")),
        limpiar(row.get("Cuadrilla")),
        nombre_cuadrilla,
        limpiar(row.get("UnidadDeRecurso")),
        num(row.get("A_COB_UNI")),
        num(row.get("A_PAG_UNI")),
        num(row.get("Textbox61")),                  # mapea a ValorTotalViaje
        limpiar(row.get("MATERIALSAP")),
        limpiar(row.get("DsDocumento")),
        limpiar(row.get("NumDocumento")),
        limpiar(row.get("EnvioPago")),
        limpiar(row.get("EnvioCobro")),
        limpiar(row.get("FacturaPago")),
        limpiar(row.get("FacturaCobro")),
        limpiar(row.get("TipoDocInternoDS")),
        limpiar(row.get("NumDocInterno")),
        limpiar(row.get("CCNomina")),
        entero(row.get("agrupador")),
        limpiar(row.get("Financiado")),
        entero(row.get("CantidadBloqueos")),
        limpiar(row.get("Activo")),
        entero(row.get("IdEnvioVario")),
        num(row.get("PesoVario")),
        parse_fecha(row.get("FechaDigita")),
    ]

    return tuple(valores)


# ============================================================
# MIGRACIÓN
# ============================================================

def migrar(conn, registros_iter, es_historico: bool,
           respetar_area: bool = False,
           truncate: bool = True,
           batch_size: int = 500):
    """
    Migra registros del Excel a costos.

    - Si truncate=True (default): TRUNCATE TABLE costos antes de insertar.
    - Si respetar_area=True: usa la columna Area del Excel sin recalcular.
    - Detecta STs duplicados en el Excel y los warn-y-continúa
      (inserta el primero, ignora el resto, los reporta al final).
    """
    cursor = conn.cursor()
    cursor.fast_executemany = True

    if truncate:
        print("  Ejecutando TRUNCATE TABLE costos...", flush=True)
        cursor.execute("TRUNCATE TABLE costos")
        conn.commit()
        print("  ✓ Tabla vacía, listo para insertar", flush=True)
    else:
        print("  Modo append (--no-truncate): no se vacía la tabla", flush=True)

    if respetar_area:
        print("  Modo --respetar-area: se conserva el campo Area del Excel", flush=True)
    else:
        print("  Modo recalcular: Area se calcula con las reglas", flush=True)

    ok = 0
    sin_st = 0
    err = 0
    duplicados = 0          # STs repetidos en el Excel — solo se inserta el primero
    sts_vistos = set()
    primeros_errores = []
    primeros_duplicados = []
    distrib_area = Counter()

    # Índice de Area en la tupla de valores:
    # COLS = [ST, EsHistorico, FechaApunte, OS1, DsEstadoOS, ID_ST,
    #         IdAgrupaST, IdProforma, Area, ...]
    #         0   1            2            3    4           5
    #         6           7           8
    IDX_AREA = 8

    for row in registros_iter:
        try:
            vals = mapear(row, es_historico, respetar_area=respetar_area)
            if vals is None:
                sin_st += 1
                continue

            st_actual = vals[0]
            if st_actual in sts_vistos:
                duplicados += 1
                if len(primeros_duplicados) < 5:
                    primeros_duplicados.append(st_actual)
                continue
            sts_vistos.add(st_actual)

            cursor.execute(INSERT_SQL, vals)
            ok += 1
            distrib_area[vals[IDX_AREA]] += 1

            if ok % batch_size == 0:
                conn.commit()
                if ok % 5000 == 0:
                    print(f"    → {ok:,} procesados...", flush=True)
        except Exception as e:
            err += 1
            if len(primeros_errores) < 3:
                primeros_errores.append((row.get("ST") or row.get("ID_ST"), str(e)))

    conn.commit()
    return ok, sin_st, err, duplicados, primeros_errores, primeros_duplicados, distrib_area


# ============================================================
# RECONCILIACIÓN
# ============================================================

def reconciliacion(conn, es_historico: bool):
    cursor = conn.cursor()

    print("\n" + "=" * 60)
    print("  RECONCILIACIÓN — costos")
    print("=" * 60)

    cursor.execute("SELECT COUNT(*) FROM costos")
    total = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM costos WHERE EsHistorico = 1")
    hist = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM costos WHERE EsHistorico = 0")
    act = cursor.fetchone()[0]

    print(f"\n  Total SQL : {total:,}")
    print(f"  Histórico : {hist:,}")
    print(f"  Actual    : {act:,}")

    print(f"\n  Distribución por Area:")
    cursor.execute("""
        SELECT Area, EsHistorico, COUNT(*) AS n
        FROM costos GROUP BY Area, EsHistorico
        ORDER BY Area, EsHistorico
    """)
    for row in cursor.fetchall():
        flag = "Hist" if row[1] else "Act"
        print(f"    {row[0] or '(NULL)':15s} {flag}: {row[2]:,}")

    print(f"\n  Distribución por año (FechaApunte):")
    cursor.execute("""
        SELECT YEAR(FechaApunte) AS anio, COUNT(*) AS n
        FROM costos
        WHERE FechaApunte IS NOT NULL
        GROUP BY YEAR(FechaApunte)
        ORDER BY anio
    """)
    for row in cursor.fetchall():
        print(f"    {row[0]}: {row[1]:,}")

    cursor.execute("""
        SELECT
            CAST(SUM(A_Pagar)  AS decimal(18,2)),
            CAST(SUM(A_Cobrar) AS decimal(18,2)),
            CAST(SUM(ValorTotalViaje) AS decimal(18,2))
        FROM costos
    """)
    tp, tc, tv = cursor.fetchone()
    print(f"\n  Sumas globales (checksum):")
    print(f"    Total A_Pagar         : {tp:,}")
    print(f"    Total A_Cobrar        : {tc:,}")
    print(f"    Total ValorTotalViaje : {tv:,}")

    print(f"\n  Compara contra tu Excel local.")


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Migra costos desde Excel/CSV a Azure SQL.")
    parser.add_argument("--archivo", required=True, help="Ruta al archivo (.xlsx, .csv, .csv.gz)")
    parser.add_argument("--es-historico", action="store_true",
                        help="Marcar registros con EsHistorico=1 (default: 0).")
    parser.add_argument("--respetar-area", action="store_true",
                        help="Usar el campo Area del Excel sin recalcular.")
    parser.add_argument("--no-truncate", action="store_true",
                        help="No vaciar la tabla antes (para concatenar histórico + actual).")
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("  MIGRACIÓN A AZURE SQL — costos")
    print(f"  Archivo        : {args.archivo}")
    print(f"  EsHistorico    : {1 if args.es_historico else 0}")
    print(f"  Respetar Area  : {args.respetar_area}")
    print(f"  Truncate       : {not args.no_truncate}")
    print("=" * 60)

    if not SQL_PASSWORD:
        print("[ERROR] Falta SQL_PASSWORD en variables de entorno.")
        sys.exit(1)

    if not os.path.exists(args.archivo):
        print(f"[ERROR] No se encontró: {args.archivo}")
        sys.exit(1)

    print("\n[1/3] Leyendo y procesando...")
    conn = get_conn()
    print("  ✓ Conectado a SQL")

    registros = leer_archivo(args.archivo)

    print("\n[2/3] Insertando...")
    ok, sin, err, dups, errs, primeros_dups, distrib = migrar(
        conn, registros, args.es_historico,
        respetar_area=args.respetar_area,
        truncate=not args.no_truncate,
    )

    print(f"\n  Insertados            : {ok:,}")
    print(f"  Sin ST (descartados)  : {sin:,}")
    print(f"  ST duplicados (warn)  : {dups:,}")
    print(f"  Errores               : {err:,}")
    if errs:
        print(f"  Primeros errores:")
        for st, e in errs:
            print(f"    ST {st}: {e[:150]}")
    if primeros_dups:
        print(f"  Primeros STs duplicados (revisar en Excel):")
        for st in primeros_dups:
            print(f"    ST {st}")

    print(f"\n  Distribución de Area en este lote:")
    for a, n in distrib.most_common():
        print(f"    {a or '(NULL)':15s}: {n:,}")

    print("\n[3/3] Reconciliación contra SQL...")
    reconciliacion(conn, args.es_historico)

    conn.close()
    print("\n✓ Migración completada.\n")


if __name__ == "__main__":
    main()
