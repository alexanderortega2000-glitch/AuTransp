"""
migrar_prog.py
==============
Migra el contenido de un archivo Excel o CSV de Programación a la tabla
`programacion` en Azure SQL.

Uso:
    # Histórico (no se usa EsHistorico aquí, pero ActualizadoPor lo distingue):
    python migrar_prog.py --archivo "C:/ruta/prog_historico.csv" --etiqueta historico

    # Actual:
    python migrar_prog.py --archivo "C:/ruta/prog_actual.xlsx" --etiqueta actual

Variables de entorno:
    SQL_SERVER, SQL_DATABASE, SQL_USER, SQL_PASSWORD

Autodetecta formato por extensión:
    .csv  → asume delimitador ';' UTF-8 con BOM (formato exportado de Excel ES)
    .xlsx → openpyxl read-only

Convenciones aplicadas:
  - snake_case (CSV/Excel)  →  PascalCase (SQL)
  - Coordinador prefiere `coordinador` sobre `coordinador_archivo`
  - `comentarios` + `comentario` se funden en Comentarios (con separador)
  - Fechas + horas se combinan en datetime2 unificado
  - Campos descartados: archivo_fuente, en_seguimiento, tipo_solicitud,
    coordinador_archivo (alias), comentario (fundido en Comentarios)
"""

import os
import sys
import csv
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
    """Texto limpio o None.
    '0' se trata como NULL solo en columnas string (reduce ruido del Excel original)."""
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    s = str(v).strip()
    if s in ("", "nan", "NaN", "None", "NaT", "0"):
        return None
    return s


def texto_libre(v):
    """Como limpiar pero conserva '0' por si era contenido válido."""
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    s = str(v).strip()
    if s in ("", "nan", "NaN", "None", "NaT"):
        return None
    return s


def num(v):
    if v is None: return None
    if isinstance(v, (int, float)) and not (isinstance(v, float) and math.isnan(v)):
        return float(v)
    s = str(v).strip().replace(",", ".")
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
    """YYYY-MM-DD o variantes → datetime a medianoche."""
    if v is None: return None
    if isinstance(v, datetime): return v
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


def combinar_fecha_hora(fecha_v, hora_v):
    """Combina fecha + hora ('HH:MM:SS') en datetime2.
    Sin fecha → None. Sin hora → fecha a 00:00."""
    f = parse_fecha_sola(fecha_v)
    if not f:
        return None
    if hora_v is None:
        return f
    s = str(hora_v).strip()
    if s in ("", "nan", "NaN", "None", "NaT", "0"):
        return f
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            t = datetime.strptime(s, fmt).time()
            return f.replace(hour=t.hour, minute=t.minute, second=t.second)
        except ValueError:
            continue
    return f


def fundir_comentarios(*partes):
    """Funde valores en string único, ignorando duplicados, '0', 'N/A'."""
    vistos = []
    for p in partes:
        v = texto_libre(p)
        if v and v not in vistos and v.upper() not in ("0", "N/A"):
            vistos.append(v)
    return " | ".join(vistos) if vistos else None


# ============================================================
# LECTURA — autodetecta CSV o Excel
# ============================================================

def leer_archivo(ruta: str):
    """Generator que rinde dicts con headers en snake_case.
    Acepta .csv, .csv.gz, .xlsx, .xlsm"""
    nombre_lower = ruta.lower()
    csv.field_size_limit(10_000_000)

    if nombre_lower.endswith(".csv.gz"):
        import gzip
        import io
        # Lee gzip y decodifica UTF-8 con BOM
        with gzip.open(ruta, "rb") as gz:
            text = io.TextIOWrapper(gz, encoding="utf-8-sig", newline="")
            reader = csv.reader(text, delimiter=";")
            headers = next(reader)
            print(f"  CSV.gz con {len(headers)} columnas", flush=True)
            for row in reader:
                if len(row) < len(headers):
                    row = row + [""] * (len(headers) - len(row))
                yield dict(zip(headers, row))
        return

    ext = os.path.splitext(ruta)[1].lower()

    if ext == ".csv":
        with open(ruta, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f, delimiter=";")
            headers = next(reader)
            print(f"  CSV con {len(headers)} columnas", flush=True)
            for row in reader:
                if len(row) < len(headers):
                    row = row + [""] * (len(headers) - len(row))
                yield dict(zip(headers, row))

    elif ext in (".xlsx", ".xlsm"):
        import openpyxl
        wb = openpyxl.load_workbook(ruta, data_only=True, read_only=True)
        ws = wb.active
        rows = ws.iter_rows(values_only=True)
        headers = list(next(rows))
        print(f"  Excel '{ws.title}' con {len(headers)} columnas", flush=True)
        for row in rows:
            yield dict(zip(headers, row))
        wb.close()

    else:
        raise ValueError(f"Extensión no soportada: {ext}")


# ============================================================
# UPSERT
# ============================================================

COLS = [
    "ST",
    # originales
    "Coordinador", "ProveedorTransp", "Subflota", "FechaEjecucion",
    "TipoProg", "NombreOrigen", "NombreDestino", "LoteDestino",
    "Zona", "Area", "Turno", "ActualizadoPor",
    # Pieza 1
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
    # Pieza 2A
    "EstatusST", "MotivoST", "KmEstimado", "KmFueraST",
    "CostoCargador", "CostoEstadia", "CostoCarga",
    "Flota",
    "FechaHoraRealInicio", "FechaHoraRealLlegada",
    "EstatusViaje",
]

_insert_cols  = ", ".join(COLS) + ", FechaActualizacion"
_insert_vals  = ", ".join("?" for _ in COLS) + ", GETUTCDATE()"

# INSERT directo (sin MERGE): cada fila del CSV/Excel = una fila en SQL.
# Razón: un ST puede tener N filas (multi-producto / multi-destino).
# La tabla debe estar vacía antes (TRUNCATE al inicio del migrador).
INSERT_SQL = f"""
INSERT INTO programacion ({_insert_cols}) VALUES ({_insert_vals});
"""


def mapear(r: dict, etiqueta: str):
    """Mapea fila CSV/Excel → tupla para UPSERT."""
    st = limpiar(r.get("st"))
    if not st:
        return None
    try:
        st = str(int(float(st)))   # normaliza '114647.0' → '114647'
    except (ValueError, TypeError):
        pass

    # Coordinador prefiere `coordinador` sobre `coordinador_archivo`
    coord = limpiar(r.get("coordinador")) or limpiar(r.get("coordinador_archivo"))

    # Comentarios fundidos
    coments = fundir_comentarios(r.get("comentarios"), r.get("comentario"))

    valores = [
        st,
        # === originales ===
        coord,
        limpiar(r.get("proveedor_transp")),
        limpiar(r.get("subflota")),
        parse_fecha_sola(r.get("fecha_ejecucion")),
        limpiar(r.get("tipo_prog")),
        limpiar(r.get("nomb_origen")),
        limpiar(r.get("nom_destino")),
        limpiar(r.get("lote_destino")),
        limpiar(r.get("zona")),
        limpiar(r.get("area_programada")),     # Area (texto, fallback)
        limpiar(r.get("turno")),
        f"migracion_{etiqueta}",                # ActualizadoPor
        # === Pieza 1 ===
        parse_fecha_sola(r.get("semana_programa")),
        parse_fecha_sola(r.get("fecha_solicitud")),
        limpiar(r.get("jefe")),
        limpiar(r.get("resp_prod")),
        limpiar(r.get("cod_actividad")),
        limpiar(r.get("actividad")),
        limpiar(r.get("proveedor")),
        limpiar(r.get("sociedad")),
        limpiar(r.get("os")),
        limpiar(r.get("cod_recurso_carga")),
        limpiar(r.get("nomb_recurso_carga")),
        limpiar(r.get("um_recurso_carga")),
        limpiar(r.get("implemento")),
        limpiar(r.get("tipo_aplicacion")),
        limpiar(r.get("recurso_ejecucion")),
        num(r.get("cantidad_programada")),
        num(r.get("area_programada")),
        num(r.get("dosis_programada")),
        num(r.get("sacos")),
        entero(r.get("cargadores")),
        limpiar(r.get("cod_recurso_serv")),
        limpiar(r.get("nomb_recurso_serv")),
        limpiar(r.get("cod_proveedor_transp")),
        limpiar(r.get("cod_origen")),
        limpiar(r.get("cod_destino")),
        limpiar(r.get("cod_recep_costo")),
        limpiar(r.get("nomb_recep_costo")),
        limpiar(r.get("lote_recep_costo")),
        coments,
        combinar_fecha_hora(r.get("fecha_solicitud"), r.get("hora_recepcion")),
        combinar_fecha_hora(r.get("fecha_ejecucion"), r.get("hora_plan_inicio")),
        combinar_fecha_hora(r.get("fecha_ejecucion"), r.get("hora_plan_llegada")),
        limpiar(r.get("tipo_grupo")),
        limpiar(r.get("grupo")),
        limpiar(r.get("encargado_grupo")),
        limpiar(r.get("contacto")),
        limpiar(r.get("cod_maquina")),
        limpiar(r.get("nomb_recurso_carga")),    # Producto (alias)
        limpiar(r.get("um_recurso_carga")),       # UM (alias)
        limpiar(r.get("controlador_seg")),        # Controlador
        texto_libre(r.get("motivo_retraso_inicio")),
        texto_libre(r.get("motivo_retraso_entrega")),
        texto_libre(r.get("motivo_estatus_viaje")),
        limpiar(r.get("documento_traslado")),
        texto_libre(r.get("motivo_devolucion")),
        num(r.get("cantidad_devolucion")),
        num(r.get("bultos_devolucion")),
        texto_libre(r.get("nota_devolucion")),
        texto_libre(r.get("comentario_cabina")),
        texto_libre(r.get("comentario_integracion")),
        # === Pieza 2A ===
        limpiar(r.get("estatus_st")),
        texto_libre(r.get("motivo_st")),
        num(r.get("km_estimado")),
        num(r.get("km_fuera_st")),
        num(r.get("costo_cargador")),
        num(r.get("costo_estadia")),
        num(r.get("costo_carga")),
        limpiar(r.get("flota")),
        combinar_fecha_hora(r.get("fecha_ejecucion"), r.get("hora_real_inicio")),
        combinar_fecha_hora(r.get("fecha_ejecucion"), r.get("hora_real_llegada")),
        limpiar(r.get("estatus_viaje")),
    ]

    return tuple(valores)


# ============================================================
# MIGRACIÓN
# ============================================================

def migrar(conn, registros_iter, etiqueta: str, truncate: bool = True, batch_size: int = 1000):
    cursor = conn.cursor()
    cursor.fast_executemany = True

    if truncate:
        print("  Ejecutando TRUNCATE TABLE programacion...", flush=True)
        cursor.execute("TRUNCATE TABLE programacion")
        conn.commit()
        print("  ✓ Tabla vacía, listo para insertar", flush=True)
        filas_a_saltear = 0
    else:
        # Contar filas ya insertadas y saltear ese número al inicio
        cursor.execute("SELECT COUNT(*) FROM programacion")
        filas_a_saltear = cursor.fetchone()[0]
        print(f"  Modo append: saltando las primeras {filas_a_saltear:,} filas ya en SQL", flush=True)

    ok = 0
    saltados = 0
    sin_st = 0
    err = 0
    primeros_errores = []
    distrib_tipo = Counter()
    IDX_TIPOPROG = 5
    lote = []

    def flush_lote():
        nonlocal ok, err, primeros_errores
        if not lote:
            return
        try:
            cursor.executemany(INSERT_SQL, lote)
            conn.commit()
            ok += len(lote)
        except Exception:
            conn.rollback()
            for vals in lote:
                try:
                    cursor.execute(INSERT_SQL, vals)
                    conn.commit()
                    ok += 1
                except Exception as e2:
                    err += 1
                    if len(primeros_errores) < 3:
                        primeros_errores.append((vals[0], str(e2)[:120]))
        lote.clear()

    filas_leidas = 0
    for row in registros_iter:
        try:
            vals = mapear(row, etiqueta)
            if vals is None:
                sin_st += 1
                continue

            filas_leidas += 1

            # Saltear filas ya insertadas en runs anteriores
            if filas_leidas <= filas_a_saltear:
                saltados += 1
                continue

            lote.append(vals)
            distrib_tipo[vals[IDX_TIPOPROG]] += 1

            if len(lote) >= batch_size:
                flush_lote()
                if ok % 5000 == 0 and ok > 0:
                    print(f"    → {ok:,} procesados...", flush=True)

        except Exception as e:
            err += 1
            if len(primeros_errores) < 3:
                primeros_errores.append((row.get("st"), str(e)))

    flush_lote()
    return ok, saltados, sin_st, err, primeros_errores, distrib_tipo


# ============================================================
# RECONCILIACIÓN
# ============================================================

def reconciliacion(conn):
    cursor = conn.cursor()

    print("\n" + "=" * 60)
    print("  RECONCILIACIÓN — programacion")
    print("=" * 60)

    cursor.execute("SELECT COUNT(*) FROM programacion")
    print(f"\n  Total SQL: {cursor.fetchone()[0]:,}")

    print(f"\n  Distribución por TipoProg:")
    cursor.execute("""
        SELECT TipoProg, COUNT(*) AS n
        FROM programacion
        GROUP BY TipoProg
        ORDER BY n DESC
    """)
    for row in cursor.fetchall():
        print(f"    {(row[0] or '(NULL)'):20s}: {row[1]:,}")

    print(f"\n  Distribución por año (SemanaPrograma):")
    cursor.execute("""
        SELECT YEAR(SemanaPrograma) AS anio, COUNT(*) AS n
        FROM programacion
        WHERE SemanaPrograma IS NOT NULL
        GROUP BY YEAR(SemanaPrograma)
        ORDER BY anio
    """)
    for row in cursor.fetchall():
        print(f"    {row[0]}: {row[1]:,}")

    print(f"\n  Distribución por Zona:")
    cursor.execute("""
        SELECT Zona, COUNT(*) AS n
        FROM programacion
        GROUP BY Zona
        ORDER BY n DESC
    """)
    for row in cursor.fetchall():
        print(f"    {(row[0] or '(NULL)'):20s}: {row[1]:,}")

    cursor.execute("""
        SELECT
            SUM(CASE WHEN ST IS NULL OR ST='' THEN 1 ELSE 0 END),
            SUM(CASE WHEN FechaEjecucion IS NULL THEN 1 ELSE 0 END),
            SUM(CASE WHEN Coordinador IS NULL THEN 1 ELSE 0 END),
            SUM(CASE WHEN TipoProg IS NULL THEN 1 ELSE 0 END)
        FROM programacion
    """)
    sst, sfe, sco, sti = cursor.fetchone()
    print(f"\n  NULLs en campos críticos:")
    print(f"    Sin ST            : {sst:,}")
    print(f"    Sin FechaEjecucion: {sfe:,}")
    print(f"    Sin Coordinador   : {sco:,}")
    print(f"    Sin TipoProg      : {sti:,}")

    print(f"\n  Compara contra tu archivo local.")


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Migra programación desde CSV/Excel a Azure SQL.")
    parser.add_argument("--archivo", required=True, help="Ruta al CSV (.csv) o Excel (.xlsx)")
    parser.add_argument("--etiqueta", default="historico",
                        choices=["historico", "actual"],
                        help="Etiqueta para ActualizadoPor (default: historico).")
    parser.add_argument("--no-truncate", action="store_true",
                        help="No vaciar la tabla antes de insertar (para concatenar múltiples archivos).")
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("  MIGRACIÓN A AZURE SQL — programacion")
    print(f"  Archivo : {args.archivo}")
    print(f"  Etiqueta: {args.etiqueta}")
    print("=" * 60)

    if not SQL_PASSWORD:
        print("[ERROR] Falta SQL_PASSWORD en variables de entorno.")
        sys.exit(1)

    if not os.path.exists(args.archivo):
        print(f"[ERROR] No se encontró: {args.archivo}")
        sys.exit(1)

    print("\n[1/3] Conectando y leyendo...")
    conn = get_conn()
    print("  ✓ Conectado a SQL")

    registros = leer_archivo(args.archivo)

    print("\n[2/3] Insertando...")
    ok, saltados, sin, err, errs, distrib = migrar(
        conn, registros, args.etiqueta,
        truncate=not args.no_truncate,
    )
    print(f"\n  Insertados            : {ok:,}")
    print(f"  Saltados (ya en SQL)  : {saltados:,}")
    print(f"  Sin ST (descartados)  : {sin:,}")
    print(f"  Errores                : {err:,}")
    if errs:
        print(f"  Primeros errores:")
        for st, e in errs:
            print(f"    ST {st}: {e[:150]}")

    print(f"\n  Distribución por TipoProg en este lote:")
    for t, n in distrib.most_common():
        print(f"    {(t or '(NULL)'):20s}: {n:,}")

    print("\n[3/3] Reconciliación contra SQL...")
    reconciliacion(conn)

    conn.close()
    print("\n✓ Migración completada.\n")


if __name__ == "__main__":
    main()
