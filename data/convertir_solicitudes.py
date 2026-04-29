"""
convertir_solicitudes.py
Convierte data/solicitudes_planificacion.xlsx → data/solicitudes_planificacion.json.gz
Se ejecuta desde GitHub Actions después de que Power Automate sube el xlsx.
"""

import os
import sys
import json
import gzip
import hashlib
import pandas as pd
from pathlib import Path
from datetime import datetime

XLSX_PATH = Path('data/solicitudes_planificacion.xlsx')
JSON_GZ_PATH = Path('data/solicitudes_planificacion.json.gz')
HASH_PATH = Path('data/solicitudes_planificacion.md5')


def normalizar_fecha(val):
    """Convierte distintos formatos de fecha a YYYY-MM-DD string."""
    if pd.isna(val) or val == '' or val is None:
        return None
    if isinstance(val, (pd.Timestamp, datetime)):
        return val.strftime('%Y-%m-%d')
    s = str(val).strip()
    # DD/MM/YYYY
    for fmt in ('%d/%m/%Y', '%Y-%m-%d', '%m/%d/%Y'):
        try:
            return datetime.strptime(s, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    return s


def limpiar(val):
    """Normaliza valores nulos y strings."""
    if pd.isna(val) or val is None:
        return None
    s = str(val).strip()
    return s if s not in ('', 'nan', 'None', 'NaT', 'NULL') else None


def procesar_xlsx(path: Path) -> list:
    # Leer — intentar primera hoja, si falla intentar hoja 'Hoja1'
    try:
        df = pd.read_excel(path, sheet_name=0, dtype=str)
    except Exception as e:
        print(f'[ERROR] No se pudo leer {path}: {e}')
        sys.exit(1)

    print(f'[OK] Leído: {len(df)} filas, {len(df.columns)} columnas')
    print(f'     Columnas: {list(df.columns)}')

    # Mapeo flexible de nombres de columna (maneja variaciones)
    col_map = {c.strip(): c for c in df.columns}

    def gc(*nombres):
        """Get column — devuelve el nombre real de la primera que exista."""
        for n in nombres:
            if n in col_map:
                return col_map[n]
        return None

    COL_FECHA    = gc('Fecha de ejecución', 'Fecha de Ejecucion', 'FechaEjecucion',
                      'FechaIntencion', 'Fecha Intencion', 'Fecha')
    COL_JEFE     = gc('Jefe')
    COL_RESP     = gc('Responsable', 'Resp Prod', 'Responsable ')
    COL_CODACT   = gc('Cod Act', 'CodAct', 'Cod. Act')
    COL_ACT      = gc('Actividad')
    COL_PROV     = gc('Proveedor')
    COL_OS       = gc('OS')
    COL_COD      = gc('Cod.', 'Cod')
    COL_NOM_HAC  = gc('Nom Hacienda', 'NomHacienda', 'Hacienda Lote', 'HaciendaLote')
    COL_LOTE     = gc('Lote')
    COL_SOC      = gc('Sociedad')
    COL_ZONA     = gc('Zona')
    COL_COD_REC  = gc('Cod. Recurso', 'Codigo Recurso', 'CodRecurso', 'Cod Recurso')
    COL_REC      = gc('Recurso de Transporte', 'Recurso')
    COL_COD_PROD = gc('Codigo Producto', 'CodProducto')
    COL_PROD     = gc('Producto')
    COL_UM       = gc('UM')
    COL_TIPO_APL = gc('Tipo Aplicación', 'Tipo Aplicacion', 'TipoAplicacion')
    COL_IMPL     = gc('Implemento')
    COL_REC_EJEC = gc('Recurso de Ejecusión', 'Recurso de Ejecucion')
    COL_CANT     = gc('Cantidad Programada', 'CantidadProgramada')
    COL_AREA     = gc('Area Programada', 'AreaProgramada', 'HA')
    COL_DOSIS    = gc('Dosis Programada', 'DosisProgramada')
    COL_SACOS    = gc('SACOS', 'Sacos')
    COL_COD_ORIG = gc('Cod. Origen', 'CodOrigen')
    COL_ORIG     = gc('Origen', 'Almacen Sugerido')
    COL_LOTE_ORIG= gc('Lote Origen', 'LoteOrigen')
    COL_VAR      = gc('Variedad caña semilla', 'Variedad')
    COL_COMENT   = gc('Comentario')
    COL_CARGO    = gc('Cargo de Transporte')
    COL_SEQ      = gc('Secuencia de aplicación', 'Secuencia')
    COL_STATUS   = gc('Status', 'Estado')
    COL_CORREO   = gc('Correo Proveedor', 'CorreoProveedor')
    COL_PLAN     = gc('Plan/Agregado', 'Plan Agregado')
    COL_ID       = gc('ID')

    def v(row, col):
        return limpiar(row[col]) if col and col in row.index else None

    registros = []
    for i, row in df.iterrows():
        # Excluir filas de transporte (Cod Recurso = 9013)
        cod_rec = v(row, COL_COD_REC)
        if cod_rec and str(cod_rec).strip() == '9013':
            continue

        # ID: usar columna ID si existe, si no generar hash de campos clave
        id_val = v(row, COL_ID)
        if not id_val:
            clave = f"{v(row,COL_OS)}_{v(row,COL_NOM_HAC)}_{v(row,COL_LOTE)}_{v(row,COL_ACT)}_{i}"
            id_val = hashlib.md5(clave.encode()).hexdigest()[:8]

        reg = {
            'ID_Solicitud':       id_val,
            'FechaEjecucion':     normalizar_fecha(v(row, COL_FECHA)),
            'Jefe':               v(row, COL_JEFE),
            'Responsable':        v(row, COL_RESP),
            'CodActividad':       v(row, COL_CODACT),
            'Actividad':          v(row, COL_ACT),
            'Proveedor':          v(row, COL_PROV),
            'OS':                 v(row, COL_OS),
            'CodDestino':         v(row, COL_COD),
            'NombreDestino':      v(row, COL_NOM_HAC),
            'LoteDestino':        v(row, COL_LOTE),
            'Sociedad':           v(row, COL_SOC),
            'Zona':               v(row, COL_ZONA),
            'CodRecurso':         cod_rec,
            'RecursoTransporte':  v(row, COL_REC),
            'CodProducto':        v(row, COL_COD_PROD),
            'Producto':           v(row, COL_PROD),
            'UM':                 v(row, COL_UM),
            'TipoAplicacion':     v(row, COL_TIPO_APL),
            'Implemento':         v(row, COL_IMPL),
            'RecursoEjecucion':   v(row, COL_REC_EJEC),
            'CantidadProgramada': v(row, COL_CANT),
            'AreaProgramada':     v(row, COL_AREA),
            'DosisProgramada':    v(row, COL_DOSIS),
            'Sacos':              v(row, COL_SACOS),
            'CodOrigen':          v(row, COL_COD_ORIG),
            'NombreOrigen':       v(row, COL_ORIG),
            'LoteOrigen':         v(row, COL_LOTE_ORIG),
            'VariedadSemilla':    v(row, COL_VAR),
            'Comentario':         v(row, COL_COMENT),
            'CargoTransporte':    v(row, COL_CARGO),
            'SecuenciaAplicacion':v(row, COL_SEQ),
            'Status':             v(row, COL_STATUS),
            'CorreoProveedor':    v(row, COL_CORREO),
            'PlanAgregado':       v(row, COL_PLAN),
            # Campos para el flujo del dashboard
            'ST':                 None,
            'Coordinador':        None,
            'EstatusViaje':       v(row, COL_STATUS) or 'Pendiente',
            'TipoProg':           v(row, COL_PLAN) or 'Planificado',
            '_origen':            'solicitud_gsheet',
            '_origenRaw':         'solicitud_planificada',
        }
        registros.append(reg)

    return registros


def main():
    if not XLSX_PATH.exists():
        print(f'[SKIP] {XLSX_PATH} no existe — nada que convertir.')
        sys.exit(0)

    # Verificar si el xlsx cambió desde la última conversión
    with open(XLSX_PATH, 'rb') as f:
        md5_actual = hashlib.md5(f.read()).hexdigest()

    if HASH_PATH.exists():
        md5_anterior = HASH_PATH.read_text().strip()
        if md5_actual == md5_anterior:
            print(f'[SKIP] {XLSX_PATH} sin cambios (md5 igual) — no se reconvierte.')
            sys.exit(0)

    registros = procesar_xlsx(XLSX_PATH)
    print(f'[OK] {len(registros)} registros procesados (filas de transporte excluidas)')

    # Guardar JSON comprimido
    json_bytes = json.dumps(registros, ensure_ascii=False, default=str).encode('utf-8')
    with gzip.open(JSON_GZ_PATH, 'wb', compresslevel=6) as f:
        f.write(json_bytes)

    size_kb = JSON_GZ_PATH.stat().st_size / 1024
    print(f'[OK] Escrito: {JSON_GZ_PATH} ({size_kb:.1f} KB)')

    # Guardar hash para evitar reconversiones innecesarias
    HASH_PATH.write_text(md5_actual)
    print(f'[OK] Hash guardado: {md5_actual[:12]}...')


if __name__ == '__main__':
    main()
