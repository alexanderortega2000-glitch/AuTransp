"""
actualizar_historico.py
=======================
Script para GitHub Actions — actualiza api_historico.json.gz
con los datos de la semana anterior.

Se ejecuta automáticamente cada domingo a las 11pm via GitHub Actions.
También se puede ejecutar manualmente desde VS Code cuando sea necesario.

Variables de entorno requeridas (configuradas en GitHub Secrets):
  TOKEN_REPO  — Personal Access Token con permisos repo
  API_USUARIO        — Usuario de la API de transportes
"""

import os
import json
import gzip
import math
import base64
import requests
import pandas as pd
from datetime import date, timedelta
from io import BytesIO

# ============================================================
# CONFIGURACIÓN
# ============================================================

GITHUB_TOKEN   = os.environ.get("TOKEN_REPO", "TU_TOKEN_AQUI")
GITHUB_USUARIO = "alexanderortega2000-glitch"
GITHUB_REPO    = "AuTransp"

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

COLS_API = [
    "ID_ST", "TipoViaje", "DsPuntoPartida", "DsPuntoEntrega",
    "Estado", "ID_EstatusST", "FechaEntrega", "FechaInicioViaje",
    "FechaFinalizacion", "FechaEntregaST", "Nom_Motorista",
    "Km", "KmReal", "CantidadCargadores", "Complemento", "Permanencia",
    "OS", "Integrado", "ObsValidaciones", "Asignado",
    "FechaFinViaje", "InicioPermanencia", "FinPermanencia",
    "HorasPermanencia", "HorasPermanenciaEst",
    "Permanencia_Aplica", "FueraPlan",
]

# ============================================================
# GITHUB API
# ============================================================

def get_headers():
    return {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }

def descargar_json_gz(ruta_repo: str) -> list:
    """Descarga y descomprime un JSON.gz desde GitHub."""
    url  = f"https://raw.githubusercontent.com/{GITHUB_USUARIO}/{GITHUB_REPO}/main/{ruta_repo}"
    resp = requests.get(url, timeout=60)
    if resp.status_code == 404:
        print(f"  Archivo no existe aún: {ruta_repo}")
        return []
    resp.raise_for_status()
    datos = pako_inflate(resp.content)
    return json.loads(datos.decode("utf-8"))


def pako_inflate(data: bytes) -> bytes:
    """Descomprime gzip."""
    import gzip as gz
    return gz.decompress(data)


def subir_json_gz(ruta_repo: str, registros: list, mensaje: str):
    """Comprime y sube un JSON.gz a GitHub."""
    json_bytes = json.dumps(registros, ensure_ascii=False, default=str).encode("utf-8")
    json_gz    = gzip.compress(json_bytes, compresslevel=6)

    url  = f"https://api.github.com/repos/{GITHUB_USUARIO}/{GITHUB_REPO}/contents/{ruta_repo}"
    resp = requests.get(url, headers=get_headers())
    sha  = resp.json().get("sha") if resp.status_code == 200 else None

    payload = {
        "message": mensaje,
        "content": base64.b64encode(json_gz).decode("utf-8"),
    }
    if sha:
        payload["sha"] = sha

    resp = requests.put(url, headers=get_headers(), json=payload)
    mb = len(json_gz) / 1024 / 1024
    if resp.status_code in (200, 201):
        print(f"  ✓ {ruta_repo} ({len(registros):,} registros | {mb:.1f}MB)")
    else:
        print(f"  ✗ Error {resp.status_code}: {ruta_repo}")
        print(f"    {resp.text[:200]}")

# ============================================================
# CONSULTA API
# ============================================================

def consultar_semana_anterior() -> pd.DataFrame:
    """
    Consulta la API para el último mes (35 días hacia atrás).
    Cubre STs que reciben actualizaciones tardías.
    """
    hoy     = date.today()
    inicio  = hoy - timedelta(days=35)
    fin     = hoy - timedelta(days=1)  # hasta ayer (hoy lo cubre api_actual)

    print(f"  Período: {inicio.strftime('%d/%m/%Y')} → {fin.strftime('%d/%m/%Y')} (últimos 35 días)")

    todos  = []
    cursor = inicio
    while cursor <= fin:
        fin_lote = min(cursor + timedelta(days=1), domingo_ant)
        params   = {
            **API_PARAMS,
            "FechaInicio": cursor.strftime("%d-%m-%Y"),
            "FechaFin":    fin_lote.strftime("%d-%m-%Y"),
        }
        try:
            resp = requests.get(API_URL, params=params, timeout=120)
            resp.raise_for_status()
            data = resp.json()
            if data:
                todos.append(pd.DataFrame(data))
                print(f"    {cursor.strftime('%d/%m')} - {fin_lote.strftime('%d/%m')}: {len(data):,} registros")
        except Exception as e:
            print(f"    ⚠️  Error {cursor}: {e}")
        cursor = fin_lote + timedelta(days=1)

    if not todos:
        print("  ⚠️  Sin datos de la API para esta semana")
        return pd.DataFrame()

    df = pd.concat(todos, ignore_index=True)
    df["ID_ST"] = df["ID_ST"].astype(str)
    df = df.sort_values("FechaFinalizacion", ascending=False, na_position="last")
    df = df.drop_duplicates(subset=["ID_ST"], keep="first").reset_index(drop=True)
    print(f"  Total semana: {len(df):,} registros únicos")
    return df


def limpiar_nan(registros: list) -> list:
    """Limpia NaN para JSON válido."""
    def limpiar(v):
        if isinstance(v, float) and math.isnan(v):
            return None
        return v
    return [{k: limpiar(v) for k, v in r.items()} for r in registros]


# ============================================================
# EJECUCIÓN PRINCIPAL
# ============================================================

def main():
    print("\n" + "=" * 60)
    print("  ACTUALIZAR HISTÓRICO API — Transporte")
    print("=" * 60)

    # Verificar conexión GitHub
    resp = requests.get(
        f"https://api.github.com/repos/{GITHUB_USUARIO}/{GITHUB_REPO}",
        headers=get_headers()
    )
    if resp.status_code != 200:
        print(f"\n[ERROR] No se pudo conectar a GitHub: {resp.status_code}")
        raise SystemExit(1)
    print(f"\n✓ Conectado: {GITHUB_USUARIO}/{GITHUB_REPO}")

    # 1. Descargar histórico actual
    print("\n[1/3] Descargando histórico actual...")
    historico = descargar_json_gz("data/api_historico.json.gz")
    print(f"  Histórico existente: {len(historico):,} registros")

    # 2. Consultar semana anterior
    print("\n[2/3] Consultando API semana anterior...")
    df_semana = consultar_semana_anterior()

    if df_semana.empty:
        print("\n⚠️  Sin datos nuevos. Histórico sin cambios.")
        return

    # Convertir a registros
    cols_pres   = [c for c in COLS_API if c in df_semana.columns]
    nuevos      = df_semana[cols_pres].copy()
    for col in nuevos.columns:
        if "datetime" in str(nuevos[col].dtype) or "date" in str(nuevos[col].dtype):
            nuevos[col] = nuevos[col].astype(str).replace({"NaT": ""})
        elif str(nuevos[col].dtype) not in ("object", "string"):
            nuevos[col] = nuevos[col].where(nuevos[col].notna(), other=None)
    nuevos_lista = limpiar_nan(nuevos.to_dict(orient="records"))

    # 3. Fusionar — nuevos registros tienen prioridad sobre histórico
    print("\n[3/3] Fusionando y subiendo histórico actualizado...")
    ids_nuevos = {str(r.get("ID_ST")) for r in nuevos_lista}
    historico_filtrado = [r for r in historico if str(r.get("ID_ST")) not in ids_nuevos]
    historico_nuevo    = historico_filtrado + nuevos_lista

    print(f"  Histórico anterior : {len(historico):,}")
    print(f"  Registros nuevos   : {len(nuevos_lista):,}")
    print(f"  Actualizados       : {len(ids_nuevos) - (len(historico_nuevo) - len(historico_filtrado)):,}")
    print(f"  Histórico final    : {len(historico_nuevo):,}")

    subir_json_gz(
        "data/api_historico.json.gz",
        historico_nuevo,
        f"Actualizar histórico API — semana {date.today().strftime('%Y-%m-%d')}"
    )

    print("\n✓ Histórico actualizado correctamente.\n")


if __name__ == "__main__":
    main()

