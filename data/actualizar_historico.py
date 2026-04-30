"""
actualizar_historico.py
=======================
Script para GitHub Actions — actualiza api_historico.json.gz
con datos de los últimos 35 días.

Se ejecuta automáticamente cada domingo a las 11pm via GitHub Actions.
También se puede ejecutar manualmente desde GitHub Actions (workflow_dispatch).

Variables de entorno requeridas (configuradas en GitHub Secrets):
  TOKEN_REPO  — Personal Access Token con permisos repo y workflow
  API_USUARIO — Usuario de la API de transportes
"""

import os
import json
import gzip
import math
import base64
import requests
import pandas as pd
from datetime import date, timedelta

# ============================================================
# CONFIGURACIÓN
# ============================================================

GITHUB_TOKEN   = os.environ.get("TOKEN_REPO", "")
GITHUB_USUARIO = "alexanderortega2000-glitch"
GITHUB_REPO    = "AuTransp"

API_URL     = "https://logistico.grupocassa.com/api-transportes-varios-web/api/SolicitudesTransporte/GetSolicitudesTransporte"
API_USUARIO = os.environ.get("API_USUARIO", "")
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
        "Accept":        "application/vnd.github.v3+json",
    }

def descargar_json_gz(ruta_repo: str) -> list:
    url  = f"https://raw.githubusercontent.com/{GITHUB_USUARIO}/{GITHUB_REPO}/main/{ruta_repo}"
    resp = requests.get(url, timeout=60)
    if resp.status_code == 404:
        print(f"  Archivo no existe aún: {ruta_repo}")
        return []
    resp.raise_for_status()
    return json.loads(gzip.decompress(resp.content).decode("utf-8"))

def subir_json_gz(ruta_repo: str, registros: list, mensaje: str):
    json_bytes = json.dumps(registros, ensure_ascii=False, default=str).encode("utf-8")
    json_gz    = gzip.compress(json_bytes, compresslevel=6)
    url        = f"https://api.github.com/repos/{GITHUB_USUARIO}/{GITHUB_REPO}/contents/{ruta_repo}"
    resp       = requests.get(url, headers=get_headers())
    sha        = resp.json().get("sha") if resp.status_code == 200 else None
    payload    = {"message": mensaje, "content": base64.b64encode(json_gz).decode("utf-8")}
    if sha:
        payload["sha"] = sha
    resp = requests.put(url, headers=get_headers(), json=payload)
    mb   = len(json_gz) / 1024 / 1024
    if resp.status_code in (200, 201):
        print(f"  ✓ {ruta_repo} ({len(registros):,} registros | {mb:.1f} MB)")
    else:
        print(f"  ✗ Error {resp.status_code}: {resp.text[:200]}")

# ============================================================
# CONSULTA API
# ============================================================

def limpiar_nan(registros: list) -> list:
    def limpiar(v):
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return None
        if str(v) in ("nan", "NaN", "None", "NaT", "inf", "-inf"):
            return None
        return v
    return [{k: limpiar(v) for k, v in r.items()} for r in registros]

def consultar_periodo(inicio: date, fin: date) -> pd.DataFrame:
    print(f"  Período: {inicio.strftime('%d/%m/%Y')} → {fin.strftime('%d/%m/%Y')}")
    todos  = []
    cursor = inicio
    while cursor <= fin:
        fin_lote = min(cursor + timedelta(days=1), fin)
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
                print(f"    {cursor.strftime('%d/%m')}: {len(data):,} registros")
        except Exception as e:
            print(f"    ⚠️  Error {cursor}: {e}")
        cursor = fin_lote + timedelta(days=1)

    if not todos:
        return pd.DataFrame()

    df = pd.concat(todos, ignore_index=True)
    df["ID_ST"] = df["ID_ST"].astype(str)
    df = df.sort_values("FechaFinalizacion", ascending=False, na_position="last")
    df = df.drop_duplicates(subset=["ID_ST"], keep="first").reset_index(drop=True)
    print(f"  Total: {len(df):,} registros únicos")
    return df

# ============================================================
# MAIN
# ============================================================

def main():
    print("\n" + "=" * 60)
    print("  ACTUALIZAR HISTÓRICO API — Transporte")
    print("=" * 60)

    if not GITHUB_TOKEN:
        print("[ERROR] TOKEN_REPO no configurado en secrets")
        raise SystemExit(1)

    if not API_USUARIO:
        print("[ERROR] API_USUARIO no configurado en secrets")
        raise SystemExit(1)

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

    # 2. Consultar últimos 35 días
    print("\n[2/3] Consultando API...")
    hoy        = date.today()
    df_periodo = consultar_periodo(hoy - timedelta(days=35), hoy - timedelta(days=1))

    if df_periodo.empty:
        print("\n⚠️  Sin datos nuevos. Histórico sin cambios.")
        return

    # Preparar registros nuevos
    cols_pres = [c for c in COLS_API if c in df_periodo.columns]
    nuevos    = df_periodo[cols_pres].copy()
    for col in nuevos.columns:
        dtype = str(nuevos[col].dtype)
        if "datetime" in dtype or "date" in dtype:
            nuevos[col] = nuevos[col].astype(str).replace({"NaT": ""})
        elif dtype not in ("object", "string"):
            nuevos[col] = nuevos[col].where(nuevos[col].notna(), other=None)
    nuevos_lista = limpiar_nan(nuevos.to_dict(orient="records"))

    # 3. Fusionar — nuevos tienen prioridad sobre histórico
    print("\n[3/3] Fusionando y publicando...")
    ids_nuevos         = {str(r.get("ID_ST")) for r in nuevos_lista}
    historico_filtrado = [r for r in historico if str(r.get("ID_ST")) not in ids_nuevos]
    historico_final    = historico_filtrado + nuevos_lista

    print(f"  Histórico anterior : {len(historico):,}")
    print(f"  Registros nuevos   : {len(nuevos_lista):,}")
    print(f"  Histórico final    : {len(historico_final):,}")

    subir_json_gz(
        "data/api_historico.json.gz",
        historico_final,
        f"Actualizar histórico API — semana {hoy.strftime('%Y-%m-%d')}"
    )

    print("\n✓ Histórico actualizado correctamente.\n")


if __name__ == "__main__":
    main()
