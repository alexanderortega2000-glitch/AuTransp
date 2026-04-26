"""
actualizar_api_actual.py
========================
Script para GitHub Actions — actualiza api_actual.json.gz
con datos de los últimos 7 días + 3 días adelante.

Se ejecuta via repository_dispatch 'actualizar_api'
disparado desde el botón Actualizar del dashboard.
"""

import os
import json
import gzip
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
    "ID_ST", "TipoViaje", "OS", "PuntoPartida", "DsPuntoPartida",
    "PuntoEntrega", "DsPuntoEntrega", "FechaEntrega", "ID_EstatusST",
    "Estado", "Asignado", "Km", "KmReal", "Estimacion", "CostoFinal",
    "Diferencia", "Comentario", "FechaFinalizacion", "Integrado",
    "ObsValidaciones", "CodEquipo", "FueraPlan", "Odometro_Inicial",
    "Odometro_Final", "Odometro_Danado", "Nom_Motorista", "OficialCosecha",
    "Frente", "IdAgrupaST", "IndAgrupa", "CantidadCargadores", "Complemento",
    "Permanencia", "Permanencia_Aplica", "InicioPermanencia", "FinPermanencia",
    "HorasPermanencia", "HorasPermanenciaEst", "FechaInicioViaje",
    "ComentInicioViaje", "FechaFinViaje", "ComentFinViaje",
    "FechaEntregaST", "ComentEntrega",
    "LatitudInicioViaje", "LongitudInicioViaje",
    "LatitudFinViaje", "LongitudFinViaje",
    "LatitudEntregaViaje", "LongitudEntregaViaje",
]

# ============================================================
# GITHUB API
# ============================================================

def get_headers():
    return {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }

def subir_json_gz(ruta_repo: str, registros: list, mensaje: str):
    json_bytes = json.dumps(registros, ensure_ascii=False, default=str).encode("utf-8")
    json_gz    = gzip.compress(json_bytes, compresslevel=6)
    url  = f"https://api.github.com/repos/{GITHUB_USUARIO}/{GITHUB_REPO}/contents/{ruta_repo}"
    resp = requests.get(url, headers=get_headers())
    sha  = resp.json().get("sha") if resp.status_code == 200 else None
    payload = {"message": mensaje, "content": base64.b64encode(json_gz).decode("utf-8")}
    if sha:
        payload["sha"] = sha
    resp = requests.put(url, headers=get_headers(), json=payload)
    mb = len(json_gz) / 1024 / 1024
    if resp.status_code in (200, 201):
        print(f"  ✓ {ruta_repo} ({len(registros):,} registros | {mb:.1f}MB)")
    else:
        print(f"  ✗ Error {resp.status_code}: {resp.text[:200]}")

# ============================================================
# CONSULTA API
# ============================================================

def consultar_api_actual() -> list:
    hoy    = date.today()
    inicio = hoy - timedelta(days=7)
    fin    = hoy + timedelta(days=3)
    print(f"  Período: {inicio.strftime('%d/%m/%Y')} → {fin.strftime('%d/%m/%Y')}")

    todos  = []
    cursor = inicio
    lote   = 0
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
                todos.extend(data)
                print(f"    {cursor.strftime('%d/%m')}: {len(data):,} registros")
            lote += 1
        except Exception as e:
            print(f"    ⚠️  Error {cursor}: {e}")
        cursor = fin_lote + timedelta(days=1)

    if not todos:
        return []

    df = pd.DataFrame(todos)
    df["ID_ST"] = df["ID_ST"].astype(str)

    # Deduplicar — prioridad a registros con más información
    def prioridad(row):
        if pd.notna(row.get("FechaFinalizacion")) and row.get("FechaFinalizacion") != "": return 0
        if pd.notna(row.get("FechaInicioViaje")) and row.get("FechaInicioViaje") != "": return 1
        return 2
    df["_prio"] = df.apply(prioridad, axis=1)
    df = df.sort_values("_prio").drop_duplicates(subset=["ID_ST"], keep="first")
    df = df.drop(columns=["_prio"]).reset_index(drop=True)

    cols_pres = [c for c in COLS_API if c in df.columns]
    registros = df[cols_pres].where(df[cols_pres].notna(), other=None).to_dict(orient="records")
    print(f"  Total: {len(registros):,} registros únicos")
    return registros

# ============================================================
# MAIN
# ============================================================

def main():
    print("\n" + "=" * 60)
    print("  ACTUALIZAR API ACTUAL — Transporte")
    print("=" * 60)

    if not GITHUB_TOKEN:
        print("[ERROR] TOKEN_REPO no configurado")
        raise SystemExit(1)

    print("\n[1/2] Consultando API...")
    registros = consultar_api_actual()

    if not registros:
        print("⚠️  Sin datos de la API.")
        return

    print("\n[2/2] Publicando api_actual.json.gz...")
    subir_json_gz(
        "data/api_actual.json.gz",
        registros,
        f"Actualizar API actual — {date.today().strftime('%Y-%m-%d')}"
    )

    print("\n✓ API actual actualizada correctamente.\n")

if __name__ == "__main__":
    main()
