"""
consolidar_costos.py
====================
Lee CostosAgricampoHist.xlsx desde GitHub (subido por Power Automate),
aplica filtros y clasificación de área, y genera:

  costos_historico.json.gz  → hasta enero 2026 (quemado, no cambia)
  costos_actual.json.gz     → febrero 2026 en adelante

MODOS:
  python consolidar_costos.py              → completo (histórico + actual)
  python consolidar_costos.py --solo-actual → solo actualiza costos_actual.json.gz

En GitHub Actions se ejecuta con --solo-actual.
En local puede ejecutarse en cualquier modo.
"""

import re
import sys
import json
import gzip
import math
import base64
import warnings
import requests
import pandas as pd
from io import BytesIO
from pathlib import Path
from datetime import datetime, date

warnings.filterwarnings("ignore")

# ============================================================
# CONFIGURACIÓN
# ============================================================

# Rutas locales (para ejecución manual en PC)
RUTA_COSTOS_LOCAL = Path(r"C:\Datos\OneDrive - Grupo CASSA\INVENTARIOS AGRICOLAS - GESTION DE TRANSPORTE\AutomatizacionTransp\CostosAgricampoHist.xlsx")
RUTA_SALIDA_LOCAL = Path(r"C:\Datos\OneDrive - Grupo CASSA\INVENTARIOS AGRICOLAS - GESTION DE TRANSPORTE\AutomatizacionTransp")

# GitHub
import os
GITHUB_TOKEN   = os.environ.get("TOKEN_REPO", "")
GITHUB_USUARIO = "alexanderortega2000-glitch"
GITHUB_REPO    = "AuTransp"
RUTA_EXCEL_REPO = "data/costos_actual.csv"

# Fecha de corte
FECHA_CORTE = date(2026, 2, 1)

# ============================================================
# CONSTANTES
# ============================================================

COLS_COSTOS = [
    "ST", "Area", "FechaApunte", "OS1", "DsEstadoOS",
    "BkActividad", "DsActividad", "BkHacienda1", "Hacienda1", "BkLote",
    "BkRecurso", "Recurso", "BkProveedor", "Proveedor",
    "CodEquipo", "Equipo",
    "OdometroInicio", "OdometroFin", "Distancia",
    "HoraInicio", "HoraFin", "DuracionHoras",
    "Paquetes", "CantidadPago1", "A_COBRAR", "A_PAGAR",
    "ValorPagoComplemento", "ValorPagoPermanencia",
    "HorasPermanencia", "CantidadCargadores",
    "Observacion", "IntegracionPago", "IntegracionCobro",
    "FechaIntegracionSAPPago", "FechaIntegracionSAPCobro",
    "SociedadGestora", "IdProforma",
    "ReferenciaPagoSAP", "ReferenciaCobroSAP",
    "UsuarioDigita", "ID_ST", "IdAgrupaST",
]

USUARIOS_RED_VIAL = {"sdeleon", "admin", "sescobar", "wmarroquin", "ccla001", "CJEFlores", "51474"}
HACIENDAS_COSECHA = {2996, 1984, 2983, 1994, 2984, 1983, 1987, 2987}

# ============================================================
# FUNCIONES UTILITARIAS
# ============================================================

def get_headers():
    return {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }

def limpiar_st(valor):
    if pd.isna(valor):
        return None
    try:
        return str(int(float(str(valor).strip())))
    except (ValueError, TypeError):
        v = str(valor).strip()
        return v if v else None

def extraer_st_observacion(texto):
    if not isinstance(texto, str):
        return None
    m = re.search(r"ST:\s*(\d+)", texto, re.IGNORECASE)
    return m.group(1) if m else None

def clasificar_area(row):
    usuario = str(row.get("UsuarioDigita", "")).strip()
    if usuario in USUARIOS_RED_VIAL:
        return "Red Vial"
    try:
        hacienda = int(float(str(row.get("BkHacienda1", ""))))
        if hacienda in HACIENDAS_COSECHA:
            return "Cosecha"
    except (ValueError, TypeError):
        pass
    return "Varios"

def limpiar_nan(registros):
    def limpiar(v):
        if isinstance(v, float) and math.isnan(v):
            return None
        if str(v) in ("nan", "None", "NaT"):
            return None
        return v
    return [{k: limpiar(v) for k, v in r.items()} for r in registros]

def a_json_gz(df, cols):
    cols_pres = [c for c in cols if c in df.columns]
    sub = df[cols_pres].copy()
    for col in sub.select_dtypes(include=["datetime64", "datetimetz"]).columns:
        sub[col] = sub[col].astype(str).replace({"NaT": None})
    registros = limpiar_nan(sub.to_dict(orient="records"))
    json_bytes = json.dumps(registros, ensure_ascii=False, default=str).encode("utf-8")
    return gzip.compress(json_bytes, compresslevel=6)

def subir_github(ruta_repo, contenido_gz, mensaje):
    url  = f"https://api.github.com/repos/{GITHUB_USUARIO}/{GITHUB_REPO}/contents/{ruta_repo}"
    resp = requests.get(url, headers=get_headers())
    sha  = resp.json().get("sha") if resp.status_code == 200 else None
    payload = {"message": mensaje, "content": base64.b64encode(contenido_gz).decode("utf-8")}
    if sha:
        payload["sha"] = sha
    resp = requests.put(url, headers=get_headers(), json=payload)
    mb = len(contenido_gz) / 1024 / 1024
    if resp.status_code in (200, 201):
        print(f"  ✓ GitHub: {ruta_repo} ({mb:.1f} MB)")
    else:
        print(f"  ✗ Error {resp.status_code}: {resp.text[:200]}")

def leer_excel_github() -> bytes:
    """Descarga el Excel de costos desde GitHub."""
    url  = f"https://api.github.com/repos/{GITHUB_USUARIO}/{GITHUB_REPO}/contents/{RUTA_EXCEL_REPO}"
    resp = requests.get(url, headers=get_headers())
    if resp.status_code == 404:
        raise FileNotFoundError(f"No se encontró {RUTA_EXCEL_REPO} en GitHub")
    resp.raise_for_status()
    return base64.b64decode(resp.json()["content"])

# ============================================================
# PROCESAMIENTO
# ============================================================

def procesar_costos(modo_github=False) -> pd.DataFrame:
    if modo_github:
        print(f"  Leyendo desde GitHub: {RUTA_EXCEL_REPO}")
        excel_bytes = leer_excel_github()
        # Power Automate sube CSV — leer directamente
        from io import StringIO
        df = pd.read_csv(StringIO(excel_bytes.decode('utf-8', errors='replace')), dtype=str)
    else:
        if not RUTA_COSTOS_LOCAL.exists():
            print(f"  [ERROR] Archivo no encontrado: {RUTA_COSTOS_LOCAL}")
            raise SystemExit(1)
        print(f"  Leyendo local: {RUTA_COSTOS_LOCAL.name}")
        df = pd.read_excel(RUTA_COSTOS_LOCAL, dtype=str)

    n_original = len(df)
    print(f"  Filas leídas      : {n_original:,}")

    # Filtrar solo transporte
    if "Recurso" in df.columns:
        df = df[df["Recurso"].astype(str).str.strip().str.lower().str.startswith("transp")]
        df = df.reset_index(drop=True)
    print(f"  Filtro transporte : {len(df):,} de {n_original:,}")

    # Clasificar área
    df["Area"] = df.apply(clasificar_area, axis=1)
    print(f"  Áreas             : {df['Area'].value_counts().to_dict()}")

    # Resolver ST
    if "ID_ST" in df.columns:
        df["ST"] = df["ID_ST"].apply(limpiar_st)
    else:
        df["ST"] = None
    if "Observacion" in df.columns:
        st_obs = df["Observacion"].apply(extraer_st_observacion)
        df["ST"] = df["ST"].combine_first(st_obs)

    sin_st = df["ST"].isna().sum()
    if sin_st > 0:
        print(f"  Sin ST            : {sin_st:,}")

    # Parsear FechaApunte
    if "FechaApunte" in df.columns:
        df["FechaApunte"] = pd.to_datetime(df["FechaApunte"], dayfirst=True, errors="coerce")

    print(f"  Total procesado   : {len(df):,}")
    return df

# ============================================================
# MAIN
# ============================================================

def main():
    solo_actual  = "--solo-actual" in sys.argv
    modo_github  = "--github" in sys.argv or (GITHUB_TOKEN and not RUTA_COSTOS_LOCAL.exists())

    # En GitHub Actions siempre modo github + solo actual
    if os.environ.get("GITHUB_ACTIONS") == "true":
        solo_actual = True
        modo_github = True

    print("\n" + "=" * 65)
    print("  CONSOLIDAR COSTOS")
    print("=" * 65)
    print(f"\n  Fecha de corte : {FECHA_CORTE.strftime('%d/%m/%Y')}")
    print(f"  Modo           : {'Solo actual' if solo_actual else 'Completo'}")
    print(f"  Fuente         : {'GitHub' if modo_github else 'Local'}\n")

    # Cargar token desde .env si no viene de entorno
    if not GITHUB_TOKEN:
        env_path = RUTA_SALIDA_LOCAL / ".env"
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                if line.startswith("GH_TOKEN_AUTRANSP="):
                    os.environ["TOKEN_REPO"] = line.split("=", 1)[1].strip()
                    globals()["GITHUB_TOKEN"] = os.environ["TOKEN_REPO"]
                    break

    print("[1/3] Procesando costos...")
    df = procesar_costos(modo_github=modo_github)

    print(f"\n[2/3] Separando por fecha de corte...")
    if "FechaApunte" in df.columns and df["FechaApunte"].notna().any():
        df_hist = df[df["FechaApunte"] < pd.Timestamp(FECHA_CORTE)].copy()
        df_act  = df[df["FechaApunte"] >= pd.Timestamp(FECHA_CORTE)].copy()
    else:
        print("  ⚠️  Sin FechaApunte — todo va a actual")
        df_hist = pd.DataFrame()
        df_act  = df.copy()

    print(f"  Histórico : {len(df_hist):,} registros")
    print(f"  Actual    : {len(df_act):,} registros")

    print(f"\n[3/3] Guardando y subiendo...")

    # Histórico — solo en modo completo
    if not solo_actual and not df_hist.empty:
        gz_hist = a_json_gz(df_hist, COLS_COSTOS)
        if not modo_github:
            ruta = RUTA_SALIDA_LOCAL / "costos_historico.json.gz"
            ruta.write_bytes(gz_hist)
            print(f"  ✓ Local: {ruta.name} ({len(gz_hist)/1024/1024:.1f} MB)")
        if GITHUB_TOKEN:
            subir_github("data/costos_historico.json.gz", gz_hist,
                        f"Costos histórico — hasta ene 2026 ({len(df_hist):,} registros)")

    # Actual — siempre
    gz_act = a_json_gz(df_act, COLS_COSTOS)
    if not modo_github:
        ruta = RUTA_SALIDA_LOCAL / "costos_actual.json.gz"
        ruta.write_bytes(gz_act)
        print(f"  ✓ Local: {ruta.name} ({len(gz_act)/1024/1024:.1f} MB)")
    if GITHUB_TOKEN:
        subir_github("data/costos_actual.json.gz", gz_act,
                    f"Costos actual — {date.today().strftime('%Y-%m-%d')} ({len(df_act):,} registros)")

    print(f"\n{'='*65}")
    print(f"  COMPLETADO — {date.today().strftime('%d/%m/%Y')}")
    print(f"{'='*65}\n")

if __name__ == "__main__":
    main()
