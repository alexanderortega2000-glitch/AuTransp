"""
procesar_solicitudes.py
=======================
Script para GitHub Actions — procesa data/solicitudes.csv
(generado por Power Automate) y publica data/solicitudes.json.gz

Se ejecuta automáticamente via repository_dispatch event
'actualizar_solicitudes' disparado por Power Automate.
"""

import os
import json
import gzip
import base64
import requests
import pandas as pd
from io import StringIO

# ============================================================
# CONFIGURACIÓN
# ============================================================

GITHUB_TOKEN   = os.environ.get("TOKEN_REPO", "")
GITHUB_USUARIO = "alexanderortega2000-glitch"
GITHUB_REPO    = "AuTransp"

# ============================================================
# GITHUB API
# ============================================================

def get_headers():
    return {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }

def descargar_csv(ruta_repo: str) -> str:
    """Descarga un CSV desde GitHub."""
    url  = f"https://raw.githubusercontent.com/{GITHUB_USUARIO}/{GITHUB_REPO}/main/{ruta_repo}"
    resp = requests.get(url, timeout=60)
    if resp.status_code == 404:
        print(f"  ⚠️  Archivo no encontrado: {ruta_repo}")
        return ""
    resp.raise_for_status()
    return resp.text

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
# PROCESAMIENTO
# ============================================================

def procesar_solicitudes(csv_texto: str) -> list:
    """
    Lee el CSV de solicitudes, filtra transporte (9013),
    agrupa por ID y genera registros para el dashboard.
    """
    df = pd.read_csv(StringIO(csv_texto), dtype=str)

    # Eliminar columnas de metadata de SharePoint
    df = df.drop(columns=["@odata.etag", "ItemInternalId"], errors="ignore")

    n_total = len(df)

    # Filtrar filas de transporte presupuestarias
    df_prod = df[df["Codigo Recurso"].str.strip() != "9013"].reset_index(drop=True)
    n_transp = n_total - len(df_prod)
    print(f"  Total filas    : {n_total:,}")
    print(f"  De transporte  : {n_transp:,} (excluidas)")
    print(f"  De producto    : {len(df_prod):,}")

    if df_prod.empty:
        print("  ⚠️  Sin filas de producto")
        return []

    # Agrupar por ID
    grupos = []
    for id_sol, grupo in df_prod.groupby("ID", sort=False):
        primera = grupo.iloc[0]
        productos = grupo["Producto"].dropna().tolist()

        def safe_sum(col):
            try:
                return pd.to_numeric(grupo[col], errors="coerce").fillna(0).sum()
            except:
                return 0

        grupos.append({
            "ID_Solicitud":      str(id_sol),
            "TipoProg":          "Produccion",
            "FechaSolicitud":    primera.get("Fecha Solicitud", ""),
            "Jefe":              primera.get("Jefe", "").strip(),
            "RespProd":          primera.get("Resp Prod", "").strip(),
            "CodActividad":      primera.get("Cod Act", ""),
            "Actividad":         primera.get("Actividad", "").strip(),
            "OS":                primera.get("OS", ""),
            "Sociedad":          primera.get("Sociedad", "").strip(),
            "Zona":              primera.get("Zona", "").strip(),
            "CodDestino":        primera.get("Cod_x002e_", ""),
            "NombreDestino":     primera.get("NomHacienda", "").strip(),
            "LoteDestino":       primera.get("Lote", ""),
            "NombreOrigen":      primera.get("Almacen Sugerido", "").strip(),
            "TipoAplicacion":    primera.get("Tipo Aplicación", "").strip(),
            "Producto":          " / ".join(productos),
            "UM":                primera.get("UM", ""),
            "CantidadProgramada": safe_sum("Cantidad Programada"),
            "AreaProgramada":    safe_sum("HA Programada"),
            "DosisProgramada":   safe_sum("Dosis Programada"),
            "Sacos":             safe_sum("Sacos"),
            "HLA":               primera.get("HLA", ""),
            "Orden":             primera.get("Orden", ""),
            # Campos del coordinador — vacíos hasta asignar
            "ST":                None,
            "Coordinador":       None,
            "Subflota":          None,
            "ProveedorTransp":   None,
            "FechaEjecucion":    None,
            "_origen":           "solicitud_planificada",
        })

    print(f"  Viajes agrupados: {len(grupos):,} (de {len(df_prod):,} filas)")
    return grupos


# ============================================================
# EJECUCIÓN PRINCIPAL
# ============================================================

def main():
    print("\n" + "=" * 60)
    print("  PROCESAR SOLICITUDES — Transporte")
    print("=" * 60)

    if not GITHUB_TOKEN:
        print("\n[ERROR] TOKEN_REPO no configurado")
        raise SystemExit(1)

    # 1. Descargar solicitudes.csv
    print("\n[1/3] Descargando solicitudes.csv...")
    csv_texto = descargar_csv("data/solicitudes.csv")
    if not csv_texto or csv_texto.strip() == "placeholder":
        print("  ⚠️  Archivo vacío o placeholder. Sin cambios.")
        return

    # 2. Procesar
    print("\n[2/3] Procesando solicitudes...")
    registros = procesar_solicitudes(csv_texto)
    if not registros:
        print("  ⚠️  Sin registros para publicar.")
        return

    # 3. Publicar
    print("\n[3/3] Publicando solicitudes.json.gz...")
    subir_json_gz(
        "data/solicitudes.json.gz",
        registros,
        f"Actualizar solicitudes planificadas — {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}"
    )

    print("\n✓ Solicitudes procesadas correctamente.\n")


if __name__ == "__main__":
    main()
