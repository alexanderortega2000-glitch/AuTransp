"""
consolidar_cambios.py
=====================
Script para GitHub Actions — consolida la cola de cambios
en los archivos principales del dashboard.

Colas procesadas:
  data/cola/cambios_seg/     → seg_web.json
  data/cola/cambios_prog/    → prog_actual.json.gz
  data/cola/solicitudes/     → solicitudes_demanda.json.gz

Se ejecuta cada 5 minutos via GitHub Actions (schedule).
También se puede disparar via repository_dispatch 'consolidar_cambios'.

Variables de entorno requeridas:
  TOKEN_REPO — Personal Access Token con permisos repo y workflow
"""

import os
import json
import gzip
import base64
import requests
from datetime import datetime, timezone
from pathlib import Path

# ============================================================
# CONFIGURACIÓN
# ============================================================

GITHUB_TOKEN   = os.environ.get("TOKEN_REPO", "")
GITHUB_USUARIO = "alexanderortega2000-glitch"
GITHUB_REPO    = "AuTransp"
BRANCH         = "main"

COLAS = {
    "cambios_seg":   "data/cola/cambios_seg",
    "cambios_prog":  "data/cola/cambios_prog",
    "solicitudes":   "data/cola/solicitudes",
}

ARCHIVOS_DESTINO = {
    "cambios_seg":  "data/seg_web.json",
    "cambios_prog": "data/prog_actual.json.gz",
    "solicitudes":  "data/solicitudes_demanda.json.gz",
}

# ============================================================
# GITHUB API — helpers
# ============================================================

def get_headers():
    return {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept":        "application/vnd.github.v3+json",
    }

def listar_cola(carpeta: str) -> list:
    """Lista archivos en una carpeta del repo. Retorna lista de {name, sha, download_url}."""
    url  = f"https://api.github.com/repos/{GITHUB_USUARIO}/{GITHUB_REPO}/contents/{carpeta}"
    resp = requests.get(url, headers=get_headers())
    if resp.status_code == 404:
        return []
    resp.raise_for_status()
    return [f for f in resp.json() if f.get("type") == "file" and f["name"].endswith(".json")]

def descargar_json(download_url: str) -> dict:
    """Descarga y parsea un JSON desde una URL raw de GitHub."""
    resp = requests.get(download_url, timeout=30)
    resp.raise_for_status()
    return resp.json()

def descargar_json_gz(ruta_repo: str) -> list:
    """Descarga y descomprime un JSON.gz desde GitHub raw."""
    url  = f"https://raw.githubusercontent.com/{GITHUB_USUARIO}/{GITHUB_REPO}/{BRANCH}/{ruta_repo}"
    resp = requests.get(url, timeout=60)
    if resp.status_code == 404:
        return []
    resp.raise_for_status()
    return json.loads(gzip.decompress(resp.content).decode("utf-8"))

def descargar_json_plano(ruta_repo: str) -> dict:
    """Descarga y parsea un JSON plano desde GitHub raw."""
    url  = f"https://raw.githubusercontent.com/{GITHUB_USUARIO}/{GITHUB_REPO}/{BRANCH}/{ruta_repo}"
    resp = requests.get(url, timeout=60)
    if resp.status_code == 404:
        return {}
    resp.raise_for_status()
    return resp.json()

def subir_json_gz(ruta_repo: str, datos: list, mensaje: str) -> bool:
    json_bytes = json.dumps(datos, ensure_ascii=False, default=str).encode("utf-8")
    json_gz    = gzip.compress(json_bytes, compresslevel=6)
    return _subir_contenido(ruta_repo, base64.b64encode(json_gz).decode("utf-8"), mensaje)

def subir_json_plano(ruta_repo: str, datos: dict, mensaje: str) -> bool:
    contenido = json.dumps(datos, ensure_ascii=False, indent=2, default=str).encode("utf-8")
    return _subir_contenido(ruta_repo, base64.b64encode(contenido).decode("utf-8"), mensaje)

def _subir_contenido(ruta_repo: str, contenido_b64: str, mensaje: str) -> bool:
    url     = f"https://api.github.com/repos/{GITHUB_USUARIO}/{GITHUB_REPO}/contents/{ruta_repo}"
    resp    = requests.get(url, headers=get_headers())
    sha     = resp.json().get("sha") if resp.status_code == 200 else None
    payload = {"message": mensaje, "content": contenido_b64, "branch": BRANCH}
    if sha:
        payload["sha"] = sha
    resp = requests.put(url, headers=get_headers(), json=payload)
    if resp.status_code in (200, 201):
        print(f"  ✓ {ruta_repo}")
        return True
    else:
        print(f"  ✗ Error {resp.status_code}: {ruta_repo} — {resp.text[:150]}")
        return False

def eliminar_archivo(ruta_repo: str, sha: str, mensaje: str):
    """Elimina un archivo del repo (vacía la cola procesada)."""
    url  = f"https://api.github.com/repos/{GITHUB_USUARIO}/{GITHUB_REPO}/contents/{ruta_repo}"
    payload = {"message": mensaje, "sha": sha, "branch": BRANCH}
    resp = requests.delete(url, headers=get_headers(), json=payload)
    if resp.status_code == 200:
        print(f"  🗑  {ruta_repo}")
    else:
        print(f"  ✗ No se pudo eliminar {ruta_repo}: {resp.status_code}")

# ============================================================
# CONSOLIDADORES POR TIPO DE COLA
# ============================================================

def consolidar_seg(archivos_cola: list) -> int:
    """
    Aplica cambios de seguimiento (motivos, comentarios) sobre seg_web.json.
    Cada archivo de cola tiene: { st, motivoRetrasoInicio, motivoRetrasoEntrega,
                                   comentarioCabina, actualizadoPor, fechaActualizacion }
    """
    if not archivos_cola:
        return 0

    print(f"  Cargando seg_web.json...")
    seg = descargar_json_plano(ARCHIVOS_DESTINO["cambios_seg"])

    cambios = 0
    archivos_procesados = []
    for archivo in archivos_cola:
        try:
            cambio = descargar_json(archivo["download_url"])
            st     = cambio.get("st") or cambio.get("ST")
            if not st:
                print(f"  ⚠️  Sin ST en {archivo['name']} — ignorado")
                continue
            seg[st] = {
                "motivoRetrasoInicio":  cambio.get("motivoRetrasoInicio", ""),
                "motivoRetrasoEntrega": cambio.get("motivoRetrasoEntrega", ""),
                "comentarioCabina":     cambio.get("comentarioCabina", ""),
                "actualizadoPor":       cambio.get("actualizadoPor", ""),
                "fechaActualizacion":   cambio.get("fechaActualizacion", ""),
            }
            cambios += 1
            archivos_procesados.append(archivo)
        except Exception as e:
            print(f"  ⚠️  Error procesando {archivo['name']}: {e}")

    if cambios == 0:
        return 0

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    ok = subir_json_plano(
        ARCHIVOS_DESTINO["cambios_seg"],
        seg,
        f"Auto: seg_web — {cambios} cambios · {ts}"
    )

    if ok:
        for archivo in archivos_procesados:
            eliminar_archivo(
                f"{COLAS['cambios_seg']}/{archivo['name']}",
                archivo["sha"],
                f"Cola procesada: {archivo['name']}"
            )

    return cambios

def consolidar_prog(archivos_cola: list) -> int:
    """
    Aplica cambios de programación sobre prog_actual.json.gz.
    Cada archivo de cola tiene los campos editados de un viaje por ST.
    """
    if not archivos_cola:
        return 0

    print(f"  Cargando prog_actual.json.gz...")
    prog = descargar_json_gz(ARCHIVOS_DESTINO["cambios_prog"])
    idx  = {str(v.get("ST") or v.get("ID_ST", "")): i for i, v in enumerate(prog)}

    cambios = 0
    archivos_procesados = []
    for archivo in archivos_cola:
        try:
            cambio = descargar_json(archivo["download_url"])
            st     = str(cambio.get("st") or cambio.get("ST", ""))
            if not st:
                print(f"  ⚠️  Sin ST en {archivo['name']} — ignorado")
                continue
            if st in idx:
                # Actualizar viaje existente
                campos = cambio.get("campos", cambio)
                for k, v in campos.items():
                    if k not in ("st", "ST", "actualizadoPor", "fechaActualizacion"):
                        prog[idx[st]][k] = v
                prog[idx[st]]["_actualizadoPor"]  = cambio.get("actualizadoPor", "")
                prog[idx[st]]["_fechaActualizacion"] = cambio.get("fechaActualizacion", "")
            else:
                # Viaje nuevo (solicitud a demanda convertida a ST)
                prog.append(cambio.get("campos", cambio))
                idx[st] = len(prog) - 1
            cambios += 1
            archivos_procesados.append(archivo)
        except Exception as e:
            print(f"  ⚠️  Error procesando {archivo['name']}: {e}")

    if cambios == 0:
        return 0

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    ok = subir_json_gz(
        ARCHIVOS_DESTINO["cambios_prog"],
        prog,
        f"Auto: prog_actual — {cambios} cambios · {ts}"
    )

    if ok:
        for archivo in archivos_procesados:
            eliminar_archivo(
                f"{COLAS['cambios_prog']}/{archivo['name']}",
                archivo["sha"],
                f"Cola procesada: {archivo['name']}"
            )

    return cambios

def consolidar_solicitudes(archivos_cola: list) -> int:
    """
    Agrega nuevas solicitudes a demanda a solicitudes_demanda.json.gz.
    Cada archivo de cola es una solicitud nueva completa.
    """
    if not archivos_cola:
        return 0

    print(f"  Cargando solicitudes_demanda.json.gz...")
    solicitudes = descargar_json_gz(ARCHIVOS_DESTINO["solicitudes"])
    ids_existentes = {str(s.get("ID_Solicitud", "")) for s in solicitudes}

    nuevas = 0
    archivos_procesados = []
    for archivo in archivos_cola:
        try:
            solicitud = descargar_json(archivo["download_url"])
            id_sol    = str(solicitud.get("ID_Solicitud", ""))
            if id_sol and id_sol in ids_existentes:
                print(f"  ⚠️  Solicitud {id_sol} ya existe — ignorada")
                archivos_procesados.append(archivo)  # igual eliminar de cola
                continue
            solicitudes.append(solicitud)
            ids_existentes.add(id_sol)
            nuevas += 1
            archivos_procesados.append(archivo)
        except Exception as e:
            print(f"  ⚠️  Error procesando {archivo['name']}: {e}")

    if not archivos_procesados:
        return 0

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    ok = subir_json_gz(
        ARCHIVOS_DESTINO["solicitudes"],
        solicitudes,
        f"Auto: solicitudes_demanda — {nuevas} nuevas · {ts}"
    )

    if ok:
        for archivo in archivos_procesados:
            eliminar_archivo(
                f"{COLAS['solicitudes']}/{archivo['name']}",
                archivo["sha"],
                f"Cola procesada: {archivo['name']}"
            )

    return nuevas

# ============================================================
# MAIN
# ============================================================

def main():
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print("\n" + "=" * 60)
    print(f"  CONSOLIDAR CAMBIOS — {ts}")
    print("=" * 60)

    if not GITHUB_TOKEN:
        print("[ERROR] TOKEN_REPO no configurado en secrets")
        raise SystemExit(1)

    total_cambios = 0

    # 1. Seguimiento
    print(f"\n[1/3] Cola cambios_seg...")
    cola_seg = listar_cola(COLAS["cambios_seg"])
    print(f"  {len(cola_seg)} archivos en cola")
    n = consolidar_seg(cola_seg)
    print(f"  {n} cambios aplicados")
    total_cambios += n

    # 2. Programación
    print(f"\n[2/3] Cola cambios_prog...")
    cola_prog = listar_cola(COLAS["cambios_prog"])
    print(f"  {len(cola_prog)} archivos en cola")
    n = consolidar_prog(cola_prog)
    print(f"  {n} cambios aplicados")
    total_cambios += n

    # 3. Solicitudes a demanda
    print(f"\n[3/3] Cola solicitudes...")
    cola_sol = listar_cola(COLAS["solicitudes"])
    print(f"  {len(cola_sol)} archivos en cola")
    n = consolidar_solicitudes(cola_sol)
    print(f"  {n} solicitudes agregadas")
    total_cambios += n

    print(f"\n{'=' * 60}")
    if total_cambios == 0:
        print("  Sin cambios pendientes — nada que hacer.")
    else:
        print(f"  ✓ {total_cambios} cambios consolidados correctamente.")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
