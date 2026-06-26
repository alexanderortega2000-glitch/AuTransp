# AuTransp TMS — Contexto para Claude Code

## Proyecto
Sistema de gestión de transporte (TMS) para Grupo CASSA. Dashboard web single-page para coordinación, planificación y seguimiento de viajes de transporte agrícola.

## Arquitectura
- **Frontend:** `index.html` — SPA única, sin framework, sin compilación
- **Backend:** Azure App Service (Node.js, plan Consumption Y1) — archivos editados en App Service Editor
- **Base de datos:** Azure SQL Server (`autransp-server.database.windows.net`, BD `autransp-db`, plan Basic 5 DTU)
- **Hosting:** Azure Static Web Apps (CDN) — auto-deploy desde rama `main`
- **Repo:** `alexanderortega2000-glitch/AuTransp` rama `main`

## URLs
- Dashboard: https://delightful-mushroom-023410f0f.7.azurestaticapps.net
- API base: https://autransp-api-awekazaja2gngee4.centralus-01.azurewebsites.net/api
- API Key header: `x-api-key: autransp-2026-k9mX4vQzRpL7nWjYeB3sC8dT`

## Archivos del repo (GitHub → Static Web Apps)
```
index.html          ← frontend completo, único archivo a editar
CLAUDE.md           ← este archivo
```

## Archivos del App Service (NO están en GitHub)
Editados directamente en Azure App Service Editor — NO hacer push desde Claude Code:
```
src/functions/viajes.js
src/functions/requerimientos.js
src/functions/usuarios.js
src/functions/seguimiento.js
src/functions/programacion.js
src/functions/proveedores.js
src/functions/inventario.js
src/functions/despachos.js
src/functions/almacenes.js
src/functions/syncapi.js        ← v5 activo — 3 timers diferenciados
db.js
auth.js
index.js
```

## Patrón de Azure Functions (v4)
```javascript
const { app } = require('@azure/functions');
const { query, TYPES } = require('../../db');

app.http('nombre-get', {
  methods: ['GET'],
  route: 'ruta',
  authLevel: 'anonymous',
  handler: async (request) => {
    const apiKey = request.headers.get('x-api-key');
    if (!apiKey || apiKey !== process.env.API_KEY) {
      return { status: 401,
        headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify({ error: 'Unauthorized' }) };
    }
    return {
      status: 200,
      headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
      body: JSON.stringify(resultado),
    };
  }
});
```

## Patrón db.js
- Driver: `tedious` (NO `mssql`)
- Exports: `{ query, TYPES }` — NO exporta `getConnection` ni `sql`
- Parámetros: `[{ name: 'param', type: TYPES.NVarChar, value: 'valor' }]`
- `requestTimeout`: 120,000ms (NO cambiar)

## Tablas principales en BD
| Tabla | Descripción |
|---|---|
| `viajes_master` | Viajes procesados — fuente del dashboard |
| `viajes_api` | Viajes raw desde API CASSA |
| `requerimientos` | Plan de transporte por producto |
| `costos` | Apuntamientos de maquinaria (carga manual) |
| `programacion` | Programación de coordinadores |
| `seguimiento` | Notas de seguimiento web |
| `usuarios` | Usuarios con PIN hasheado |
| `proveedores` | Maestro de proveedores de transporte |
| `equipos` | Equipos/vehículos por proveedor |
| `inventario_alm` | Inventario de insumos por almacén |
| `despachos_alm` | Despachos confirmados |
| `almacenes_cat` | Catálogo A01=COPAL, A05=CANTOR, etc. |
| `apuntamientos_odata` | Apuntamientos desde OData CASE (pendiente) |

## Roles de usuario
- `admin` — acceso total, todas las pestañas
- `coordinador` — Planificación, Almacén, Proveedores
- `controlador` — Detalle, seguimiento

## Variables globales críticas en index.html
```javascript
viajes          // array de viajes procesados (fuente del dashboard)
vFilt           // array filtrado para la tabla
reqPendientes   // array de requerimientos (fuente de Planificación y Almacén)
sesion          // { usuario, nombre, rol }
ALM             // estado del módulo almacén
ALM_CAT         // { A01:'COPAL', A02:'DIAMANTE', ... } catálogo almacenes
PROV            // estado del módulo proveedores
_pendingAdvCallbacks // callbacks de advertencias de programación
```

## Lógica de estados de viaje (determinarEstado)
Orden de evaluación — NO alterar:
1. `pagado = true` → **Contabilizado**
2. Con API (`enAPI`): evalúa `estadoAPI` normalizado:
   - `eaFinal` = `'Finalizada'` OR `'Finalizado'` (SP guarda TMS, API envía CASSA)
   - `eaFinal` + costos → **Integrado**
   - `eaFinal` → **Finalizado** ← DEBE ir ANTES de En proceso
   - `eaProc` OR `v.fechaInicio` → **En proceso**
   - `eaCarga` → **En carga**
   - `'Aceptada'` → **Asignado**
3. Sin API: Pendiente / Cancelado

> ⚠️ El SP guarda `'Finalizado'` (TMS), no `'Finalizada'` (CASSA).
> El código cubre ambas con `eaFinal = ea === 'Finalizada' || ea === 'Finalizado'`
> NO usar `v.fechaInicio` como único criterio de En proceso — causa bug donde Finalizado → En proceso

## Mapeo estados CASSA → TMS (en SP)
| CASSA | TMS |
|---|---|
| No Aceptada | Prov. sin confirmar |
| Aceptada | Asignado |
| En Carga | En carga |
| En Proceso | En proceso |
| Finalizada | Finalizado |
| Finalizada sin Aceptar / Finalizado sin Reanudar / Inconsistente | Fin. c/obs |
| Cancelada | Cancelado |
| Rechazada | Rechazado |

## syncapi.js v5 — Arquitectura de timers
```
Timer 1: syncAPI          → */5 min, solo 04-20h CST → hoy-3d a hoy+30d  (85% viajes)
Timer 2: syncAPI-reciente → cada hora, 24/7          → hoy-14d a hoy+30d (10% viajes)
Timer 3: syncAPI-historico → 01:00 AM diario         → hoy-90d a hoy+30d (5% viajes)
Endpoint: POST /api/sync  → backfill manual con diasAtras/diasAdelante
```
- API CASSA: `logistico.grupocassa.com` — NO soporta filtro por timestamp de modificación
- El skip nocturno usa `horaCST() = getUTCHours() - 6` — pendiente validar que funcione
- Costos: carga manual hasta que IT habilite endpoint OData con filtro de fecha

## sp_refresh_viajes_master — Estado actual
- **Versión:** v2 (ALTER aplicado en BD)
- **Optimización clave:** filtro de fecha aplicado en UNION antes del JOIN
- **Tiempo:** 24,630ms → 8,944ms (mejora 63%)
- **Índices usados:** `IX_vapi_Fecha`, `IX_programacion_FechaEjec`, `IX_costos_Fecha`
- Tabla temporal `#todos_st` tiene índice clustered antes del MERGE

## Performance baseline (26-Jun-2026)
| Operación | P50 | Promedio | Notas |
|---|---|---|---|
| GET /api/viajes | 677ms | 1,003ms | Lectura usuario — aceptable |
| GET /api/requerimientos | 85ms | 114ms | ✅ |
| sp_refresh_viajes_master | ~9,000ms | — | Background, usuario no lo siente |
| SELECT viajes_master (1 día) | ~0ms | — | Instantáneo |
| syncAPI duración total | — | 76,258ms | 73s = API CASSA día por día + SP |
| syncAPI-reciente duración | — | 214,136ms | 214s = 44 días de API CASSA |

## Plan de BD y escalado
- **Actual:** Basic (5 DTU, $4.99/mes)
- **Umbral de alerta:** P50 de viajes > 3,000ms o errores de timeout en logs
- **Acción:** subir a Standard S1 (20 DTU, $14.72/mes) cuando lleguen 10-12 usuarios
- **Producción esperada:** 15 usuarios en ~5 meses, max 8 actualmente
- **Sin downtime** al cambiar de plan en Azure Portal

## OData CASE — Estado de integración
- **URL base:** `https://telemetriacase.grupocassa.com/ODataServices/odata/`
- **Entidades disponibles:** `CombustibleEquipo`, `ReporteApuntesMaquinaria`
- **Limitación crítica:** usa Prisma ORM resolviendo filtros en memoria — NO soporta `$filter` por fecha
- **ReporteApuntesMaquinaria:** equivale a tabla `costos` — mismos campos
- **Solicitud enviada a IT:** habilitar `$filter=FechaApunte ge {fecha}` en ReporteApuntesMaquinaria
- **CombustibleEquipo:** telemetría de maquinaria agrícola — reservado para fase futura
- **Tabla `apuntamientos_odata`:** creada en BD, sin datos — esperando endpoint con filtro

## Reglas para editar index.html

### NUNCA hacer
- Cambiar el orden de condiciones en `determinarEstado` sin entender la lógica completa
- Usar `localStorage` o `sessionStorage`
- Agregar dependencias externas no existentes
- Hacer push de archivos del App Service desde GitHub

### Siempre verificar al editar
- `const reqs = reqPendientes || []` en `almActualizarCruce` (NO `requerimientos`)
- `ALM_CAT[codAlm]` como fallback en `almCargarInventario`
- `page-proveedores` en array de `setPage`: `['resumen','detalle','solicitudes','almacen','proveedores','info']`
- `eaFinal` cubre tanto `'Finalizada'` como `'Finalizado'` en `determinarEstado`
- Importación Excel de proveedores: lotes de 25 (`LOTE = 25`)

## Convenciones de código
- Módulo almacén: prefijo `alm`
- Módulo proveedores: prefijo `prov`
- Módulo planificación: sin prefijo (`renderPlan`, `filtrarPlan`)
- Panel planificación: prefijo `pp-`
- Filtros almacén: prefijo `af-`
- Filtros proveedores: prefijo `prov-fil-`

## PENDIENTES GLOBALES — Programa de trabajo

### Validaciones pendientes (próxima sesión)
1. **Validar skip nocturno syncAPI** — confirmar que entre 20:00-04:00 CST el timer de 5 min hace skip y el nocturno toma el relevo
2. **Auditoría de estados vs API** — comparar estados en viajes_master vs API CASSA para detectar incongruencias
3. **Confirmación volumen viajes** — backend vs frontend, verificar que los conteos coincidan

### Pruebas funcionales pendientes
4. **Planificación — asignación total** — asignar ST completa a requerimiento
5. **Planificación — asignación parcial** — asignar cantidad parcial de un requerimiento
6. **Planificación — múltiples reqs en una ST** — una ST cubre varios requerimientos
7. **Planificación — req parcial en varias STs** — distribuir un req entre múltiples STs
8. **Proveedores — limpiar base y recargar** — probar importación masiva en lotes de 25
9. **Proveedores — nuevo proveedor manual** — crear, editar, desactivar
10. **Proveedores — validar equipos** — verificar que equipos se asocian correctamente al proveedor

### Desarrollo pendiente
11. **syncApuntamientos.js** — bloqueado: IT debe habilitar `$filter` por fecha en OData `ReporteApuntesMaquinaria`
12. **sync_log tabla** — logging persistente de ejecuciones del sync para detectar throttling
13. **Alerta reqs vencidos** — banner si FechaEjecucion < hoy y estado parcial/pendiente
14. **Módulo almacén Paso 3** — conectar `almCargarInventario` y `almConfirmarDespacho` a API
15. **Maestro motoristas** — módulo pendiente de diseño
16. **config.json en repo** — eliminar 404 en carga del dashboard
17. **Rotar clientSecret Service Principal** — clientId: c5232026-a5a0-4060-beaa-89f24e2a59d9 (EXPIRADO)

### Infraestructura pendiente
18. **Monitorear DTU 24h** con syncapi v5 + SP optimizado para decidir si Basic es suficiente
19. **Subir a Standard S1** cuando lleguen 10-12 usuarios simultáneos (~3 meses)
20. **Application Logging** — activar en App Service para historial persistente de errores

## Flujo de datos
```
API CASSA (viajes) → syncapi.js (3 timers) → viajes_api → sp_refresh_viajes_master → viajes_master
                                                                                            ↓
OData CASE (apuntamientos) → syncApuntamientos.js (futuro) → costos (recientes)
                                                                                            ↓
index.html → GET /api/viajes → procesarViajes() → viajes[] → renderTabla()
```

## Flujo Claude Code vs este chat
- **Este chat:** diseño, diagnóstico, decisiones, SQL, archivos App Service, análisis
- **Claude Code:** editar index.html, commitear a GitHub, actualizar CLAUDE.md
- **App Service Editor (siempre manual):** syncapi.js, proveedores.js, almacenes.js, inventario.js, despachos.js
