# AuTransp TMS — Contexto para Claude Code

## Proyecto
Sistema de gestión de transporte (TMS) para Grupo CASSA. Dashboard web single-page para coordinación, planificación y seguimiento de viajes de transporte agrícola.

## Arquitectura
- **Frontend:** `index.html` — SPA única, sin framework, sin compilación
- **Backend:** Azure App Service (Node.js) — archivos editados en App Service Editor
- **Base de datos:** Azure SQL Server (`autransp-server.database.windows.net`, BD `autransp-db`)
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
src/functions/syncapi.js
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
    // ...
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
| `costos` | Apuntamientos de maquinaria (histórico) |
| `programacion` | Programación de coordinadores |
| `seguimiento` | Notas de seguimiento web |
| `usuarios` | Usuarios con PIN hasheado |
| `proveedores` | Maestro de proveedores de transporte |
| `equipos` | Equipos/vehículos por proveedor |
| `inventario_alm` | Inventario de insumos por almacén |
| `despachos_alm` | Despachos confirmados |
| `almacenes_cat` | Catálogo A01=COPAL, A05=CANTOR, etc. |
| `apuntamientos_odata` | Apuntamientos desde OData CASE (futuro) |

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
   - `'Finalizada'` OR `'Finalizado'` + costos → **Integrado**
   - `'Finalizada'` OR `'Finalizado'` → **Finalizado** ← DEBE ir antes de En proceso
   - `'En Proceso'` OR `v.fechaInicio` → **En proceso**
   - `'En Carga'` → **En carga**
   - `'Aceptada'` → **Asignado**
3. Sin API: Pendiente / Cancelado

> ⚠️ El SP `sp_refresh_viajes_master` guarda `'Finalizado'` (TMS), no `'Finalizada'` (CASSA).
> El código cubre ambas formas con `eaFinal = ea === 'Finalizada' || ea === 'Finalizado'`.

## Mapeo estados CASSA → TMS
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

## Reglas para editar index.html

### NUNCA hacer
- Cambiar el orden de condiciones en `determinarEstado` sin entender la lógica completa
- Usar `localStorage` o `sessionStorage` — no funcionan en este contexto
- Agregar dependencias externas que no estén ya en el archivo
- Usar `document.write()` ni `innerHTML` con datos de usuario sin sanitizar
- Cambiar `requestTimeout` en db.js
- Hacer push de archivos del App Service desde GitHub (rompe el App Service Editor)

### Siempre verificar al editar
- Que `const reqs = reqPendientes || []` se use en `almActualizarCruce` (NO `requerimientos`)
- Que `ALM_CAT[codAlm]` sea el fallback en `almCargarInventario`
- Que `page-proveedores` esté en el array de `setPage`: `['resumen','detalle','solicitudes','almacen','proveedores','info']`
- Que el módulo JS de proveedores no use concatenación de strings con comillas anidadas — usar template literals

### Importación Excel de proveedores
Enviar en lotes de 25 filas (`LOTE = 25`) para evitar timeout en Azure Function Y1.

## Flujo de datos
```
API CASSA (viajes) → syncapi.js (timer) → viajes_api → sp_refresh_viajes_master → viajes_master
                                                                                        ↓
OData CASE (apuntamientos) → syncApuntamientos.js (futuro, c/hora) → costos (recientes)
                                                                                        ↓
index.html → GET /api/viajes → procesarViajes() → viajes[] → renderTabla()
```

## Pendientes activos
1. `syncApuntamientos.js` — sync OData ReporteApuntesMaquinaria → costos (bloqueado: IT debe habilitar $filter por fecha)
2. Alerta de reqs vencidos (banner si FechaEjecucion < hoy y estado parcial/pendiente)
3. Maestro motoristas
4. Rotar clientSecret Service Principal (clientId: c5232026-a5a0-4060-beaa-89f24e2a59d9) — EXPIRADO
5. Módulo almacén: conectar almCargarInventario y almConfirmarDespacho a API (Paso 3)

## Convenciones de código
- Funciones del módulo almacén: prefijo `alm`
- Funciones del módulo proveedores: prefijo `prov`
- Funciones del módulo planificación: sin prefijo específico (`renderPlan`, `filtrarPlan`, etc.)
- IDs de elementos HTML del panel planificación: prefijo `pp-`
- IDs de filtros de almacén: prefijo `af-`
- IDs de filtros de proveedores: prefijo `prov-fil-`
