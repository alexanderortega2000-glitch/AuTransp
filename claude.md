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

## ⚠️ REGLA CRÍTICA — index.html
**NUNCA usar `/mnt/user-data/uploads/index.html` como base para ediciones.**
**SIEMPRE pedir al usuario que suba el index.html actual del repo antes de editar.**
El archivo en uploads puede ser una versión antigua y causa pérdida de funcionalidad.

## Archivos del App Service (NO están en GitHub)
Editados directamente en Azure App Service Editor:
```
src/functions/viajes.js
src/functions/requerimientos.js    ← fix POST a_demanda + PATCH Coordinador aplicado
src/functions/usuarios.js          ← fix PinHash case-insensitive aplicado
src/functions/seguimiento.js
src/functions/programacion.js
src/functions/proveedores.js
src/functions/inventario.js
src/functions/despachos.js
src/functions/almacenes.js
src/functions/syncapi.js           ← v5 con 3 timers + guard + sync_log
src/functions/syncApuntamientos.js ← NUEVO — sync OData CASE → costos
db.js                              ← connection pool 2-10 conexiones
host.json                          ← functionTimeout: 10 minutos
```

## Patrón de Azure Functions (v4)
```javascript
const { app } = require('@azure/functions');
const { query, TYPES } = require('../../db');
app.http('nombre', {
  methods: ['GET'],
  route: 'ruta',
  authLevel: 'anonymous',
  handler: async (request) => {
    const apiKey = request.headers.get('x-api-key');
    if (!apiKey || apiKey !== process.env.API_KEY)
      return { status: 401, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify({ error: 'Unauthorized' }) };
    return { status: 200, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify(resultado) };
  }
});
```

## Tablas principales en BD
| Tabla | Descripción |
|---|---|
| `viajes_master` | Viajes procesados — fuente del dashboard |
| `viajes_api` | Viajes raw desde API CASSA |
| `requerimientos` | Plan de transporte por producto |
| `costos` | Apuntamientos desde OData CASE (desde jun-2026) + histórico CSV |
| `costos_sin_st` | Cuarentena — apuntamientos sin ST para auditoría |
| `programacion` | Programación de coordinadores |
| `seguimiento` | Notas de seguimiento web |
| `usuarios` | Usuarios con PinHash SHA2-256 en formato hex uppercase |
| `proveedores` | Maestro de proveedores de transporte |
| `equipos` | Equipos/vehículos por proveedor |
| `sync_log` | Log de ejecuciones del sync |

## Columnas tabla requerimientos
```
ID_Req, Tipo, Estado, ST, OS, NumReqExcel, Jefe, RespProd, Coordinador,
CodActividad, Actividad, Zona, Sociedad, CodOrigen, NombreOrigen,
CodDestino, NombreDestino, Lote, CodRecurso, RecursoNombre, Producto,
UM, CantidadTotal, AreaTotal, Implemento, Comentario, CreadoPor,
FechaCreacion, ActualizadoPor, FechaActualizacion, FechaEjecucion,
MotivoCancel, STAnterior, MotivoCorreccion, FechaCorreccion,
ST2, ST3, ST4, CantidadST1, CantidadST2, CantidadST3, CantidadST4
```

## Tipos de requerimiento
- `programa` — viene del Excel de planificación
- `a_demanda` — creado desde el dashboard web

## syncapi.js v5 — Arquitectura de timers
```
Timer 1: syncAPI          → */5 min, 04-20h CST → hoy-3d a hoy+30d
Timer 2: syncAPI-reciente → c/hora, 24/7        → hoy-7d a hoy+30d
Timer 3: syncAPI-historico → 01:00 AM diario    → hoy-90d a hoy+30d
```

## syncApuntamientos.js — OData CASE
```
URL: https://telemetriacase.grupocassa.com/ODataServices/odata/ReporteApuntesMaquinaria
Filtro: FechaDigitacion ge 'YYYY-MM-DDT00:00:00.000Z' and FechaDigitacion le 'YYYY-MM-DDT23:59:59.999Z'
ST: campo nativo ST (Int32) o regex /ST:\s*(\d+)/i en Observacion
Timer 1: core-5min   → hoy-7d,  04-20h CST
Timer 2: reciente-1h → hoy-15d, 24/7
Timer 3: historico   → hoy-60d, 04:00 AM UTC
Chunks: 3 días por consulta para evitar timeouts
Sin ST → costos_sin_st (cuarentena)
```

## sp_refresh_viajes_master v2
- Filtro de fecha en UNION antes del JOIN (optimización crítica)
- Tiempo: ~9s (antes 24s)
- Índices usados: IX_vapi_Fecha, IX_programacion_FechaEjec, IX_costos_Fecha

## Reglas de Área (consistentes en SP, syncApuntamientos, scripts Python)
```
Cosecha:  BkRecurso IN (42012,83008) AND BkHacienda1 IN (1983,1984,1987,1994,2983,2984,2987,2996)
Red Vial: BkRecurso IN (42004,40003,40002,42007,40001,83003)
Varios:   BkRecurso IN (9013,83007,83005,83006,41002,42008,42001,42005)
EsHistorico: FechaApunte < hoy-60d (dinámico)
Pagado: IntegracionPago > 0 OR IntegracionCobro > 0
```

## Lógica de estados (determinarEstado en index.html)
Orden de evaluación:
1. `pagado = true` → **Contabilizado**
2. Con API (`enAPI`):
   - `eaFinal && enCostos` → **Integrado**
   - `eaFinal` → **Finalizado** (cubre 'Finalizada' y 'Finalizado')
   - `eaObs` → **Fin. c/obs**
   - `eaProc || fechaInicio` → **En proceso**
   - `eaCarga` → **En carga**
   - `'Aceptada'/'Asignado'` → **Asignado**
3. Sin API → Pendiente/Cancelado

## enCostos — regla importante
`enCostos = true` solo si hay dato económico real:
`r.AreaCosto || r.A_Pagar || r.ValorTotalViaje || r.CostoReal`
**NO** activar solo por `AreaEfectiva` (que se calcula desde tipo de viaje)

## Mapeo estados SP → frontend
SP guarda estados ya mapeados (no originales de CASSA):
- 'Finalizada' → 'Finalizado'
- 'Cancelada' → 'Cancelado'
- 'En Carga' → 'En carga'
- 'Aceptada' → 'Asignado'
`determinarEstado` cubre ambas formas.

## Modal Solicitud a Demanda — estado actual
Campos implementados:
- Fecha ejecución + ETD (hora) con validación mínimo hoy+16h
- Label parámetro de servicio (dentro/fuera de parámetro)
- Solicitante: readonly, prellenado con sesion.nombre
- Descripción de carga/Producto: dropdown (Personal/Tubería de Riego/Otro+texto max 5 palabras)
- Cantidad: entero, sin decimales
- UM: TM, BULTOS, PERSONAS, UNIDADES
- Tipo de servicio: dropdown agrupado por Cosecha/Red Vial/Varios
- Proveedor Sugerido, Origen, Destino, Lote, OS, Observaciones
- Restricción: solo rol coordinador/controlador/admin

## Dropdown Tipo de Servicio (recursos reales del histórico)
```
Cosecha:  Transporte de Personal, Transporte de Tripulaciones
Red Vial: Transporte en Lowboy
Varios:   Transporte de Fertilizantes, Transporte de Material Riego,
          Transporte de Agua, Transporte de Materiales Varios,
          Transporte de Caña Semilla, Transporte de Caña Semilla - Viaje
```

## ⚠️ PROBLEMA ACTIVO — Pestaña Proveedores perdida
El index.html actual en GitHub perdió la pestaña de Proveedores.
**Antes de cualquier edición al index.html:**
1. Recuperar la pestaña de Proveedores del historial de commits de GitHub
2. O pedir al usuario que suba el index.html correcto
3. NUNCA usar el archivo de uploads como base

## Performance baseline
| Operación | P50 | Estado |
|---|---|---|
| GET /api/viajes | 677ms | ✅ |
| sp_refresh_viajes_master | ~9s | ✅ |
| syncAPI core-5min | ~60s | ✅ |
| syncApuntamientos core-5min | ~15s | ✅ |
| syncApuntamientos reciente-1h (15d) | ~42s | ✅ |

## Plan de BD y escalado
- Actual: Basic 5 DTU
- Subir a Standard S1 cuando lleguen 10-12 usuarios (~3 meses)
- Node.js 22 → 24 antes de abril 2027

## PENDIENTES — Programa de trabajo

### Inmediato
1. **Recuperar pestaña Proveedores** en index.html
2. **Req a demanda en lista planificación** — no aparece en lista (Tipo='demanda' vs filtro)
3. **Cantidad en panel planificación** — mostrar CantidadTotal del req a demanda
4. **Cantidad preestablecida al programar** — prellenar campo con CantidadTotal

### Fixes pendientes de validar
5. ETD aviso parámetro — div ns-etd-aviso no aparece en producción
6. Servicios dropdown — confirmar que llegó la versión correcta
7. eBadge label en_proceso → "En proceso" en panel planificación

### Pruebas funcionales pendientes
8. Proveedores — importación masiva lotes de 25
9. Proveedores — nuevo manual, editar, desactivar
10. Proveedores — equipos asociados

### Desarrollo pendiente
11. Log de modificaciones viajes/requerimientos (tabla viajes_log + botón historial)
12. Alerta reqs vencidos — banner FechaEjecucion < hoy
13. Módulo almacén Paso 3 — conectar API
14. Maestro motoristas
15. config.json en repo — eliminar 404
16. Rotar clientSecret Service Principal (EXPIRADO) — clientId: c5232026-a5a0-4060-beaa-89f24e2a59d9
17. syncApuntamientos — validar registros en costos_sin_st
18. Validar skip nocturno syncApuntamientos

### Infraestructura
19. Subir Standard S1 cuando lleguen 10-12 usuarios
20. Application Logging activar en App Service
21. Node.js 22 → 24 (antes abril 2027)

## Archivos generados en esta sesión (en /mnt/user-data/outputs)
| Archivo | Estado |
|---|---|
| `syncapi.js` | ✅ v5 activo |
| `syncApuntamientos.js` | ✅ activo en App Service |
| `requerimientos.js` | ✅ con fix POST a_demanda + PATCH Coordinador |
| `sp_refresh_viajes_master_v2.sql` | ✅ aplicado en BD |
| `autransp_sync_log.sql` | ✅ tabla creada |
| `costos_sin_st.sql` | ✅ tabla creada |
| `consolidar_costos_actual.py` | ✅ con rename columnas |
| `consolidar_costos_hist.py` | ✅ con rename columnas |
| `migrar_costos.py` | ✅ con rename columnas |
| `host.json` | ✅ timeout 10 min aplicado |
| `index.html` | ⚠️ versión en outputs puede no tener proveedores |
| `CLAUDE.md` | ✅ este archivo |
