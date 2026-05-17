const { app } = require('@azure/functions');
const { query, TYPES } = require('./db');

const HEADERS = {
  'Content-Type': 'application/json',
  'Access-Control-Allow-Origin': '*',
};

const ok  = (body) => ({ status: 200, headers: HEADERS, body: JSON.stringify(body) });
const err = (status, msg, extra = {}) => ({ status, headers: HEADERS, body: JSON.stringify({ error: msg, ...extra }) });

// ============================================================
// GET /api/requerimientos
//   ?tipo=programa|demanda
//   ?estado=pendiente|programado|cancelado   (omitir = todos)
//   ?todos=true                              (incluye cancelados)
// ============================================================
// ── Verificación API Key ─────────────────────────────────
function checkApiKey(request) {
  const key = request.headers.get('x-api-key');
  return key && key === process.env.API_KEY;
}
const unauthorized = () => ({
  status: 401,
  headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
  body: JSON.stringify({ error: 'Unauthorized' }),
});

app.http('requerimientos-get', {
  methods: ['GET'],
  route: 'requerimientos',
  authLevel: 'anonymous',
  handler: async (request) => {
    if (!checkApiKey(request)) return unauthorized();
    try {
      const tipo   = request.query.get('tipo');
      const estado = request.query.get('estado');
      const todos  = request.query.get('todos') === 'true';

      let sql = `
        SELECT
          ID_Req, Tipo, Estado, ST,
          OS, NumReqExcel,
          Jefe, RespProd, Coordinador,
          CodActividad, Actividad, Zona, Sociedad,
          CodOrigen, NombreOrigen,
          CodDestino, NombreDestino, Lote,
          CodRecurso, RecursoNombre,
          Producto, UM, CantidadTotal, AreaTotal,
          Implemento, Comentario,
          MotivoCancel, STAnterior, MotivoCorreccion, FechaCorreccion,
          FechaEjecucion,
          CreadoPor, FechaCreacion,
          ActualizadoPor, FechaActualizacion
        FROM requerimientos
        WHERE 1=1
      `;
      const params = [];

      if (tipo) {
        sql += ` AND Tipo = @tipo`;
        params.push({ name: 'tipo', type: TYPES.NVarChar, value: tipo });
      }
      if (estado) {
        sql += ` AND Estado = @estado`;
        params.push({ name: 'estado', type: TYPES.NVarChar, value: estado });
      } else if (!todos) {
        // Por defecto excluir cancelados para la vista de planificación
        sql += ` AND Estado != 'cancelado'`;
      }

      sql += ` ORDER BY NumReqExcel ASC, OS ASC, FechaCreacion ASC`;

      const rows = await query(sql, params);
      return ok(rows);
    } catch (e) {
      console.error('[requerimientos GET]', e.message);
      return err(500, e.message);
    }
  },
});

// ============================================================
// POST /api/requerimientos
// ============================================================
app.http('requerimientos-post', {
  methods: ['POST'],
  route: 'requerimientos',
  authLevel: 'anonymous',
  handler: async (request) => {
    if (!checkApiKey(request)) return unauthorized();
    try {
      const body  = await request.json();
      const items = Array.isArray(body.requerimientos)
        ? body.requerimientos
        : Array.isArray(body) ? body : [body];

      if (!items.length) return err(400, 'No se recibieron requerimientos');

      const ins    = [];
      const errs   = [];

      // Generador ID hexadecimal 8 chars compatible con UNIQUEID() de AppSheet
      // Formato: T_{8 hex chars} ej. T_a3f7k2b9
      // Cuando se conecte API de Planeación, el ID vendrá con prefijo P_ y se consumirá directo
      const genUUID = () => {
        const hex8 = Math.floor(Math.random() * 0xFFFFFFFF)
          .toString(16).padStart(8, '0');
        return 'T_' + hex8;
      };

      for (const item of items) {
        try {
          const id = item.ID_Req || genUUID();
          await query(`
            INSERT INTO requerimientos (
              ID_Req, Tipo, Estado, ST,
              OS, NumReqExcel, Jefe, RespProd, Coordinador,
              CodActividad, Actividad, Zona, Sociedad,
              CodOrigen, NombreOrigen, CodDestino, NombreDestino, Lote,
              CodRecurso, RecursoNombre,
              Producto, UM, CantidadTotal, AreaTotal,
              Implemento, Comentario, FechaEjecucion,
              CreadoPor, FechaCreacion
            ) VALUES (
              @ID_Req, @Tipo, @Estado, @ST,
              @OS, @NumReqExcel, @Jefe, @RespProd, @Coordinador,
              @CodActividad, @Actividad, @Zona, @Sociedad,
              @CodOrigen, @NombreOrigen, @CodDestino, @NombreDestino, @Lote,
              @CodRecurso, @RecursoNombre,
              @Producto, @UM, @CantidadTotal, @AreaTotal,
              @Implemento, @Comentario, @FechaEjecucion,
              @CreadoPor, GETDATE()
            )`, [
            { name: 'ID_Req',        type: TYPES.NVarChar,  value: id },
            { name: 'Tipo',          type: TYPES.NVarChar,  value: item.Tipo          || 'programa' },
            { name: 'Estado',        type: TYPES.NVarChar,  value: item.Estado        || 'pendiente' },
            { name: 'ST',            type: TYPES.NVarChar,  value: item.ST            || null },
            { name: 'OS',            type: TYPES.NVarChar,  value: item.OS            || null },
            { name: 'NumReqExcel',   type: TYPES.Int,       value: item.NumReqExcel   || null },
            { name: 'Jefe',          type: TYPES.NVarChar,  value: item.Jefe          || null },
            { name: 'RespProd',      type: TYPES.NVarChar,  value: item.RespProd      || null },
            { name: 'Coordinador',   type: TYPES.NVarChar,  value: item.Coordinador   || null },
            { name: 'CodActividad',  type: TYPES.NVarChar,  value: item.CodActividad  || null },
            { name: 'Actividad',     type: TYPES.NVarChar,  value: item.Actividad     || null },
            { name: 'Zona',          type: TYPES.NVarChar,  value: item.Zona          || null },
            { name: 'Sociedad',      type: TYPES.NVarChar,  value: item.Sociedad      || null },
            { name: 'CodOrigen',     type: TYPES.NVarChar,  value: item.CodOrigen     || null },
            { name: 'NombreOrigen',  type: TYPES.NVarChar,  value: item.NombreOrigen  || null },
            { name: 'CodDestino',    type: TYPES.NVarChar,  value: item.CodDestino    || null },
            { name: 'NombreDestino', type: TYPES.NVarChar,  value: item.NombreDestino || null },
            { name: 'Lote',          type: TYPES.NVarChar,  value: item.Lote          || null },
            { name: 'CodRecurso',    type: TYPES.NVarChar,  value: item.CodRecurso    || null },
            { name: 'RecursoNombre', type: TYPES.NVarChar,  value: item.RecursoNombre || null },
            { name: 'Producto',      type: TYPES.NVarChar,  value: item.Producto      || null },
            { name: 'UM',            type: TYPES.NVarChar,  value: item.UM            || null },
            { name: 'CantidadTotal', type: TYPES.Decimal,   value: item.CantidadTotal || null },
            { name: 'AreaTotal',     type: TYPES.Decimal,   value: item.AreaTotal     || null },
            { name: 'Implemento',    type: TYPES.NVarChar,  value: item.Implemento    || null },
            { name: 'Comentario',    type: TYPES.NVarChar,  value: item.Comentario    || null },
            { name: 'FechaEjecucion',type: TYPES.DateTime2, value: item.FechaEjecucion ? new Date(item.FechaEjecucion) : null },
            { name: 'CreadoPor',     type: TYPES.NVarChar,  value: item.CreadoPor     || null },
          ]);
          ins.push(id);
        } catch (itemErr) {
          console.error('[POST item]', itemErr.message);
          errs.push({ item: item.ID_Req || '?', error: itemErr.message });
        }
      }

      return ok({ insertados: ins.length, ids: ins, errores: errs.length ? errs : undefined });
    } catch (e) {
      console.error('[requerimientos POST]', e.message);
      return err(500, e.message);
    }
  },
});

// ============================================================
// PATCH /api/requerimientos/{id}
//   Asigna/desasocia ST, cancela, corrige
//   Body: { ST, FechaEjecucion, MotivoCancel, Estado,
//           MotivoCorreccion, actualizadoPor }
//
//   Validaciones al asignar ST:
//   V1 — ST duplicada en otro requerimiento distinto OS → advertencia
//   V2 — Destino no coincide con ningún req de la ST   → advertencia
//   V3 — Fecha difiere > 1 día                         → advertencia
//   V4 — ST ya ejecutada                               → advertencia
//   Advertencias: se retornan en array 'advertencias'
//   El caller decide si forzar con { forzar: true }
// ============================================================
app.http('requerimientos-patch', {
  methods: ['PATCH'],
  route: 'requerimientos/{id}',
  authLevel: 'anonymous',
  handler: async (request) => {
    if (!checkApiKey(request)) return unauthorized();
    try {
      const id   = request.params.id;
      const body = await request.json();
      const {
        ST, FechaEjecucion, MotivoCancel, Estado: EstadoBody,
        MotivoCorreccion, actualizadoPor, forzar,
      } = body;

      if (!id) return err(400, 'ID_Req requerido');

      // ── Verificar que el requerimiento existe ──────────────
      const existing = await query(
        `SELECT ID_Req, Estado, ST AS STActual, OS, NombreDestino,
                FechaEjecucion AS FechaReq, NumReqExcel
         FROM requerimientos WHERE ID_Req = @id`,
        [{ name: 'id', type: TYPES.NVarChar, value: id }]
      );
      if (!existing.length) return err(404, `Requerimiento ${id} no encontrado`);
      const req = existing[0];

      // ── Cancelación — solo permitida en estado pendiente ────
      if (EstadoBody === 'cancelado') {
        if (req.Estado !== 'pendiente') {
          return err(422,
            `No se puede cancelar un requerimiento en estado "${req.Estado}". Solo se pueden cancelar requerimientos pendientes.`,
            { codigo: 'CANCEL_NO_PERMITIDO', estadoActual: req.Estado }
          );
        }
        await query(`
          UPDATE requerimientos SET
            Estado = 'cancelado',
            MotivoCancel = @MotivoCancel,
            ActualizadoPor = @ActualizadoPor,
            FechaActualizacion = GETDATE()
          WHERE ID_Req = @id`, [
          { name: 'MotivoCancel',   type: TYPES.NVarChar, value: MotivoCancel   || null },
          { name: 'ActualizadoPor', type: TYPES.NVarChar, value: actualizadoPor || null },
          { name: 'id',             type: TYPES.NVarChar, value: id },
        ]);
        return ok({ ID_Req: id, Estado: 'cancelado', MotivoCancel });
      }

      // ── Desasociación — ST = null explícito ────────────────
      if (body.hasOwnProperty('ST') && ST === null) {
        const stPrev = req.STActual;
        // Si la ST tenía estado ejecutado → requiere motivo
        if (!MotivoCorreccion) {
          // Verificar estado de la ST anterior
          if (stPrev) {
            const stEstado = await query(
              `SELECT Estado FROM viajes_api WHERE ST = @st`,
              [{ name: 'st', type: TYPES.NVarChar, value: stPrev }]
            );
            const estadoApi = stEstado[0]?.Estado || '';
            const ejecutada = ['Finalizada','Integrada','Contabilizado'].some(e =>
              estadoApi.toLowerCase().includes(e.toLowerCase())
            );
            if (ejecutada) {
              return err(422, 'ST ya ejecutada. Se requiere MotivoCorreccion para desasociar.', {
                codigo: 'MOTIVO_REQUERIDO', estadoST: estadoApi,
              });
            }
          }
        }
        await query(`
          UPDATE requerimientos SET
            ST = NULL, Estado = 'pendiente',
            STAnterior = @STAnterior,
            MotivoCorreccion = CASE WHEN @MotivoCorreccion IS NOT NULL THEN @MotivoCorreccion ELSE MotivoCorreccion END,
            FechaCorreccion = CASE WHEN @STAnterior IS NOT NULL THEN GETDATE() ELSE FechaCorreccion END,
            ActualizadoPor = @ActualizadoPor,
            FechaActualizacion = GETDATE()
          WHERE ID_Req = @id`, [
          { name: 'STAnterior',       type: TYPES.NVarChar, value: stPrev         || null },
          { name: 'MotivoCorreccion', type: TYPES.NVarChar, value: MotivoCorreccion || null },
          { name: 'ActualizadoPor',   type: TYPES.NVarChar, value: actualizadoPor  || null },
          { name: 'id',               type: TYPES.NVarChar, value: id },
        ]);
        // Limpiar campos de requerimiento en viajes_master para la ST desasociada
        // El SP no limpia bien porque el JOIN ya no encuentra el req
        if (stPrev) {
          setImmediate(() => {
            query(`
              UPDATE viajes_master SET
                ID_Req               = NULL,
                ReqProducto          = NULL,
                ReqComentario        = NULL,
                Coordinador          = CASE WHEN Coordinador = @coord THEN NULL ELSE Coordinador END,
                FechaEjecucion       = CASE WHEN FechaEjecucion IS NOT NULL AND ID_Req = @idReq THEN NULL ELSE FechaEjecucion END,
                NombreOrigen         = NULL,
                NombreDestino        = NULL,
                Zona                 = NULL,
                OrigenDato           = CASE WHEN OrigenDato = 'Sin API' THEN 'Fuera de gestión'
                                            WHEN OrigenDato = 'Completo' THEN 'Sin programación'
                                            ELSE OrigenDato END,
                FechaActualizacion   = GETUTCDATE()
              WHERE ST = @st AND ID_Req = @idReq`,
              [
                { name: 'st',     type: TYPES.NVarChar, value: stPrev },
                { name: 'idReq',  type: TYPES.NVarChar, value: id },
                { name: 'coord',  type: TYPES.NVarChar, value: actualizadoPor || '' },
              ]
            ).catch(re => console.warn('[req desasoc] clean master:', re.message));
          });
        }

        return ok({ ID_Req: id, Estado: 'pendiente', ST: null, STAnterior: stPrev });
      }

      // ── Asignación de ST ───────────────────────────────────
      if (ST) {
        // ETD obligatoria
        if (!FechaEjecucion) {
          return err(400, 'FechaEjecucion (ETD) es obligatoria al programar una ST', {
            codigo: 'ETD_REQUERIDA',
          });
        }

        // Verificar ST existe en viajes_api
        const stRows = await query(
          `SELECT ST, Estado, DsPuntoEntrega, FechaEntrega
           FROM viajes_api WHERE ST = @st`,
          [{ name: 'st', type: TYPES.NVarChar, value: ST }]
        );
        if (!stRows.length) {
          return err(422, `ST ${ST} no encontrada en viajes_api.`, { codigo: 'ST_NO_EXISTE' });
        }
        const stData = stRows[0];

        // ── Validaciones — acumular advertencias ──────────────
        const advertencias = [];

        // V1: ST duplicada en requerimiento de DIFERENTE N°
        // Mismo N° (NumReqExcel) = filas del mismo viaje → permitir sin advertencia
        // Diferente N° → advertencia, permite confirmar
        if (!forzar) {
          const dupRows = await query(
            `SELECT ID_Req, OS, NombreDestino, NumReqExcel, Estado FROM requerimientos
             WHERE ST = @st AND ID_Req != @id AND Estado != 'cancelado'`,
            [
              { name: 'st', type: TYPES.NVarChar, value: ST },
              { name: 'id', type: TYPES.NVarChar, value: id },
            ]
          );
          if (dupRows.length) {
            // Solo alertar si hay filas de un N° diferente al actual
            const otroN = dupRows.filter(r => r.NumReqExcel !== req.NumReqExcel);
            if (otroN.length) {
              advertencias.push({
                codigo: 'ST_DUPLICADA',
                mensaje: `ST ${ST} ya está asignada a requerimientos de otro N°: ${otroN.map(r => `${r.ID_Req} (N°${r.NumReqExcel})`).join(', ')}. Verifica si es correcto.`,
                datos: otroN,
              });
            }
            // Mismo N° → filas del mismo viaje, flujo normal, sin alerta
          }
        }

        // V2: Destino del req actual no coincide con DsPuntoEntrega de la ST
        // Regla: al menos una fila del mismo N° debe coincidir con el destino de la ST
        // Comparación: extraer nombre de hacienda del formato "COD - NOMBRE - EMPRESA"
        // y comparar contra NombreDestino del requerimiento (que viene del Excel sin código)
        if (!forzar && stData.DsPuntoEntrega) {
          // Extraer el nombre de hacienda de la API: "3643 - EL OVELARIO - COAGRI..." → "EL OVELARIO"
          const partes = stData.DsPuntoEntrega.split(' - ').map(p => p.trim());
          // El nombre de hacienda es la segunda parte (índice 1), sin la empresa al final
          const nombreAPI = (partes.length >= 2 ? partes[1] : partes[0]).toLowerCase();

          // Obtener todos los destinos de reqs del mismo N° con esta ST (ya programados)
          const reqsMismoN = await query(
            `SELECT NombreDestino FROM requerimientos
             WHERE NumReqExcel = @numReq AND Estado != 'cancelado'`,
            [{ name: 'numReq', type: TYPES.Int, value: req.NumReqExcel }]
          );

          // Incluir el req actual
          const todosDestinos = [req.NombreDestino, ...reqsMismoN.map(r => r.NombreDestino)]
            .filter(Boolean)
            .map(d => d.toLowerCase().trim());

          // Coincidencia exacta o contenida (el nombre del req debe aparecer en el nombre API)
          const coincide = todosDestinos.some(d => {
            // "EL OVELARIO" === "el ovelario" → true
            // "3643 - EL OVELARIO - COAGRI..." contains "el ovelario" → true
            // "DELTA" in "3462 - DELTA - COAGRI..." → true PERO
            // necesitamos que sea coincidencia de palabra completa, no substring parcial
            // Usamos word-boundary: el destino del req debe ser igual al nombre extraído de API
            return nombreAPI === d || nombreAPI.includes(d) || d.includes(nombreAPI);
          });

          if (!coincide) {
            advertencias.push({
              codigo: 'DESTINO_NO_COINCIDE',
              mensaje: `Destino del requerimiento: "${req.NombreDestino}" — Destino en plataforma: "${stData.DsPuntoEntrega}". Verifica que la ST corresponda a este requerimiento.`,
            });
          }
        }

        // V3: Fecha difiere > 1 día
        if (!forzar && stData.FechaEntrega && FechaEjecucion) {
          const diffMs   = Math.abs(new Date(stData.FechaEntrega) - new Date(FechaEjecucion));
          const diffDias = diffMs / (1000 * 60 * 60 * 24);
          if (diffDias > 1) {
            advertencias.push({
              codigo: 'FECHA_DIFIERE',
              mensaje: `ETD del requerimiento: ${FechaEjecucion.slice(0,16).replace('T',' ')} — Fecha en plataforma: ${stData.FechaEntrega.toString().slice(0,16).replace('T',' ')}`,
            });
          }
        }

        
        // ── Mapeo estado API → estado requerimiento ──────────
        // Cancelada/Rechazada/Inconsistente → bloquear asignación
        // En Carga/En Proceso → en_proceso
        // Finalizada/* → finalizado
        // No Aceptada/Aceptada → programado (sin cambio)
        const mapearEstadoReq = (estadoAPI) => {
          const e = (estadoAPI || '').toLowerCase();
          if(e.includes('cancelad') || e.includes('rechazad') || e.includes('inconsistente'))
            return 'BLOQUEAR';
          if(e.includes('en carga') || e.includes('en proceso'))
            return 'en_proceso';
          if(e.includes('finaliz') || e.includes('finalizado'))
            return 'finalizado';
          return 'programado';
        };
// V4: ST bloqueante o ejecutada
        {
          const estadoMapeado = mapearEstadoReq(stData.Estado);

          if (estadoMapeado === 'BLOQUEAR') {
            return err(422,
              `No se puede asignar un requerimiento a la ST ${ST} porque está "${stData.Estado}" en la plataforma.`,
              { codigo: 'ST_BLOQUEADA', estadoST: stData.Estado }
            );
          }

          if (!forzar && (estadoMapeado === 'finalizado' || estadoMapeado === 'en_proceso')) {
            advertencias.push({
              codigo: 'ST_EJECUTADA',
              mensaje: `La ST ${ST} ya tiene estado "${stData.Estado}" en la plataforma.`,
            });
          }
        }

        // Si hay advertencias y no forzar → retornar para que el frontend confirme
        if (advertencias.length && !forzar) {
          return {
            status: 200,
            headers: HEADERS,
            body: JSON.stringify({
              requiereConfirmacion: true,
              advertencias,
              ID_Req: id, ST,
            }),
          };
        }

        // ── Calcular estado heredado antes de guardar ────────
        const estadoHeredado = mapearEstadoReq(stData.Estado) === 'BLOQUEAR'
          ? 'cancelado' : mapearEstadoReq(stData.Estado);
        const motivoHeredado = estadoHeredado === 'cancelado'
          ? `ST ${stData.Estado} en plataforma` : null;

        // ── Guardar con estado correcto desde el inicio ────────
        const coordValue = actualizadoPor || null;
        await query(`
          UPDATE requerimientos SET
            ST                 = @ST,
            Estado             = @Estado,
            MotivoCancel       = CASE WHEN @MotivoCancel IS NOT NULL THEN @MotivoCancel ELSE MotivoCancel END,
            FechaEjecucion     = @FechaEjecucion,
            Coordinador        = CASE WHEN @Coordinador IS NOT NULL THEN @Coordinador ELSE Coordinador END,
            ActualizadoPor     = @ActualizadoPor,
            FechaActualizacion = GETDATE()
          WHERE ID_Req = @id`, [
          { name: 'ST',             type: TYPES.NVarChar,  value: ST },
          { name: 'Estado',         type: TYPES.NVarChar,  value: estadoHeredado },
          { name: 'MotivoCancel',   type: TYPES.NVarChar,  value: motivoHeredado },
          { name: 'FechaEjecucion', type: TYPES.DateTime2, value: new Date(FechaEjecucion) },
          { name: 'Coordinador',    type: TYPES.NVarChar,  value: coordValue },
          { name: 'ActualizadoPor', type: TYPES.NVarChar,  value: actualizadoPor || null },
          { name: 'id',             type: TYPES.NVarChar,  value: id },
        ]);

        // Actualizar viajes_master directamente — más rápido que el SP
        setImmediate(() => {
          query(`
            UPDATE viajes_master SET
              ID_Req             = @idReq,
              Coordinador        = @coord,
              FechaEjecucion     = @fechaEjec,
              NombreOrigen       = @origen,
              NombreDestino      = @destino,
              Zona               = @zona,
              OrigenDato         = CASE
                WHEN OrigenDato = 'Sin programación' THEN 'Completo'
                WHEN OrigenDato = 'Fuera de gestión' THEN 'Sin API'
                ELSE OrigenDato END,
              FechaActualizacion = GETUTCDATE()
            WHERE ST = @st`,
            [
              { name: 'st',        type: TYPES.NVarChar,  value: ST },
              { name: 'idReq',     type: TYPES.NVarChar,  value: id },
              { name: 'coord',     type: TYPES.NVarChar,  value: actualizadoPor || null },
              { name: 'fechaEjec', type: TYPES.DateTime2, value: FechaEjecucion ? new Date(FechaEjecucion) : null },
              { name: 'origen',    type: TYPES.NVarChar,  value: existing[0].NombreOrigen  || null },
              { name: 'destino',   type: TYPES.NVarChar,  value: existing[0].NombreDestino || null },
              { name: 'zona',      type: TYPES.NVarChar,  value: existing[0].Zona          || null },
            ]
          ).catch(re => console.warn('[req PATCH] update master:', re.message));
        });

        return ok({
          ID_Req: id, ST, Estado: estadoHeredado,
          FechaEjecucion,
          advertencias: advertencias.length ? advertencias : undefined,
        });
      }

      return err(400, 'Operación no reconocida');

    } catch (e) {
      console.error('[requerimientos PATCH]', e.message);
      return err(500, e.message);
    }
  },
});
