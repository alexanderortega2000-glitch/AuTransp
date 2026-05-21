const { app } = require('@azure/functions');
const { query, TYPES } = require('../../db');

// ── API Key ───────────────────────────────────────────────────
function checkApiKey(request) {
  const key = request.headers.get('x-api-key');
  return key && key === process.env.API_KEY;
}
const unauthorized = () => ({
  status: 401,
  headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
  body: JSON.stringify({ error: 'Unauthorized' }),
});
const ok  = (data, status = 200) => ({
  status, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
  body: JSON.stringify(data),
});
const err = (status, msg, extra = {}) => ({
  status, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
  body: JSON.stringify({ error: msg, ...extra }),
});

// ── Calcular estado de un req según sus STs ───────────────────
async function recalcularEstadoReq(idReq) {
  const rows = await query(`
    SELECT r.ID_Req, r.ST, r.ST2, r.ST3, r.ST4,
           r.CantidadTotal, r.CantidadST1, r.CantidadST2, r.CantidadST3, r.CantidadST4,
           v1.Estado AS EstadoST1, v2.Estado AS EstadoST2,
           v3.Estado AS EstadoST3, v4.Estado AS EstadoST4
    FROM requerimientos r
    LEFT JOIN viajes_api v1 ON r.ST  = v1.ST
    LEFT JOIN viajes_api v2 ON r.ST2 = v2.ST
    LEFT JOIN viajes_api v3 ON r.ST3 = v3.ST
    LEFT JOIN viajes_api v4 ON r.ST4 = v4.ST
    WHERE r.ID_Req = @id`,
    [{ name: 'id', type: TYPES.NVarChar, value: idReq }]
  );
  if (!rows.length) return 'pendiente';
  const r = rows[0];

  const slots = [
    { st: r.ST,  cant: r.CantidadST1, estado: r.EstadoST1 },
    { st: r.ST2, cant: r.CantidadST2, estado: r.EstadoST2 },
    { st: r.ST3, cant: r.CantidadST3, estado: r.EstadoST3 },
    { st: r.ST4, cant: r.CantidadST4, estado: r.EstadoST4 },
  ].filter(s => s.st);

  if (!slots.length) return 'pendiente';

  const cantTotal    = parseFloat(r.CantidadTotal) || 0;
  const cantAsignada = slots.reduce((s, x) => s + (parseFloat(x.cant) || 0), 0);
  const parcial      = cantTotal > 0 && cantAsignada < cantTotal;

  const mapEstado = e => {
    if (!e) return 'programado';
    const el = e.toLowerCase();
    if (['cancelada','rechazada','inconsistente'].some(x => el.includes(x))) return 'cancelado';
    if (['finalizada','finalizado'].some(x => el.includes(x))) return 'finalizado';
    if (['en proceso','en carga'].some(x => el.includes(x))) return 'en_proceso';
    return 'programado';
  };

  const estados = slots.map(s => mapEstado(s.estado));
  const todasFin = estados.every(e => e === 'finalizado');
  const algunaEP = estados.some(e => e === 'en_proceso');

  if (todasFin) return parcial ? 'finalizado parcial' : 'finalizado';
  if (algunaEP) return parcial ? 'en proceso parcial' : 'en_proceso';
  return parcial ? 'programado parcial' : 'programado';
}

// ── GET /api/requerimientos ───────────────────────────────────
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
        SELECT ID_Req, Tipo, Estado, ST, ST2, ST3, ST4,
               CantidadST1, CantidadST2, CantidadST3, CantidadST4,
               OS, NumReqExcel, Jefe, RespProd, Coordinador,
               CodActividad, Actividad, Zona, Sociedad,
               CodOrigen, NombreOrigen, CodDestino, NombreDestino, Lote,
               CodRecurso, RecursoNombre, Producto, UM,
               CantidadTotal, AreaTotal, Implemento, Comentario,
               CreadoPor, FechaCreacion, ActualizadoPor, FechaActualizacion,
               FechaEjecucion, MotivoCancel, STAnterior, MotivoCorreccion, FechaCorreccion
        FROM requerimientos WHERE 1=1`;
      const params = [];

      if (!todos) {
        if (estado) {
          sql += ` AND Estado = @estado`;
          params.push({ name: 'estado', type: TYPES.NVarChar, value: estado });
        } else {
          sql += ` AND Estado NOT IN ('cancelado','finalizado','finalizado parcial')`;
        }
      }
      if (tipo) {
        sql += ` AND Tipo = @tipo`;
        params.push({ name: 'tipo', type: TYPES.NVarChar, value: tipo });
      }
      sql += ` ORDER BY NumReqExcel ASC, FechaCreacion ASC`;

      const rows = await query(sql, params);
      return ok(rows);
    } catch (e) {
      console.error('[requerimientos-get]', e.message);
      return err(500, e.message);
    }
  },
});

// ── POST /api/requerimientos ──────────────────────────────────
app.http('requerimientos-post', {
  methods: ['POST'],
  route: 'requerimientos',
  authLevel: 'anonymous',
  handler: async (request) => {
    if (!checkApiKey(request)) return unauthorized();
    try {
      const body = await request.json();

      if (body.requerimientos) {
        const items = body.requerimientos;
        let insertados = 0; const errores = [];
        for (const item of items) {
          try {
            const id = 'T_' + Math.random().toString(36).substr(2, 8);
            await query(`
              INSERT INTO requerimientos (
                ID_Req, Tipo, Estado, ST, OS, NumReqExcel,
                Jefe, RespProd, Coordinador, CodActividad, Actividad,
                Zona, Sociedad, CodOrigen, NombreOrigen, CodDestino,
                NombreDestino, Lote, CodRecurso, RecursoNombre,
                Producto, UM, CantidadTotal, AreaTotal,
                CantidadST1, Implemento, Comentario,
                CreadoPor, FechaCreacion, FechaActualizacion, FechaEjecucion
              ) VALUES (
                @id, @tipo, @estado, @st, @os, @numReq,
                @jefe, @respProd, @coord, @codAct, @actividad,
                @zona, @sociedad, @codOrigen, @nomOrigen, @codDestino,
                @nomDestino, @lote, @codRecurso, @recursoNom,
                @producto, @um, @cantTotal, @areaTotal,
                @cantST1, @implemento, @comentario,
                @creadoPor, GETDATE(), GETDATE(), @fechaEjec
              )`, [
              { name: 'id',         type: TYPES.NVarChar,  value: id },
              { name: 'tipo',       type: TYPES.NVarChar,  value: item.Tipo          || 'programa' },
              { name: 'estado',     type: TYPES.NVarChar,  value: item.ST ? 'programado' : 'pendiente' },
              { name: 'st',         type: TYPES.NVarChar,  value: item.ST            || null },
              { name: 'os',         type: TYPES.NVarChar,  value: item.OS            || null },
              { name: 'numReq',     type: TYPES.Int,       value: item.NumReqExcel   || null },
              { name: 'jefe',       type: TYPES.NVarChar,  value: item.Jefe          || null },
              { name: 'respProd',   type: TYPES.NVarChar,  value: item.RespProd      || null },
              { name: 'coord',      type: TYPES.NVarChar,  value: item.Coordinador   || null },
              { name: 'codAct',     type: TYPES.NVarChar,  value: item.CodActividad  || null },
              { name: 'actividad',  type: TYPES.NVarChar,  value: item.Actividad     || null },
              { name: 'zona',       type: TYPES.NVarChar,  value: item.Zona          || null },
              { name: 'sociedad',   type: TYPES.NVarChar,  value: item.Sociedad      || null },
              { name: 'codOrigen',  type: TYPES.NVarChar,  value: item.CodOrigen     || null },
              { name: 'nomOrigen',  type: TYPES.NVarChar,  value: item.NombreOrigen  || null },
              { name: 'codDestino', type: TYPES.NVarChar,  value: item.CodDestino    || null },
              { name: 'nomDestino', type: TYPES.NVarChar,  value: item.NombreDestino || null },
              { name: 'lote',       type: TYPES.NVarChar,  value: item.Lote          || null },
              { name: 'codRecurso', type: TYPES.NVarChar,  value: item.CodRecurso    || null },
              { name: 'recursoNom', type: TYPES.NVarChar,  value: item.RecursoNombre || null },
              { name: 'producto',   type: TYPES.NVarChar,  value: item.Producto      || null },
              { name: 'um',         type: TYPES.NVarChar,  value: item.UM            || null },
              { name: 'cantTotal',  type: TYPES.Decimal,   value: item.CantidadTotal || null },
              { name: 'areaTotal',  type: TYPES.Decimal,   value: item.AreaTotal     || null },
              { name: 'cantST1',    type: TYPES.Float,     value: item.ST ? (item.CantidadTotal || null) : null },
              { name: 'implemento', type: TYPES.NVarChar,  value: item.Implemento    || null },
              { name: 'comentario', type: TYPES.NVarChar,  value: item.Comentario    || null },
              { name: 'creadoPor',  type: TYPES.NVarChar,  value: item.CreadoPor     || null },
              { name: 'fechaEjec',  type: TYPES.DateTime2, value: item.FechaEjecucion ? new Date(item.FechaEjecucion) : null },
            ]);
            insertados++;
          } catch (e2) {
            errores.push({ item: item.Producto || '?', error: e2.message });
          }
        }
        return ok({ insertados, errores });
      }

      // Solicitud a demanda
      const id = 'T_' + Math.random().toString(36).substr(2, 8);
      await query(`
        INSERT INTO requerimientos (
          ID_Req, Tipo, Estado, Actividad, NombreOrigen, NombreDestino,
          Lote, Comentario, RecursoNombre, CreadoPor,
          FechaCreacion, FechaActualizacion
        ) VALUES (
          @id, 'demanda', 'pendiente', @actividad, @origen, @destino,
          @lote, @comentario, @recurso, @creadoPor, GETDATE(), GETDATE()
        )`, [
        { name: 'id',         type: TYPES.NVarChar, value: id },
        { name: 'actividad',  type: TYPES.NVarChar, value: body.Actividad     || null },
        { name: 'origen',     type: TYPES.NVarChar, value: body.NombreOrigen  || null },
        { name: 'destino',    type: TYPES.NVarChar, value: body.NombreDestino || null },
        { name: 'lote',       type: TYPES.NVarChar, value: body.Lote          || null },
        { name: 'comentario', type: TYPES.NVarChar, value: body.Comentario    || null },
        { name: 'recurso',    type: TYPES.NVarChar, value: body.RecursoNombre || null },
        { name: 'creadoPor',  type: TYPES.NVarChar, value: body.CreadoPor     || null },
      ]);
      return ok({ ID_Req: id, Estado: 'pendiente' }, 201);
    } catch (e) {
      console.error('[requerimientos-post]', e.message);
      return err(500, e.message);
    }
  },
});

// ── PATCH /api/requerimientos/:id ─────────────────────────────
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
        ST, slot, cantidad, FechaEjecucion,
        MotivoCancel, Estado: EstadoBody,
        MotivoCorreccion, actualizadoPor, forzar,
        desasociarSlot,
      } = body;

      if (!id) return err(400, 'ID_Req requerido');

      const existing = await query(`
        SELECT ID_Req, Estado, ST AS STActual, ST2, ST3, ST4,
               CantidadST1, CantidadST2, CantidadST3, CantidadST4,
               OS, NombreDestino, CodDestino, FechaEjecucion AS FechaReq,
               NumReqExcel, CantidadTotal, UM
        FROM requerimientos WHERE ID_Req = @id`,
        [{ name: 'id', type: TYPES.NVarChar, value: id }]
      );
      if (!existing.length) return err(404, `Requerimiento ${id} no encontrado`);
      const req = existing[0];

      // ── Cancelación ───────────────────────────────────────
      if (EstadoBody === 'cancelado') {
        if (!['pendiente','programado','programado parcial'].includes(req.Estado)) {
          return err(422,
            `No se puede cancelar un requerimiento en estado "${req.Estado}".`,
            { codigo: 'CANCEL_NO_PERMITIDO', estadoActual: req.Estado }
          );
        }
        await query(`
          UPDATE requerimientos SET Estado='cancelado', MotivoCancel=@mc,
            ActualizadoPor=@ap, FechaActualizacion=GETDATE()
          WHERE ID_Req=@id`, [
          { name: 'mc', type: TYPES.NVarChar, value: MotivoCancel   || null },
          { name: 'ap', type: TYPES.NVarChar, value: actualizadoPor || null },
          { name: 'id', type: TYPES.NVarChar, value: id },
        ]);
        return ok({ ID_Req: id, Estado: 'cancelado' });
      }

      // ── Desasociar slot ───────────────────────────────────
      if (desasociarSlot) {
        const sn     = parseInt(desasociarSlot);
        const stCol  = sn === 1 ? 'ST' : `ST${sn}`;
        const cCol   = `CantidadST${sn}`;
        const stPrev = sn === 1 ? req.STActual : req[`ST${sn}`];

        if (stPrev && !MotivoCorreccion) {
          const stRow = await query(
            `SELECT Estado FROM viajes_api WHERE ST=@st`,
            [{ name: 'st', type: TYPES.NVarChar, value: stPrev }]
          );
          const eApi = (stRow[0]?.Estado || '').toLowerCase();
          if (['finalizada','integrada','contabilizado'].some(x => eApi.includes(x))) {
            return err(422, 'ST ya ejecutada. Se requiere MotivoCorreccion.',
              { codigo: 'MOTIVO_REQUERIDO', estadoST: stRow[0]?.Estado });
          }
        }
        await query(`
          UPDATE requerimientos SET
            ${stCol}=NULL, ${cCol}=NULL,
            STAnterior=@sp, MotivoCorreccion=COALESCE(@mc,MotivoCorreccion),
            FechaCorreccion=GETDATE(), ActualizadoPor=@ap, FechaActualizacion=GETDATE()
          WHERE ID_Req=@id`, [
          { name: 'sp', type: TYPES.NVarChar, value: stPrev          || null },
          { name: 'mc', type: TYPES.NVarChar, value: MotivoCorreccion || null },
          { name: 'ap', type: TYPES.NVarChar, value: actualizadoPor   || null },
          { name: 'id', type: TYPES.NVarChar, value: id },
        ]);
        const nuevoEstado = await recalcularEstadoReq(id);
        await query(`UPDATE requerimientos SET Estado=@e, FechaActualizacion=GETDATE() WHERE ID_Req=@id`, [
          { name: 'e',  type: TYPES.NVarChar, value: nuevoEstado },
          { name: 'id', type: TYPES.NVarChar, value: id },
        ]);
        return ok({ ID_Req: id, Estado: nuevoEstado, slotDesasociado: sn });
      }

      // ── Asignar ST a slot ─────────────────────────────────
      if (ST !== undefined && ST !== null) {
        // V0: ST duplicada en el mismo requerimiento
        const slotsActuales = [req.STActual, req.ST2, req.ST3, req.ST4].filter(Boolean);
        if (slotsActuales.includes(ST)) {
          return err(422, `La ST ${ST} ya está asignada a este requerimiento.`, { codigo: 'ST_DUPLICADA_MISMO_REQ' });
        }

        const slotNum = slot || (() => {
          if (!req.STActual) return 1;
          if (!req.ST2)      return 2;
          if (!req.ST3)      return 3;
          if (!req.ST4)      return 4;
          return null;
        })();
        if (!slotNum) return err(422, 'Máximo 4 STs por requerimiento.', { codigo: 'MAX_ST_ALCANZADO' });

        // Verificar ST en plataforma
        const stData = await query(
          `SELECT ST, Estado, DsPuntoEntrega, FechaEntrega FROM viajes_api WHERE ST=@st`,
          [{ name: 'st', type: TYPES.NVarChar, value: ST }]
        );
        if (!stData.length) return err(422, `ST ${ST} no encontrada en la plataforma.`, { codigo: 'ST_NO_EXISTE' });

        const eApi = (stData[0].Estado || '').toLowerCase();
        const eMap = (() => {
          if (['cancelada','rechazada','inconsistente'].some(x => eApi.includes(x))) return 'bloqueado';
          if (['finalizada','finalizado'].some(x => eApi.includes(x))) return 'finalizado';
          if (['en proceso','en carga'].some(x => eApi.includes(x))) return 'en_proceso';
          return 'programado';
        })();

        if (eMap === 'bloqueado') return err(422,
          `No se puede asignar la ST ${ST} porque está "${stData[0].Estado}".`,
          { codigo: 'ST_BLOQUEADA', estadoST: stData[0].Estado });

        // FIX: parsear cantidad correctamente — null solo si genuinamente ausente
        // (no usar || null que convierte 0 a null)
        const cantNueva = (cantidad !== undefined && cantidad !== null && cantidad !== '')
          ? parseFloat(cantidad)
          : null;

        const advertencias = [];

        if (!forzar && eMap === 'finalizado') {
          advertencias.push({ codigo: 'ST_EJECUTADA',
            mensaje: `La ST ${ST} ya tiene estado "${stData[0].Estado}" en la plataforma.` });
        }

        // V1: ST duplicada en otro requerimiento distinto
        const dup = await query(`
          SELECT ID_Req, NumReqExcel FROM requerimientos
          WHERE (ST=@st OR ST2=@st OR ST3=@st OR ST4=@st)
            AND ID_Req!=@id AND Estado NOT IN ('cancelado')`,
          [{ name: 'st', type: TYPES.NVarChar, value: ST },
           { name: 'id', type: TYPES.NVarChar, value: id }]
        );
        if (dup.length && dup[0].NumReqExcel !== req.NumReqExcel) {
          advertencias.push({ codigo: 'ST_DUPLICADA',
            mensaje: `La ST ${ST} ya está asignada al requerimiento ${dup[0].ID_Req}.` });
        }

        // V2: Destino no coincide
        if (stData[0].DsPuntoEntrega && req.NombreDestino) {
          const dA = stData[0].DsPuntoEntrega.replace(/^\d+\s*-\s*/,'').replace(/\s*-\s*.*$/,'').trim().toLowerCase();
          const dR = req.NombreDestino.replace(/^\d+\s*-\s*/,'').replace(/\s*-\s*.*$/,'').trim().toLowerCase();
          if (dA && dR && !dA.includes(dR) && !dR.includes(dA)) {
            advertencias.push({ codigo: 'DESTINO_NO_COINCIDE',
              mensaje: `Destino del requerimiento: ${req.NombreDestino} — Destino en plataforma: ${stData[0].DsPuntoEntrega}` });
          }
        }

        // V3: Fecha difiere más de 1 día
        if (FechaEjecucion && stData[0].FechaEntrega) {
          const diff = Math.abs(new Date(FechaEjecucion) - new Date(stData[0].FechaEntrega)) / 86400000;
          if (diff > 1) {
            advertencias.push({ codigo: 'FECHA_DIFIERE',
              mensaje: `ETD del requerimiento: ${FechaEjecucion.slice(0,16).replace('T',' ')} — Fecha en plataforma: ${new Date(stData[0].FechaEntrega).toISOString().slice(0,16).replace('T',' ')}` });
          }
        }

        // V4b: Cantidad supera total — advertencia no restrictiva
        // Solo evaluar si se envió cantidad; no bloquear si cantNueva es null
        if (cantNueva !== null && req.CantidadTotal) {
          const cantOtros = [
            slotNum !== 1 ? (parseFloat(req.CantidadST1) || 0) : 0,
            slotNum !== 2 ? (parseFloat(req.CantidadST2) || 0) : 0,
            slotNum !== 3 ? (parseFloat(req.CantidadST3) || 0) : 0,
            slotNum !== 4 ? (parseFloat(req.CantidadST4) || 0) : 0,
          ].reduce((s, v) => s + v, 0);
          if ((cantOtros + cantNueva) > parseFloat(req.CantidadTotal)) {
            advertencias.push({ codigo: 'CANTIDAD_SUPERA_TOTAL',
              mensaje: `La cantidad asignada (${cantOtros + cantNueva} ${req.UM||''}) supera la programada (${req.CantidadTotal} ${req.UM||''}).` });
          }
        }

        // Solo bloquear por advertencias que NO sean CANTIDAD_SUPERA_TOTAL
        // V4b es informativa — no impide guardar sin confirmación
        const advsBlockeantes = advertencias.filter(a => a.codigo !== 'CANTIDAD_SUPERA_TOTAL');
        if (advsBlockeantes.length && !forzar) {
          return ok({ requiereConfirmacion: true, advertencias });
        }

        // Guardar aunque haya advertencia de cantidad (V4b no bloquea)
        const stCol = slotNum === 1 ? 'ST' : `ST${slotNum}`;
        const cCol  = `CantidadST${slotNum}`;
        await query(`
          UPDATE requerimientos SET
            ${stCol}=@st, ${cCol}=@cant,
            FechaEjecucion=COALESCE(@fe, FechaEjecucion),
            ActualizadoPor=@ap, FechaActualizacion=GETDATE()
          WHERE ID_Req=@id`, [
          { name: 'st',   type: TYPES.NVarChar,  value: ST },
          { name: 'cant', type: TYPES.Float,      value: cantNueva },
          { name: 'fe',   type: TYPES.DateTime2,  value: FechaEjecucion ? new Date(FechaEjecucion) : null },
          { name: 'ap',   type: TYPES.NVarChar,   value: actualizadoPor || null },
          { name: 'id',   type: TYPES.NVarChar,   value: id },
        ]);

        const nuevoEstado = await recalcularEstadoReq(id);
        await query(`UPDATE requerimientos SET Estado=@e, FechaActualizacion=GETDATE() WHERE ID_Req=@id`, [
          { name: 'e',  type: TYPES.NVarChar, value: nuevoEstado },
          { name: 'id', type: TYPES.NVarChar, value: id },
        ]);

        // Devolver advertencias junto con el éxito para que el frontend las muestre
        return ok({ ID_Req: id, ST, slot: slotNum, Estado: nuevoEstado, advertencias });
      }

      return err(400, 'Operación no reconocida');
    } catch (e) {
      console.error('[requerimientos-patch]', e.message, e.stack);
      return err(500, e.message);
    }
  },
});
