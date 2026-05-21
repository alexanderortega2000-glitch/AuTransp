const { app } = require('@azure/functions');
const { query, TYPES } = require('../../db');

// GET /api/solicitudes?tipo=demanda&estado=Pendiente
// POST /api/solicitudes — crear nueva solicitud
// PATCH /api/solicitudes — actualizar estado/ST de solicitud
app.http('solicitudes', {
  methods: ['GET', 'POST', 'PATCH'],
  authLevel: 'anonymous',
  handler: async (request) => {
    try {
      const method = request.method.toUpperCase();

      // ── GET ──────────────────────────────────────────────
      if (method === 'GET') {
        const tipo   = request.query.get('tipo')   || null;
        const estado = request.query.get('estado') || null;
        const desde  = request.query.get('desde')  || null;
        const hasta  = request.query.get('hasta')  || null;

        let sql = `SELECT * FROM solicitudes WHERE 1=1`;
        const params = [];

        if (tipo) {
          sql += ` AND Tipo = @tipo`;
          params.push({ name: 'tipo', type: TYPES.NVarChar, value: tipo });
        }
        if (estado) {
          sql += ` AND Estado = @estado`;
          params.push({ name: 'estado', type: TYPES.NVarChar, value: estado });
        }
        if (desde) {
          sql += ` AND FechaEjecucion >= @desde`;
          params.push({ name: 'desde', type: TYPES.DateTime2, value: new Date(desde) });
        }
        if (hasta) {
          sql += ` AND FechaEjecucion <= @hasta`;
          params.push({ name: 'hasta', type: TYPES.DateTime2, value: new Date(hasta + 'T23:59:59') });
        }
        sql += ` ORDER BY FechaCreacion DESC`;

        const rows = await query(sql, params);
        return ok(rows);
      }

      // ── POST — nueva solicitud ────────────────────────────
      if (method === 'POST') {
        const b = await request.json();
        const sql = `
          INSERT INTO solicitudes (
            ID_Solicitud, Tipo, FechaEjecucion, Solicitante, NombreDestino,
            LoteDestino, Actividad, OS, TipoServicio, NombreOrigen,
            Proveedor, Observaciones, Estado, Jefe, Responsable,
            Zona, Sociedad, CantidadProgramada, AreaProgramada, PlanAgregado
          ) VALUES (
            @id, @tipo, @fechaEjec, @solicitante, @destino,
            @lote, @actividad, @os, @tipoServicio, @origen,
            @proveedor, @obs, @estado, @jefe, @responsable,
            @zona, @sociedad, @cantidad, @area, @planAgr
          )`;
        const params = [
          { name: 'id',           type: TYPES.NVarChar,  value: b.ID_Solicitud },
          { name: 'tipo',         type: TYPES.NVarChar,  value: b.Tipo || 'demanda' },
          { name: 'fechaEjec',    type: TYPES.DateTime2, value: b.FechaEjecucion ? new Date(b.FechaEjecucion) : null },
          { name: 'solicitante',  type: TYPES.NVarChar,  value: b.Solicitante || null },
          { name: 'destino',      type: TYPES.NVarChar,  value: b.NombreDestino || b.Destino || null },
          { name: 'lote',         type: TYPES.NVarChar,  value: b.LoteDestino || b.Lote || null },
          { name: 'actividad',    type: TYPES.NVarChar,  value: b.Actividad || null },
          { name: 'os',           type: TYPES.NVarChar,  value: b.OS || null },
          { name: 'tipoServicio', type: TYPES.NVarChar,  value: b.TipoServicio || null },
          { name: 'origen',       type: TYPES.NVarChar,  value: b.NombreOrigen || b.Origen || null },
          { name: 'proveedor',    type: TYPES.NVarChar,  value: b.Proveedor || null },
          { name: 'obs',          type: TYPES.NVarChar,  value: b.Observaciones || null },
          { name: 'estado',       type: TYPES.NVarChar,  value: b.Estado || 'Pendiente' },
          { name: 'jefe',         type: TYPES.NVarChar,  value: b.Jefe || null },
          { name: 'responsable',  type: TYPES.NVarChar,  value: b.Responsable || null },
          { name: 'zona',         type: TYPES.NVarChar,  value: b.Zona || null },
          { name: 'sociedad',     type: TYPES.NVarChar,  value: b.Sociedad || null },
          { name: 'cantidad',     type: TYPES.Decimal,   value: b.CantidadProgramada || null },
          { name: 'area',         type: TYPES.Decimal,   value: b.AreaProgramada || null },
          { name: 'planAgr',      type: TYPES.NVarChar,  value: b.PlanAgregado || null },
        ];
        await query(sql, params);
        return ok({ ok: true, id: b.ID_Solicitud });
      }

      // ── PATCH — asignar ST / cambiar estado ──────────────
      if (method === 'PATCH') {
        const b = await request.json();
        if (!b.ID_Solicitud) return err('ID_Solicitud requerido');

        const campos = [];
        const params = [{ name: 'id', type: TYPES.NVarChar, value: b.ID_Solicitud }];

        if (b.Estado !== undefined) {
          campos.push('Estado = @estado');
          params.push({ name: 'estado', type: TYPES.NVarChar, value: b.Estado });
        }
        if (b.ST_Asignada !== undefined) {
          campos.push('ST_Asignada = @st');
          params.push({ name: 'st', type: TYPES.NVarChar, value: b.ST_Asignada });
        }
        if (b.Coordinador !== undefined) {
          campos.push('Coordinador = @coord');
          params.push({ name: 'coord', type: TYPES.NVarChar, value: b.Coordinador });
        }

        if (!campos.length) return err('Sin campos para actualizar');

        campos.push('FechaActualizacion = GETUTCDATE()');
        await query(
          `UPDATE solicitudes SET ${campos.join(', ')} WHERE ID_Solicitud = @id`,
          params
        );
        return ok({ ok: true });
      }

      return err('Método no soportado', 405);

    } catch (e) {
      console.error('[solicitudes]', e.message);
      return err(e.message, 500);
    }
  }
});

const ok  = (data, status = 200) => ({
  status,
  headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
  body: JSON.stringify(data),
});
const err = (msg, status = 400) => ({
  status,
  headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
  body: JSON.stringify({ error: msg }),
});
