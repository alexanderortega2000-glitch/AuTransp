const { app } = require('@azure/functions');
const { query, TYPES } = require('../../db');

// GET /api/programacion?desde=2026-04-01&hasta=2026-05-01
// PUT /api/programacion — upsert programación de un viaje
app.http('programacion', {
  methods: ['GET', 'PUT'],
  authLevel: 'anonymous',
  handler: async (request) => {
    try {
      const method = request.method.toUpperCase();

      // ── GET ──────────────────────────────────────────────
      if (method === 'GET') {
        const desde = request.query.get('desde') || null;
        const hasta = request.query.get('hasta') || null;
        const st    = request.query.get('st')    || null;

        let sql = `SELECT * FROM programacion WHERE 1=1`;
        const params = [];

        if (st) {
          sql += ` AND ST = @st`;
          params.push({ name: 'st', type: TYPES.NVarChar, value: st });
        }
        if (desde) {
          sql += ` AND FechaEjecucion >= @desde`;
          params.push({ name: 'desde', type: TYPES.DateTime2, value: new Date(desde) });
        }
        if (hasta) {
          sql += ` AND FechaEjecucion <= @hasta`;
          params.push({ name: 'hasta', type: TYPES.DateTime2, value: new Date(hasta + 'T23:59:59') });
        }
        sql += ` ORDER BY FechaEjecucion DESC`;

        const rows = await query(sql, params);
        return ok(rows);
      }

      // ── PUT — upsert programación ─────────────────────────
      if (method === 'PUT') {
        const b = await request.json();
        const st = b.st || b.ST || (b.campos && (b.campos.st || b.campos.ST));
        if (!st) return err('ST requerido');

        const campos = b.campos || b;

        const sql = `
          MERGE programacion AS target
          USING (SELECT @st AS ST) AS source ON target.ST = source.ST
          WHEN MATCHED THEN UPDATE SET
            Coordinador      = COALESCE(@coord,    target.Coordinador),
            ProveedorTransp  = COALESCE(@prov,     target.ProveedorTransp),
            Subflota         = COALESCE(@subflota, target.Subflota),
            FechaEjecucion   = COALESCE(@fechaEjec,target.FechaEjecucion),
            TipoProg         = COALESCE(@tipoProg, target.TipoProg),
            NombreOrigen     = COALESCE(@origen,   target.NombreOrigen),
            NombreDestino    = COALESCE(@destino,  target.NombreDestino),
            LoteDestino      = COALESCE(@lote,     target.LoteDestino),
            Zona             = COALESCE(@zona,     target.Zona),
            ActualizadoPor   = @actualizadoPor,
            FechaActualizacion = GETUTCDATE()
          WHEN NOT MATCHED THEN INSERT
            (ST, Coordinador, ProveedorTransp, Subflota, FechaEjecucion,
             TipoProg, NombreOrigen, NombreDestino, LoteDestino, Zona, ActualizadoPor)
          VALUES
            (@st, @coord, @prov, @subflota, @fechaEjec,
             @tipoProg, @origen, @destino, @lote, @zona, @actualizadoPor);`;

        const fec = campos.FechaEjecucion || campos.fechaEjec;
        const params = [
          { name: 'st',           type: TYPES.NVarChar,  value: st },
          { name: 'coord',        type: TYPES.NVarChar,  value: campos.Coordinador    || campos.coordinador    || null },
          { name: 'prov',         type: TYPES.NVarChar,  value: campos.ProveedorTransp|| campos.proveedor      || null },
          { name: 'subflota',     type: TYPES.NVarChar,  value: campos.Subflota       || campos.subflota       || null },
          { name: 'fechaEjec',    type: TYPES.DateTime2, value: fec ? new Date(fec) : null },
          { name: 'tipoProg',     type: TYPES.NVarChar,  value: campos.TipoProg       || campos.tipoProg       || null },
          { name: 'origen',       type: TYPES.NVarChar,  value: campos.NombreOrigen   || campos.origen         || null },
          { name: 'destino',      type: TYPES.NVarChar,  value: campos.NombreDestino  || campos.destino        || null },
          { name: 'lote',         type: TYPES.NVarChar,  value: campos.LoteDestino    || campos.lote           || null },
          { name: 'zona',         type: TYPES.NVarChar,  value: campos.Zona           || campos.zona           || null },
          { name: 'actualizadoPor',type: TYPES.NVarChar, value: b.actualizadoPor      || b.ActualizadoPor      || '' },
        ];

        await query(sql, params);
        return ok({ ok: true, st });
      }

      return err('Método no soportado', 405);
    } catch (e) {
      console.error('[programacion]', e.message);
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
