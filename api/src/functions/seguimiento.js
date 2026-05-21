const { app } = require('@azure/functions');
const { query, TYPES } = require('../../db');

const ok  = (data, status = 200) => ({ status, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify(data) });
const err = (msg, status = 400) => ({ status, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify({ error: msg }) });

app.http('seguimiento', {
  methods: ['GET', 'PUT'],
  authLevel: 'anonymous',
  handler: async (request) => {
    try {
      const method = request.method.toUpperCase();

      // ── GET ──────────────────────────────────────────────
      if (method === 'GET') {
        const st = request.query.get('st') || null;
        let sql = `SELECT *, FechaActualizacion AS Version FROM seguimiento`;
        const params = [];
        if (st) {
          sql += ` WHERE ST = @st`;
          params.push({ name: 'st', type: TYPES.NVarChar, value: st });
        }
        const rows = await query(sql, params);
        return ok(st ? (rows[0] || null) : rows);
      }

      // ── PUT — upsert con control de versión ──────────────
      if (method === 'PUT') {
        const b  = await request.json();
        const st = b.st || b.ST;
        if (!st) return err('ST requerido');

        if (b.versionCargada) {
          const existing = await query(
            `SELECT FechaActualizacion FROM seguimiento WHERE ST = @st`,
            [{ name: 'st', type: TYPES.NVarChar, value: st }]
          );
          if (existing.length) {
            const versionBD     = existing[0].FechaActualizacion?.toISOString();
            const versionCliente = new Date(b.versionCargada).toISOString();
            const diffMs = Math.abs(new Date(versionBD) - new Date(versionCliente));
            if (diffMs > 1000) {
              return { status: 409,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ error: 'CONFLICTO: El registro fue modificado por otro usuario.', codigo: 'CONFLICTO', versionBD, versionCliente }),
              };
            }
          }
        }

        const sql = `
          MERGE seguimiento AS target
          USING (SELECT @st AS ST) AS source ON target.ST = source.ST
          WHEN MATCHED THEN UPDATE SET
            MotivoRetrasoInicio  = @motivoInicio,
            MotivoRetrasoEntrega = @motivoEntrega,
            ComentarioCabina     = @comentario,
            ActualizadoPor       = @actualizadoPor,
            FechaActualizacion   = GETUTCDATE()
          WHEN NOT MATCHED THEN INSERT
            (ST, MotivoRetrasoInicio, MotivoRetrasoEntrega, ComentarioCabina, ActualizadoPor, FechaActualizacion)
          VALUES (@st, @motivoInicio, @motivoEntrega, @comentario, @actualizadoPor, GETUTCDATE());`;

        await query(sql, [
          { name: 'st',             type: TYPES.NVarChar, value: st },
          { name: 'motivoInicio',   type: TYPES.NVarChar, value: b.motivoRetrasoInicio  || b.MotivoRetrasoInicio  || '' },
          { name: 'motivoEntrega',  type: TYPES.NVarChar, value: b.motivoRetrasoEntrega || b.MotivoRetrasoEntrega || '' },
          { name: 'comentario',     type: TYPES.NVarChar, value: b.comentarioCabina     || b.ComentarioCabina     || '' },
          { name: 'actualizadoPor', type: TYPES.NVarChar, value: b.actualizadoPor       || b.ActualizadoPor       || '' },
        ]);
        return ok({ ok: true, st });
      }

      return err('Método no soportado', 405);
    } catch (e) {
      console.error('[seguimiento]', e.message, e.stack);
      return err(e.message, 500);
    }
  },
});
