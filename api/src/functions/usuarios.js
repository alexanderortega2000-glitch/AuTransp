const { app } = require('@azure/functions');
const { query, TYPES } = require('../../db');

const ok  = (data, status = 200) => ({ status, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify(data) });
const err = (msg, status = 400) => ({ status, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify({ error: msg }) });

app.http('usuarios', {
  methods: ['GET', 'POST', 'PATCH'],
  authLevel: 'anonymous',
  handler: async (request) => {
    try {
      const method = request.method.toUpperCase();

      // ── POST /login ───────────────────────────────────────
      if (method === 'POST') {
        const b = await request.json();
        const { usuario, pinHash } = b;
        if (!usuario || !pinHash) return err('usuario y pinHash requeridos');
        const rows = await query(
          `SELECT Usuario, Nombre, Rol, PinHash, Activo FROM usuarios WHERE Usuario = @u`,
          [{ name: 'u', type: TYPES.NVarChar, value: usuario.toUpperCase() }]
        );
        if (!rows.length)                    return err('Usuario no encontrado', 401);
        if (!rows[0].Activo)                 return err('Usuario inactivo', 401);
        if (rows[0].PinHash !== pinHash)     return err('PIN incorrecto', 401);
        await query(
          `UPDATE usuarios SET UltimoAcceso = GETUTCDATE() WHERE Usuario = @u`,
          [{ name: 'u', type: TYPES.NVarChar, value: usuario.toUpperCase() }]
        );
        return ok({ usuario: rows[0].Usuario, nombre: rows[0].Nombre, rol: rows[0].Rol });
      }

      // ── GET — listar usuarios ─────────────────────────────
      if (method === 'GET') {
        const rows = await query(
          `SELECT Usuario, Nombre, Rol, Activo, FechaCreacion, UltimoAcceso FROM usuarios ORDER BY Rol, Nombre`
        );
        return ok(rows);
      }

      // ── PATCH — cambiar PIN ───────────────────────────────
      if (method === 'PATCH') {
        const b = await request.json();
        const { usuario, pinHashActual, pinHashNuevo } = b;
        if (!usuario || !pinHashActual || !pinHashNuevo) return err('Datos incompletos');
        const rows = await query(
          `SELECT PinHash FROM usuarios WHERE Usuario = @u AND Activo = 1`,
          [{ name: 'u', type: TYPES.NVarChar, value: usuario.toUpperCase() }]
        );
        if (!rows.length)                      return err('Usuario no encontrado', 401);
        if (rows[0].PinHash !== pinHashActual) return err('PIN actual incorrecto', 401);
        await query(
          `UPDATE usuarios SET PinHash = @nuevo WHERE Usuario = @u`,
          [
            { name: 'nuevo', type: TYPES.NVarChar, value: pinHashNuevo },
            { name: 'u',     type: TYPES.NVarChar, value: usuario.toUpperCase() },
          ]
        );
        return ok({ ok: true });
      }

      return err('Método no soportado', 405);
    } catch (e) {
      console.error('[usuarios]', e.message);
      return err(e.message, 500);
    }
  },
});
