const { app } = require('@azure/functions');
const { query, TYPES } = require('./db');

// ============================================================
// CACHÉ SELECTIVO
// Histórico (hasta < hoy-2)   → TTL 30 min
// Reciente (hasta < hoy)      → TTL 5 min
// Operativo (incluye hoy+)    → SIN caché
// _bust                       → bypass total
// ============================================================
const cache = new Map();
const TTL_HIST = 30 * 60 * 1000;
const TTL_REC  =  5 * 60 * 1000;

function esHistorico(h) {
  if (!h) return false;
  const limite = new Date(); limite.setDate(limite.getDate() - 2);
  return new Date(h) < limite;
}
function esOperativo(h) {
  if (!h) return true;
  return h >= new Date().toISOString().slice(0, 10);
}
function key(d, h, hist) { return `${d}|${h}|${hist}`; }
function getCache(k) {
  const e = cache.get(k);
  if (!e) return null;
  if (Date.now() - e.ts > e.ttl) { cache.delete(k); return null; }
  return e.data;
}
function setCache(k, data, ttl) {
  if (cache.size >= 20) {
    const old = [...cache.entries()].sort((a,b) => a[1].ts - b[1].ts)[0];
    cache.delete(old[0]);
  }
  cache.set(k, { data, ts: Date.now(), ttl });
}

app.http('viajes', {
  methods: ['GET'],
  authLevel: 'anonymous',
  handler: async (request) => {
    try {
      const desdeRaw  = request.query.get('desde');
      const hastaRaw  = request.query.get('hasta');
      const historico = request.query.get('historico') === 'true';
      const traerTodo = request.query.get('all') === 'true';
      const bust      = request.query.get('_bust');

      let desde = desdeRaw;
      if (!traerTodo && !desde) {
        const d = new Date(); d.setMonth(d.getMonth() - 1);
        desde = d.toISOString().slice(0, 10);
      }

      // Caché solo para rangos que no incluyen hoy
      let cacheKey = null;
      if (!bust && !traerTodo && !historico) {
        if (esHistorico(hastaRaw)) {
          cacheKey = key(desde, hastaRaw || '', historico);
          const hit = getCache(cacheKey);
          if (hit) return { status: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*', 'X-Cache': 'HIT' },
            body: JSON.stringify(hit) };
        } else if (!esOperativo(hastaRaw)) {
          cacheKey = key(desde, hastaRaw || '', historico);
          const hit = getCache(cacheKey);
          if (hit) return { status: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*', 'X-Cache': 'HIT' },
            body: JSON.stringify(hit) };
        }
      }

      // ── Query simple sobre viajes_master ─────────────────
      // Sin JOINs — todos los campos ya están precalculados
      let sql = `SELECT * FROM viajes_master WHERE 1=1`;
      const params = [];

      if (!historico) {
        sql += ` AND (EsHistorico = 0 OR EsHistorico IS NULL)`;
      }
      if (desde) {
        sql += ` AND (
          FechaPrincipal >= @desde
          OR (FechaPrincipal IS NULL AND (ID_Req IS NOT NULL OR Coordinador IS NOT NULL))
        )`;
        params.push({ name: 'desde', type: TYPES.DateTime2, value: new Date(desde) });
      }
      if (hastaRaw) {
        sql += ` AND FechaPrincipal <= @hasta`;
        params.push({ name: 'hasta', type: TYPES.DateTime2, value: new Date(hastaRaw + 'T23:59:59') });
      }
      sql += ` ORDER BY FechaPrincipal DESC OPTION (RECOMPILE)`;

      const rows = await query(sql, params);

      if (cacheKey) {
        const ttl = esHistorico(hastaRaw) ? TTL_HIST : TTL_REC;
        setCache(cacheKey, rows, ttl);
      }

      return {
        status: 200,
        headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*',
          'Cache-Control': 'no-store', 'X-Cache': cacheKey ? 'MISS' : 'BYPASS' },
        body: JSON.stringify(rows),
      };

    } catch (e) {
      console.error('[viajes]', e.message, e.stack);
      return { status: 500,
        headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify({ error: e.message }) };
    }
  },
});
