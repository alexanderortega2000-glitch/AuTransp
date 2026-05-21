const { app }          = require('@azure/functions');
const { query, TYPES } = require('../../db');

// ============================================================
// syncAPI v3 — Timer trigger cada 5 minutos
// Estrategia segura: COALESCE en UPDATE — nunca sobreescribe
// con NULL un campo que ya tiene valor en BD.
// Solo INSERT registros nuevos.
// ============================================================

const API_URL     = 'https://logistico.grupocassa.com/api-transportes-varios-web/api/SolicitudesTransporte/GetSolicitudesTransporte';
const API_USUARIO = process.env.API_USUARIO || 'arivas';
const FECHA_CORTE = new Date('2025-11-01T00:00:00');

// ── Utilidades ─────────────────────────────────────────────
function fechaStr(d) {
  return `${String(d.getDate()).padStart(2,'0')}-${String(d.getMonth()+1).padStart(2,'0')}-${d.getFullYear()}`;
}
function addDays(d, n) {
  const r = new Date(d); r.setDate(r.getDate() + n); return r;
}
function limpiar(v) {
  if (v == null) return null;
  const s = String(v).trim();
  return ['','nan','NaN','None','NaT'].includes(s) ? null : s;
}
function parseFecha(v) {
  if (v == null) return null;
  const s = String(v).trim();
  if (['','nan','NaN','None','NaT'].includes(s)) return null;

  // Formato DD/MM/YYYY HH:MM o DD/MM/YYYY (API de CASSA)
  const m1 = s.match(/^(\d{2})\/(\d{2})\/(\d{4})\s+(\d{2}):(\d{2})/);
  if (m1) return new Date(`${m1[3]}-${m1[2]}-${m1[1]}T${m1[4]}:${m1[5]}:00`).toISOString();
  const m2 = s.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (m2) return new Date(`${m2[3]}-${m2[2]}-${m2[1]}T00:00:00`).toISOString();

  // Formato ISO estándar
  const d = new Date(s);
  return isNaN(d.getTime()) ? null : d.toISOString();
}
function num(v)    { if (v == null) return null; const f = parseFloat(v); return isNaN(f) ? null : f; }
function entero(v) { const f = num(v); return f != null ? Math.round(f) : null; }
function esHistorico(r) {
  const raw = r.FechaEntrega || r.FechaInicioViaje || r.FechaEntregaST;
  if (!raw) return 0;
  const d = new Date(raw);
  return (!isNaN(d.getTime()) && d < FECHA_CORTE) ? 1 : 0;
}

// ── Normalizar registro ────────────────────────────────────
function normalizar(r) {
  const st = limpiar(String(r.ID_ST || ''));
  if (!st) return null;
  return {
    ST:                  st,
    TipoViaje:           limpiar(r.TipoViaje),
    OS:                  limpiar(r.OS),
    PuntoPartida:        limpiar(r.PuntoPartida),
    DsPuntoPartida:      limpiar(r.DsPuntoPartida),
    PuntoEntrega:        limpiar(r.PuntoEntrega),
    DsPuntoEntrega:      limpiar(r.DsPuntoEntrega),
    FechaEntrega:        parseFecha(r.FechaEntrega),
    ID_EstatusST:        entero(r.ID_EstatusST),
    Estado:              limpiar(r.Estado),
    Asignado:            limpiar(r.Asignado),
    Km:                  num(r.Km),
    KmReal:              num(r.KmReal),
    Estimacion:          num(r.Estimacion),
    CostoFinal:          num(r.CostoFinal),
    Diferencia:          num(r.Diferencia),
    Comentario:          limpiar(r.Comentario),
    FechaFinalizacion:   parseFecha(r.FechaFinalizacion),
    Integrado:           limpiar(r.Integrado),
    ObsValidaciones:     limpiar(r.ObsValidaciones),
    CodEquipo:           limpiar(r.CodEquipo),
    FueraPlan:           limpiar(r.FueraPlan),
    Nom_Motorista:       limpiar(r.Nom_Motorista),
    FechaInicioViaje:    parseFecha(r.FechaInicioViaje),
    ComentInicioViaje:   limpiar(r.ComentInicioViaje),
    FechaFinViaje:       parseFecha(r.FechaFinViaje),
    ComentFinViaje:      limpiar(r.ComentFinViaje),
    FechaEntregaST:      parseFecha(r.FechaEntregaST),
    ComentEntrega:       limpiar(r.ComentEntrega),
    CantidadCargadores:  entero(r.CantidadCargadores),
    Permanencia:         num(r.Permanencia),
    Permanencia_Aplica:  limpiar(r.Permanencia_Aplica),
    InicioPermanencia:   parseFecha(r.InicioPermanencia),
    FinPermanencia:      parseFecha(r.FinPermanencia),
    HorasPermanencia:    num(r.HorasPermanencia),
    HorasPermanenciaEst: num(r.HorasPermanenciaEst),
    OficialCosecha:      limpiar(r.OficialCosecha),
    Frente:              limpiar(r.Frente),
    EsHistorico:         esHistorico(r),
  };
}

// ── MERGE con COALESCE — nunca sobreescribe con NULL ───────
// UPDATE: usa COALESCE(nuevo, existente) → conserva valor si nuevo es NULL
// INSERT: solo para STs que no existen en BD
const MERGE_JSON_SQL = `
  MERGE viajes_api AS t
  USING (
    SELECT
      ST, TipoViaje, OS, PuntoPartida, DsPuntoPartida,
      PuntoEntrega, DsPuntoEntrega,
      CAST(FechaEntrega        AS datetime2) AS FechaEntrega,
      CAST(ID_EstatusST        AS int)       AS ID_EstatusST,
      Estado, Asignado,
      CAST(Km                  AS float)     AS Km,
      CAST(KmReal              AS float)     AS KmReal,
      CAST(Estimacion          AS float)     AS Estimacion,
      CAST(CostoFinal          AS float)     AS CostoFinal,
      CAST(Diferencia          AS float)     AS Diferencia,
      Comentario,
      CAST(FechaFinalizacion   AS datetime2) AS FechaFinalizacion,
      Integrado, ObsValidaciones, CodEquipo, FueraPlan, Nom_Motorista,
      CAST(FechaInicioViaje    AS datetime2) AS FechaInicioViaje,
      ComentInicioViaje,
      CAST(FechaFinViaje       AS datetime2) AS FechaFinViaje,
      ComentFinViaje,
      CAST(FechaEntregaST      AS datetime2) AS FechaEntregaST,
      ComentEntrega,
      CAST(CantidadCargadores  AS int)       AS CantidadCargadores,
      CAST(Permanencia         AS float)     AS Permanencia,
      Permanencia_Aplica,
      CAST(InicioPermanencia   AS datetime2) AS InicioPermanencia,
      CAST(FinPermanencia      AS datetime2) AS FinPermanencia,
      CAST(HorasPermanencia    AS float)     AS HorasPermanencia,
      CAST(HorasPermanenciaEst AS float)     AS HorasPermanenciaEst,
      OficialCosecha, Frente,
      CAST(EsHistorico         AS int)       AS EsHistorico
    FROM OPENJSON(@json) WITH (
      ST                  nvarchar(50)  '$.ST',
      TipoViaje           nvarchar(200) '$.TipoViaje',
      OS                  nvarchar(50)  '$.OS',
      PuntoPartida        nvarchar(50)  '$.PuntoPartida',
      DsPuntoPartida      nvarchar(200) '$.DsPuntoPartida',
      PuntoEntrega        nvarchar(50)  '$.PuntoEntrega',
      DsPuntoEntrega      nvarchar(200) '$.DsPuntoEntrega',
      FechaEntrega        nvarchar(30)  '$.FechaEntrega',
      ID_EstatusST        nvarchar(10)  '$.ID_EstatusST',
      Estado              nvarchar(100) '$.Estado',
      Asignado            nvarchar(200) '$.Asignado',
      Km                  nvarchar(20)  '$.Km',
      KmReal              nvarchar(20)  '$.KmReal',
      Estimacion          nvarchar(20)  '$.Estimacion',
      CostoFinal          nvarchar(20)  '$.CostoFinal',
      Diferencia          nvarchar(20)  '$.Diferencia',
      Comentario          nvarchar(500) '$.Comentario',
      FechaFinalizacion   nvarchar(30)  '$.FechaFinalizacion',
      Integrado           nvarchar(100) '$.Integrado',
      ObsValidaciones     nvarchar(500) '$.ObsValidaciones',
      CodEquipo           nvarchar(50)  '$.CodEquipo',
      FueraPlan           nvarchar(10)  '$.FueraPlan',
      Nom_Motorista       nvarchar(200) '$.Nom_Motorista',
      FechaInicioViaje    nvarchar(30)  '$.FechaInicioViaje',
      ComentInicioViaje   nvarchar(500) '$.ComentInicioViaje',
      FechaFinViaje       nvarchar(30)  '$.FechaFinViaje',
      ComentFinViaje      nvarchar(500) '$.ComentFinViaje',
      FechaEntregaST      nvarchar(30)  '$.FechaEntregaST',
      ComentEntrega       nvarchar(500) '$.ComentEntrega',
      CantidadCargadores  nvarchar(10)  '$.CantidadCargadores',
      Permanencia         nvarchar(20)  '$.Permanencia',
      Permanencia_Aplica  nvarchar(50)  '$.Permanencia_Aplica',
      InicioPermanencia   nvarchar(30)  '$.InicioPermanencia',
      FinPermanencia      nvarchar(30)  '$.FinPermanencia',
      HorasPermanencia    nvarchar(20)  '$.HorasPermanencia',
      HorasPermanenciaEst nvarchar(20)  '$.HorasPermanenciaEst',
      OficialCosecha      nvarchar(100) '$.OficialCosecha',
      Frente              nvarchar(100) '$.Frente',
      EsHistorico         nvarchar(5)   '$.EsHistorico'
    )
  ) AS s ON t.ST = s.ST
  WHEN MATCHED THEN UPDATE SET
    TipoViaje           = COALESCE(s.TipoViaje,           t.TipoViaje),
    OS                  = COALESCE(s.OS,                  t.OS),
    PuntoPartida        = COALESCE(s.PuntoPartida,        t.PuntoPartida),
    DsPuntoPartida      = COALESCE(s.DsPuntoPartida,      t.DsPuntoPartida),
    PuntoEntrega        = COALESCE(s.PuntoEntrega,        t.PuntoEntrega),
    DsPuntoEntrega      = COALESCE(s.DsPuntoEntrega,      t.DsPuntoEntrega),
    FechaEntrega        = COALESCE(s.FechaEntrega,        t.FechaEntrega),
    ID_EstatusST        = COALESCE(s.ID_EstatusST,        t.ID_EstatusST),
    Estado              = COALESCE(s.Estado,              t.Estado),
    Asignado            = COALESCE(s.Asignado,            t.Asignado),
    Km                  = COALESCE(s.Km,                  t.Km),
    KmReal              = COALESCE(s.KmReal,              t.KmReal),
    Estimacion          = COALESCE(s.Estimacion,          t.Estimacion),
    CostoFinal          = COALESCE(s.CostoFinal,          t.CostoFinal),
    Diferencia          = COALESCE(s.Diferencia,          t.Diferencia),
    Comentario          = COALESCE(s.Comentario,          t.Comentario),
    FechaFinalizacion   = COALESCE(s.FechaFinalizacion,   t.FechaFinalizacion),
    Integrado           = COALESCE(s.Integrado,           t.Integrado),
    ObsValidaciones     = COALESCE(s.ObsValidaciones,     t.ObsValidaciones),
    CodEquipo           = COALESCE(s.CodEquipo,           t.CodEquipo),
    FueraPlan           = COALESCE(s.FueraPlan,           t.FueraPlan),
    Nom_Motorista       = COALESCE(s.Nom_Motorista,       t.Nom_Motorista),
    FechaInicioViaje    = COALESCE(s.FechaInicioViaje,    t.FechaInicioViaje),
    ComentInicioViaje   = COALESCE(s.ComentInicioViaje,   t.ComentInicioViaje),
    FechaFinViaje       = COALESCE(s.FechaFinViaje,       t.FechaFinViaje),
    ComentFinViaje      = COALESCE(s.ComentFinViaje,      t.ComentFinViaje),
    FechaEntregaST      = COALESCE(s.FechaEntregaST,      t.FechaEntregaST),
    ComentEntrega       = COALESCE(s.ComentEntrega,       t.ComentEntrega),
    CantidadCargadores  = COALESCE(s.CantidadCargadores,  t.CantidadCargadores),
    Permanencia         = COALESCE(s.Permanencia,         t.Permanencia),
    Permanencia_Aplica  = COALESCE(s.Permanencia_Aplica,  t.Permanencia_Aplica),
    InicioPermanencia   = COALESCE(s.InicioPermanencia,   t.InicioPermanencia),
    FinPermanencia      = COALESCE(s.FinPermanencia,      t.FinPermanencia),
    HorasPermanencia    = COALESCE(s.HorasPermanencia,    t.HorasPermanencia),
    HorasPermanenciaEst = COALESCE(s.HorasPermanenciaEst, t.HorasPermanenciaEst),
    OficialCosecha      = COALESCE(s.OficialCosecha,      t.OficialCosecha),
    Frente              = COALESCE(s.Frente,              t.Frente),
    EsHistorico         = COALESCE(s.EsHistorico,         t.EsHistorico),
    FechaActualizacion  = GETUTCDATE()
  WHEN NOT MATCHED THEN INSERT (
    ST, TipoViaje, OS, PuntoPartida, DsPuntoPartida,
    PuntoEntrega, DsPuntoEntrega, FechaEntrega, ID_EstatusST,
    Estado, Asignado, Km, KmReal, Estimacion, CostoFinal,
    Diferencia, Comentario, FechaFinalizacion, Integrado,
    ObsValidaciones, CodEquipo, FueraPlan, Nom_Motorista,
    FechaInicioViaje, ComentInicioViaje, FechaFinViaje,
    ComentFinViaje, FechaEntregaST, ComentEntrega,
    CantidadCargadores, Permanencia, Permanencia_Aplica,
    InicioPermanencia, FinPermanencia, HorasPermanencia,
    HorasPermanenciaEst, OficialCosecha, Frente,
    EsHistorico, FechaActualizacion
  ) VALUES (
    s.ST, s.TipoViaje, s.OS, s.PuntoPartida, s.DsPuntoPartida,
    s.PuntoEntrega, s.DsPuntoEntrega, s.FechaEntrega, s.ID_EstatusST,
    s.Estado, s.Asignado, s.Km, s.KmReal, s.Estimacion, s.CostoFinal,
    s.Diferencia, s.Comentario, s.FechaFinalizacion, s.Integrado,
    s.ObsValidaciones, s.CodEquipo, s.FueraPlan, s.Nom_Motorista,
    s.FechaInicioViaje, s.ComentInicioViaje, s.FechaFinViaje,
    s.ComentFinViaje, s.FechaEntregaST, s.ComentEntrega,
    s.CantidadCargadores, s.Permanencia, s.Permanencia_Aplica,
    s.InicioPermanencia, s.FinPermanencia, s.HorasPermanencia,
    s.HorasPermanenciaEst, s.OficialCosecha, s.Frente,
    s.EsHistorico, GETUTCDATE()
  );`;

// ── Consultar API CASSA ────────────────────────────────────
async function consultarAPI(desde, hasta, context) {
  const baseParams = {
    Movil: '0', Usuario: API_USUARIO, Integrado: '0',
    CorporativoAlmacenes: '1', CorporativoHaciendas: '0', FueraPlan: '0',
  };
  const todos  = {};
  let   cursor = new Date(desde);

  while (cursor <= hasta) {
    const p = new URLSearchParams({
      ...baseParams,
      FechaInicio: fechaStr(cursor),
      FechaFin:    fechaStr(cursor),
    });
    try {
      const resp = await fetch(`${API_URL}?${p}`, {
        signal: AbortSignal.timeout(60000),
      });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const data = await resp.json();
      if (Array.isArray(data) && data.length) {
        context.log(`  ${fechaStr(cursor)}: ${data.length}`);
        for (const r of data) {
          const st = limpiar(String(r.ID_ST || ''));
          if (!st) continue;
          const ex = todos[st];
          if (!ex) { todos[st] = r; continue; }
          // Priorizar registro más completo
          const prio = x => x.FechaFinalizacion ? 0 : x.FechaInicioViaje ? 1 : 2;
          if (prio(r) < prio(ex)) todos[st] = r;
        }
      }
    } catch (e) {
      context.log(`  ⚠ ${fechaStr(cursor)}: ${e.message}`);
    }
    cursor = addDays(cursor, 1);
  }
  return Object.values(todos);
}

// ── UPSERT bulk via OPENJSON con COALESCE ─────────────────
async function upsertBulk(registros, context) {
  let ok = 0, err = 0;
  const LOTE = 500;

  for (let i = 0; i < registros.length; i += LOTE) {
    const lote = registros.slice(i, i + LOTE)
      .map(normalizar).filter(Boolean);
    if (!lote.length) continue;
    try {
      await query(MERGE_JSON_SQL, [{
        name:  'json',
        type:  TYPES.NVarChar,
        value: JSON.stringify(lote),
      }]);
      ok += lote.length;
    } catch (e) {
      context.log(`  ⚠ Lote ${i}: ${e.message.slice(0, 200)}`);
      err += lote.length;
    }
  }
  return { ok, err };
}

// ── Timer trigger — cada 5 minutos ────────────────────────
app.timer('syncAPI', {
  schedule: '0 */5 * * * *',
  runOnStartup: false,
  handler: async (timer, context) => {
    const t0 = Date.now();
    context.log(`[syncAPI] ${new Date().toISOString()}`);
    try {
      const hoy   = new Date();
      const desde = addDays(hoy, -3);
      const hasta = addDays(hoy,  1);
      const registros = await consultarAPI(desde, hasta, context);
      context.log(`  API: ${registros.length} únicos`);
      if (!registros.length) return;
      const { ok, err } = await upsertBulk(registros, context);

      // Refrescar viajes_master para el rango sincronizado
      try {
        const hastaFin = new Date(hasta); hastaFin.setHours(23,59,59,999);
        await query(
          `EXEC sp_refresh_viajes_master @desde = @d, @hasta = @h`,
          [
            { name: 'd', type: TYPES.DateTime2, value: desde },
            { name: 'h', type: TYPES.DateTime2, value: hastaFin },
          ]
        );
        context.log('  viajes_master refreshed');
      } catch(re) {
        context.log(`  ⚠ refresh master: ${re.message}`);
      }

      // Heredar estado ST → requerimientos (mapeo completo)
      // Cancelada/Rechazada/Inconsistente → cancelado
      // En Carga/En Proceso → en_proceso
      // Finalizada/Finalizada sin Aceptar/Finalizado sin Reanudar → finalizado
      try {
        await query(`
          UPDATE r SET
            r.Estado = CASE
              WHEN v.Estado IN ('En Carga','En Proceso')
                THEN CASE WHEN r.Estado LIKE '%parcial%' THEN 'en proceso parcial' ELSE 'en_proceso' END
              WHEN v.Estado IN ('Finalizada','Finalizada sin Aceptar',
                                'Finalizado sin Reanudar')
                THEN CASE WHEN r.Estado LIKE '%parcial%' THEN 'finalizado parcial' ELSE 'finalizado' END
              WHEN v.Estado IN ('Cancelada','Rechazada','Inconsistente') THEN 'cancelado'
              ELSE r.Estado
            END,
            r.MotivoCancel = CASE
              WHEN v.Estado IN ('Cancelada','Rechazada','Inconsistente')
                THEN CONCAT('ST ', v.Estado, ' en plataforma')
              ELSE r.MotivoCancel
            END,
            r.FechaActualizacion = GETUTCDATE()
          FROM requerimientos r
          INNER JOIN viajes_api v ON r.ST = v.ST
          WHERE r.Estado IN ('programado','programado parcial','en_proceso','en proceso parcial')
            AND v.Estado IN (
              'En Carga','En Proceso',
              'Finalizada','Finalizada sin Aceptar','Finalizado sin Reanudar',
              'Cancelada','Rechazada','Inconsistente'
            )
            AND r.Estado != CASE
              WHEN v.Estado IN ('En Carga','En Proceso')
                THEN CASE WHEN r.Estado LIKE '%parcial%' THEN 'en proceso parcial' ELSE 'en_proceso' END
              WHEN v.Estado IN ('Finalizada','Finalizada sin Aceptar',
                                'Finalizado sin Reanudar')
                THEN CASE WHEN r.Estado LIKE '%parcial%' THEN 'finalizado parcial' ELSE 'finalizado' END
              WHEN v.Estado IN ('Cancelada','Rechazada','Inconsistente') THEN 'cancelado'
              ELSE r.Estado
            END`, []
        );
        context.log('  herencia estado ST→req completada');
      } catch(he) {
        context.log(`  ⚠ herencia estado: ${he.message}`);
      }

      context.log(`[syncAPI] ✓ ok=${ok} err=${err} ms=${Date.now()-t0}`);
    } catch (e) {
      context.log(`[syncAPI] ERROR: ${e.message}`);
      throw e;
    }
  },
});

// ── HTTP manual — pruebas y backfill ──────────────────────
app.http('syncAPIManual', {
  methods: ['POST'],
  route: 'sync',
  authLevel: 'anonymous',
  handler: async (request, context) => {
    const body         = await request.json().catch(() => ({}));
    const diasAtras    = body.diasAtras    ?? 3;
    const diasAdelante = body.diasAdelante ?? 1;
    const t0           = Date.now();

    const hoy   = new Date();
    const desde = addDays(hoy, -diasAtras);
    const hasta = addDays(hoy,  diasAdelante);

    context.log(`[syncManual] ${fechaStr(desde)} → ${fechaStr(hasta)}`);

    const registros = await consultarAPI(desde, hasta, context);
    if (!registros.length) {
      return {
        status: 200,
        headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify({ ok: 0, err: 0, registros: 0, ms: Date.now()-t0 }),
      };
    }

    const { ok, err } = await upsertBulk(registros, context);

    // Refrescar viajes_master
    try {
      const hastaFin = new Date(hasta); hastaFin.setHours(23,59,59,999);
      await query(
        `EXEC sp_refresh_viajes_master @desde = @d, @hasta = @h`,
        [
          { name: 'd', type: TYPES.DateTime2, value: desde },
          { name: 'h', type: TYPES.DateTime2, value: hastaFin },
        ]
      );
    } catch(re) { context.log(`⚠ refresh master: ${re.message}`); }

    return {
      status: 200,
      headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
      body: JSON.stringify({
        ok, err, registros: registros.length,
        ventana: `${fechaStr(desde)} → ${fechaStr(hasta)}`,
        ms: Date.now() - t0,
      }),
    };
  },
});
