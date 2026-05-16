//Prueba//
const { Connection, Request, TYPES } = require('tedious');

const config = {
  server: 'autransp-server.database.windows.net',
  authentication: {
    type: 'default',
    options: {
      userName: 'autransp_admin',
      password: process.env.SQL_PASSWORD,
    },
  },
  options: {
    database:               'autransp-db',
    encrypt:                true,
    trustServerCertificate: false,
    connectTimeout:         30000,
    requestTimeout:         30000,
  },
};

function getConnection() {
  return new Promise((resolve, reject) => {
    const conn = new Connection(config);
    conn.on('connect', err => (err ? reject(err) : resolve(conn)));
    conn.connect();
  });
}

function query(sql, params = []) {
  return new Promise(async (resolve, reject) => {
    try {
      const conn = await getConnection();
      const rows = [];
      const req  = new Request(sql, (err) => {
        conn.close();
        if (err) reject(err);
        else resolve(rows);
      });
      params.forEach(p => req.addParameter(p.name, p.type, p.value));
      req.on('row', cols => {
        const row = {};
        cols.forEach(c => (row[c.metadata.colName] = c.value));
        rows.push(row);
      });
      conn.execSql(req);
    } catch (e) {
      reject(e);
    }
  });
}
module.exports = { query, TYPES };
