// SQL Server access for the web service, used to exercise `ops connect` and
// `ops proxy` against an RDS SQL Server dependency.
//
// The platform exports the dependency as MSSQL_URL:
//   sqlserver://user:password@host:1433
// SQL Server on RDS is created with no database, so the URL names none. The
// service creates its own database on first use instead of writing into master.
const sql = require('mssql')

const DB_NAME = 'sampledb'

let poolPromise = null

// The platform percent-encodes the password with Go's url.QueryEscape, which
// writes a space as "+", so "+" is turned back into a space before decoding.
function decode(s) {
  return decodeURIComponent(s.replace(/\+/g, ' '))
}

function configFromUrl(raw) {
  const u = new URL(raw)
  if (u.protocol !== 'sqlserver:') {
    throw new Error(`MSSQL_URL is a ${u.protocol.replace(/:$/, '')} URL, expected sqlserver`)
  }
  return {
    server: u.hostname,
    port: u.port ? Number(u.port) : 1433,
    user: decode(u.username),
    password: decode(u.password),
    options: {
      encrypt: true,
      // The RDS certificate chains to the RDS CA, which Node does not trust.
      trustServerCertificate: true,
    },
    pool: { max: 5 },
  }
}

async function setup() {
  const base = configFromUrl(process.env.MSSQL_URL)

  const admin = await new sql.ConnectionPool({ ...base, database: 'master' }).connect()
  try {
    await admin.request().query(`IF DB_ID(N'${DB_NAME}') IS NULL CREATE DATABASE [${DB_NAME}]`)
  } finally {
    await admin.close()
  }

  const pool = await new sql.ConnectionPool({ ...base, database: DB_NAME }).connect()
  await pool.request().query(`
    IF OBJECT_ID(N'dbo.visits', N'U') IS NULL
      CREATE TABLE dbo.visits (
        id         INT IDENTITY(1,1) PRIMARY KEY,
        message    NVARCHAR(200) NOT NULL,
        user_agent NVARCHAR(400) NULL,
        visited_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
      )
  `)
  console.log(`SQL Server ready: ${base.server}:${base.port}/${DB_NAME}`)
  return pool
}

// getPool connects once and reuses the pool. A failed attempt is forgotten, so
// the next request retries rather than failing forever.
function getPool() {
  if (!process.env.MSSQL_URL) {
    return null
  }
  if (!poolPromise) {
    poolPromise = setup().catch((err) => {
      poolPromise = null
      throw err
    })
  }
  return poolPromise
}

// recordVisit inserts one row and returns the total, or null when the service
// has no SQL Server dependency.
async function recordVisit(message, userAgent) {
  const pool = await getPool()
  if (!pool) {
    return null
  }
  const result = await pool.request()
    .input('message', sql.NVarChar(200), message.slice(0, 200))
    .input('userAgent', sql.NVarChar(400), (userAgent || '').slice(0, 400))
    .query(`
      INSERT INTO dbo.visits (message, user_agent) VALUES (@message, @userAgent);
      SELECT COUNT(*) AS total FROM dbo.visits;
    `)
  return result.recordset[0].total
}

async function recentVisits(limit) {
  const pool = await getPool()
  if (!pool) {
    return null
  }
  const result = await pool.request()
    .input('limit', sql.Int, limit)
    .query('SELECT TOP (@limit) id, message, user_agent, visited_at FROM dbo.visits ORDER BY id DESC')
  return result.recordset
}

module.exports = { recordVisit, recentVisits, configFromUrl }
