"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getPool = getPool;
exports.pingDB = pingDB;
// functions/src/lib/db.ts
const pg_1 = require("pg");
const cloud_sql_connector_1 = require("@google-cloud/cloud-sql-connector");
let _pool = null;
/** Returns a singleton pg Pool connected via Cloud SQL Connector */
async function getPool() {
    if (_pool)
        return _pool;
    const connector = new cloud_sql_connector_1.Connector();
    // Choose PUBLIC since your instance has a public IP. Use PRIVATE if you only have private IP + VPC.
    const clientOpts = await connector.getOptions({
        instanceConnectionName: process.env.INSTANCE_CONNECTION_NAME, // e.g. "kidflix-4cda0:asia-southeast1:mydb"
        ipType: cloud_sql_connector_1.IpAddressTypes.PUBLIC, // <-- enum, not the string "PUBLIC"
    });
    _pool = new pg_1.Pool({
        ...clientOpts, // supplies host/port/ssl socket factory
        user: process.env.PG_USER,
        password: process.env.PG_PASSWORD,
        database: process.env.PG_DATABASE,
        // connectionTimeoutMillis: 10000,
        // idleTimeoutMillis: 30000,
        // max: 5,
    });
    return _pool;
}
/** Optional health check */
async function pingDB() {
    const pool = await getPool();
    const { rows } = await pool.query("select 1 as ok");
    return rows[0].ok === 1;
}
//# sourceMappingURL=db.js.map