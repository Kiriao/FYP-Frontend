// functions/src/lib/db.ts
import { Pool } from "pg";
import { Connector, IpAddressTypes } from "@google-cloud/cloud-sql-connector";

let _pool: Pool | null = null;

/** Returns a singleton pg Pool connected via Cloud SQL Connector */
export async function getPool(): Promise<Pool> {
  if (_pool) return _pool;

  const connector = new Connector();

  // Choose PUBLIC since your instance has a public IP. Use PRIVATE if you only have private IP + VPC.
  const clientOpts = await connector.getOptions({
    instanceConnectionName: process.env.INSTANCE_CONNECTION_NAME!, // e.g. "kidflix-4cda0:asia-southeast1:mydb"
    ipType: IpAddressTypes.PUBLIC,  // <-- enum, not the string "PUBLIC"
  });

  _pool = new Pool({
    ...clientOpts,                    // supplies host/port/ssl socket factory
    user: process.env.PG_USER!,
    password: process.env.PG_PASSWORD!,
    database: process.env.PG_DATABASE!,
    // connectionTimeoutMillis: 10000,
    // idleTimeoutMillis: 30000,
    // max: 5,
  });

  return _pool;
}

/** Optional health check */
export async function pingDB() {
  const pool = await getPool();
  const { rows } = await pool.query("select 1 as ok");
  return rows[0].ok === 1;
}
