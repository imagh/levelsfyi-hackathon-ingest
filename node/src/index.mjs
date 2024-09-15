import express from "express";
import pg from "pg";
import { router } from "./sensorData/index.mjs";

const app = express();
// console.log(process.env);
const pool = new pg.Pool({
  host: process.env.PGHOST,
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  database: process.env.PGDATABASE,
  idleTimeoutMillis: 1000 * 3000
});


const createTableQuery = `CREATE TABLE IF NOT EXISTS ${process.env.PGTABLE} (
  id uuid NOT NULL,
  type uuid NOT NULL,
  subtype uuid NOT NULL,
  reading integer NOT NULL,
  location uuid NOT NULL,
  timestamp timestamp NOT NULL
)`;
await pool.query(createTableQuery);

app.use('/', (req, res, next) => {
  req.pgpool = pool;
  return next();
});

app.get("/", async (req, res) => {
  const { rows } = await pool.query("SELECT 5 * 5 AS value");
  res.send({ result: rows.at(0).value });
});

app.listen(process.env.PORT);

app.use('/', router);