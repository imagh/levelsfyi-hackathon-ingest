import express from "express";
import pg from "pg";

const app = express();
const pool = new pg.Pool();

app.get("/", async (req, res) => {
  const { rows } = await pool.query("SELECT 5 * 5 AS value");
  res.send({ result: rows.at(0).value });
});

app.listen(process.env.PORT);
