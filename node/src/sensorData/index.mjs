import * as https from "https";
import * as http from "http";
import { parse } from 'csv-parse';
import { Router } from "express";
const router = Router();

// ingest waterfall
router.post ("/ingest", [
  fetchCSV,
  saveIntoDB
]);

/**
 * A helper function to throw consistent error object
 */
function throwErrorResp(res, err, message, statusCode) {
  return res
    .status(statusCode)
    .send({
      success: false,
      err: err && err.toString() || err || "Something went wrong.",
      message
    });
}

/**
 * Express middleware to fetch CSV file data and collect in memory
 */
function fetchCSV(req, res, next) {
  if (!req.query.url) {
    return throwErrorResp(res, "CSV url missing", "CSV url missing", 400);
  }
  let httpGet = http.get;
  if (req.query.url.startsWith("https")) {
    httpGet = https.get;
  }
  const data = [];
  httpGet(req.query.url, function (response) {
    response
      .pipe(parse())
      .on('data', (chunk) => {
        // each chunk is a row, processing proper number and timestamp conversion
        chunk[3] = parseInt(chunk[3]);
        chunk[5] = new Date(chunk[5]);
        data.push(chunk);
      }).on('end', () => {
        req.csvBuffer = data;
        return next();
      }).on('error', (err) => {
        return throwErrorResp(res, err , "Download error", 500);
      });
  });
}

/**
 * Express middleware to save in-memory CSV data into database
 */
async function saveIntoDB(req, res, next) {
  const client = await req.pgpool.connect();
  req.csvBuffer = req.csvBuffer.splice(1);
  for (const row of req.csvBuffer) {
    let queryString = `INSERT into ${process.env.PGTABLE}` +
      ` (id,type,subtype,reading,location,timestamp)` +
      ` VALUES ($1,$2,$3,$4,$5,$6)`;
    
    await client.query(queryString, row);
  }
  await client.end();
  client.release();
  return res.send({
    success: true,
    message: "Data ingested"
  });
}

export { throwErrorResp, fetchCSV, saveIntoDB, router };