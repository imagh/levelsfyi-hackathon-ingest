import * as https from "https";
import * as http from "http";
import { stringify } from "csv-stringify";
import { from as copyFrom, to as copyTo } from "pg-copy-streams";
import { pipeline } from "stream/promises";
import { parse } from 'csv-parse';
import { Router } from "express";
const router = Router();

// ingest waterfall
router.post ("/ingest", streamCSVIntoDB);

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
 * Express middleware to stream CSV file into DB
 */
async function streamCSVIntoDB(req, res, next) {
  if (!req.query.url) {
    return res.status(400).send("CSV url missing");
  }
  // decide on http request type
  let httpGet = http.get;
  if (req.query.url.startsWith("https")) {
    httpGet = https.get;
  }
  // get client
  const client = await req.pgpool.connect();
  let parser, transform, pgStream;
  try {
    // create copy query
    let query = "COPY test1 FROM STDIN ";
    query += "( ";
    query += "FORMAT CSV, ";
    query += "DELIMITER ',' ";
    query += ") ";

    // get csv stream parser, it can handle rows
    parser = parse({});
    // create array to csv row transfrom stream, it will ignore header row
    let transform = stringify({
      columns: [ "id", "type", "subtype", "reading", "location", "timestamp" ],
      quote: false,
      quotedEmpty: false,
      delimiter: ',',
      rowDelimiter: 'unix',
      transform: (data, encoding, callback) => {
        if (data && data[0] === "id") {
          callback(null, Buffer.alloc(0));
        } else {
          callback(null, data.join(',') + '\n');
        }
      }
    });

    // create write stream
    const pgStream = client.query(copyFrom(query));
    // fetch csv
    httpGet(req.query.url, async function (response) {
      // response is a stream of the csv file
      // pipeline is forming the stream chain from response -> parser -> transform -> pgStream
      await pipeline(response, parser, transform, pgStream);
      res.send({ success: true, message: "Data ingested" });
    });

  } catch (err) {
    console.error("Internal server error: ", err);
    if (parser) {
      parser.destroy();
    }
    if (transform) {
      transform.destroy();
    }
    if (pgStream) {
      pgStream.destroy();
    }
    throwErrorResp(res, err, err && err.toString() || "Internal server error", 500);
  } finally {
    client.release();
  }
}

router.get("/median", getMedian);

async function getMedian(req, res, next) {
  // get client
  const client = await req.pgpool.connect();
  let copyQuery, median, pgStream, parser;
  const list = [];

  try {
    // construct copy query
    copyQuery = "COPY ";
    let selectQuery = "( SELECT reading FROM test1";

    if (req.query.filter) {
      // apply filters
      const filters = JSON.parse(req.query.filter);
      const keys = Object.keys(filters);

      if (keys.length) {
        selectQuery += ` WHERE ${keys[0]} IN ( '${filters[keys[0]].join("', '")}' )`;

        for (let i = 1; i < keys.length; i++) {

          if (!filters[keys[i]]) {
            continue;
          }
          selectQuery += ` AND ${keys[i]} IN ( '${filters[keys[i]].join("', '")}' )`;
        }
      }
    }
    selectQuery += " ORDER BY reading ASC )";
    copyQuery += selectQuery + " TO STDOUT ";

    pgStream = client.query(copyTo(copyQuery));
    parser = parse()
      .on("data", (data) => {
        list.push(data[0]);
      });

    await pipeline(pgStream, parser);

    // find median
    if (list.length % 2 === 0) {
      median = (Number(list[(list.length / 2) - 1]) + Number(list[list.length / 2])) / 2;  // 0, 1, 2, 3, 4, 5
    } else {
      median = Number(list[parseInt(list.length / 2)]);
    }

    res.send({
      count: list.length,
      median
    });
  } catch(err) {
    if (pgStream) {
      pgStream.destroy();
    }
    if (parser) {
      parser.destroy();
    }
    throwErrorResp(res, err, err && err.toString() || "Interal Server Error", 500);
  } finally {
    client.release();
  }
}

export { throwErrorResp, router };