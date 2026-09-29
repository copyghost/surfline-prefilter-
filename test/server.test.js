const { describe, test, before, after } = require("node:test");
const assert = require("node:assert/strict");
const http = require("node:http");

const app = require("../server");

function request(port, method, urlPath, body) {
  return new Promise((resolve, reject) => {
    const data = body == null ? null : JSON.stringify(body);
    const req = http.request(
      {
        hostname: "127.0.0.1",
        port,
        path: urlPath,
        method,
        headers: data
          ? { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(data) }
          : {},
      },
      (res) => {
        const chunks = [];
        res.on("data", (chunk) => chunks.push(chunk));
        res.on("end", () => {
          const raw = Buffer.concat(chunks).toString("utf8");
          let json = null;
          try {
            json = JSON.parse(raw);
          } catch {
            json = null;
          }
          resolve({ status: res.statusCode, headers: res.headers, raw, json });
        });
      }
    );
    req.on("error", reject);
    if (data) req.write(data);
    req.end();
  });
}

describe("server", () => {
  let server;
  let port;

  before(async () => {
    server = http.createServer(app);
    await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
    port = server.address().port;
  });

  after(() => new Promise((resolve, reject) => server.close((err) => (err ? reject(err) : resolve()))));

  test("GET / renders the form and the csv helpers", async () => {
    const res = await request(port, "GET", "/");
    assert.equal(res.status, 200);
    assert.match(res.headers["content-type"], /html/);
    assert.match(res.raw, /Run pipeline/);
    assert.match(res.raw, /function parseCsv/);
    assert.match(res.raw, /function splitCsvChunks/);
    assert.match(res.raw, /CHUNK_SIZE/);
  });

  test("POST /run returns JSON for a dead-domain batch and /output serves it", async () => {
    const csv = "company_name,domain\nNope,not-a-real-domain.invalid\n";
    const res = await request(port, "POST", "/run", {
      csv,
      primary: "septic",
      secondary: "tank, drain",
      negative: "restaurant",
      apiKey: "",
    });
    assert.equal(res.status, 200);
    assert.equal(res.json.ok, true);
    assert.equal(res.json.summary.total, 1);
    assert.equal(res.json.summary.fail, 1);
    assert.match(res.json.csv, /filter_decision/);

    const output = await request(port, "GET", "/output");
    assert.equal(output.status, 200);
    assert.match(output.headers["content-type"], /csv/);
    assert.match(output.raw, /not-a-real-domain.invalid/);
  });

  test("POST /run/next explains that batches are stateless", async () => {
    const res = await request(port, "POST", "/run/next", {});
    assert.equal(res.status, 400);
    assert.equal(res.json.ok, false);
    assert.match(res.json.error, /POST \/run/);
  });

  test("unknown routes return JSON instead of crashing", async () => {
    const res = await request(port, "GET", "/favicon.ico");
    assert.equal(res.status, 404);
    assert.equal(res.json.ok, false);
  });
});
