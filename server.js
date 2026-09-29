/**
 * Pre-filter Pipeline API.
 *
 * Vercel runs this file as a serverless function. Do not call app.listen()
 * here — that crashes the invocation (FUNCTION_INVOCATION_FAILED). Local and
 * Docker use start.js, which listens.
 *
 * Each POST /run is stateless: the browser sends one CSV batch and gets that
 * batch's filtered CSV back. /tmp is not shared across Vercel instances, and
 * python3 is not available in the Node function, so the filter runs in-process.
 */
const express = require("express");
const path = require("node:path");
const fs = require("node:fs");
const { loadConfig, configFromKeywords, runPipeline } = require("./lib/pipeline");

const app = express();
const PROJECT_ROOT = path.resolve(__dirname);
const DEFAULT_INPUT = path.join(PROJECT_ROOT, "raw_tam.csv");
const DEFAULT_CONFIG = path.join(PROJECT_ROOT, "config.yaml");
const OUTPUT_CSV = process.env.OUTPUT_CSV || path.join("/tmp", "filtered_output.csv");
const CHUNK_SIZE = Math.min(50, Math.max(1, parseInt(process.env.CHUNK_SIZE || "20", 10) || 20));

let csvLibSource = "";
try {
  csvLibSource = fs.readFileSync(path.join(PROJECT_ROOT, "lib", "csv.js"), "utf8");
} catch (err) {
  console.error("Could not read lib/csv.js for the browser:", err.message);
  csvLibSource = "function parseCsv(){return [];} function stringifyCsv(){return '';} function splitCsvChunks(){return [];}";
}

app.disable("x-powered-by");
app.use(express.json({ limit: "4mb" }));

function parseKeywords(value) {
  if (!value || typeof value !== "string") return [];
  return value
    .split(/[\n,]+/)
    .map((keyword) => keyword.trim().toLowerCase())
    .filter(Boolean);
}

function readDefaultCsv() {
  if (!fs.existsSync(DEFAULT_INPUT)) return null;
  return fs.readFileSync(DEFAULT_INPUT, "utf8");
}

function resolveConfig(body) {
  const primary = parseKeywords(body.primary);
  const secondary = parseKeywords(body.secondary);
  const negative = parseKeywords(body.negative);
  if (primary.length || secondary.length) {
    return configFromKeywords(primary, secondary, negative, loadConfig(DEFAULT_CONFIG));
  }
  return loadConfig(fs.existsSync(DEFAULT_CONFIG) ? DEFAULT_CONFIG : null);
}

function persistOutput(csv) {
  try {
    fs.writeFileSync(OUTPUT_CSV, csv, "utf8");
  } catch (err) {
    console.error("Could not persist output CSV:", err.message);
  }
}

const pageStart = `<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Pre-filter Pipeline</title>
  <style>
    * { box-sizing: border-box; }
    body { font-family: system-ui, sans-serif; max-width: 520px; margin: 2rem auto; padding: 0 1rem; }
    h1 { font-size: 1.35rem; margin-bottom: 0.25rem; }
    label { display: block; font-weight: 600; margin-top: 1rem; margin-bottom: 0.25rem; }
    .hint { font-size: 0.875rem; color: #666; font-weight: normal; margin-top: 0.15rem; }
    input[type="text"], input[type="password"], textarea { width: 100%; padding: 0.5rem; border: 1px solid #ccc; border-radius: 6px; font: inherit; }
    textarea { min-height: 64px; resize: vertical; }
    input[type="file"] { width: 100%; padding: 0.35rem 0; }
    button {
      font-size: 1rem; padding: 0.65rem 1.25rem; margin: 1.25rem 0 0;
      background: #0d6efd; color: #fff; border: none; border-radius: 6px; cursor: pointer;
    }
    button:hover { background: #0b5ed7; }
    button:disabled { opacity: 0.6; cursor: not-allowed; }
    #result { margin-top: 1rem; padding: 1rem; border-radius: 8px; white-space: pre-wrap; }
    #result.success { background: #d1e7dd; }
    #result.error { background: #f8d7da; }
    #result .summary { font-size: 1.1rem; font-weight: 600; margin-bottom: 0.5rem; }
    #result a { color: #0d6efd; }
    .progress-bar { width: 100%; height: 18px; background: #e9ecef; border-radius: 9px; overflow: hidden; margin-top: 0.5rem; }
    .progress-fill { height: 100%; background: #0d6efd; border-radius: 9px; transition: width 0.3s ease; }
    .chunk-info { font-size: 0.85rem; color: #555; margin-top: 0.35rem; }
  </style>
</head>
<body>
  <h1>Pre-filter Pipeline</h1>
  <form id="form">
    <label>ZenRows API key <span class="hint">(optional; improves Layer 2 scraping)</span></label>
    <input type="password" name="apiKey" id="apiKey" placeholder="Leave blank to use free fallback" autocomplete="off">

    <label>Input CSV <span class="hint">(must have company_name and domain columns)</span></label>
    <input type="file" name="csv" id="csv" accept=".csv,text/csv">

    <label>Primary keywords <span class="hint">(comma or newline; 1+ match = pass)</span></label>
    <textarea name="primary" id="primary" placeholder="e.g. septic, wastewater, pumping">septic, wastewater, pumping</textarea>

    <label>Secondary keywords <span class="hint">(2+ matches = pass)</span></label>
    <textarea name="secondary" id="secondary" placeholder="e.g. tank, drain, cleanout">tank, drain, cleanout</textarea>

    <label>Exclusion keywords <span class="hint">(any match = fail)</span></label>
    <textarea name="negative" id="negative" placeholder="e.g. restaurant, retail">restaurant, retail</textarea>

    <button type="submit" id="runBtn">Run pipeline</button>
  </form>
  <div id="result"></div>
  <script>
`;

const pageClient = `
    var CHUNK_SIZE = ${CHUNK_SIZE};
    function escapeHtml(s) { return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }
    var form = document.getElementById('form');
    var runBtn = document.getElementById('runBtn');
    var resultEl = document.getElementById('result');

    function showProgress(chunk, totalChunks, summary) {
      var pct = Math.round(((chunk + 1) / totalChunks) * 100);
      var parts = '<div class="summary">Processing batch ' + (chunk + 1) + ' of ' + totalChunks + '</div>';
      parts += '<div class="progress-bar"><div class="progress-fill" style="width:' + pct + '%"></div></div>';
      if (summary && summary.total) {
        parts += '<div class="chunk-info">' + summary.total + ' rows processed so far — ' +
          summary.pass + ' pass, ' + summary.fail + ' fail, ' + summary.review + ' review</div>';
      }
      resultEl.className = '';
      resultEl.innerHTML = parts;
    }

    function showDone(summary, csvText) {
      var blob = new Blob([csvText || ''], { type: 'text/csv;charset=utf-8' });
      var url = URL.createObjectURL(blob);
      resultEl.className = 'success';
      resultEl.innerHTML = '<div class="summary">Done: ' + summary.pass + ' pass, ' +
        summary.fail + ' fail, ' + summary.review + ' review (' + summary.total + ' total)</div>' +
        '<a id="download" download="filtered_output.csv">Download filtered_output.csv</a>';
      document.getElementById('download').href = url;
    }

    function showError(msg, chunk) {
      resultEl.className = 'error';
      var prefix = typeof chunk === 'number' ? 'Error on batch ' + (chunk + 1) : 'Error';
      resultEl.innerHTML = '<div class="summary">' + prefix + '</div>' + escapeHtml(msg);
    }

    function addSummary(total, part) {
      total.total += part.total || 0;
      total.pass += part.pass || 0;
      total.fail += part.fail || 0;
      total.review += part.review || 0;
      return total;
    }

    async function readError(response) {
      var data;
      try { data = await response.json(); } catch (_) { data = null; }
      if (data && data.error) return data.error;
      if (response.status === 504 || response.status === 408) {
        return 'The request timed out. Use a smaller CSV or set CHUNK_SIZE lower.';
      }
      return 'Server returned a non-JSON error (status ' + response.status + '). Check the Vercel function logs.';
    }

    form.onsubmit = async function(e) {
      e.preventDefault();
      runBtn.disabled = true;
      resultEl.className = '';
      resultEl.innerHTML = 'Uploading and starting…';

      var fields = {
        apiKey: document.getElementById('apiKey').value,
        primary: document.getElementById('primary').value,
        secondary: document.getElementById('secondary').value,
        negative: document.getElementById('negative').value
      };
      var chunks = [null];
      var file = document.getElementById('csv').files[0];
      try {
        if (file) {
          var text = await file.text();
          chunks = splitCsvChunks(text, CHUNK_SIZE);
          if (!chunks.length) {
            showError('That CSV has no rows.');
            runBtn.disabled = false;
            return;
          }
        }

        var merged = null;
        var totals = { total: 0, pass: 0, fail: 0, review: 0 };
        for (var i = 0; i < chunks.length; i++) {
          showProgress(i, chunks.length, totals);
          var payload = {
            apiKey: fields.apiKey,
            primary: fields.primary,
            secondary: fields.secondary,
            negative: fields.negative
          };
          if (chunks[i] != null) payload.csv = chunks[i];
          var response = await fetch('/run', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
          });
          var data;
          try { data = await response.json(); } catch (_) {
            showError(await readError(response), i);
            runBtn.disabled = false;
            return;
          }
          if (!response.ok || !data.ok) {
            showError(data.error || 'Unknown error', i);
            runBtn.disabled = false;
            return;
          }
          addSummary(totals, data.summary || {});
          var parsed = parseCsv(data.csv || '');
          if (!merged) merged = parsed;
          else if (parsed.length > 1) merged = merged.concat(parsed.slice(1));
          showProgress(i, chunks.length, totals);
        }
        showDone(totals, stringifyCsv(merged || []));
      } catch (err) {
        showError(err.message || String(err));
      }
      runBtn.disabled = false;
    };
`;

function pageHtml() {
  const lib = csvLibSource.replace(/<\/script/gi, "<\\/script>");
  return `${pageStart}${lib}\n${pageClient}\n  </script>\n</body>\n</html>\n`;
}

function asyncRoute(fn) {
  return (req, res, next) => Promise.resolve(fn(req, res, next)).catch(next);
}

app.get("/", (req, res) => {
  res.set("Cache-Control", "no-store");
  res.type("text/html").send(pageHtml());
});

app.post("/run", asyncRoute(async (req, res) => {
  const contentType = req.headers["content-type"] || "";
  if (contentType.includes("multipart/form-data")) {
    return res.status(400).json({
      ok: false,
      error: "Send JSON { csv, apiKey, primary, secondary, negative } instead of multipart form data.",
    });
  }

  const body = req.body && typeof req.body === "object" ? req.body : {};
  let csvText = typeof body.csv === "string" ? body.csv : "";
  if (!csvText.trim()) {
    csvText = readDefaultCsv();
    if (!csvText) {
      return res.status(400).json({
        ok: false,
        error: "No input CSV. Upload a file or add raw_tam.csv to the server.",
      });
    }
  }

  const config = resolveConfig(body);
  const apiKey = (body.apiKey || process.env.ZENROWS_API_KEY || "").trim();
  const result = await runPipeline({ csvText, config, apiKey });
  persistOutput(result.csv);
  console.log(
    `pipeline rows=${result.summary.total} pass=${result.summary.pass} fail=${result.summary.fail} review=${result.summary.review}`
  );
  res.json({
    ok: true,
    done: true,
    summary: result.summary,
    csv: result.csv,
  });
}));

app.get("/run", asyncRoute(async (req, res) => {
  const csvText = readDefaultCsv();
  if (!csvText) {
    return res.status(400).json({ ok: false, error: "No input CSV. Upload one via the form or add raw_tam.csv." });
  }
  const result = await runPipeline({
    csvText,
    config: loadConfig(fs.existsSync(DEFAULT_CONFIG) ? DEFAULT_CONFIG : null),
    apiKey: process.env.ZENROWS_API_KEY || "",
  });
  persistOutput(result.csv);
  res.json({ ok: true, summary: result.summary, csv: result.csv });
}));

app.post("/run/next", (req, res) => {
  res.status(400).json({
    ok: false,
    error: "Batches are stateless. Send the next CSV batch to POST /run. Server-side chunk files are not kept between requests.",
  });
});

app.get("/status", (req, res) => {
  res.json({ ok: true, running: false, chunk: 0, totalChunks: 0, done: false, summary: null });
});

app.get("/output", (req, res) => {
  if (!fs.existsSync(OUTPUT_CSV)) {
    return res.status(404).json({ ok: false, error: "No output yet. Run the pipeline first." });
  }
  res.type("text/csv").attachment("filtered_output.csv").send(fs.readFileSync(OUTPUT_CSV));
});

app.use((req, res) => {
  res.status(404).json({ ok: false, error: "Not found" });
});

app.use((err, req, res, next) => {
  console.error(err);
  if (res.headersSent) return next(err);
  const tooLarge = err.type === "entity.too.large" || err.status === 413;
  const status = tooLarge ? 413 : err.status || 500;
  res.status(status).json({
    ok: false,
    error: tooLarge
      ? "Request body is too large. Split the CSV into smaller batches."
      : err.message || "Server error",
  });
});

module.exports = app;
