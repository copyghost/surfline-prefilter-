/**
 * Pre-filter Pipeline API with chunked processing for Vercel serverless.
 * Splits large CSVs into CHUNK_SIZE-row pieces so each invocation finishes
 * within the serverless execution timeout.  The client drives the loop,
 * calling POST /run (upload + chunk 0) then POST /run/next for each
 * subsequent chunk.
 */
const express = require("express");
const multer = require("multer");
const { exec } = require("node:child_process");
const { promisify } = require("node:util");
const path = require("node:path");
const fs = require("node:fs");

const execAsync = promisify(exec);
const app = express();
const PORT = process.env.PORT || 3000;

const PROJECT_ROOT = path.resolve(__dirname);
const TMP_DIR = "/tmp";
const UPLOAD_DIR = path.join(TMP_DIR, "uploads");
const CHUNK_SIZE = parseInt(process.env.CHUNK_SIZE || "50", 10);

const FULL_INPUT_PATH = path.join(TMP_DIR, "full_input.csv");
const CHUNK_INPUT_PATH = path.join(TMP_DIR, "chunk_input.csv");
const CHUNK_OUTPUT_PATH = path.join(TMP_DIR, "chunk_output.csv");
const CHUNK_STATE_PATH = path.join(TMP_DIR, "chunk_state.json");
const OUTPUT_CSV = process.env.OUTPUT_CSV || path.join(TMP_DIR, "filtered_output.csv");

const DEFAULT_INPUT = path.join(PROJECT_ROOT, "raw_tam.csv");
const DEFAULT_CONFIG = path.join(PROJECT_ROOT, "config.yaml");
const SCRIPT_PATH = path.join(PROJECT_ROOT, "pre_filter.py");

if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });

const upload = multer({
  dest: UPLOAD_DIR,
  limits: { fileSize: 50 * 1024 * 1024 },
}).single("csv");

/* ── helpers ─────────────────────────────────────────────── */

function parseKeywords(s) {
  if (!s || typeof s !== "string") return [];
  return s.split(/[\n,]+/).map((k) => k.trim().toLowerCase()).filter(Boolean);
}

function buildConfigYaml(primary, secondary, negative) {
  const p = primary.length ? primary : ["septic", "wastewater", "pumping"];
  const s = secondary.length ? secondary : ["tank", "drain", "cleanout"];
  const n = negative.length ? negative : [];
  return `industry_keywords:
  primary: ${JSON.stringify(p)}
  secondary: ${JSON.stringify(s)}
  negative: ${JSON.stringify(n)}
`;
}

function runPipeline(opts) {
  const inputPath = opts.inputPath || DEFAULT_INPUT;
  const outputPath = opts.outputPath || OUTPUT_CSV;
  const configPath = opts.configPath || DEFAULT_CONFIG;
  const apiKey = (opts.apiKey || "").trim();
  const useConfig = configPath && fs.existsSync(configPath);
  const configArg = useConfig ? `--config "${configPath}"` : "--layer1-only";
  const cmd = `python3 "${SCRIPT_PATH}" --input "${inputPath}" --output "${outputPath}" ${configArg}`.trim();
  const env = { ...process.env };
  if (apiKey) env.ZENROWS_API_KEY = apiKey;
  return execAsync(cmd, { cwd: PROJECT_ROOT, maxBuffer: 100 * 1024 * 1024, env });
}

function getSummaryFromCsv(csvPath) {
  if (!fs.existsSync(csvPath)) return null;
  const content = fs.readFileSync(csvPath, "utf-8");
  const lines = content.trim().split("\n");
  if (lines.length < 2) return { total: 0, pass: 0, fail: 0, review: 0 };
  let pass = 0, fail = 0, review = 0;
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    if (/,PASS,/.test(line) || line.endsWith(",PASS")) pass++;
    else if (/,FAIL,/.test(line) || line.endsWith(",FAIL")) fail++;
    else if (/,REVIEW,/.test(line) || line.endsWith(",REVIEW")) review++;
  }
  return { total: lines.length - 1, pass, fail, review };
}

function getChunkState() {
  if (!fs.existsSync(CHUNK_STATE_PATH)) return null;
  try { return JSON.parse(fs.readFileSync(CHUNK_STATE_PATH, "utf-8")); } catch { return null; }
}

function saveChunkState(state) {
  fs.writeFileSync(CHUNK_STATE_PATH, JSON.stringify(state), "utf-8");
}

function countInputRows() {
  const content = fs.readFileSync(FULL_INPUT_PATH, "utf-8");
  return content.trim().split("\n").length - 1;
}

function extractChunkCsv(chunkIndex) {
  const content = fs.readFileSync(FULL_INPUT_PATH, "utf-8");
  const lines = content.trim().split("\n");
  const header = lines[0];
  const dataLines = lines.slice(1);
  const start = chunkIndex * CHUNK_SIZE;
  const end = Math.min(start + CHUNK_SIZE, dataLines.length);
  return header + "\n" + dataLines.slice(start, end).join("\n") + "\n";
}

function mergeChunkOutput(chunkIndex) {
  if (!fs.existsSync(CHUNK_OUTPUT_PATH)) return;
  const output = fs.readFileSync(CHUNK_OUTPUT_PATH, "utf-8").trim();
  if (!output) return;
  const lines = output.split("\n");
  if (chunkIndex === 0) {
    fs.writeFileSync(OUTPUT_CSV, output + "\n", "utf-8");
  } else if (lines.length > 1) {
    fs.appendFileSync(OUTPUT_CSV, lines.slice(1).join("\n") + "\n", "utf-8");
  }
}

async function processChunk(chunkIndex, opts) {
  const chunkCsv = extractChunkCsv(chunkIndex);
  fs.writeFileSync(CHUNK_INPUT_PATH, chunkCsv, "utf-8");
  await runPipeline({
    inputPath: CHUNK_INPUT_PATH,
    outputPath: CHUNK_OUTPUT_PATH,
    configPath: opts.configPath,
    apiKey: opts.apiKey,
  });
  mergeChunkOutput(chunkIndex);
  return getSummaryFromCsv(OUTPUT_CSV);
}

/* ── HTML form ───────────────────────────────────────────── */

const formHtml = `
<!DOCTYPE html>
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
    #result { margin-top: 1rem; padding: 1rem; border-radius: 8px; }
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
    function escapeHtml(s) { return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }
    var form = document.getElementById('form');
    var runBtn = document.getElementById('runBtn');
    var resultEl = document.getElementById('result');

    function showProgress(chunk, totalChunks, summary) {
      var pct = Math.round(((chunk + 1) / totalChunks) * 100);
      var parts = '<div class="summary">Processing chunk ' + (chunk + 1) + ' of ' + totalChunks + '</div>';
      parts += '<div class="progress-bar"><div class="progress-fill" style="width:' + pct + '%"></div></div>';
      if (summary) {
        parts += '<div class="chunk-info">' + summary.total + ' rows processed so far — ' +
          summary.pass + ' pass, ' + summary.fail + ' fail, ' + summary.review + ' review</div>';
      }
      resultEl.className = '';
      resultEl.innerHTML = parts;
    }

    function showDone(summary) {
      resultEl.className = 'success';
      resultEl.innerHTML = '<div class="summary">Done: ' + summary.pass + ' pass, ' +
        summary.fail + ' fail, ' + summary.review + ' review (' + summary.total + ' total)</div>' +
        '<a href="/output">Download filtered_output.csv</a>';
    }

    function showError(msg, chunk) {
      resultEl.className = 'error';
      var prefix = typeof chunk === 'number' ? 'Error on chunk ' + (chunk + 1) : 'Error';
      resultEl.innerHTML = '<div class="summary">' + prefix + '</div>' + escapeHtml(msg);
    }

    form.onsubmit = async function(e) {
      e.preventDefault();
      runBtn.disabled = true;
      resultEl.className = '';
      resultEl.innerHTML = 'Uploading and starting…';

      var fd = new FormData(form);
      try {
        var r = await fetch('/run', { method: 'POST', body: fd });
        var data;
        try { data = await r.json(); } catch(_) {
          showError('Server returned non-JSON (status ' + r.status + '). Check Vercel logs.');
          runBtn.disabled = false;
          return;
        }
        if (!data.ok) { showError(data.error || 'Unknown error', data.chunk); runBtn.disabled = false; return; }

        showProgress(data.chunk, data.totalChunks, data.summary);

        while (!data.done) {
          r = await fetch('/run/next', { method: 'POST' });
          try { data = await r.json(); } catch(_) {
            showError('Server returned non-JSON during chunk processing (status ' + r.status + ').');
            runBtn.disabled = false;
            return;
          }
          if (!data.ok) { showError(data.error || 'Unknown error', data.chunk); runBtn.disabled = false; return; }
          showProgress(data.chunk, data.totalChunks, data.summary);
        }

        showDone(data.summary || { total: 0, pass: 0, fail: 0, review: 0 });
      } catch (err) {
        showError(err.message);
      }
      runBtn.disabled = false;
    };
  </script>
</body>
</html>
`;

/* ── routes ──────────────────────────────────────────────── */

app.get("/", (req, res) => {
  res.type("text/html").send(formHtml);
});

app.post("/run", (req, res, next) => {
  upload(req, res, function (err) {
    if (err) return res.status(400).json({ ok: false, error: err.message || "Upload failed" });
    next();
  });
}, async (req, res) => {
  if (!fs.existsSync(SCRIPT_PATH)) {
    return res.status(500).json({ ok: false, error: "pre_filter.py not found" });
  }

  if (req.file && req.file.path) {
    fs.copyFileSync(req.file.path, FULL_INPUT_PATH);
    try { fs.unlinkSync(req.file.path); } catch {}
  } else if (fs.existsSync(DEFAULT_INPUT)) {
    fs.copyFileSync(DEFAULT_INPUT, FULL_INPUT_PATH);
  } else {
    return res.status(400).json({ ok: false, error: "No input CSV. Upload a file or add raw_tam.csv to the server." });
  }

  const primary = parseKeywords(req.body.primary);
  const secondary = parseKeywords(req.body.secondary);
  const negative = parseKeywords(req.body.negative);
  const hasKeywords = primary.length > 0 || secondary.length > 0;
  const runConfigPath = path.join(UPLOAD_DIR, "config_run.yaml");
  if (hasKeywords) {
    fs.writeFileSync(runConfigPath, buildConfigYaml(primary, secondary, negative), "utf-8");
  }
  const configPath = hasKeywords ? runConfigPath : (fs.existsSync(DEFAULT_CONFIG) ? DEFAULT_CONFIG : null);
  const apiKey = (req.body.apiKey || "").trim();

  const totalRows = countInputRows();
  const totalChunks = Math.ceil(totalRows / CHUNK_SIZE);

  if (fs.existsSync(OUTPUT_CSV)) fs.unlinkSync(OUTPUT_CSV);

  const state = { currentChunk: 0, totalChunks, totalRows, apiKey, configPath };
  saveChunkState(state);

  try {
    const summary = await processChunk(0, { configPath, apiKey });
    state.currentChunk = 1;
    saveChunkState(state);
    res.json({
      ok: true,
      chunk: 0,
      totalChunks,
      totalRows,
      done: totalChunks <= 1,
      summary: summary || { total: 0, pass: 0, fail: 0, review: 0 },
    });
  } catch (err) {
    res.status(500).json({
      ok: false,
      chunk: 0,
      totalChunks,
      error: err.message || String(err),
    });
  }
});

app.post("/run/next", async (req, res) => {
  const state = getChunkState();
  if (!state) {
    return res.status(400).json({ ok: false, error: "No active job. Upload a CSV via the form first." });
  }
  if (!fs.existsSync(FULL_INPUT_PATH)) {
    return res.status(400).json({ ok: false, error: "Input file missing (server cold-started). Please re-upload your CSV." });
  }
  if (state.currentChunk >= state.totalChunks) {
    const summary = getSummaryFromCsv(OUTPUT_CSV);
    return res.json({ ok: true, chunk: state.currentChunk - 1, totalChunks: state.totalChunks, done: true, summary });
  }

  const chunkIndex = state.currentChunk;
  try {
    const summary = await processChunk(chunkIndex, { configPath: state.configPath, apiKey: state.apiKey });
    state.currentChunk = chunkIndex + 1;
    saveChunkState(state);
    res.json({
      ok: true,
      chunk: chunkIndex,
      totalChunks: state.totalChunks,
      totalRows: state.totalRows,
      done: state.currentChunk >= state.totalChunks,
      summary: summary || { total: 0, pass: 0, fail: 0, review: 0 },
    });
  } catch (err) {
    res.status(500).json({
      ok: false,
      chunk: chunkIndex,
      totalChunks: state.totalChunks,
      error: err.message || String(err),
    });
  }
});

app.get("/run", async (req, res) => {
  if (!fs.existsSync(SCRIPT_PATH)) {
    return res.status(500).json({ ok: false, error: "pre_filter.py not found" });
  }
  if (!fs.existsSync(DEFAULT_INPUT)) {
    return res.status(400).json({ ok: false, error: "No input CSV. Upload one via the form or add raw_tam.csv." });
  }
  try {
    const { stdout, stderr } = await runPipeline({ outputPath: OUTPUT_CSV });
    const summary = getSummaryFromCsv(OUTPUT_CSV);
    res.json({
      ok: true,
      summary: summary || { total: 0, pass: 0, fail: 0, review: 0 },
      logTail: (stdout + "\n" + stderr).slice(-1500),
    });
  } catch (err) {
    res.status(500).json({
      ok: false,
      error: err.message || String(err),
      logTail: (err.stdout || "") + (err.stderr || "").slice(-1500),
    });
  }
});

app.get("/status", (req, res) => {
  const state = getChunkState();
  if (!state) return res.json({ running: false, chunk: 0, totalChunks: 0, done: false, summary: null });
  const done = state.currentChunk >= state.totalChunks;
  res.json({
    running: false,
    chunk: state.currentChunk,
    totalChunks: state.totalChunks,
    done,
    summary: done ? getSummaryFromCsv(OUTPUT_CSV) : null,
  });
});

app.get("/output", (req, res) => {
  if (!fs.existsSync(OUTPUT_CSV)) {
    return res.status(404).json({ error: "No output yet. Run the pipeline first." });
  }
  res.type("text/csv").attachment("filtered_output.csv").send(fs.readFileSync(OUTPUT_CSV));
});

app.listen(PORT, () => {
  console.log(`Pipeline: http://localhost:${PORT} (chunk size: ${CHUNK_SIZE} rows)`);
});
