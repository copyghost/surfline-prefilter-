/**
 * Pre-filter Pipeline API: form for ZenRows API key, CSV upload, and keywords.
 * POST /run runs the pipeline with the provided options.
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
const UPLOAD_DIR = path.join(PROJECT_ROOT, "uploads");
const DEFAULT_INPUT = path.join(PROJECT_ROOT, "raw_tam.csv");
const OUTPUT_CSV = process.env.OUTPUT_CSV || path.join(PROJECT_ROOT, "filtered_output.csv");
const DEFAULT_CONFIG = path.join(PROJECT_ROOT, "config.yaml");
const SCRIPT_PATH = path.join(PROJECT_ROOT, "pre_filter.py");

if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });

const upload = multer({
  dest: UPLOAD_DIR,
  limits: { fileSize: 50 * 1024 * 1024 },
}).single("csv");

let lastSummary = null;

function parseKeywords(s) {
  if (!s || typeof s !== "string") return [];
  return s
    .split(/[\n,]+/)
    .map((k) => k.trim().toLowerCase())
    .filter(Boolean);
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
  const configPath = opts.configPath || DEFAULT_CONFIG;
  const apiKey = (opts.apiKey || "").trim();
  const useConfig = fs.existsSync(configPath);
  const configArg = useConfig ? `--config "${configPath}"` : "--layer1-only";
  const cmd = `python3 "${SCRIPT_PATH}" --input "${inputPath}" --output "${OUTPUT_CSV}" ${configArg}`.trim();
  const env = { ...process.env };
  if (apiKey) env.ZENROWS_API_KEY = apiKey;
  return execAsync(cmd, { cwd: PROJECT_ROOT, maxBuffer: 10 * 1024 * 1024, env });
}

function getSummaryFromCsv(outputPath) {
  if (!fs.existsSync(outputPath)) return null;
  const content = fs.readFileSync(outputPath, "utf-8");
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
    #log { font-size: 0.8rem; margin-top: 0.5rem; white-space: pre-wrap; max-height: 180px; overflow-y: auto; }
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
    const form = document.getElementById('form');
    const runBtn = document.getElementById('runBtn');
    const resultEl = document.getElementById('result');
    form.onsubmit = async function(e) {
      e.preventDefault();
      runBtn.disabled = true;
      resultEl.innerHTML = 'Running…';
      resultEl.className = '';
      const fd = new FormData(form);
      try {
        const r = await fetch('/run', { method: 'POST', body: fd });
        const data = await r.json();
        if (data.ok) {
          const s = data.summary;
          resultEl.className = 'success';
          resultEl.innerHTML = '<div class="summary">' + s.pass + ' pass, ' + s.fail + ' fail, ' + s.review + ' review (' + s.total + ' total)</div>' +
            '<a href="/output">Download filtered_output.csv</a>' +
            (data.logTail ? '<pre id="log">' + escapeHtml(data.logTail.slice(-800)) + '</pre>' : '');
        } else {
          resultEl.className = 'error';
          resultEl.innerHTML = '<div class="summary">Error</div>' + escapeHtml(data.error || 'Unknown error') +
            (data.logTail ? '<pre id="log">' + escapeHtml(data.logTail.slice(-800)) + '</pre>' : '');
        }
      } catch (err) {
        resultEl.className = 'error';
        resultEl.innerHTML = '<div class="summary">Error</div>' + escapeHtml(err.message);
      }
      runBtn.disabled = false;
    };
  </script>
</body>
</html>
`;

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

  let inputPath = DEFAULT_INPUT;
  if (req.file && req.file.path) {
    const dest = path.join(UPLOAD_DIR, "input.csv");
    fs.renameSync(req.file.path, dest);
    inputPath = dest;
  }
  if (!fs.existsSync(inputPath)) {
    return res.status(400).json({ ok: false, error: "No input CSV. Upload a file or add raw_tam.csv to the server." });
  }

  const primary = parseKeywords(req.body.primary);
  const secondary = parseKeywords(req.body.secondary);
  const negative = parseKeywords(req.body.negative);
  const hasKeywords = primary.length > 0 || secondary.length > 0;
  const runConfigPath = path.join(UPLOAD_DIR, "config_run.yaml");
  if (hasKeywords) {
    fs.writeFileSync(runConfigPath, buildConfigYaml(primary, secondary, negative), "utf-8");
  } else if (fs.existsSync(runConfigPath)) {
    try { fs.unlinkSync(runConfigPath); } catch (_) {}
  }
  const configToUse = hasKeywords ? runConfigPath : (fs.existsSync(DEFAULT_CONFIG) ? DEFAULT_CONFIG : null);
  const apiKey = (req.body.apiKey || "").trim();

  try {
    const { stdout, stderr } = await runPipeline({
      inputPath,
      configPath: configToUse,
      apiKey,
    });
    const summary = getSummaryFromCsv(OUTPUT_CSV);
    lastSummary = summary;
    res.json({
      ok: true,
      summary: summary || { total: 0, pass: 0, fail: 0, review: 0 },
      logTail: (stdout + "\n" + stderr).slice(-1500),
    });
  } catch (err) {
    lastSummary = null;
    res.status(500).json({
      ok: false,
      error: err.message || String(err),
      logTail: (err.stdout || "") + (err.stderr || "").slice(-1500),
    });
  }
});

// Backward compat: GET /run still works (uses defaults)
app.get("/run", async (req, res) => {
  if (!fs.existsSync(SCRIPT_PATH)) {
    return res.status(500).json({ ok: false, error: "pre_filter.py not found" });
  }
  if (!fs.existsSync(DEFAULT_INPUT)) {
    return res.status(400).json({ ok: false, error: "No input CSV. Upload one via the form or add raw_tam.csv." });
  }
  try {
    const { stdout, stderr } = await runPipeline({});
    const summary = getSummaryFromCsv(OUTPUT_CSV);
    lastSummary = summary;
    res.json({
      ok: true,
      summary: summary || { total: 0, pass: 0, fail: 0, review: 0 },
      logTail: (stdout + "\n" + stderr).slice(-1500),
    });
  } catch (err) {
    lastSummary = null;
    res.status(500).json({
      ok: false,
      error: err.message || String(err),
      logTail: (err.stdout || "") + (err.stderr || "").slice(-1500),
    });
  }
});

app.get("/output", (req, res) => {
  if (!fs.existsSync(OUTPUT_CSV)) {
    return res.status(404).json({ error: "No output yet. Run the pipeline first." });
  }
  res.type("text/csv").attachment("filtered_output.csv").send(fs.readFileSync(OUTPUT_CSV));
});

app.listen(PORT, () => {
  console.log(`Pipeline: http://localhost:${PORT}`);
});
