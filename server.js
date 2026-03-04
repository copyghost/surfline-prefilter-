/**
 * Single-URL pipeline API: GET /run runs the pre_filter pipeline and returns the summary.
 * GET /output returns the last filtered_output.csv. GET / returns a short info page.
 *
 * Deploy to Railway, Render, or any Node host. Requires Python 3 + requests, pyyaml, tqdm.
 */
const express = require("express");
const { exec } = require("node:child_process");
const { promisify } = require("node:util");
const path = require("node:path");
const fs = require("node:fs");

const execAsync = promisify(exec);
const app = express();
const PORT = process.env.PORT || 3000;

const PROJECT_ROOT = path.resolve(__dirname);
const INPUT_CSV = process.env.INPUT_CSV || path.join(PROJECT_ROOT, "raw_tam.csv");
const OUTPUT_CSV = process.env.OUTPUT_CSV || path.join(PROJECT_ROOT, "filtered_output.csv");
const CONFIG_PATH = process.env.CONFIG_PATH || path.join(PROJECT_ROOT, "config.yaml");
const SCRIPT_PATH = path.join(PROJECT_ROOT, "pre_filter.py");

// Last run summary (so /output and / can show it)
let lastSummary = null;

function runPipeline() {
  const configArg = fs.existsSync(CONFIG_PATH) ? `--config "${CONFIG_PATH}"` : "--layer1-only";
  const cmd = `python3 "${SCRIPT_PATH}" --input "${INPUT_CSV}" --output "${OUTPUT_CSV}" ${configArg}`;
  return execAsync(cmd, {
    cwd: PROJECT_ROOT,
    maxBuffer: 10 * 1024 * 1024,
  });
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

app.get("/", (req, res) => {
  res.type("text/html").send(`
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Pre-filter Pipeline</title>
  <style>
    * { box-sizing: border-box; }
    body { font-family: system-ui, sans-serif; max-width: 480px; margin: 2rem auto; padding: 0 1rem; }
    h1 { font-size: 1.5rem; margin-bottom: 0.5rem; }
    p { color: #555; margin: 0.5rem 0; }
    button {
      font-size: 1rem; padding: 0.6rem 1.2rem; margin: 1rem 0;
      background: #0d6efd; color: #fff; border: none; border-radius: 6px; cursor: pointer;
    }
    button:hover { background: #0b5ed7; }
    button:disabled { opacity: 0.6; cursor: not-allowed; }
    #result { margin-top: 1rem; padding: 1rem; border-radius: 8px; }
    #result.success { background: #d1e7dd; }
    #result.error { background: #f8d7da; }
    #result .summary { font-size: 1.1rem; font-weight: 600; margin-bottom: 0.5rem; }
    #result a { color: #0d6efd; }
    #log { font-size: 0.85rem; margin-top: 0.5rem; white-space: pre-wrap; max-height: 200px; overflow-y: auto; }
  </style>
</head>
<body>
  <h1>Pre-filter Pipeline</h1>
  <p>Run the domain + keyword filter on your input CSV.</p>
  <button type="button" id="runBtn">Run pipeline</button>
  <div id="result"></div>

  <script>
    const runBtn = document.getElementById('runBtn');
    const resultEl = document.getElementById('result');
    runBtn.onclick = async function() {
      runBtn.disabled = true;
      resultEl.innerHTML = 'Running…';
      resultEl.className = '';
      try {
        const r = await fetch('/run');
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
      } catch (e) {
        resultEl.className = 'error';
        resultEl.innerHTML = '<div class="summary">Error</div>' + escapeHtml(e.message);
      }
      runBtn.disabled = false;
    };
    function escapeHtml(s) { return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }
  </script>
</body>
</html>`);
});

app.get("/run", async (req, res) => {
  if (!fs.existsSync(SCRIPT_PATH)) {
    return res.status(500).json({ ok: false, error: "pre_filter.py not found" });
  }
  if (!fs.existsSync(INPUT_CSV)) {
    return res.status(400).json({ ok: false, error: "Input CSV not found. Set INPUT_CSV or add raw_tam.csv" });
  }

  try {
    const { stdout, stderr } = await runPipeline();
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
    return res.status(404).json({ error: "No output yet. Run GET /run first." });
  }
  res.type("text/csv").attachment("filtered_output.csv").send(fs.readFileSync(OUTPUT_CSV));
});

app.listen(PORT, () => {
  console.log(`Pipeline API: http://localhost:${PORT} — GET /run to run, GET /output to download`);
});
