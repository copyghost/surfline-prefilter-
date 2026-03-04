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
<head><meta charset="utf-8"><title>Pre-filter Pipeline</title></head>
<body>
  <h1>Pre-filter Pipeline</h1>
  <p>Run the pipeline: <a href="/run">GET /run</a></p>
  <p>Download last result: <a href="/output">GET /output</a> (CSV)</p>
  ${lastSummary ? `<p>Last run: ${lastSummary.pass} pass, ${lastSummary.fail} fail, ${lastSummary.review} review (${lastSummary.total} total)</p>` : ""}
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
