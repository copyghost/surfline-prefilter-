"use client";

import { ChangeEvent, useMemo, useState } from "react";

type CsvRow = Record<string, string>;
type OutputRow = Record<string, string | boolean | number>;

type AnalyzeResult = OutputRow & {
  active: boolean;
  qualified: boolean;
  blocked: boolean;
  needsManualReview: boolean;
  consentDetected: boolean;
};

const API_BATCH_SIZE = 25;
const MAX_PREVIEW_ROWS = 500;

export default function Home() {
  const [rows, setRows] = useState<CsvRow[]>([]);
  const [headers, setHeaders] = useState<string[]>([]);
  const [includeKeywords, setIncludeKeywords] = useState("");
  const [excludeKeywords, setExcludeKeywords] = useState("");
  const [renderMode, setRenderMode] = useState<"auto" | "html" | "js">("auto");
  const [results, setResults] = useState<AnalyzeResult[]>([]);
  const [fileName, setFileName] = useState("");
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  const [isAnalyzing, setIsAnalyzing] = useState(false);
  const [processedRows, setProcessedRows] = useState(0);
  const [failedBatches, setFailedBatches] = useState(0);

  const domainCount = rows.filter((row) => row.domain?.trim()).length;
  const qualifiedCount = results.filter((result) => result.qualified).length;
  const activeCount = results.filter((result) => result.active).length;
  const reviewCount = results.filter((result) => result.needsManualReview).length;

  const canAnalyze = domainCount > 0 && !isAnalyzing;
  const progressPercent = rows.length > 0 ? Math.round((processedRows / rows.length) * 100) : 0;
  const displayedResults = results.slice(0, MAX_PREVIEW_ROWS);

  const outputHeaders = useMemo(() => {
    if (results.length === 0) return [];
    const resultKeys = Object.keys(results[0]);
    return [...headers, ...resultKeys.filter((key) => !headers.includes(key))];
  }, [headers, results]);

  async function handleFileChange(event: ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0];
    setError("");
    setNotice("");
    setResults([]);
    setProcessedRows(0);
    setFailedBatches(0);

    if (!file) return;

    const text = await file.text();
    const parsed = parseCsv(text);

    const domainHeader = parsed.headers.find((header) => header.trim().toLowerCase() === "domain");

    if (!domainHeader) {
      setRows([]);
      setHeaders([]);
      setFileName("");
      setError("The CSV needs a column named domain.");
      return;
    }

    const loadedRows = parsed.rows.map((row) => ({
      ...row,
      domain: row[domainHeader],
    }));
    setRows(loadedRows);
    setHeaders(parsed.headers);
    setFileName(file.name);

    if (loadedRows.length > 5000) {
      setNotice(`Loaded ${loadedRows.length.toLocaleString()} rows. Large runs process in batches; keep this tab open.`);
    }
  }

  async function analyzeRows() {
    setIsAnalyzing(true);
    setError("");
    setNotice("");
    setResults([]);
    setProcessedRows(0);
    setFailedBatches(0);

    try {
      const allResults: AnalyzeResult[] = [];
      let jsRenderingConfigured = renderMode === "html";
      let sawRenderMeta = false;
      let batchFailures = 0;

      for (let start = 0; start < rows.length; start += API_BATCH_SIZE) {
        const batchRows = rows.slice(start, start + API_BATCH_SIZE);

        try {
          const response = await fetch("/api/analyze", {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              rows: batchRows.map((row, batchIndex) => ({
                domain: row.domain,
                original: row,
                rowNumber: start + batchIndex + 1,
              })),
              includeKeywords: keywordList(includeKeywords),
              excludeKeywords: keywordList(excludeKeywords),
              renderMode,
            }),
          });

          const payload = (await response.json()) as {
            results?: AnalyzeResult[];
            error?: string;
            meta?: { jsRenderingConfigured?: boolean; renderMode?: string };
          };

          if (!response.ok || !payload.results) {
            throw new Error(payload.error || "Analysis failed.");
          }

          allResults.push(...payload.results);
          if (typeof payload.meta?.jsRenderingConfigured === "boolean") {
            sawRenderMeta = true;
            jsRenderingConfigured = jsRenderingConfigured || payload.meta.jsRenderingConfigured;
          }
        } catch (batchError) {
          batchFailures += 1;
          allResults.push(
            ...batchRows.map((row, batchIndex) =>
              failedRow(row, start + batchIndex + 1, batchError instanceof Error ? batchError.message : "Batch failed."),
            ),
          );
        }

        setFailedBatches(batchFailures);
        setProcessedRows(Math.min(start + batchRows.length, rows.length));
        setResults([...allResults]);
      }

      if (renderMode !== "html" && sawRenderMeta && !jsRenderingConfigured) {
        setNotice("JS rendering needs Chrome or Chromium installed on the computer running this server.");
      } else if (batchFailures > 0) {
        setNotice(`${batchFailures} batch${batchFailures === 1 ? "" : "es"} failed and were marked for review.`);
      } else {
        setNotice(`Finished ${rows.length.toLocaleString()} rows. Download the CSV for the full output.`);
      }
    } catch (analysisError) {
      setError(analysisError instanceof Error ? analysisError.message : "Analysis failed.");
    } finally {
      setIsAnalyzing(false);
    }
  }

  function downloadCsv() {
    if (results.length === 0) return;

    const csv = toCsv(results, outputHeaders);
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = "surfline-lead-checker-output.csv";
    link.click();
    URL.revokeObjectURL(url);
  }

  return (
    <main className="min-h-screen">
      <section className="border-b border-[var(--line)] bg-[var(--panel)]">
        <div className="mx-auto flex max-w-7xl flex-col gap-5 px-5 py-8 md:flex-row md:items-end md:justify-between">
          <div className="max-w-3xl">
            <p className="text-sm font-semibold uppercase tracking-[0.18em] text-[var(--accent)]">
              Surfline Capital
            </p>
            <h1 className="mt-2 text-3xl font-semibold md:text-5xl">Lead list domain checker</h1>
            <p className="mt-3 max-w-2xl text-base leading-7 text-[var(--muted)]">
              Upload a domain CSV, scrape each homepage, flag keywords, detect blocks and consent banners, then
              export the enriched list. Large files run in batches.
            </p>
          </div>
          <div className="grid grid-cols-2 gap-3 text-center sm:grid-cols-4">
            <Metric label="Rows" value={rows.length} />
            <Metric label="Active" value={activeCount} />
            <Metric label="Qualified" value={qualifiedCount} />
            <Metric label="Review" value={reviewCount} />
          </div>
        </div>
      </section>

      <section className="mx-auto grid max-w-7xl gap-6 px-5 py-6 lg:grid-cols-[390px_1fr]">
        <div className="space-y-4">
          <div className="rounded-lg border border-[var(--line)] bg-[var(--panel)] p-4">
            <label className="block text-sm font-semibold" htmlFor="lead-csv">
              CSV file
            </label>
            <input
              id="lead-csv"
              type="file"
              accept=".csv,text/csv"
              onChange={handleFileChange}
              className="mt-3 w-full rounded-md border border-[var(--line)] bg-white px-3 py-2 text-sm"
            />
            {fileName ? <p className="mt-2 text-sm text-[var(--muted)]">{fileName}</p> : null}
          </div>

          {rows.length > 0 ? (
            <div className="rounded-lg border border-[var(--line)] bg-[var(--panel)] p-4">
              <div className="flex items-center justify-between text-sm">
                <span className="font-semibold">Batch progress</span>
                <span className="font-mono text-[var(--muted)]">
                  {processedRows.toLocaleString()} / {rows.length.toLocaleString()}
                </span>
              </div>
              <div className="mt-3 h-2 overflow-hidden rounded-full bg-stone-200">
                <div
                  className="h-full bg-[var(--accent)] transition-all"
                  style={{ width: `${progressPercent}%` }}
                />
              </div>
              <p className="mt-2 text-xs text-[var(--muted)]">
                Batches of {API_BATCH_SIZE} domains. Keep this tab open until the run finishes.
              </p>
              {failedBatches > 0 ? (
                <p className="mt-2 text-xs text-[var(--warning)]">
                  {failedBatches} batch{failedBatches === 1 ? "" : "es"} failed.
                </p>
              ) : null}
            </div>
          ) : null}

          <div className="rounded-lg border border-[var(--line)] bg-[var(--panel)] p-4">
            <label className="block text-sm font-semibold" htmlFor="render-mode">
              Rendering
            </label>
            <p className="mt-1 text-xs leading-5 text-[var(--muted)]">
              JavaScript rendering uses Chrome installed on this computer. No Browserless account is required.
            </p>
            <select
              id="render-mode"
              value={renderMode}
              onChange={(event) => setRenderMode(event.target.value as "auto" | "html" | "js")}
              className="mt-3 h-10 w-full rounded-md border border-[var(--line)] bg-white px-3 text-sm"
            >
              <option value="auto">Auto: HTML, then JS if thin</option>
              <option value="html">HTML only</option>
              <option value="js">JS render first</option>
            </select>
          </div>

          <div className="rounded-lg border border-[var(--line)] bg-[var(--panel)] p-4">
            <label className="block text-sm font-semibold" htmlFor="include-keywords">
              Include keywords
            </label>
            <p className="mt-1 text-xs leading-5 text-[var(--muted)]">
              Similar words match, so pumping matches pumps and sepic matches septic. Use * or % as a wildcard, such as pump*.
            </p>
            <textarea
              id="include-keywords"
              value={includeKeywords}
              onChange={(event) => setIncludeKeywords(event.target.value)}
              rows={5}
              placeholder="commercial real estate, private equity, M&A"
              className="mt-3 w-full resize-none rounded-md border border-[var(--line)] bg-white px-3 py-2 text-sm leading-6"
            />
          </div>

          <div className="rounded-lg border border-[var(--line)] bg-[var(--panel)] p-4">
            <label className="block text-sm font-semibold" htmlFor="exclude-keywords">
              Exclude keywords
            </label>
            <p className="mt-1 text-xs leading-5 text-[var(--muted)]">
              Same similar-word and wildcard rules. Any match disqualifies the domain.
            </p>
            <textarea
              id="exclude-keywords"
              value={excludeKeywords}
              onChange={(event) => setExcludeKeywords(event.target.value)}
              rows={5}
              placeholder="careers, franchise, nonprofit"
              className="mt-3 w-full resize-none rounded-md border border-[var(--line)] bg-white px-3 py-2 text-sm leading-6"
            />
          </div>

          {error ? (
            <div className="rounded-md border border-amber-300 bg-amber-50 px-3 py-2 text-sm text-[var(--warning)]">
              {error}
            </div>
          ) : null}
          {notice ? (
            <div className="rounded-md border border-sky-300 bg-sky-50 px-3 py-2 text-sm text-sky-800">
              {notice}
            </div>
          ) : null}

          <div className="flex gap-3">
            <button
              type="button"
              disabled={!canAnalyze}
              onClick={analyzeRows}
              className="h-11 flex-1 rounded-md bg-[var(--accent)] px-4 text-sm font-semibold text-white hover:bg-[var(--accent-strong)]"
            >
              {isAnalyzing ? "Analyzing..." : "Analyze domains"}
            </button>
            <button
              type="button"
              disabled={results.length === 0}
              onClick={downloadCsv}
              className="h-11 rounded-md border border-[var(--line)] bg-white px-4 text-sm font-semibold hover:bg-stone-50"
            >
              Download CSV
            </button>
          </div>
        </div>

        <div className="overflow-hidden rounded-lg border border-[var(--line)] bg-[var(--panel)]">
          <div className="flex items-center justify-between border-b border-[var(--line)] px-4 py-3">
            <h2 className="text-base font-semibold">Results</h2>
            <p className="text-sm text-[var(--muted)]">
              {(results.length || rows.length).toLocaleString()} rows
            </p>
          </div>
          {results.length > 0 ? (
            <div className="overflow-x-auto">
              {results.length > MAX_PREVIEW_ROWS ? (
                <div className="border-b border-[var(--line)] bg-sky-50 px-4 py-2 text-sm text-sky-800">
                  Previewing first {MAX_PREVIEW_ROWS.toLocaleString()} rows. Download CSV for all{" "}
                  {results.length.toLocaleString()} results.
                </div>
              ) : null}
              <table className="w-full min-w-[1320px] border-collapse text-left text-sm">
                <thead className="bg-stone-100 text-xs uppercase tracking-wide text-[var(--muted)]">
                  <tr>
                    <th className="px-3 py-3">Domain</th>
                    <th className="px-3 py-3">Active</th>
                    <th className="px-3 py-3">Qualified</th>
                    <th className="px-3 py-3">Review</th>
                    <th className="px-3 py-3">Mode</th>
                    <th className="px-3 py-3">Text</th>
                    <th className="px-3 py-3">Block</th>
                    <th className="px-3 py-3">Consent</th>
                    <th className="px-3 py-3">Include</th>
                    <th className="px-3 py-3">Exclude</th>
                    <th className="px-3 py-3">Title</th>
                    <th className="px-3 py-3">Snippet</th>
                  </tr>
                </thead>
                <tbody>
                  {displayedResults.map((result, index) => (
                    <tr key={`${result.domain}-${index}`} className="border-t border-[var(--line)]">
                      <td className="max-w-[180px] px-3 py-3 font-medium">{result.domain}</td>
                      <td className="px-3 py-3">
                        <StatusPill active={Boolean(result.active)} trueLabel="Active" falseLabel="Inactive" />
                      </td>
                      <td className="px-3 py-3">
                        <StatusPill active={Boolean(result.qualified)} trueLabel="Yes" falseLabel="No" />
                      </td>
                      <td className="px-3 py-3">
                        <StatusPill active={!Boolean(result.needsManualReview)} trueLabel="Clean" falseLabel="Review" />
                      </td>
                      <td className="px-3 py-3">{result.renderMode || "-"}</td>
                      <td className="px-3 py-3 font-mono">{result.textLength || 0}</td>
                      <td className="max-w-[160px] px-3 py-3">
                        {result.blocked ? result.blockedReason || "blocked" : "-"}
                      </td>
                      <td className="max-w-[160px] px-3 py-3">
                        {result.consentDetected ? result.consentAction || "detected" : "-"}
                      </td>
                      <td className="max-w-[180px] px-3 py-3">{result.includeMatched || "-"}</td>
                      <td className="max-w-[180px] px-3 py-3">{result.excludeMatched || "-"}</td>
                      <td className="max-w-[240px] px-3 py-3">{result.pageTitle || "-"}</td>
                      <td className="max-w-[360px] px-3 py-3 text-[var(--muted)]">{result.textSnippet || result.error || "-"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="flex min-h-[420px] items-center justify-center px-6 text-center text-[var(--muted)]">
              <div>
                <p className="font-medium text-foreground">Upload a CSV to begin.</p>
                <p className="mt-2 text-sm">The file only needs a domain column. Extra columns stay with the output.</p>
              </div>
            </div>
          )}
        </div>
      </section>
    </main>
  );
}

function Metric({ label, value }: { label: string; value: number }) {
  return (
    <div className="min-w-24 rounded-lg border border-[var(--line)] bg-stone-50 px-4 py-3">
      <div className="font-mono text-2xl font-semibold">{value}</div>
      <div className="text-xs uppercase tracking-wide text-[var(--muted)]">{label}</div>
    </div>
  );
}

function StatusPill({
  active,
  trueLabel,
  falseLabel,
}: {
  active: boolean;
  trueLabel: string;
  falseLabel: string;
}) {
  return (
    <span
      className={`inline-flex min-w-20 justify-center rounded-full px-2.5 py-1 text-xs font-semibold ${
        active ? "bg-emerald-100 text-emerald-800" : "bg-stone-200 text-stone-700"
      }`}
    >
      {active ? trueLabel : falseLabel}
    </span>
  );
}

function keywordList(value: string) {
  return value
    .split(/[\n,]/)
    .map((keyword) => keyword.trim())
    .filter(Boolean);
}

function failedRow(row: CsvRow, rowNumber: number, error: string): AnalyzeResult {
  return {
    ...row,
    rowNumber,
    normalizedDomain: row.domain,
    active: false,
    status: "batch_failed",
    analysisStatus: "batch_failed",
    resolvedUrl: "",
    renderMode: "none",
    renderAttempted: false,
    jsRenderConfigured: false,
    jsRenderUsed: false,
    textLength: 0,
    pageTitle: "",
    includeMatched: "",
    excludeMatched: "",
    includeFlag: false,
    excludeFlag: false,
    qualified: false,
    blocked: false,
    blockedReason: "",
    consentDetected: false,
    consentAction: "none",
    needsManualReview: true,
    textSnippet: "",
    error,
  };
}

function parseCsv(text: string) {
  const rows: string[][] = [];
  let current = "";
  let row: string[] = [];
  let inQuotes = false;

  for (let index = 0; index < text.length; index += 1) {
    const char = text[index];
    const next = text[index + 1];

    if (char === '"' && inQuotes && next === '"') {
      current += '"';
      index += 1;
      continue;
    }

    if (char === '"') {
      inQuotes = !inQuotes;
      continue;
    }

    if (char === "," && !inQuotes) {
      row.push(current);
      current = "";
      continue;
    }

    if ((char === "\n" || char === "\r") && !inQuotes) {
      if (char === "\r" && next === "\n") index += 1;
      row.push(current);
      rows.push(row);
      row = [];
      current = "";
      continue;
    }

    current += char;
  }

  if (current || row.length > 0) {
    row.push(current);
    rows.push(row);
  }

  const [headerRow = [], ...dataRows] = rows.filter((csvRow) => csvRow.some((cell) => cell.trim()));
  const headers = headerRow.map((header) => header.trim());

  return {
    headers,
    rows: dataRows.map((dataRow) =>
      headers.reduce<CsvRow>((record, header, index) => {
        record[header] = dataRow[index]?.trim() ?? "";
        return record;
      }, {}),
    ),
  };
}

function toCsv(rows: OutputRow[], headers: string[]) {
  return [
    headers.map(escapeCsvCell).join(","),
    ...rows.map((row) => headers.map((header) => escapeCsvCell(String(row[header] ?? ""))).join(",")),
  ].join("\n");
}

function escapeCsvCell(value: string) {
  if (/[",\n\r]/.test(value)) {
    return `"${value.replace(/"/g, '""')}"`;
  }

  return value;
}
