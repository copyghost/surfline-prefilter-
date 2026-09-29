/**
 * Small RFC-style CSV parse/stringify shared by the server and the browser.
 * Classic script: function declarations stay global when injected into the page.
 */
function parseCsv(text) {
  if (text == null) return [];
  let s = String(text);
  if (s.charCodeAt(0) === 0xfeff) s = s.slice(1);
  s = s.replace(/\r\n/g, "\n").replace(/\r/g, "\n");

  const rows = [];
  let row = [];
  let field = "";
  let inQuotes = false;

  for (let i = 0; i < s.length; i++) {
    const c = s[i];
    if (inQuotes) {
      if (c === '"') {
        if (s[i + 1] === '"') {
          field += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        field += c;
      }
      continue;
    }
    if (c === '"') {
      inQuotes = true;
    } else if (c === ",") {
      row.push(field);
      field = "";
    } else if (c === "\n") {
      row.push(field);
      field = "";
      if (!(row.length === 1 && row[0] === "")) rows.push(row);
      row = [];
    } else {
      field += c;
    }
  }

  if (inQuotes) {
    // Unbalanced quote: keep the remainder as a field rather than dropping the row.
    row.push(field);
    if (!(row.length === 1 && row[0] === "")) rows.push(row);
  } else if (field.length || row.length) {
    row.push(field);
    if (!(row.length === 1 && row[0] === "")) rows.push(row);
  }

  return rows;
}

function stringifyCsv(rows) {
  return rows.map((row) => row.map(escapeCsvField).join(",")).join("\n") + (rows.length ? "\n" : "");
}

function escapeCsvField(value) {
  const s = value == null ? "" : String(value);
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function rowsToObjects(rows) {
  if (!rows.length) return { columns: [], records: [] };
  const columns = rows[0].map((c) => String(c).trim());
  const records = [];
  for (const row of rows.slice(1)) {
    if (!row.some((cell) => String(cell).trim() !== "")) continue;
    const record = {};
    columns.forEach((col, i) => {
      record[col] = row[i] != null ? String(row[i]) : "";
    });
    records.push(record);
  }
  return { columns, records };
}

function objectsToCsv(columns, records) {
  const rows = [
    columns,
    ...records.map((record) => columns.map((col) => (record[col] == null ? "" : record[col]))),
  ];
  return stringifyCsv(rows);
}

function splitCsvChunks(text, size) {
  const rows = parseCsv(text);
  if (!rows.length) return [];
  const header = rows[0];
  const data = rows.slice(1).filter((row) => row.some((cell) => String(cell).trim() !== ""));
  if (!data.length) return [stringifyCsv([header])];
  const n = Math.max(1, Number(size) || 1);
  const chunks = [];
  for (let i = 0; i < data.length; i += n) {
    chunks.push(stringifyCsv([header, ...data.slice(i, i + n)]));
  }
  return chunks;
}

if (typeof module !== "undefined" && module.exports) {
  module.exports = {
    parseCsv,
    stringifyCsv,
    rowsToObjects,
    objectsToCsv,
    splitCsvChunks,
  };
}
