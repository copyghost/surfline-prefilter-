const { describe, test } = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const {
  loadConfig,
  normalizeDomain,
  matchKeywords,
  classifyRow,
  cleanCompanyName,
  extractCompanyNameFromHtml,
  checkParked,
  runPipeline,
  configFromKeywords,
} = require("../lib/pipeline");
const { parseCsv, stringifyCsv, splitCsvChunks } = require("../lib/csv");

describe("csv", () => {
  test("round-trips commas, quotes, and newlines", () => {
    const rows = [
      ["company_name", "domain"],
      ['Acme, "North"', "acme.com"],
      ["Line\nBreak", "example.com"],
    ];
    const parsed = parseCsv(stringifyCsv(rows));
    assert.deepEqual(parsed, rows);
  });

  test("splits chunks without breaking quoted newlines", () => {
    const text = stringifyCsv([
      ["company_name", "domain"],
      ["One", "one.example"],
      ["Two\nLines", "two.example"],
      ["Three", "three.example"],
    ]);
    const chunks = splitCsvChunks(text, 2);
    assert.equal(chunks.length, 2);
    assert.equal(parseCsv(chunks[0]).length, 3);
    assert.equal(parseCsv(chunks[1]).length, 2);
    assert.equal(parseCsv(chunks[1])[1][0], "Three");
  });
});

describe("pipeline rules", () => {
  const config = configFromKeywords(["septic"], ["tank", "drain"], ["restaurant"]);

  test("loads keywords from config.yaml", () => {
    const loaded = loadConfig(path.join(__dirname, "..", "config.yaml"));
    assert.ok(loaded.industry_keywords.primary.includes("septic"));
    assert.ok(loaded.industry_keywords.negative.includes("restaurant"));
  });

  test("normalizes domains and drops social links", () => {
    assert.equal(normalizeDomain("https://www.Example.com/path?q=1"), "https://example.com");
    assert.equal(normalizeDomain("septic"), "https://septic.com");
    assert.equal(normalizeDomain("https://facebook.com/acme"), null);
    assert.equal(normalizeDomain(""), null);
  });

  test("matches primary, secondary, and negative keywords", () => {
    assert.equal(matchKeywords("We do septic pumping", config)[0], "match");
    assert.equal(matchKeywords("tank and drain service", config)[0], "match");
    assert.equal(matchKeywords("just a tank", config)[0], "weak_match");
    assert.equal(matchKeywords("a restaurant with a septic tank", config)[0], "negative_match");
    assert.equal(matchKeywords("unrelated bakery", config)[0], "no_match");
  });

  test("classifies pass, fail, and review", () => {
    assert.deepEqual(classifyRow({ domain_status: "dead", industry_match: "domain_dead" }), ["FAIL", "Domain: dead"]);
    assert.deepEqual(classifyRow({ domain_status: "live", industry_match: "match", matched_keywords: "septic" }), ["PASS", "Keywords: septic"]);
    assert.deepEqual(classifyRow({ domain_status: "live", industry_match: "weak_match", matched_keywords: "tank" }), ["REVIEW", "Weak match: tank"]);
    assert.equal(classifyRow({ domain_status: "parked", industry_match: "parked_domain" })[0], "FAIL");
  });

  test("reads the company name from the title", () => {
    const html = "<html><head><title>ABC Septic Services | Trusted Pumping</title></head><body>hello</body></html>";
    const [name, source] = extractCompanyNameFromHtml(html, "Fallback LLC");
    assert.equal(source, "title_tag");
    assert.equal(name, "ABC Septic Services");
    assert.equal(cleanCompanyName("ACME SEPTIC LLC"), "Acme Septic");
  });

  test("treats short pages as parked", () => {
    assert.equal(checkParked("hi"), true);
    assert.equal(checkParked("this domain is for sale ".padEnd(120, "x")), true);
    assert.equal(checkParked("x".repeat(120)), false);
  });
});

describe("runPipeline", () => {
  test("fails dead domains without scraping them", async () => {
    const csvText = "company_name,domain\nNope,not-a-real-domain.invalid\nAlso,another-fake-domain.invalid\n";
    const result = await runPipeline({
      csvText,
      config: configFromKeywords(["septic"], ["tank", "drain"], ["restaurant"]),
      apiKey: "",
    });
    assert.equal(result.summary.total, 2);
    assert.equal(result.summary.fail, 2);
    assert.match(result.csv, /FAIL/);
    assert.match(result.csv, /Domain: dead/);
    assert.doesNotMatch(result.rows[0].scraped_text || "", /HOMEPAGE/);
  });
});
