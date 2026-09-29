/**
 * In-process port of pre_filter.py so the Vercel Node function does not
 * need a Python runtime. Decision rules match the Python pipeline.
 */
const fs = require("node:fs");
const yaml = require("js-yaml");
const { parseCsv, rowsToObjects, objectsToCsv } = require("./csv");

const PARKED_DOMAIN_INDICATORS = [
  "this domain is for sale",
  "domain is parked",
  "buy this domain",
  "parked by",
  "godaddy",
  "sedo.com",
  "hugedomains",
  "dan.com",
  "afternic",
  "domain for sale",
  "this website is for sale",
  "under construction",
  "coming soon",
  "site not found",
  "page not found",
  "default web page",
  "apache2 ubuntu default page",
  "welcome to nginx",
  "403 forbidden",
  "account suspended",
  "this account has been suspended",
  "website expired",
  "hosting expired",
];

const SOCIAL_MEDIA_DOMAINS = [
  "facebook.com",
  "instagram.com",
  "twitter.com",
  "x.com",
  "linkedin.com",
  "tiktok.com",
  "youtube.com",
  "pinterest.com",
  "yelp.com",
  "bbb.org",
  "google.com/maps",
  "maps.google.com",
];

const DEFAULT_SCRAPE_PATHS = ["/services", "/services/", "/about", "/about-us", "/what-we-do"];

const FILTER_COL_NAMES = [
  "domain_status",
  "http_code",
  "redirect_url",
  "is_parked",
  "error",
  "industry_match",
  "matched_keywords",
  "negative_keywords",
  "homepage_snippet",
  "resolved_name",
  "name_source",
  "pages_scraped",
  "scraped_text",
  "filter_decision",
  "filter_reason",
];

const OUTPUT_FILTER_COLS = [
  "domain_status",
  "http_code",
  "redirect_url",
  "industry_match",
  "matched_keywords",
  "negative_keywords",
  "resolved_name",
  "name_source",
  "pages_scraped",
  "scraped_text",
  "homepage_snippet",
  "filter_decision",
  "filter_reason",
];

const DEFAULT_CONFIG = {
  industry_keywords: {
    primary: [],
    secondary: [],
    negative: [],
  },
  keyword_match_rules: {
    primary_threshold: 1,
    secondary_threshold: 2,
    negative_override: true,
  },
  concurrency: {
    layer1_workers: 20,
    layer2_workers: 10,
    request_timeout: 10,
    layer2_delay: 0.1,
  },
  zenrows: {
    js_render: false,
    premium_proxy: false,
    autoparse: false,
  },
  scrape_paths: [],
  scrape_max_subpages: 3,
};

const BROWSER_UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

function cloneConfig(config) {
  return {
    industry_keywords: {
      primary: [...(config.industry_keywords?.primary || [])],
      secondary: [...(config.industry_keywords?.secondary || [])],
      negative: [...(config.industry_keywords?.negative || [])],
    },
    keyword_match_rules: { ...DEFAULT_CONFIG.keyword_match_rules, ...(config.keyword_match_rules || {}) },
    concurrency: { ...DEFAULT_CONFIG.concurrency, ...(config.concurrency || {}) },
    zenrows: { ...DEFAULT_CONFIG.zenrows, ...(config.zenrows || {}) },
    scrape_paths: [...(config.scrape_paths || [])],
    scrape_max_subpages: config.scrape_max_subpages ?? DEFAULT_CONFIG.scrape_max_subpages,
  };
}

function loadConfig(configPath) {
  const config = cloneConfig(DEFAULT_CONFIG);
  if (configPath && fs.existsSync(configPath)) {
    const userConfig = yaml.load(fs.readFileSync(configPath, "utf8")) || {};
    for (const key of Object.keys(userConfig)) {
      const value = userConfig[key];
      if (value && typeof value === "object" && !Array.isArray(value) && config[key] && typeof config[key] === "object" && !Array.isArray(config[key])) {
        Object.assign(config[key], value);
      } else {
        config[key] = value;
      }
    }
  }
  return applyRuntimeLimits(config);
}

function applyRuntimeLimits(config) {
  const next = cloneConfig(config);
  if (process.env.VERCEL) {
    next.concurrency.request_timeout = Math.min(Number(next.concurrency.request_timeout) || 10, 8);
    next.concurrency.layer1_workers = Math.min(Number(next.concurrency.layer1_workers) || 8, 8);
    next.concurrency.layer2_workers = Math.min(Number(next.concurrency.layer2_workers) || 8, 8);
    next.scrape_max_subpages = Math.min(Number(next.scrape_max_subpages) || 3, 2);
  }
  return next;
}

function configFromKeywords(primary, secondary, negative, base) {
  const config = cloneConfig(base || DEFAULT_CONFIG);
  config.industry_keywords = {
    primary: primary.length ? primary : ["septic", "wastewater", "pumping"],
    secondary: secondary.length ? secondary : ["tank", "drain", "cleanout"],
    negative: negative.length ? negative : [],
  };
  return applyRuntimeLimits(config);
}

function normalizeDomain(domain) {
  if (!domain || typeof domain !== "string") return null;
  let value = domain.trim().toLowerCase();
  value = value.replace(/^https?:\/\//, "");
  value = value.replace(/^\/\/+/, "");
  value = value.replace(/[/?#].*$/, "");
  value = value.replace(/^www\./, "");
  value = value.trim();
  for (const social of SOCIAL_MEDIA_DOMAINS) {
    if (value.includes(social)) return null;
  }
  if (!value) return null;
  if (value.includes(".")) return `https://${value}`;
  if (value === "www" || value === "http" || value === "https") return null;
  if (/^[a-z0-9][-a-z0-9]{0,62}$/.test(value)) return `https://${value}.com`;
  return null;
}

function hostOf(url) {
  try {
    return new URL(url).hostname.replace(/^www\./, "").toLowerCase();
  } catch {
    return "";
  }
}

function isTimeoutError(err) {
  const name = err?.name || "";
  const code = err?.cause?.code || err?.code || "";
  return name === "TimeoutError" || name === "AbortError" || code === "ABORT_ERR" || /timeout/i.test(String(err?.message || ""));
}

function isSslError(err) {
  const code = String(err?.cause?.code || err?.code || "");
  const message = String(err?.message || "");
  return /CERT_|SSL|TLS|UNABLE_TO_VERIFY|DEPTH_ZERO|ERR_TLS/.test(code + " " + message);
}

async function fetchWithTimeout(url, options, timeoutMs) {
  return fetch(url, { ...options, signal: AbortSignal.timeout(timeoutMs) });
}

async function tryGet(url, timeoutMs) {
  try {
    const resp = await fetchWithTimeout(
      url,
      {
        method: "GET",
        redirect: "follow",
        headers: { "User-Agent": BROWSER_UA },
      },
      timeoutMs
    );
    try {
      await resp.body?.cancel();
    } catch {
      /* ignore */
    }
    return { status: resp.status, url: resp.url };
  } catch {
    return null;
  }
}

async function checkDomain(url, timeoutSec) {
  const result = {
    domain_status: "unknown",
    http_code: null,
    redirect_url: null,
    is_parked: false,
    error: null,
  };
  if (!url) {
    result.domain_status = "invalid_domain";
    return result;
  }

  const timeoutMs = Math.max(1, timeoutSec) * 1000;
  let headFailed = false;
  const headHeaders = { "User-Agent": "Mozilla/5.0 (compatible; SurflineBot/1.0)" };

  try {
    const resp = await fetchWithTimeout(url, { method: "HEAD", redirect: "follow", headers: headHeaders }, timeoutMs);
    result.http_code = resp.status;
    try {
      await resp.body?.cancel();
    } catch {
      /* ignore */
    }
    const finalHost = hostOf(resp.url);
    const originalHost = hostOf(url);
    if (finalHost && originalHost && finalHost !== originalHost) {
      if (SOCIAL_MEDIA_DOMAINS.some((social) => finalHost.includes(social))) {
        result.domain_status = "redirects_to_social";
        result.redirect_url = resp.url;
        return result;
      }
      result.redirect_url = resp.url;
    }
    if (resp.status === 200) result.domain_status = "live";
    else if ([301, 302, 303, 307, 308].includes(resp.status)) result.domain_status = "redirect";
    else if (resp.status === 403 || resp.status === 405) {
      result.domain_status = "needs_get";
      headFailed = true;
    } else if (resp.status === 404) result.domain_status = "not_found";
    else if (resp.status >= 500) result.domain_status = "server_error";
    else result.domain_status = `http_${resp.status}`;
  } catch (err) {
    if (isSslError(err)) {
      const httpUrl = url.replace(/^https:\/\//, "http://");
      try {
        const resp = await fetchWithTimeout(
          httpUrl,
          { method: "HEAD", redirect: "follow", headers: headHeaders },
          timeoutMs
        );
        result.http_code = resp.status;
        result.domain_status = resp.status === 200 ? "live_http_only" : `http_${resp.status}`;
        try {
          await resp.body?.cancel();
        } catch {
          /* ignore */
        }
      } catch {
        const got = await tryGet(httpUrl, timeoutMs);
        if (got && got.status === 200) {
          result.http_code = 200;
          result.domain_status = "live_http_only";
          result.redirect_url = got.url || result.redirect_url;
        } else {
          result.domain_status = "ssl_error";
          result.error = "SSL error and HTTP fallback failed";
        }
      }
    } else if (isTimeoutError(err)) {
      result.domain_status = "timeout";
      result.error = `No response within ${timeoutSec}s`;
      headFailed = true;
    } else {
      result.domain_status = "dead";
      result.error = "Connection refused or DNS failure";
      headFailed = true;
    }
  }

  if (headFailed && ["dead", "timeout", "error", "needs_get", "http_403", "http_405"].includes(result.domain_status)) {
    const got = await tryGet(url, timeoutMs);
    if (got && got.status === 200) {
      result.domain_status = "live";
      result.http_code = 200;
      result.error = null;
      if (got.url) result.redirect_url = got.url;
    } else if (got && [301, 302, 303, 307, 308].includes(got.status)) {
      result.domain_status = "redirect";
      result.http_code = got.status;
      result.error = null;
      if (got.url) result.redirect_url = got.url;
    }
    if (result.http_code == null && got && got.status) result.http_code = got.status;
  }

  return result;
}

async function mapPool(items, limit, fn, delayMs = 0) {
  const results = new Array(items.length);
  let cursor = 0;
  const workerCount = items.length === 0 ? 0 : Math.max(1, Math.min(limit || 1, items.length));

  async function worker() {
    while (cursor < items.length) {
      const index = cursor++;
      if (delayMs > 0 && index > 0) await new Promise((resolve) => setTimeout(resolve, delayMs));
      results[index] = await fn(items[index], index);
    }
  }

  await Promise.all(Array.from({ length: workerCount }, () => worker()));
  return results;
}

async function runLayer1(rows, config) {
  const timeout = config.concurrency.request_timeout;
  const workers = config.concurrency.layer1_workers;
  const results = await mapPool(rows, workers, async (row) => {
    const url = normalizeDomain(row.domain || row.website || "");
    row._normalized_url = url;
    try {
      return await checkDomain(url, timeout);
    } catch (err) {
      return {
        domain_status: "error",
        http_code: null,
        redirect_url: null,
        is_parked: false,
        error: String(err.message || err).slice(0, 200),
      };
    }
  });
  results.forEach((result, i) => Object.assign(rows[i], result));
  return rows;
}

function htmlToText(html) {
  if (!html) return null;
  let text = String(html);
  text = text.replace(/<script[^>]*>[\s\S]*?<\/script>/gi, " ");
  text = text.replace(/<style[^>]*>[\s\S]*?<\/style>/gi, " ");
  text = text.replace(/<[^>]+>/g, " ");
  text = text.replace(/&amp;/g, "&").replace(/&lt;/g, "<").replace(/&gt;/g, ">");
  text = text.replace(/&nbsp;/g, " ").replace(/&#39;/g, "'").replace(/&quot;/g, '"');
  text = text.replace(/\s+/g, " ").trim();
  return text.slice(0, 10000);
}

async function readHtml(resp, max = 50000) {
  if (!resp.body || !resp.body.getReader) {
    const text = await resp.text();
    return text.slice(0, max);
  }
  const reader = resp.body.getReader();
  const decoder = new TextDecoder();
  let out = "";
  while (out.length < max) {
    const { done, value } = await reader.read();
    if (done) break;
    out += decoder.decode(value, { stream: true });
  }
  try {
    await reader.cancel();
  } catch {
    /* ignore */
  }
  return out.slice(0, max);
}

async function extractTextZenrows(url, apiKey, config) {
  const timeoutMs = Math.min(30000, (process.env.VERCEL ? 12 : 30) * 1000);
  const params = new URLSearchParams({
    apikey: apiKey,
    url,
    autoparse: String(Boolean(config.zenrows?.autoparse)),
  });
  if (config.zenrows?.js_render) params.set("js_render", "true");
  if (config.zenrows?.premium_proxy) params.set("premium_proxy", "true");
  try {
    const resp = await fetchWithTimeout(`https://api.zenrows.com/v1/?${params.toString()}`, { method: "GET" }, timeoutMs);
    if (resp.status !== 200) {
      try {
        await resp.body?.cancel();
      } catch {
        /* ignore */
      }
      return [null, null];
    }
    const rawHtml = await readHtml(resp, 50000);
    const text = htmlToText(rawHtml);
    return [text ? text.slice(0, 10000) : null, rawHtml];
  } catch {
    return [null, null];
  }
}

async function extractTextFallback(url, timeoutSec) {
  try {
    const resp = await fetchWithTimeout(
      url,
      { method: "GET", redirect: "follow", headers: { "User-Agent": BROWSER_UA } },
      Math.max(1, timeoutSec) * 1000
    );
    if (resp.status !== 200) {
      try {
        await resp.body?.cancel();
      } catch {
        /* ignore */
      }
      return [null, null];
    }
    const rawHtml = await readHtml(resp, 50000);
    const text = htmlToText(rawHtml);
    return [text ? text.slice(0, 10000) : null, rawHtml];
  } catch {
    return [null, null];
  }
}

async function scrapeSubpages(baseUrl, apiKey, config) {
  let pathsToTry = [...DEFAULT_SCRAPE_PATHS];
  const extra = config.scrape_paths || [];
  if (extra.length) pathsToTry = [...extra, ...pathsToTry];
  const seen = new Set();
  const unique = [];
  for (const p of pathsToTry) {
    const key = String(p).replace(/\/+$/, "").toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    unique.push(p);
  }
  const maxPages = config.scrape_max_subpages ?? 3;
  const limited = unique.slice(0, maxPages);
  const results = {};
  for (const pagePath of limited) {
    const fullUrl = `${baseUrl.replace(/\/+$/, "")}/${String(pagePath).replace(/^\/+/, "")}`;
    let text = null;
    if (apiKey) {
      [text] = await extractTextZenrows(fullUrl, apiKey, config);
    } else {
      [text] = await extractTextFallback(fullUrl, config.concurrency.request_timeout);
    }
    if (text && text.trim().length > 150) results[pagePath] = text;
  }
  return results;
}

function buildScrapedTextBundle(homepageText, subpageTexts) {
  const parts = [];
  if (homepageText) parts.push(`=== HOMEPAGE ===\n${homepageText.slice(0, 3000)}`);
  for (const [pagePath, text] of Object.entries(subpageTexts || {})) {
    const label = pagePath.replace(/^\/+|\/+$/g, "").toUpperCase().replace(/-/g, " ").replace(/\//g, " > ");
    parts.push(`=== ${label} PAGE ===\n${text.slice(0, 2500)}`);
  }
  return parts.join("\n\n").slice(0, 8000);
}

function checkParked(text) {
  if (!text) return true;
  const lower = text.toLowerCase();
  if (text.trim().length < 100) return true;
  return PARKED_DOMAIN_INDICATORS.some((indicator) => lower.includes(indicator));
}

function cleanCompanyName(name) {
  if (!name) return name;
  let value = name.replace(/&amp;/g, "&").replace(/&#39;/g, "'").replace(/&quot;/g, '"');
  value = value.replace(/&#x27;/g, "'").replace(/&apos;/g, "'");
  const suffixes = [
    /\bLLC\b\.?/i,
    /\bL\.L\.C\.?/i,
    /\bInc\.?\b/i,
    /\bCorp\.?\b/i,
    /\bCorporation\b/i,
    /\bLtd\.?\b/i,
    /\bLimited\b/i,
    /\bLP\b\.?/i,
    /\bL\.P\.?/i,
    /\bLLP\b\.?/i,
    /\bL\.L\.P\.?/i,
    /\bPC\b\.?/i,
    /\bP\.C\.?/i,
    /\bPLC\b\.?/i,
    /\bCo\.?\b/i,
  ];
  for (const suffix of suffixes) value = value.replace(new RegExp(`,?\\s*${suffix.source}`, suffix.flags), "");
  value = value.replace(/[\s,.\-|]+$/g, "").trim();
  const cased = value !== value.toLowerCase() || value !== value.toUpperCase();
  const isUpper = cased && value === value.toUpperCase();
  const isLower = cased && value === value.toLowerCase();
  if (isUpper || isLower) {
    const small = new Set(["and", "of", "the", "in", "for", "at", "by", "or"]);
    const acronyms = new Set(["HVAC", "USA", "US", "LLC", "CEO", "RV", "FOG", "HQ"]);
    const words = value.split(/\s+/).filter(Boolean);
    const titled = words.map((word, index) => {
      if (acronyms.has(word.toUpperCase())) return word.toUpperCase();
      if (index > 0 && small.has(word.toLowerCase())) return word.toLowerCase();
      return word.charAt(0).toUpperCase() + word.slice(1).toLowerCase();
    });
    if (titled.length) titled[0] = titled[0].charAt(0).toUpperCase() + titled[0].slice(1);
    value = titled.join(" ");
  }
  return value.trim();
}

function metaContent(html, property) {
  const prop = property.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const a = html.match(new RegExp(`<meta[^>]*property=["']${prop}["'][^>]*content=["']([^"']+)["']`, "i"));
  if (a) return a[1];
  const b = html.match(new RegExp(`<meta[^>]*content=["']([^"']+)["'][^>]*property=["']${prop}["']`, "i"));
  return b ? b[1] : null;
}

function extractCompanyNameFromHtml(html, fallbackName = "") {
  if (!html) return [fallbackName, "no_html"];
  const ogSite = metaContent(html, "og:site_name");
  if (ogSite && ogSite.trim() && ogSite.trim().length < 100) return [cleanCompanyName(ogSite.trim()), "og:site_name"];

  const titleMatch = html.match(/<title[^>]*>([^<]+)<\/title>/i);
  if (titleMatch) {
    const segments = titleMatch[1].trim().split(/\s*[|–—\-:]\s*/);
    const generic = new Set(["home", "welcome", "homepage", "main", "index", "official site", "official website", "website", "site", "page"]);
    const candidates = segments.map((s) => s.trim()).filter((s) => s && !generic.has(s.toLowerCase()) && s.length > 1);
    const nameCandidates = candidates.filter((c) => c.length < 60);
    if (nameCandidates.length) return [cleanCompanyName(nameCandidates[0]), "title_tag"];
  }

  const ogTitle = metaContent(html, "og:title");
  if (ogTitle) {
    const segments = ogTitle.trim().split(/\s*[|–—\-:]\s*/);
    const candidates = segments.map((s) => s.trim()).filter((s) => s.length > 1);
    if (candidates.length && candidates[0].length < 60) return [cleanCompanyName(candidates[0]), "og:title"];
  }

  const schema = html.match(/"name"\s*:\s*"([^"]{2,80})"/);
  if (schema) {
    const name = schema[1].trim();
    const words = name.split(/\s+/);
    if (words.length > 1 && words.length <= 8) return [cleanCompanyName(name), "schema_org"];
  }

  return [fallbackName, "fallback"];
}

function matchKeywords(text, config) {
  if (!text) return ["no_text", [], []];
  const lower = text.toLowerCase();
  const keywords = config.industry_keywords || {};
  const rules = config.keyword_match_rules || {};
  const negativeMatches = (keywords.negative || []).filter((kw) => kw && lower.includes(String(kw).toLowerCase()));
  if (negativeMatches.length && rules.negative_override !== false) return ["negative_match", [], negativeMatches];
  const primaryMatches = (keywords.primary || []).filter((kw) => kw && lower.includes(String(kw).toLowerCase()));
  if (primaryMatches.length >= (rules.primary_threshold ?? 1)) return ["match", primaryMatches, negativeMatches];
  const secondaryMatches = (keywords.secondary || []).filter((kw) => kw && lower.includes(String(kw).toLowerCase()));
  if (secondaryMatches.length >= (rules.secondary_threshold ?? 2)) {
    return ["match", [...primaryMatches, ...secondaryMatches], negativeMatches];
  }
  if (primaryMatches.length || secondaryMatches.length) {
    return ["weak_match", [...primaryMatches, ...secondaryMatches], negativeMatches];
  }
  return ["no_match", [], negativeMatches];
}

async function processLayer2Row(row, apiKey, config) {
  const url = row._normalized_url;
  const rawName = row.company_name || row["Company Name"] || "";
  if (!url) {
    return {
      industry_match: "skipped",
      matched_keywords: "",
      homepage_snippet: "",
      resolved_name: rawName,
      name_source: "skipped",
      scraped_text: "",
      pages_scraped: "",
    };
  }

  let text = null;
  let rawHtml = null;
  const timeout = config.concurrency.request_timeout;
  if (apiKey) {
    [text, rawHtml] = await extractTextZenrows(url, apiKey, config);
    if (!text) [text, rawHtml] = await extractTextFallback(url, timeout);
  } else {
    [text, rawHtml] = await extractTextFallback(url, timeout);
  }

  const [resolvedName, nameSource] = extractCompanyNameFromHtml(rawHtml, rawName);
  if (checkParked(text)) {
    row.domain_status = "parked";
    return {
      industry_match: "parked_domain",
      matched_keywords: "",
      homepage_snippet: (text || "").slice(0, 200),
      resolved_name: rawName,
      name_source: "parked_domain",
      scraped_text: "",
      pages_scraped: "",
    };
  }

  const [matchResult, matched, negatives] = matchKeywords(text, config);
  let scrapedText = "";
  let pagesScraped = "homepage";
  if (matchResult === "match" || matchResult === "weak_match") {
    const subpages = await scrapeSubpages(url, apiKey, config);
    scrapedText = buildScrapedTextBundle(text, subpages);
    const paths = Object.keys(subpages);
    if (paths.length) pagesScraped = `homepage;${paths.join(";")}`;
  } else {
    scrapedText = buildScrapedTextBundle(text, {});
  }

  return {
    industry_match: matchResult,
    matched_keywords: matched.length ? matched.join("; ") : "",
    negative_keywords: negatives.length ? negatives.join("; ") : "",
    homepage_snippet: (text || "").slice(0, 300),
    resolved_name: resolvedName || rawName,
    name_source: nameSource,
    scraped_text: scrapedText,
    pages_scraped: pagesScraped,
  };
}

const LIVE_STATUSES = new Set(["live", "live_http_only", "redirect", "needs_get"]);

async function runLayer2(rows, apiKey, config) {
  const liveRows = rows.filter((row) => LIVE_STATUSES.has(row.domain_status));
  const workers = config.concurrency.layer2_workers;
  const delayMs = Math.round((config.concurrency.layer2_delay || 0) * 1000);
  await mapPool(
    liveRows,
    workers,
    async (row) => {
      try {
        Object.assign(row, await processLayer2Row(row, apiKey, config));
      } catch (err) {
        row.industry_match = "error";
        row.matched_keywords = "";
        row.homepage_snippet = String(err.message || err).slice(0, 200);
        row.scraped_text = row.scraped_text || "";
        row.pages_scraped = row.pages_scraped || "";
      }
    },
    delayMs
  );

  for (const row of rows) {
    if (row.industry_match == null) {
      row.industry_match = "domain_dead";
      row.matched_keywords = "";
      row.homepage_snippet = "";
      row.negative_keywords = row.negative_keywords || "";
      row.resolved_name = row.company_name || row["Company Name"] || "";
      row.name_source = "domain_dead";
      row.scraped_text = "";
      row.pages_scraped = "";
    }
  }
  return rows;
}

function classifyRow(row) {
  const domainStatus = row.domain_status || "unknown";
  const industryMatch = row.industry_match || "unknown";
  if (["dead", "timeout", "ssl_error", "invalid_domain", "not_found", "server_error", "error", "redirects_to_social"].includes(domainStatus)) {
    return ["FAIL", `Domain: ${domainStatus}`];
  }
  if (industryMatch === "parked_domain") return ["FAIL", "Parked/dead domain"];
  if (industryMatch === "negative_match") return ["FAIL", `Negative keyword match: ${row.negative_keywords || ""}`];
  if (industryMatch === "no_match") return ["FAIL", "No industry keyword matches on homepage"];
  if (industryMatch === "match") return ["PASS", `Keywords: ${row.matched_keywords || ""}`];
  if (industryMatch === "weak_match") return ["REVIEW", `Weak match: ${row.matched_keywords || ""}`];
  if (["no_text", "error", "skipped"].includes(industryMatch)) return ["REVIEW", `Could not extract text: ${industryMatch}`];
  return ["REVIEW", `Unclassified: ${domainStatus}/${industryMatch}`];
}

function blank(value) {
  return value == null ? "" : value;
}

function toOutputRecords(columns, rows) {
  const original = columns.filter((col) => !col.startsWith("_") && !FILTER_COL_NAMES.includes(col));
  const allCols = [...original];
  for (const col of OUTPUT_FILTER_COLS) {
    if (!allCols.includes(col)) allCols.push(col);
  }
  for (const row of rows) {
    const [decision, reason] = classifyRow(row);
    row.filter_decision = decision;
    row.filter_reason = reason;
    row.http_code = blank(row.http_code);
    row.redirect_url = blank(row.redirect_url);
    row.negative_keywords = blank(row.negative_keywords);
    row.homepage_snippet = blank(row.homepage_snippet);
    row.resolved_name = blank(row.resolved_name);
    row.name_source = blank(row.name_source);
    row.pages_scraped = blank(row.pages_scraped);
    row.scraped_text = blank(row.scraped_text);
    row.matched_keywords = blank(row.matched_keywords);
    row.industry_match = blank(row.industry_match);
    row.domain_status = blank(row.domain_status);
    row.filter_decision = decision;
    row.filter_reason = reason;
  }
  return { columns: allCols, records: rows };
}

function summaryFromRows(rows) {
  const summary = { total: rows.length, pass: 0, fail: 0, review: 0 };
  for (const row of rows) {
    if (row.filter_decision === "PASS") summary.pass++;
    else if (row.filter_decision === "FAIL") summary.fail++;
    else if (row.filter_decision === "REVIEW") summary.review++;
  }
  return summary;
}

function normalizeRecordColumns(columns, records) {
  const cols = [...columns];
  for (const record of records) {
    if (record.website && !record.domain) record.domain = record.website;
    if (record.Domain && !record.domain) record.domain = record.Domain;
    if (record.Website && !record.domain) record.domain = record.Website;
    if (record["Company Name"] && !record.company_name) record.company_name = record["Company Name"];
    if (record.CompanyName && !record.company_name) record.company_name = record.CompanyName;
  }
  if (!cols.includes("domain") && records.some((record) => record.domain)) cols.push("domain");
  if (!cols.includes("company_name") && records.some((record) => record.company_name)) cols.push("company_name");
  return cols;
}

async function runPipeline({ csvText, config, apiKey, layer1Only = false }) {
  const loaded = applyRuntimeLimits(config || cloneConfig(DEFAULT_CONFIG));
  const keywords = loaded.industry_keywords || {};
  const hasKeywords = (keywords.primary || []).length + (keywords.secondary || []).length > 0;
  if (!layer1Only && !hasKeywords) {
    const error = new Error("No industry keywords configured. Add primary or secondary keywords.");
    error.status = 400;
    throw error;
  }

  const parsed = rowsToObjects(parseCsv(csvText || ""));
  if (!parsed.columns.length) {
    return { csv: "", summary: { total: 0, pass: 0, fail: 0, review: 0 }, rows: [] };
  }

  const columns = normalizeRecordColumns(parsed.columns, parsed.records);
  let rows = parsed.records;
  rows = await runLayer1(rows, loaded);
  if (!layer1Only) rows = await runLayer2(rows, (apiKey || "").trim(), loaded);
  else {
    for (const row of rows) {
      row.industry_match = row.industry_match || "not_run";
      row.matched_keywords = "";
      row.negative_keywords = "";
      row.homepage_snippet = "";
      row.resolved_name = row.company_name || row["Company Name"] || "";
      row.name_source = "not_run";
      row.scraped_text = "";
      row.pages_scraped = "";
    }
  }

  const output = toOutputRecords(columns, rows);
  return {
    csv: objectsToCsv(output.columns, output.records),
    summary: summaryFromRows(output.records),
    rows: output.records,
  };
}

module.exports = {
  DEFAULT_CONFIG,
  loadConfig,
  configFromKeywords,
  normalizeDomain,
  matchKeywords,
  classifyRow,
  cleanCompanyName,
  extractCompanyNameFromHtml,
  checkParked,
  runPipeline,
};
