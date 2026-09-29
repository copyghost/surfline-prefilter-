import { NextRequest, NextResponse } from "next/server";

export const runtime = "nodejs";
export const maxDuration = 60;

type RenderMode = "auto" | "html" | "js";

type InputRow = {
  domain: string;
  original: Record<string, string>;
  rowNumber?: number;
};

type AnalyzeRequest = {
  rows: InputRow[];
  includeKeywords?: string[];
  excludeKeywords?: string[];
  renderMode?: RenderMode;
};

type PageFetch = {
  html: string;
  status: number | string;
  url: string;
  renderMode: "html" | "js";
  error?: string;
};

type AnalyzeResult = Record<string, string | boolean | number>;

const MAX_BATCH_ROWS = 25;
const FETCH_TIMEOUT_MS = 9000;
const RENDER_TIMEOUT_MS = 18000;
const MAX_HTML_CHARS = 1_200_000;
const MAX_TEXT_CHARS = 5000;
const MIN_USEFUL_TEXT_CHARS = 450;
const ANALYSIS_CONCURRENCY = 4;

const STRONG_BLOCK_PATTERNS = [
  /access denied/i,
  /are you a human/i,
  /attention required/i,
  /checking your browser/i,
  /ddos protection/i,
  /enable cookies/i,
  /enable javascript and cookies/i,
  /forbidden/i,
  /just a moment/i,
  /please complete the security check/i,
  /request blocked/i,
  /security check/i,
  /unusual traffic/i,
  /verify you are human/i,
];

const CONSENT_PATTERNS = [
  /accept all cookies/i,
  /cookie consent/i,
  /cookie preferences/i,
  /privacy preferences/i,
  /reject all/i,
  /this website uses cookies/i,
  /we use cookies/i,
];

export async function POST(request: NextRequest) {
  let body: AnalyzeRequest;

  try {
    body = (await request.json()) as AnalyzeRequest;
  } catch {
    return NextResponse.json({ error: "Invalid request body." }, { status: 400 });
  }

  const rows = Array.isArray(body.rows) ? body.rows.slice(0, MAX_BATCH_ROWS) : [];
  const includeKeywords = normalizeKeywords(body.includeKeywords);
  const excludeKeywords = normalizeKeywords(body.excludeKeywords);
  const renderMode = normalizeRenderMode(body.renderMode);

  if (rows.length === 0) {
    return NextResponse.json({ error: "Add at least one row with a domain." }, { status: 400 });
  }

  const results = await mapWithConcurrency(rows, ANALYSIS_CONCURRENCY, (row, index) =>
    analyzeDomain(row, index, includeKeywords, excludeKeywords, renderMode),
  );

  return NextResponse.json({
    results,
    meta: {
      jsRenderingConfigured: hasBrowserlessConfig(),
      renderMode,
    },
  });
}

async function analyzeDomain(
  row: InputRow,
  index: number,
  includeKeywords: string[],
  excludeKeywords: string[],
  renderMode: RenderMode,
): Promise<AnalyzeResult> {
  const normalizedDomain = normalizeDomain(row.domain);
  const base = {
    ...row.original,
    domain: row.domain,
    normalizedDomain,
    rowNumber: row.rowNumber ?? index + 1,
  };

  if (!normalizedDomain) {
    return emptyResult(base, "invalid", "Missing or invalid domain.");
  }

  try {
    const page = await getBestPage(normalizedDomain, renderMode);
    const extracted = extractUsableText(page.html);
    const detectionText = `${page.html} ${extracted.title} ${extracted.text}`;
    const block = detectBlock(detectionText, page.status, extracted.text.length);
    const consent = detectConsent(detectionText);
    const haystack = `${extracted.title} ${extracted.text}`.toLowerCase();
    const includeMatched = findKeywordMatches(haystack, includeKeywords);
    const excludeMatched = findKeywordMatches(haystack, excludeKeywords);
    const includeFlag = includeKeywords.length === 0 || includeMatched.length > 0;
    const excludeFlag = excludeMatched.length > 0;
    const thinText = extracted.text.length < MIN_USEFUL_TEXT_CHARS;
    const needsManualReview = block.blocked || thinText || Boolean(page.error);

    return {
      ...base,
      active: !block.blocked,
      status: page.status,
      analysisStatus: block.blocked ? "blocked" : thinText ? "thin_text" : "ok",
      resolvedUrl: page.url,
      renderMode: page.renderMode,
      renderAttempted: renderMode !== "html",
      jsRenderConfigured: hasBrowserlessConfig(),
      jsRenderUsed: page.renderMode === "js",
      textLength: extracted.text.length,
      pageTitle: extracted.title,
      includeMatched: includeMatched.join("; "),
      excludeMatched: excludeMatched.join("; "),
      includeFlag,
      excludeFlag,
      qualified: !block.blocked && includeFlag && !excludeFlag,
      blocked: block.blocked,
      blockedReason: block.reason,
      consentDetected: consent.detected,
      consentAction: consent.detected ? "detected_not_bypassed" : "none",
      needsManualReview,
      textSnippet: extracted.text.slice(0, MAX_TEXT_CHARS),
      error: page.error ?? "",
    };
  } catch (error) {
    return emptyResult(
      base,
      "inactive",
      error instanceof Error ? error.message : "Unable to analyze domain.",
      renderMode,
    );
  }
}

function emptyResult(
  base: Record<string, string | number>,
  status: string,
  error: string,
  requestedRenderMode: RenderMode = "auto",
): AnalyzeResult {
  return {
    ...base,
    active: false,
    status,
    analysisStatus: status,
    resolvedUrl: "",
    renderMode: "none",
    renderAttempted: requestedRenderMode !== "html",
    jsRenderConfigured: hasBrowserlessConfig(),
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

async function getBestPage(domain: string, renderMode: RenderMode): Promise<PageFetch> {
  if (renderMode === "js") {
    return renderHomePage(domain);
  }

  const htmlPage = await fetchHomePage(domain);
  const extracted = extractUsableText(htmlPage.html);
  const block = detectBlock(`${htmlPage.html} ${extracted.text}`, htmlPage.status, extracted.text.length);

  if (renderMode === "html" || block.blocked || extracted.text.length >= MIN_USEFUL_TEXT_CHARS) {
    return htmlPage;
  }

  try {
    const renderedPage = await renderHomePage(domain, htmlPage.url);
    const renderedText = extractUsableText(renderedPage.html);

    if (renderedText.text.length > extracted.text.length) {
      return renderedPage;
    }

    return {
      ...htmlPage,
      error: "JS rendering did not add more usable text.",
    };
  } catch (error) {
    return {
      ...htmlPage,
      error: error instanceof Error ? error.message : "JS rendering failed.",
    };
  }
}

async function fetchHomePage(domain: string): Promise<PageFetch> {
  const candidates = [`https://${domain}`, `http://${domain}`];
  let lastError = "";

  for (const url of candidates) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);

    try {
      const response = await fetch(url, {
        redirect: "follow",
        signal: controller.signal,
        headers: {
          accept: "text/html,application/xhtml+xml,application/xml;q=0.9,text/plain;q=0.8,*/*;q=0.7",
          "accept-language": "en-US,en;q=0.9",
          "user-agent":
            "Mozilla/5.0 (compatible; SurflineCapitalLeadChecker/1.0; +https://surflinecapital.com)",
        },
      });

      const contentType = response.headers.get("content-type") ?? "";
      if (!response.ok) {
        const html = isTextLikeContent(contentType) ? (await response.text()).slice(0, MAX_HTML_CHARS) : "";
        const extracted = extractUsableText(html);
        const block = detectBlock(`${html} ${extracted.text}`, response.status, extracted.text.length);
        if (block.blocked) {
          return {
            status: response.status,
            url: response.url,
            html,
            renderMode: "html",
          };
        }

        lastError = `HTTP ${response.status}`;
        continue;
      }

      if (!isTextLikeContent(contentType)) {
        lastError = `Unsupported content type: ${contentType || "unknown"}`;
        continue;
      }

      const html = (await response.text()).slice(0, MAX_HTML_CHARS);
      return {
        status: response.status,
        url: response.url,
        html,
        renderMode: "html",
      };
    } catch (error) {
      lastError = error instanceof Error && error.name === "AbortError" ? "Request timed out" : "Fetch failed";
    } finally {
      clearTimeout(timeout);
    }
  }

  throw new Error(lastError || "No homepage response.");
}

async function renderHomePage(domain: string, knownUrl?: string): Promise<PageFetch> {
  const token = process.env.BROWSERLESS_TOKEN;
  const endpoint = normalizeEndpoint(process.env.BROWSERLESS_ENDPOINT ?? "https://chrome.browserless.io");

  if (!token) {
    throw new Error("JS rendering is not configured. Add BROWSERLESS_TOKEN in Vercel.");
  }

  const targetUrl = knownUrl ?? `https://${domain}`;
  const apiUrl = `${endpoint}/content?token=${encodeURIComponent(token)}`;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), RENDER_TIMEOUT_MS);

  try {
    const response = await fetch(apiUrl, {
      method: "POST",
      signal: controller.signal,
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({
        url: targetUrl,
        gotoOptions: {
          waitUntil: "networkidle2",
          timeout: RENDER_TIMEOUT_MS,
        },
        rejectResourceTypes: ["image", "media", "font"],
      }),
    });

    const html = (await response.text()).slice(0, MAX_HTML_CHARS);

    if (!response.ok) {
      throw new Error(`JS render failed with HTTP ${response.status}.`);
    }

    return {
      status: response.status,
      url: targetUrl,
      html,
      renderMode: "js",
    };
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") {
      throw new Error("JS rendering timed out.");
    }

    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

function extractUsableText(html: string) {
  const title = decodeEntities(matchFirst(html, /<title[^>]*>([\s\S]*?)<\/title>/i));
  let text = html
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<noscript[\s\S]*?<\/noscript>/gi, " ")
    .replace(/<svg[\s\S]*?<\/svg>/gi, " ")
    .replace(/<nav[\s\S]*?<\/nav>/gi, " ")
    .replace(/<footer[\s\S]*?<\/footer>/gi, " ")
    .replace(/<header[\s\S]*?<\/header>/gi, " ")
    .replace(/<aside[\s\S]*?<\/aside>/gi, " ")
    .replace(/<form[\s\S]*?<\/form>/gi, " ")
    .replace(/<!--[\s\S]*?-->/g, " ")
    .replace(/<[^>]+>/g, " ");

  text = decodeEntities(text)
    .replace(/\s+/g, " ")
    .replace(/\s([,.!?;:])/g, "$1")
    .trim();

  return { title, text };
}

function normalizeDomain(value: string) {
  const trimmed = String(value ?? "").trim();
  if (!trimmed) return "";

  try {
    const withProtocol = /^[a-z][a-z\d+\-.]*:\/\//i.test(trimmed) ? trimmed : `https://${trimmed}`;
    const url = new URL(withProtocol);
    return url.hostname.replace(/^www\./i, "").toLowerCase();
  } catch {
    return trimmed
      .replace(/^https?:\/\//i, "")
      .replace(/^www\./i, "")
      .split("/")[0]
      .trim()
      .toLowerCase();
  }
}

function normalizeKeywords(keywords: unknown) {
  if (!Array.isArray(keywords)) return [];

  return keywords
    .flatMap((keyword) => String(keyword).split(/[\n,]/))
    .map((keyword) => keyword.trim().toLowerCase())
    .filter(Boolean);
}

function normalizeRenderMode(value: unknown): RenderMode {
  return value === "html" || value === "js" || value === "auto" ? value : "auto";
}

function normalizeEndpoint(value: string) {
  return value.replace(/\/+$/, "");
}

function findKeywordMatches(text: string, keywords: string[]) {
  return keywords.filter((keyword) => text.includes(keyword));
}

function detectBlock(text: string, status: number | string, textLength = 0) {
  const httpStatus = typeof status === "number" ? status : 0;
  const statusBlocked = [401, 403, 407, 429, 503].includes(httpStatus);
  const strongMatch = STRONG_BLOCK_PATTERNS.find((pattern) => pattern.test(text));
  const lower = text.toLowerCase();
  const cloudflareChallenge =
    lower.includes("cloudflare") &&
    (lower.includes("checking your browser") ||
      lower.includes("cloudflare ray id") ||
      lower.includes("just a moment") ||
      lower.includes("attention required"));
  const captchaChallenge =
    /\bcaptcha\b|recaptcha/i.test(text) &&
    textLength < 900 &&
    (lower.includes("verify") || lower.includes("human") || lower.includes("security check"));

  return {
    blocked: statusBlocked || Boolean(strongMatch) || cloudflareChallenge || captchaChallenge,
    reason: statusBlocked
      ? `HTTP ${httpStatus}`
      : strongMatch
        ? readablePattern(strongMatch)
        : cloudflareChallenge
          ? "cloudflare challenge"
          : captchaChallenge
            ? "captcha challenge"
            : "",
  };
}

function detectConsent(text: string) {
  const match = CONSENT_PATTERNS.find((pattern) => pattern.test(text));

  return {
    detected: Boolean(match),
    reason: match ? readablePattern(match) : "",
  };
}

function readablePattern(pattern: RegExp) {
  return pattern.source.replace(/\\/g, "").replace(/\|/g, " or ");
}

function hasBrowserlessConfig() {
  return Boolean(process.env.BROWSERLESS_TOKEN);
}

function isTextLikeContent(contentType: string) {
  const lower = contentType.toLowerCase();
  return !lower || lower.includes("text/") || lower.includes("html") || lower.includes("xml");
}

function matchFirst(value: string, pattern: RegExp) {
  const match = value.match(pattern);
  return match?.[1]?.trim() ?? "";
}

function decodeEntities(value: string) {
  return value
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/&#(\d+);/g, (_, code: string) => String.fromCharCode(Number(code)))
    .replace(/&#x([a-f\d]+);/gi, (_, code: string) => String.fromCharCode(Number.parseInt(code, 16)));
}

async function mapWithConcurrency<T, R>(
  items: T[],
  concurrency: number,
  callback: (item: T, index: number) => Promise<R>,
) {
  const results = new Array<R>(items.length);
  let nextIndex = 0;

  async function worker() {
    while (nextIndex < items.length) {
      const currentIndex = nextIndex;
      nextIndex += 1;
      results[currentIndex] = await callback(items[currentIndex], currentIndex);
    }
  }

  await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, worker));
  return results;
}
