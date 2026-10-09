import fs from "node:fs";
import puppeteer, { type Browser } from "puppeteer-core";

const CHROME_CANDIDATES = [
  "/usr/bin/google-chrome",
  "/usr/bin/google-chrome-stable",
  "/usr/bin/chromium",
  "/usr/bin/chromium-browser",
  "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
  "/Applications/Chromium.app/Contents/MacOS/Chromium",
];

let browserPromise: Promise<Browser> | null = null;

export function findChromeExecutable(
  env: Record<string, string | undefined> = process.env,
  exists: (path: string) => boolean = fs.existsSync,
) {
  const explicit = [env.CHROME_PATH, env.PUPPETEER_EXECUTABLE_PATH].find((value) => value && exists(value));
  if (explicit) return explicit;
  return CHROME_CANDIDATES.find((candidate) => exists(candidate)) ?? null;
}

export async function renderWithLocalChrome(url: string, timeoutMs: number) {
  const executablePath = findChromeExecutable();
  if (!executablePath) {
    throw new Error("JS rendering needs Chrome or Chromium installed on the computer running this server.");
  }

  const browser = await launchChrome(executablePath);
  const page = await browser.newPage();

  try {
    await page.setUserAgent("Mozilla/5.0 (compatible; SurflineCapitalLeadChecker/1.0; +https://surflinecapital.com)");
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: timeoutMs });
    await page.waitForNetworkIdle({ idleTime: 400, timeout: Math.min(5000, timeoutMs) }).catch(() => undefined);
    return await page.content();
  } finally {
    await page.close().catch(() => undefined);
  }
}

export async function closeLocalChrome() {
  const pending = browserPromise;
  browserPromise = null;
  if (!pending) return;
  const browser = await pending.catch(() => null);
  await browser?.close().catch(() => undefined);
}

function launchChrome(executablePath: string): Promise<Browser> {
  if (!browserPromise) {
    browserPromise = startBrowser(executablePath);
  }

  return browserPromise.then(
    (browser) => {
      if (browser.connected) return browser;
      browserPromise = startBrowser(executablePath);
      return browserPromise;
    },
    () => {
      browserPromise = startBrowser(executablePath);
      return browserPromise;
    },
  );
}

function startBrowser(executablePath: string) {
  const launched = puppeteer
    .launch({
      executablePath,
      headless: true,
      args: ["--no-sandbox", "--disable-dev-shm-usage"],
    })
    .then((browser) => {
      browser.once("disconnected", () => {
        if (browserPromise === launched) browserPromise = null;
      });
      return browser;
    });

  browserPromise = launched;
  launched.catch(() => {
    if (browserPromise === launched) browserPromise = null;
  });
  return launched;
}
