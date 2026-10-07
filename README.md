# Surfline Capital Lead Checker

Prefilter script for searches: domain resolution, homepage scrape, keyword flags, JavaScript-rendered text fallback, and manual-review signals.

Upload a CSV with a `domain` column, check large lead lists in batches, scrape usable homepage text, optionally retry with JavaScript rendering, match include/exclude keywords, detect block/consent states, and download an enriched CSV.

## Local setup

```bash
npm install
npm run dev
```

Open `http://localhost:3000`.

## JavaScript rendering

The app works without extra services using normal server-side HTML fetches.

When a page needs JavaScript, the server launches Chrome or Chromium installed on the same computer. Install Google Chrome locally, run `npm run dev` there, and choose `Auto` or `JS render first`. Set `CHROME_PATH` if Chrome is not on the default install path.

A hosted Vercel function cannot launch Chrome. Run JavaScript rendering on your own machine. Thin pages stay on the HTML result and are flagged for review when Chrome is unavailable.

Rendering modes:

- `Auto`: fetch HTML first, then retry with local Chrome when the usable text is thin
- `HTML only`: fastest, and does not open Chrome
- `JS render first`: opens each homepage in local Chrome

Large uploads are processed in batches of 25 domains per API request. A 25k-row CSV can be uploaded at once, but the browser tab must stay open until processing finishes. For very large first-pass screening, start with `HTML only` and use `Auto` or `JS render first` on a smaller follow-up list when needed.

The app does not bypass CAPTCHA, Cloudflare challenges, paywalls, login walls, or required privacy gates. It detects those situations and flags the row for manual review.

## CSV input

Required column:

- `domain`

Any extra columns are preserved in the output.

## Output columns

- `active`
- `status`
- `analysisStatus`
- `resolvedUrl`
- `renderMode`
- `renderAttempted`
- `jsRenderConfigured`
- `jsRenderUsed`
- `textLength`
- `pageTitle`
- `includeMatched`
- `excludeMatched`
- `includeFlag`
- `excludeFlag`
- `qualified`
- `blocked`
- `blockedReason`
- `consentDetected`
- `consentAction`
- `needsManualReview`
- `textSnippet`
- `error`

`qualified` is true when the domain is active, not blocked, at least one include keyword matches if include keywords were provided, and no exclude keyword matches.

`needsManualReview` is true when the page appears blocked, has unusually thin text, or the JavaScript rendering fallback could not run.

## Deploy

1. Push this folder to GitHub.
2. In Vercel, import the GitHub repository.
3. Keep the default Next.js settings.
4. Deploy.

Vercel serves the HTML-only path. JavaScript rendering runs when you start the app on a computer that has Chrome installed.

No environment variables are required for HTML-only mode.
