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

For JavaScript-rendered sites, add Browserless credentials:

```bash
BROWSERLESS_TOKEN=your_token_here
BROWSERLESS_ENDPOINT=https://chrome.browserless.io
```

On Vercel, add those as Project Settings -> Environment Variables.

Rendering modes:

- `Auto`: fetch HTML first, then retry with JavaScript rendering when the usable text is thin
- `HTML only`: fastest and cheapest
- `JS render first`: uses Browserless for every row

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
3. Add `BROWSERLESS_TOKEN` if you want JavaScript rendering.
4. Keep the default Next.js settings.
5. Deploy.

No environment variables are required for HTML-only mode.
