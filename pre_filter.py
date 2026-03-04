"""
Surfline Capital - Pre-Filter Pipeline Script
==============================================
Runs BEFORE Clay import to eliminate dead domains and obvious industry mismatches.

Two-layer cheap filter:
  Layer 1: Domain Resolution (free - HTTP HEAD requests)
  Layer 2: Homepage Text Extraction + Keyword Match (ZenRows API - $0.001-0.003/row)

Usage:
  python pre_filter.py --input raw_tam.csv --output filtered_output.csv --config config.yaml

Requirements:
  pip install requests pyyaml tqdm

Optional (for Layer 2):
  Set ZENROWS_API_KEY environment variable or pass via --api-key
  If no API key provided, Layer 2 uses requests + BeautifulSoup as fallback (free but less reliable)

Input CSV must have at minimum: company_name, domain
Output CSV adds: domain_status, http_code, redirect_url, industry_match, matched_keywords, homepage_snippet
"""

import csv
import os
import sys
import time
import argparse
import logging
import re
from urllib.parse import urlparse, urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

import requests
import yaml
from tqdm import tqdm

# ─── Logging ───
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("pre_filter")

# ─── Constants ───
PARKED_DOMAIN_INDICATORS = [
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
]

SOCIAL_MEDIA_DOMAINS = [
    "facebook.com", "instagram.com", "twitter.com", "x.com",
    "linkedin.com", "tiktok.com", "youtube.com", "pinterest.com",
    "yelp.com", "bbb.org", "google.com/maps", "maps.google.com",
]

# ─── Config Loading ───
DEFAULT_CONFIG = {
    "industry_keywords": {
        "primary": [],       # High-signal keywords (any match = likely fit)
        "secondary": [],     # Supporting keywords (need 2+ matches)
        "negative": [],      # Anti-keywords (any match = likely NOT fit)
    },
    "keyword_match_rules": {
        "primary_threshold": 1,    # Need at least 1 primary keyword
        "secondary_threshold": 2,  # Or at least 2 secondary keywords
        "negative_override": True,  # Negative keywords override positive matches
    },
    "concurrency": {
        "layer1_workers": 20,      # Parallel HTTP HEAD requests
        "layer2_workers": 10,      # Parallel ZenRows/scrape requests
        "request_timeout": 10,     # Seconds per request
        "layer2_delay": 0.1,       # Delay between Layer 2 requests (rate limiting)
    },
    "zenrows": {
        "js_render": False,        # Set True for JS-heavy sites (costs more)
        "premium_proxy": False,    # Set True if getting blocked (costs more)
        "autoparse": False,        # We do our own parsing
    }
}


def load_config(config_path=None):
    """Load config from YAML file, falling back to defaults."""
    config = DEFAULT_CONFIG.copy()
    if config_path and os.path.exists(config_path):
        with open(config_path, "r") as f:
            user_config = yaml.safe_load(f) or {}
        # Deep merge
        for key in user_config:
            if isinstance(user_config[key], dict) and key in config:
                config[key].update(user_config[key])
            else:
                config[key] = user_config[key]
    return config


# ─── Layer 1: Domain Resolution ───

def normalize_domain(domain):
    """
    Normalize a domain string to a clean URL for requests.
    Handles: full URLs with paths/UTM, protocol-relative //www.example.com,
    and malformed 'http://septic' (no TLD) by trying .com.
    """
    if not domain or not isinstance(domain, str):
        return None
    domain = domain.strip().lower()
    # Remove protocol first so path stripping doesn't eat the host
    domain = re.sub(r'^https?://', '', domain)
    domain = re.sub(r'^//+', '', domain)  # protocol-relative //www.example.com
    # Remove path, query params, fragment
    domain = re.sub(r'[/\?#].*$', '', domain)
    # Remove www. prefix for consistency
    domain = re.sub(r'^www\.', '', domain)
    domain = domain.strip()
    # Skip if it looks like a social media link
    for social in SOCIAL_MEDIA_DOMAINS:
        if social in domain:
            return None
    if not domain:
        return None
    # Already has a TLD (e.g. example.com)
    if '.' in domain:
        return f"https://{domain}"
    # Malformed: no TLD (e.g. "septic" from "http://septic" or "https://www" -> "www")
    if domain in ("www", "http", "https"):
        return None
    # Single hostname word: try .com so we don't fail entire rows (e.g. septic -> septic.com)
    if re.match(r'^[a-z0-9][-a-z0-9]{0,62}$', domain):
        return f"https://{domain}.com"
    return None


def _try_get_fallback(url, timeout=10):
    """
    Try a minimal GET request (stream, read first 1KB). Some servers block HEAD but allow GET.
    Returns (status_code, final_url) or (None, None) on failure.
    """
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    try:
        resp = requests.get(url, timeout=timeout, allow_redirects=True, headers=headers, stream=True)
        resp.raw.read(1024, decode_content=False)
        return resp.status_code, resp.url
    except Exception:
        return None, None


def check_domain(url, timeout=10):
    """
    Layer 1: Check if domain resolves to a live website.
    Tries HEAD first; on failure or 403, tries GET so more sites pass to Layer 2.
    Returns dict with status info. Cost: $0.00 (just HTTP requests).
    """
    result = {
        "domain_status": "unknown",
        "http_code": None,
        "redirect_url": None,
        "is_parked": False,
        "error": None,
    }

    if not url:
        result["domain_status"] = "invalid_domain"
        return result

    head_failed = False  # True if we should try GET fallback

    try:
        # First try HEAD (cheaper, faster)
        resp = requests.head(
            url,
            timeout=timeout,
            allow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (compatible; SurflineBot/1.0)"},
        )
        result["http_code"] = resp.status_code

        # Check for redirects to unrelated domains
        if resp.url:
            final_domain = urlparse(resp.url).netloc.replace("www.", "")
            original_domain = urlparse(url).netloc.replace("www.", "")
            if final_domain != original_domain:
                # Check if redirect is to a social media or parked page
                for social in SOCIAL_MEDIA_DOMAINS:
                    if social in final_domain:
                        result["domain_status"] = "redirects_to_social"
                        result["redirect_url"] = resp.url
                        return result
                result["redirect_url"] = resp.url

        if resp.status_code == 200:
            result["domain_status"] = "live"
        elif resp.status_code in (301, 302, 303, 307, 308):
            result["domain_status"] = "redirect"
        elif resp.status_code == 403:
            # Many sites block HEAD but allow GET - try GET before giving up
            result["domain_status"] = "needs_get"
            head_failed = True
        elif resp.status_code == 404:
            result["domain_status"] = "not_found"
        elif resp.status_code == 405:
            result["domain_status"] = "needs_get"
            head_failed = True
        elif resp.status_code >= 500:
            result["domain_status"] = "server_error"
        else:
            result["domain_status"] = f"http_{resp.status_code}"

    except requests.exceptions.SSLError:
        # Try HTTP fallback (HEAD then GET if needed)
        try:
            http_url = url.replace("https://", "http://")
            resp = requests.head(http_url, timeout=timeout, allow_redirects=True,
                                 headers={"User-Agent": "Mozilla/5.0 (compatible; SurflineBot/1.0)"})
            result["http_code"] = resp.status_code
            result["domain_status"] = "live_http_only" if resp.status_code == 200 else f"http_{resp.status_code}"
        except Exception:
            code, final_url = _try_get_fallback(http_url, timeout)
            if code == 200:
                result["http_code"] = 200
                result["domain_status"] = "live_http_only"
                result["redirect_url"] = final_url or result.get("redirect_url")
            else:
                result["domain_status"] = "ssl_error"
                result["error"] = "SSL error and HTTP fallback failed"

    except requests.exceptions.ConnectionError:
        result["domain_status"] = "dead"
        result["error"] = "Connection refused or DNS failure"
        head_failed = True

    except requests.exceptions.Timeout:
        result["domain_status"] = "timeout"
        result["error"] = f"No response within {timeout}s"
        head_failed = True

    except Exception as e:
        result["domain_status"] = "error"
        result["error"] = str(e)[:200]
        head_failed = True

    # GET fallback: when HEAD failed or was blocked, try GET so Layer 2 can still run
    if head_failed and result["domain_status"] in ("dead", "timeout", "error", "needs_get", "http_403", "http_405"):
        code, final_url = _try_get_fallback(url, timeout)
        if code == 200:
            result["domain_status"] = "live"
            result["http_code"] = 200
            result["error"] = None
            if final_url:
                result["redirect_url"] = final_url
        elif code and code in (301, 302, 303, 307, 308):
            result["domain_status"] = "redirect"
            result["http_code"] = code
            result["error"] = None
            if final_url:
                result["redirect_url"] = final_url
        # If HEAD was 403/405 we may not have set http_code yet
        if result["http_code"] is None and code:
            result["http_code"] = code

    return result


def run_layer1(rows, config):
    """Run domain resolution on all rows in parallel."""
    log.info(f"Layer 1: Checking {len(rows)} domains...")
    timeout = config["concurrency"]["request_timeout"]
    workers = config["concurrency"]["layer1_workers"]

    results = [None] * len(rows)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {}
        for i, row in enumerate(rows):
            url = normalize_domain(row.get("domain", "") or row.get("website", ""))
            row["_normalized_url"] = url
            futures[executor.submit(check_domain, url, timeout)] = i

        for future in tqdm(as_completed(futures), total=len(futures), desc="Layer 1: Domain Check"):
            idx = futures[future]
            try:
                results[idx] = future.result()
            except Exception as e:
                results[idx] = {"domain_status": "error", "http_code": None,
                                "redirect_url": None, "is_parked": False, "error": str(e)}

    # Merge results back into rows
    live_count = 0
    for i, row in enumerate(rows):
        row.update(results[i])
        if row["domain_status"] in ("live", "live_http_only", "redirect", "needs_get"):
            live_count += 1

    log.info(f"Layer 1 complete: {live_count}/{len(rows)} domains live ({live_count/len(rows)*100:.1f}%)")
    return rows


# ─── Layer 2: Homepage Text Extraction + Keyword Match ───

def extract_text_zenrows(url, api_key, config):
    """
    Use ZenRows API to extract homepage content.
    Returns (text_content, raw_html) tuple.
    Cost: ~$0.001-0.003 per request depending on plan.
    """
    params = {
        "apikey": api_key,
        "url": url,
        "autoparse": str(config["zenrows"]["autoparse"]).lower(),
    }
    if config["zenrows"]["js_render"]:
        params["js_render"] = "true"
    if config["zenrows"]["premium_proxy"]:
        params["premium_proxy"] = "true"

    try:
        resp = requests.get(
            "https://api.zenrows.com/v1/",
            params=params,
            timeout=30,
        )
        if resp.status_code == 200:
            raw_html = resp.text[:50000]
            # Extract text from HTML for keyword matching
            text = re.sub(r'<script[^>]*>.*?</script>', ' ', raw_html, flags=re.DOTALL | re.IGNORECASE)
            text = re.sub(r'<style[^>]*>.*?</style>', ' ', text, flags=re.DOTALL | re.IGNORECASE)
            text = re.sub(r'<[^>]+>', ' ', text)
            text = text.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
            text = text.replace('&nbsp;', ' ').replace('&#39;', "'").replace('&quot;', '"')
            text = re.sub(r'\s+', ' ', text).strip()
            return text[:10000], raw_html
        else:
            return None, None
    except Exception:
        return None, None


def extract_text_fallback(url, timeout=10):
    """
    Fallback: use requests + basic HTML tag stripping.
    Returns (text_content, raw_html) tuple.
    Free but less reliable (blocked by JS-rendered sites, anti-bot).
    """
    try:
        resp = requests.get(
            url,
            timeout=timeout,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36"
            },
        )
        if resp.status_code != 200:
            return None, None

        raw_html = resp.text[:50000]

        # Basic HTML to text extraction
        text = re.sub(r'<script[^>]*>.*?</script>', ' ', raw_html, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<style[^>]*>.*?</style>', ' ', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<[^>]+>', ' ', text)
        text = text.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
        text = text.replace('&nbsp;', ' ').replace('&#39;', "'").replace('&quot;', '"')
        text = re.sub(r'\s+', ' ', text).strip()

        return text[:10000], raw_html

    except Exception:
        return None, None


def check_parked(text):
    """Check if homepage text indicates a parked/dead domain."""
    if not text:
        return True
    text_lower = text.lower()
    # Very short pages are suspicious
    if len(text.strip()) < 100:
        return True
    for indicator in PARKED_DOMAIN_INDICATORS:
        if indicator in text_lower:
            return True
    return False


def extract_company_name_from_html(html, fallback_name=""):
    """
    Extract the company name from homepage HTML using structured signals.
    Priority: og:site_name > <title> tag > og:title > fallback.
    Cost: $0.00 (already have the HTML from ZenRows).
    """
    if not html:
        return fallback_name, "no_html"

    # 1. Try og:site_name (most reliable - this is explicitly the site/brand name)
    og_site = re.search(
        r'<meta[^>]*property=["\']og:site_name["\'][^>]*content=["\']([^"\']+)["\']',
        html, re.IGNORECASE
    )
    if not og_site:
        og_site = re.search(
            r'<meta[^>]*content=["\']([^"\']+)["\'][^>]*property=["\']og:site_name["\']',
            html, re.IGNORECASE
        )
    if og_site:
        name = og_site.group(1).strip()
        if name and len(name) < 100:
            return _clean_company_name(name), "og:site_name"

    # 2. Try <title> tag (very common, but often includes taglines)
    title_match = re.search(r'<title[^>]*>([^<]+)</title>', html, re.IGNORECASE)
    if title_match:
        raw_title = title_match.group(1).strip()
        # Split on common separators and take the first or shortest meaningful segment
        # e.g. "ABC Septic Services | Trusted Pumping Since 1985" -> "ABC Septic Services"
        # e.g. "Home - XYZ Environmental" -> "XYZ Environmental"
        segments = re.split(r'\s*[\|–—\-:]\s*', raw_title)
        # Filter out generic segments
        generic = {"home", "welcome", "homepage", "main", "index", "official site",
                   "official website", "website", "site", "page"}
        candidates = [s.strip() for s in segments
                      if s.strip().lower() not in generic and len(s.strip()) > 1]
        if candidates:
            # Prefer shorter segments that look like company names (< 60 chars)
            name_candidates = [c for c in candidates if len(c) < 60]
            if name_candidates:
                # Take the first non-generic segment (usually the company name)
                name = name_candidates[0]
                return _clean_company_name(name), "title_tag"

    # 3. Try og:title (fallback, similar to <title>)
    og_title = re.search(
        r'<meta[^>]*property=["\']og:title["\'][^>]*content=["\']([^"\']+)["\']',
        html, re.IGNORECASE
    )
    if not og_title:
        og_title = re.search(
            r'<meta[^>]*content=["\']([^"\']+)["\'][^>]*property=["\']og:title["\']',
            html, re.IGNORECASE
        )
    if og_title:
        name = og_title.group(1).strip()
        segments = re.split(r'\s*[\|–—\-:]\s*', name)
        candidates = [s.strip() for s in segments if len(s.strip()) > 1]
        if candidates and len(candidates[0]) < 60:
            return _clean_company_name(candidates[0]), "og:title"

    # 4. Try schema.org Organization name
    schema_name = re.search(
        r'"name"\s*:\s*"([^"]{2,80})"',
        html
    )
    if schema_name:
        name = schema_name.group(1).strip()
        # Avoid grabbing random JSON values by checking it looks like a name
        if 1 < len(name.split()) <= 8:
            return _clean_company_name(name), "schema_org"

    return fallback_name, "fallback"


def _clean_company_name(name):
    """
    Clean and normalize a company name.
    Removes legal suffixes, normalizes case, strips excess whitespace.
    """
    if not name:
        return name

    # Remove common HTML entities that survived
    name = name.replace("&amp;", "&").replace("&#39;", "'").replace("&quot;", '"')
    name = name.replace("&#x27;", "'").replace("&apos;", "'")

    # Remove legal suffixes
    suffixes = [
        r'\bLLC\b\.?', r'\bL\.L\.C\.?', r'\bInc\.?\b', r'\bCorp\.?\b',
        r'\bCorporation\b', r'\bLtd\.?\b', r'\bLimited\b', r'\bLP\b\.?',
        r'\bL\.P\.?', r'\bLLP\b\.?', r'\bL\.L\.P\.?', r'\bPC\b\.?',
        r'\bP\.C\.?', r'\bPLC\b\.?', r'\bCo\.?\b',
    ]
    for suffix in suffixes:
        name = re.sub(r',?\s*' + suffix, '', name, flags=re.IGNORECASE)

    # Strip trailing punctuation and whitespace
    name = re.sub(r'[\s,.\-|]+$', '', name).strip()

    # Title case if ALL CAPS or all lower
    if name.isupper() or name.islower():
        # Smart title case: preserve known acronyms
        words = name.split()
        titled = []
        for w in words:
            if w.upper() in ("HVAC", "USA", "US", "LLC", "CEO", "RV", "FOG", "HQ"):
                titled.append(w.upper())
            elif w.lower() in ("and", "of", "the", "in", "for", "at", "by", "or"):
                titled.append(w.lower())
            else:
                titled.append(w.capitalize())
        # Always capitalize first word
        if titled:
            titled[0] = titled[0].capitalize()
        name = " ".join(titled)

    return name.strip()


def match_keywords(text, config):
    """
    Match homepage text against industry keyword lists.
    Returns (match_result, matched_keywords, negative_matches).
    """
    if not text:
        return "no_text", [], []

    text_lower = text.lower()
    keywords = config["industry_keywords"]
    rules = config["keyword_match_rules"]

    # Check negative keywords first
    negative_matches = [kw for kw in keywords.get("negative", []) if kw.lower() in text_lower]
    if negative_matches and rules.get("negative_override", True):
        return "negative_match", [], negative_matches

    # Check primary keywords
    primary_matches = [kw for kw in keywords.get("primary", []) if kw.lower() in text_lower]
    if len(primary_matches) >= rules.get("primary_threshold", 1):
        return "match", primary_matches, negative_matches

    # Check secondary keywords
    secondary_matches = [kw for kw in keywords.get("secondary", []) if kw.lower() in text_lower]
    if len(secondary_matches) >= rules.get("secondary_threshold", 2):
        return "match", primary_matches + secondary_matches, negative_matches

    # Some primary but not enough
    if primary_matches or secondary_matches:
        return "weak_match", primary_matches + secondary_matches, negative_matches

    return "no_match", [], negative_matches


def process_layer2_row(row, api_key, config):
    """Process a single row through Layer 2: text extraction, keyword match, and company name resolution."""
    url = row.get("_normalized_url")
    raw_name = row.get("company_name", "") or row.get("Company Name", "") or ""

    if not url:
        return {
            "industry_match": "skipped",
            "matched_keywords": "",
            "homepage_snippet": "",
            "resolved_name": raw_name,
            "name_source": "skipped",
        }

    # Extract homepage text + raw HTML
    text, raw_html = None, None
    if api_key:
        text, raw_html = extract_text_zenrows(url, api_key, config)
        if not text:
            text, raw_html = extract_text_fallback(url, config["concurrency"]["request_timeout"])
    else:
        text, raw_html = extract_text_fallback(url, config["concurrency"]["request_timeout"])

    # ── Company Name Resolution (from HTML, $0.00) ──
    resolved_name, name_source = extract_company_name_from_html(raw_html, fallback_name=raw_name)

    # Check if parked
    if check_parked(text):
        row["domain_status"] = "parked"
        return {
            "industry_match": "parked_domain",
            "matched_keywords": "",
            "homepage_snippet": (text or "")[:200],
            "resolved_name": raw_name,  # Don't trust names from parked pages
            "name_source": "parked_domain",
        }

    # Keyword matching
    match_result, matched, negatives = match_keywords(text, config)

    return {
        "industry_match": match_result,
        "matched_keywords": "; ".join(matched) if matched else "",
        "negative_keywords": "; ".join(negatives) if negatives else "",
        "homepage_snippet": (text or "")[:300],
        "resolved_name": resolved_name if resolved_name else raw_name,
        "name_source": name_source,
    }


def run_layer2(rows, api_key, config):
    """Run homepage extraction + keyword matching on live domains."""
    # Only process rows with live domains
    live_rows = [r for r in rows if r["domain_status"] in ("live", "live_http_only", "redirect", "needs_get")]
    log.info(f"Layer 2: Processing {len(live_rows)} live domains...")

    if not api_key:
        log.warning("No ZenRows API key provided. Using free fallback (less reliable).")

    workers = config["concurrency"]["layer2_workers"]
    delay = config["concurrency"]["layer2_delay"]

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {}
        for i, row in enumerate(live_rows):
            futures[executor.submit(process_layer2_row, row, api_key, config)] = row
            if delay > 0:
                time.sleep(delay)

        for future in tqdm(as_completed(futures), total=len(futures), desc="Layer 2: Text Extract + Keywords"):
            row = futures[future]
            try:
                result = future.result()
                row.update(result)
            except Exception as e:
                row["industry_match"] = "error"
                row["matched_keywords"] = ""
                row["homepage_snippet"] = str(e)[:200]

    # Set non-live rows
    for row in rows:
        if "industry_match" not in row:
            row["industry_match"] = "domain_dead"
            row["matched_keywords"] = ""
            row["homepage_snippet"] = ""
            row.setdefault("negative_keywords", "")
            row.setdefault("resolved_name", row.get("company_name", "") or row.get("Company Name", ""))
            row.setdefault("name_source", "domain_dead")

    # Stats
    match_counts = defaultdict(int)
    for row in rows:
        match_counts[row["industry_match"]] += 1

    log.info("Layer 2 Results:")
    for status, count in sorted(match_counts.items(), key=lambda x: -x[1]):
        log.info(f"  {status}: {count} ({count/len(rows)*100:.1f}%)")

    return rows


# ─── Output ───

def classify_row(row):
    """Final classification: PASS / FAIL / REVIEW for Clay import."""
    domain_status = row.get("domain_status", "unknown")
    industry_match = row.get("industry_match", "unknown")

    # Hard fails
    if domain_status in ("dead", "timeout", "ssl_error", "invalid_domain",
                         "not_found", "server_error", "error", "redirects_to_social"):
        return "FAIL", f"Domain: {domain_status}"

    if industry_match == "parked_domain":
        return "FAIL", "Parked/dead domain"

    if industry_match == "negative_match":
        return "FAIL", f"Negative keyword match: {row.get('negative_keywords', '')}"

    if industry_match == "no_match":
        return "FAIL", "No industry keyword matches on homepage"

    # Passes
    if industry_match == "match":
        return "PASS", f"Keywords: {row.get('matched_keywords', '')}"

    # Review cases
    if industry_match == "weak_match":
        return "REVIEW", f"Weak match: {row.get('matched_keywords', '')}"

    if industry_match in ("no_text", "error", "skipped"):
        return "REVIEW", f"Could not extract text: {industry_match}"

    return "REVIEW", f"Unclassified: {domain_status}/{industry_match}"


def write_output(rows, output_path):
    """Write classified results to CSV files (pass, fail, review)."""
    # Classify all rows
    for row in rows:
        decision, reason = classify_row(row)
        row["filter_decision"] = decision
        row["filter_reason"] = reason

    # Output columns (preserve original + add filter columns)
    filter_col_names = (
        "domain_status", "http_code", "redirect_url", "is_parked",
        "error", "industry_match", "matched_keywords",
        "negative_keywords", "homepage_snippet",
        "resolved_name", "name_source",
        "filter_decision", "filter_reason"
    )
    original_cols = [c for c in rows[0].keys()
                     if not c.startswith("_") and c not in filter_col_names]

    filter_cols = ["domain_status", "http_code", "redirect_url",
                   "industry_match", "matched_keywords", "negative_keywords",
                   "resolved_name", "name_source",
                   "homepage_snippet", "filter_decision", "filter_reason"]

    all_cols = original_cols + filter_cols

    # Write main output (all rows with classifications)
    base, ext = os.path.splitext(output_path)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_cols, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    # Write separate pass/fail/review files
    pass_rows = [r for r in rows if r["filter_decision"] == "PASS"]
    fail_rows = [r for r in rows if r["filter_decision"] == "FAIL"]
    review_rows = [r for r in rows if r["filter_decision"] == "REVIEW"]

    for suffix, subset in [("_PASS", pass_rows), ("_FAIL", fail_rows), ("_REVIEW", review_rows)]:
        path = f"{base}{suffix}{ext}"
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=all_cols, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(subset)

    log.info(f"\nFinal Results:")
    log.info(f"  PASS:   {len(pass_rows)} ({len(pass_rows)/len(rows)*100:.1f}%) → Import to Clay")
    log.info(f"  FAIL:   {len(fail_rows)} ({len(fail_rows)/len(rows)*100:.1f}%) → DNC")
    log.info(f"  REVIEW: {len(review_rows)} ({len(review_rows)/len(rows)*100:.1f}%) → Manual check")
    log.info(f"\nOutput files:")
    log.info(f"  All:    {output_path}")
    log.info(f"  Pass:   {base}_PASS{ext}")
    log.info(f"  Fail:   {base}_FAIL{ext}")
    log.info(f"  Review: {base}_REVIEW{ext}")

    return pass_rows, fail_rows, review_rows


# ─── CLI ───

def main():
    parser = argparse.ArgumentParser(
        description="Surfline Capital Pre-Filter Pipeline: Domain validation + industry keyword matching"
    )
    parser.add_argument("--input", "-i", required=True, help="Input CSV file (must have company_name and domain columns)")
    parser.add_argument("--output", "-o", required=True, help="Output CSV file path")
    parser.add_argument("--config", "-c", help="Config YAML file with industry keywords and settings")
    parser.add_argument("--api-key", help="ZenRows API key (or set ZENROWS_API_KEY env var)")
    parser.add_argument("--layer1-only", action="store_true", help="Run only Layer 1 (domain resolution)")
    parser.add_argument("--skip-layer1", action="store_true", help="Skip Layer 1 (if already run)")
    args = parser.parse_args()

    # Load config
    config = load_config(args.config)

    # Validate keywords are configured for Layer 2
    if not args.layer1_only:
        all_keywords = (
            config["industry_keywords"].get("primary", []) +
            config["industry_keywords"].get("secondary", [])
        )
        if not all_keywords:
            log.error("No industry keywords configured. Create a config YAML with industry_keywords.")
            log.error("See sample config file for template.")
            sys.exit(1)

    # Get API key
    api_key = args.api_key or os.environ.get("ZENROWS_API_KEY")

    # Read input CSV
    log.info(f"Reading input: {args.input}")
    with open(args.input, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    log.info(f"Loaded {len(rows)} rows")

    # Normalize column names (handle common variations)
    for row in rows:
        if "website" in row and "domain" not in row:
            row["domain"] = row["website"]
        if "Domain" in row and "domain" not in row:
            row["domain"] = row["Domain"]
        if "Website" in row and "domain" not in row:
            row["domain"] = row["Website"]
        if "Company Name" in row and "company_name" not in row:
            row["company_name"] = row["Company Name"]
        if "CompanyName" in row and "company_name" not in row:
            row["company_name"] = row["CompanyName"]

    # Run Layer 1
    if not args.skip_layer1:
        rows = run_layer1(rows, config)
    else:
        log.info("Skipping Layer 1 (--skip-layer1 flag)")

    # Run Layer 2
    if not args.layer1_only:
        rows = run_layer2(rows, api_key, config)
    else:
        log.info("Skipping Layer 2 (--layer1-only flag)")
        for row in rows:
            row.setdefault("industry_match", "not_run")
            row.setdefault("matched_keywords", "")
            row.setdefault("negative_keywords", "")
            row.setdefault("homepage_snippet", "")
            row.setdefault("resolved_name", row.get("company_name", "") or row.get("Company Name", ""))
            row.setdefault("name_source", "not_run")

    # Write output
    write_output(rows, args.output)


if __name__ == "__main__":
    main()
