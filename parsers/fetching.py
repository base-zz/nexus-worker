"""app/parsers/fetching.py — Shared fetch utilities for content parsers.

Provides multiple fetch strategies (requests, Playwright, fallback chain)
plus HTML post-processing helpers like Cloudflare email deobfuscation.

All parsers import from here rather than duplicating fetch logic.
"""

from __future__ import annotations

import re
from typing import Any

import requests


def fetch_with_requests(url: str, timeout: int = 30) -> str:
    """Fetch HTML via plain HTTP GET.

    Args:
        url: Target URL (HTTP upgraded to HTTPS automatically)
        timeout: Request timeout in seconds

    Returns:
        Raw HTML string

    Raises:
        requests.RequestException: On network or HTTP failure
    """
    if url.startswith("http://"):
        url = "https://" + url[7:]

    response = requests.get(
        url,
        timeout=timeout,
        headers={"User-Agent": "Mozilla/5.0"},
    )
    response.raise_for_status()
    return response.text


def fetch_with_playwright(
    url: str,
    browser: Any | None = None,
    timeout_ms: int = 30000,
) -> str:
    """Fetch HTML via Playwright for JS-rendered or bot-protected sites.

    Args:
        url: Target URL
        browser: Shared Playwright browser instance (launches new if None)
        timeout_ms: Page navigation timeout in milliseconds

    Returns:
        Raw HTML string

    Raises:
        ImportError: If playwright package is not installed
        Exception: On navigation or fetch failure
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise ImportError(
            "Playwright not installed. Install with: pip install playwright"
        ) from exc

    if browser:
        page = browser.new_page()
        page.goto(url, timeout=timeout_ms)
        html = page.content()
        page.close()
        return html

    with sync_playwright() as p:
        browser_instance = p.chromium.launch(headless=True)
        page = browser_instance.new_page()
        page.goto(url, timeout=timeout_ms)
        html = page.content()
        browser_instance.close()
        return html


def fetch_with_fallback(
    url: str,
    *,
    browser: Any | None = None,
    request_timeout: int = 30,
    playwright_timeout: int = 30000,
) -> tuple[str, str]:
    """Try requests first, fall back to Playwright on any failure.

    Returns:
        Tuple of (raw_html, fetch_method) where fetch_method is
        "requests" or "playwright_fallback".

    Raises:
        Exception: If both requests and Playwright fail
    """
    try:
        raw_html = fetch_with_requests(url, timeout=request_timeout)
        return raw_html, "requests"
    except Exception:
        raw_html = fetch_with_playwright(url, browser=browser, timeout_ms=playwright_timeout)
        return raw_html, "playwright_fallback"


def _decode_cf_email(encoded_string: str) -> str | None:
    """Decode a Cloudflare email protection obfuscated string.

    Cloudflare replaces email addresses with:
      <a href="/cdn-cgi/l/email-protection" data-cfemail="...">...</a>
    The data-cfemail attribute contains a hex-encoded XOR cipher.
    """
    try:
        key = int(encoded_string[:2], 16)
        decoded = "".join(
            chr(int(encoded_string[i : i + 2], 16) ^ key)
            for i in range(2, len(encoded_string), 2)
        )
        return decoded
    except (ValueError, IndexError):
        return None


def deobfuscate_cloudflare_emails(html: str) -> str:
    """Replace Cloudflare email-protection obfuscation with real email addresses.

    Searches for both data-cfemail attributes and inline encoded spans,
    decodes them, and replaces in the HTML string.
    """
    # Pattern 1: data-cfemail attribute
    pattern1 = re.compile(
        r'<a[^>]*href="/cdn-cgi/l/email-protection"[^>]*data-cfemail="([a-f0-9]+)"[^>]*>(.*?)</a>',
        re.IGNORECASE | re.DOTALL,
    )
    html = pattern1.sub(
        lambda m: _decode_cf_email(m.group(1)) or m.group(0), html
    )

    # Pattern 2: standalone cf-email spans
    pattern2 = re.compile(
        r'<span[^>]*class="__cf_email__"[^>]*data-cfemail="([a-f0-9]+)"[^>]*>(.*?)</span>',
        re.IGNORECASE | re.DOTALL,
    )
    html = pattern2.sub(
        lambda m: _decode_cf_email(m.group(1)) or m.group(0), html
    )

    return html
