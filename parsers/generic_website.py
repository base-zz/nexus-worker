"""app/parsers/generic_website.py — Generic website parser for unknown or standard HTML sites.

Uses requests with Playwright fallback, BeautifulSoup text extraction,
and optional haulout section detection. This is the default parser when
no site-specific handler matches the URL.
"""

from __future__ import annotations

from typing import Any

from bs4 import BeautifulSoup

from .base import BaseParser, FetchOutput, ParseResult, ParserError
from .fetching import deobfuscate_cloudflare_emails, fetch_with_fallback


def _clean_text(text: str) -> str:
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    return "\n".join(chunk for chunk in chunks if chunk)


def _extract_haulout_section(soup: BeautifulSoup) -> str | None:
    haulout_heading = soup.find(
        string=lambda text: text and "Haulout Capabilities" in text
    )
    if not haulout_heading:
        return None

    haulout_parent = haulout_heading.find_parent("div")
    if not haulout_parent:
        return None

    haulout_data: list[str] = []
    for div in haulout_parent.find_all("div"):
        text = div.get_text(strip=True)
        if text and ":" in text:
            haulout_data.append(text)

    return "\n".join(haulout_data) if haulout_data else None


class GenericWebsiteParser(BaseParser):
    """Default parser for standard HTML sites."""

    name = "generic"

    @staticmethod
    def can_handle(url: str) -> bool:
        """Always returns True — this is the catch-all default."""
        return True

    def fetch(self, url: str, *, browser: Any | None = None) -> FetchOutput:
        try:
            raw_html, method = fetch_with_fallback(url, browser=browser)
        except Exception as exc:
            raise ParserError(f"Failed to fetch {url}: {exc}") from exc

        raw_html = deobfuscate_cloudflare_emails(raw_html)
        return FetchOutput(raw=raw_html, method=method)

    def parse(self, raw: str, url: str, *, fetch_method: str = "") -> ParseResult:
        try:
            soup = BeautifulSoup(raw, "html.parser")
            for script in soup(["script", "style"]):
                script.decompose()

            haulout = _extract_haulout_section(soup)
            text = _clean_text(soup.get_text())
        except Exception as exc:
            raise ParserError(f"Failed to parse HTML from {url}: {exc}") from exc

        return ParseResult(
            text=text,
            haulout_section=haulout,
            source_url=url,
            fetch_method=fetch_method or "generic",
        )
