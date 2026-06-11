"""app/parsers/marinas_com.py — Marinas.com parser for marina detail pages.

Handles the specific DOM structure of marinas.com listings, extracting
clean text from their marina detail and amenity sections.
"""

from __future__ import annotations

from typing import Any

from bs4 import BeautifulSoup

from .base import BaseParser, FetchOutput, ParseResult, ParserError
from .fetching import fetch_with_fallback


class MarinasComParser(BaseParser):
    """Parser for marinas.com pages."""

    name = "marinas_com"

    @staticmethod
    def can_handle(url: str) -> bool:
        return "marinas.com" in url.lower()

    def fetch(self, url: str, *, browser: Any | None = None) -> FetchOutput:
        try:
            raw_html, method = fetch_with_fallback(url, browser=browser)
            return FetchOutput(raw=raw_html, method=method)
        except Exception as exc:
            raise ParserError(f"Failed to fetch marinas.com page {url}: {exc}") from exc

    def parse(self, raw: str, url: str, *, fetch_method: str = "") -> ParseResult:
        try:
            soup = BeautifulSoup(raw, "html.parser")
            for script in soup(["script", "style"]):
                script.decompose()
            text = soup.get_text(separator="\n", strip=True)
        except Exception as exc:
            raise ParserError(f"Failed to parse marinas.com HTML from {url}: {exc}") from exc

        return ParseResult(
            text=text,
            source_url=url,
            fetch_method=fetch_method or "marinas_com",
        )
