"""app/parsers/__init__.py — Parser registry for content fetcher.

All parsers are auto-imported and registered here. To add a new parser,
create a new file in this package and import/register it below.
"""

from __future__ import annotations

from typing import Any

from .base import BaseParser, ContentFetcherError, FetchOutput, ParseResult, ParserError
from .dockwa import DockwaParser
from .generic_website import GenericWebsiteParser
from .marinas_com import MarinasComParser
from .waterway_guide import WaterwayGuideParser

# Ordered list: first match wins. GenericWebsiteParser must be LAST (catch-all).
_REGISTRY: list[type[BaseParser]] = [
    DockwaParser,
    MarinasComParser,
    WaterwayGuideParser,
    GenericWebsiteParser,
]


def get_parser(url: str) -> BaseParser:
    """Return the first parser that can_handle the given URL.

    Raises:
        ParserError: If no parser matches the URL.
    """
    for parser_cls in _REGISTRY:
        if parser_cls.can_handle(url):
            return parser_cls()

    raise ParserError(f"No parser registered for URL: {url}")


def fetch_and_parse(url: str, *, browser: Any | None = None) -> ParseResult:
    """Convenience: route URL to correct parser and return parsed result.

    Args:
        url: Target URL
        browser: Optional shared Playwright browser instance

    Returns:
        ParseResult with clean text for LLM ingestion
    """
    parser = get_parser(url)
    return parser.process(url, browser=browser)


__all__ = [
    "BaseParser",
    "ContentFetcherError",
    "FetchOutput",
    "ParseResult",
    "ParserError",
    "DockwaParser",
    "GenericWebsiteParser",
    "MarinasComParser",
    "WaterwayGuideParser",
    "get_parser",
    "fetch_and_parse",
]
