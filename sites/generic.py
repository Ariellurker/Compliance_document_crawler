import os
import re
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import quote_plus, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from dateutil import parser as date_parser
from playwright.sync_api import sync_playwright

from .base import DetailInfo, SearchResult, SiteAdapter

DATE_REGEXES = [
    re.compile(r"\d{4}[./-]\d{1,2}[./-]\d{1,2}"),
    re.compile(r"\d{4}年\d{1,2}月\d{1,2}日"),
]

DEFAULT_ATTACHMENT_EXTENSIONS = [
    "pdf",
    "doc",
    "docx",
    "xls",
    "xlsx",
    "zip",
    "rar",
    "7z",
    "csv",
    "ppt",
    "pptx",
]

DEFAULT_TITLE_SELECTORS = ["h1", "title"]
DEFAULT_ATTACHMENT_SELECTORS = ["a[href]"]


def _normalize_text(value: str) -> str:
    """去除空白并小写化文本用于匹配。"""
    return re.sub(r"\s+", "", value or "").lower()


def _matches_keywords(text: str, keyword: str) -> bool:
    """判断文本是否包含关键词（忽略空白与大小写）。"""
    normalized_text = _normalize_text(text)
    normalized_keyword = _normalize_text(keyword)
    if not normalized_keyword:
        return False
    return normalized_keyword in normalized_text


def _extract_dates(text: str) -> List[datetime]:
    """从文本中提取所有日期。"""
    dates: List[datetime] = []
    for regex in DATE_REGEXES:
        for match in regex.findall(text):
            try:
                dt = date_parser.parse(match, fuzzy=True)
                dates.append(dt)
            except (ValueError, OverflowError):
                continue
    return dates


def _best_date(text: str) -> Optional[datetime]:
    """从文本中挑选最新日期。"""
    dates = _extract_dates(text)
    if not dates:
        return None
    return max(dates)


def _normalize_selectors(values: Any, fallback: List[str]) -> List[str]:
    """规范化 CSS 选择器配置。"""
    if not values:
        return list(fallback)
    if isinstance(values, str):
        values = [values]
    return [str(item) for item in values if str(item).strip()]


def _apply_response_encoding(resp: requests.Response) -> None:
    """尽可能修正响应编码以便解析。"""
    encoding = (resp.encoding or "").lower()
    if not encoding or encoding in ("iso-8859-1", "latin-1"):
        apparent = getattr(resp, "apparent_encoding", None)
        if apparent:
            resp.encoding = apparent


def _normalize_extensions(values: Optional[Iterable[str]]) -> set:
    """规范化附件后缀集合。"""
    items = list(values or [])
    if not items:
        items = DEFAULT_ATTACHMENT_EXTENSIONS
    normalized = set()
    for item in items:
        text = str(item).lower().strip().lstrip(".")
        if text:
            normalized.add(text)
    return normalized


def _normalize_text_keywords(values: Optional[Iterable[str]]) -> List[str]:
    """规范化附件文本关键词列表。"""
    if not values:
        return []
    return [str(item).strip().lower() for item in values if str(item).strip()]


def _is_attachment_url(url: str, extensions: set) -> bool:
    """判断链接是否为附件链接。"""
    if not url:
        return False
    lowered = url.lower()
    if lowered.startswith(("javascript:", "mailto:", "#")):
        return False
    parsed = urlparse(lowered)
    ext = os.path.splitext(parsed.path)[1].lstrip(".")
    if ext and ext in extensions:
        return True
    for item in extensions:
        if f".{item}" in lowered:
            return True
    return False


def _clean_attachment_name(text: str) -> str:
    """清理附件名称前缀与多余空白。"""
    if not text:
        return ""
    cleaned = re.sub(r"^\s*附件\s*\d*\s*[:：]?\s*", "", text.strip())
    return cleaned.strip()


def _extract_title(soup: BeautifulSoup, selectors: List[str]) -> Optional[str]:
    """从页面中提取标题。"""
    for selector in selectors:
        node = soup.select_one(selector)
        if not node:
            continue
        if node.name == "meta":
            content = node.get("content")
            if content:
                return content.strip()
        text = node.get_text(" ", strip=True)
        if text:
            return text
    og_title = soup.select_one("meta[property='og:title']")
    if og_title and og_title.get("content"):
        return og_title.get("content", "").strip()
    if soup.title and soup.title.string:
        return soup.title.string.strip()
    return None


def _extract_attachments(
    soup: BeautifulSoup,
    base_url: str,
    selectors: List[str],
    extensions: Optional[Iterable[str]],
    text_keywords: Optional[Iterable[str]] = None,
) -> List[str]:
    """从详情页中提取附件链接与名称。"""
    normalized_exts = _normalize_extensions(extensions)
    normalized_keywords = _normalize_text_keywords(text_keywords)
    results: List[str] = []
    seen = set()
    for selector in selectors:
        for node in soup.select(selector):
            href = node.get("href")
            if not href:
                continue
            href = href.strip()
            if not href:
                continue
            full_url = urljoin(base_url, href)
            if not _is_attachment_url(full_url, normalized_exts):
                continue
            if normalized_keywords:
                node_text = node.get_text(" ", strip=True)
                parent_text = node.parent.get_text(" ", strip=True) if node.parent else ""
                combined_text = f"{node_text} {parent_text}".lower()
                if not any(keyword in combined_text for keyword in normalized_keywords):
                    continue
            if full_url in seen:
                continue
            seen.add(full_url)
            name = node.get_text(" ", strip=True) or node.get("title") or node.get("aria-label")
            name = _clean_attachment_name(name)
            if name:
                results.append({"url": full_url, "name": name})
            else:
                results.append({"url": full_url})
    return results


class GenericHtmlAdapter(SiteAdapter):
    def __init__(
        self,
        base_url: str,
        timeout_seconds: int,
        user_agent: str,
        search_url_template: Optional[str] = None,
        rules: Optional[Dict[str, Any]] = None,
    ):
        """初始化通用 HTML 适配器。"""
        super().__init__(base_url, timeout_seconds, user_agent)
        self.search_url_template = search_url_template
        self.rules = rules or {}
        self.detail_page_rules = self.rules.get("detail_page", {}) or {}

    def _fetch_detail_html(self, url: str, fetch_mode: str) -> Optional[str]:
        """获取详情页 HTML（支持 requests/playwright）。"""
        if fetch_mode == "playwright":
            timeout_ms = self.timeout_seconds * 1000
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(headless=True)
                page = browser.new_page(user_agent=self.user_agent)
                page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                page.wait_for_timeout(500)
                html = page.content()
                browser.close()
                return html
        headers = {"User-Agent": self.user_agent}
        resp = requests.get(url, headers=headers, timeout=self.timeout_seconds)
        resp.raise_for_status()
        content_type = resp.headers.get("content-type", "").lower()
        if "application/pdf" in content_type:
            return None
        _apply_response_encoding(resp)
        resp.encoding = resp.encoding or "utf-8"
        return resp.text

    def _build_url(self, file_name: str) -> str:
        """构建搜索 URL。"""
        if self.search_url_template:
            return self.search_url_template.format(query=quote_plus(file_name))
        if "{query}" in self.base_url:
            return self.base_url.format(query=quote_plus(file_name))
        return self.base_url

    def search(self, file_name: str) -> Iterable[SearchResult]:
        """执行搜索并解析候选结果。"""
        url = self._build_url(file_name)
        headers = {"User-Agent": self.user_agent}
        resp = requests.get(url, headers=headers, timeout=self.timeout_seconds)
        resp.raise_for_status()
        _apply_response_encoding(resp)
        soup = BeautifulSoup(resp.text, "html.parser")

        results: List[SearchResult] = []
        for link in soup.find_all("a", href=True):
            link_text = link.get_text(" ", strip=True)
            parent_text = link.parent.get_text(" ", strip=True) if link.parent else ""
            combined_text = f"{link_text} {parent_text}"
            if not _matches_keywords(combined_text, file_name):
                continue

            href = link["href"]
            full_url = urljoin(url, href)
            date_context = f"{combined_text} {href}"
            publish_time = _best_date(date_context)

            title = link_text or file_name
            results.append(SearchResult(title=title, url=full_url, publish_time=publish_time))
        return results

    def fetch_detail_info(self, result: SearchResult) -> DetailInfo:
        """获取详情页内容及附件列表。"""
        rules = self.detail_page_rules or {}
        enabled = rules.get("enabled", True)
        if not enabled:
            return DetailInfo(title=result.title, html=None, attachments=[])

        extensions = _normalize_extensions(rules.get("attachment_extensions"))
        if _is_attachment_url(result.url, extensions):
            return DetailInfo(title=result.title, html=None, attachments=[])

        fetch_mode = rules.get("fetch_mode") or "requests"
        html = self._fetch_detail_html(result.url, fetch_mode)
        if not html:
            return DetailInfo(title=result.title, html=None, attachments=[])

        soup = BeautifulSoup(html, "html.parser")
        title_selectors = _normalize_selectors(rules.get("title_selectors"), DEFAULT_TITLE_SELECTORS)
        title = _extract_title(soup, title_selectors) or result.title
        attachment_selectors = _normalize_selectors(
            rules.get("attachment_selectors"), DEFAULT_ATTACHMENT_SELECTORS
        )
        attachments = _extract_attachments(
            soup=soup,
            base_url=result.url,
            selectors=attachment_selectors,
            extensions=extensions,
            text_keywords=rules.get("attachment_text_keywords"),
        )
        return DetailInfo(title=title, html=html, attachments=attachments)
