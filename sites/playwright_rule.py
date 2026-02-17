"""
基于 Playwright 与规则配置的站点适配器。

支持通过 config 指定选择器、搜索编码、抓取模式、详情页日期提取等。
适用于需要 JS 渲染或结构较复杂的站点。
"""

import os
import re
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import quote_plus, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from dateutil import parser as date_parser
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from .base import DetailInfo, SearchResult, SiteAdapter
from .generic import _best_date, _matches_keywords


# 从详情页提取发布日期的默认正则（匹配 "发布日期：2024-01-01" 等）
DEFAULT_DETAIL_REGEXES = [
    r"(?:发布日期|发布时间|日期)[：:\s]*([0-9]{4}[./-][0-9]{1,2}[./-][0-9]{1,2})",
    r"(?:发布日期|发布时间|日期)[：:\s]*([0-9]{4}年[0-9]{1,2}月[0-9]{1,2}日)",
]

# 默认识别为附件的文件扩展名
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

# 默认标题选择器
DEFAULT_TITLE_SELECTORS = ["h1", "title"]
# 默认附件链接选择器
DEFAULT_ATTACHMENT_SELECTORS = ["a[href]"]


def _encode_query(value: str, mode: str) -> str:
    """按配置编码搜索关键词。"""
    if mode == "none":
        return value
    if mode == "double":
        return quote_plus(quote_plus(value))
    return quote_plus(value)


def _compile_regexes(values: Iterable[str]) -> List[re.Pattern]:
    """编译正则规则并忽略无效项。"""
    compiled: List[re.Pattern] = []
    for item in values:
        try:
            compiled.append(re.compile(item))
        except re.error:
            continue
    return compiled


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


def _extract_text_from_node(node: Any) -> str:
    """提取节点文本，兼容 meta 等标签。"""
    if not node:
        return ""
    if getattr(node, "name", "") == "meta":
        return (node.get("content") or "").strip()
    return node.get_text(" ", strip=True)


def _build_trimmed_html(html: str, extract_rules: Dict[str, Any]) -> Optional[str]:
    """根据配置裁剪详情页，仅保留标题、日期和正文。"""
    if not extract_rules or not extract_rules.get("enabled", False):
        return html

    soup = BeautifulSoup(html, "html.parser")
    fallback_to_original = extract_rules.get("fallback_to_original_on_empty", True)

    title_selectors = _normalize_selectors(extract_rules.get("title_selectors"), DEFAULT_TITLE_SELECTORS)
    title_text = _extract_title(soup, title_selectors) or ""

    date_selectors = _normalize_selectors(extract_rules.get("date_selectors"), [])
    date_text = ""
    for selector in date_selectors:
        node = soup.select_one(selector)
        if not node:
            continue
        date_text = _extract_text_from_node(node)
        if date_text:
            break

    body_selectors = _normalize_selectors(extract_rules.get("body_selectors"), [])
    remove_selectors = _normalize_selectors(extract_rules.get("remove_selectors"), [])
    body_blocks: List[str] = []
    for selector in body_selectors:
        nodes = soup.select(selector)
        if not nodes:
            continue
        for node in nodes:
            fragment = BeautifulSoup(str(node), "html.parser")
            for remove_selector in remove_selectors:
                for remove_node in fragment.select(remove_selector):
                    remove_node.decompose()
            rendered = str(fragment).strip()
            if rendered:
                body_blocks.append(rendered)
        if body_blocks:
            break

    if not body_blocks:
        return html if fallback_to_original else None

    out = BeautifulSoup("<html><head><meta charset='utf-8'/></head><body></body></html>", "html.parser")
    if title_text:
        title_tag = out.new_tag("h1")
        title_tag.string = title_text
        out.body.append(title_tag)
    if date_text:
        date_tag = out.new_tag("div")
        date_tag["class"] = ["publish-date"]
        date_tag.string = date_text
        out.body.append(date_tag)
    content_tag = out.new_tag("div")
    content_tag["class"] = ["content"]
    for block in body_blocks:
        block_soup = BeautifulSoup(block, "html.parser")
        for child in list(block_soup.contents):
            content_tag.append(child)
    out.body.append(content_tag)
    return str(out)


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


def _extract_detail_date(html: str, selectors: List[str], regexes: List[re.Pattern]) -> Optional[Any]:
    """从详情页 HTML 中提取发布日期。"""
    soup = BeautifulSoup(html, "html.parser")
    for selector in selectors:
        node = soup.select_one(selector)
        if not node:
            continue
        parsed = _best_date(node.get_text(" ", strip=True))
        if parsed:
            return parsed
    for regex in regexes:
        match = regex.search(html)
        if not match:
            continue
        try:
            return date_parser.parse(match.group(1), fuzzy=True)
        except (ValueError, OverflowError):
            continue
    for text in soup.stripped_strings:
        if "日期" in text or "发布时间" in text or "发布日期" in text:
            parsed = _best_date(text)
            if parsed:
                return parsed
    return None


class PlaywrightRuleAdapter(SiteAdapter):
    def __init__(
        self,
        base_url: str,  # 站点基础 URL
        timeout_seconds: int,  # 请求超时秒数
        user_agent: str,  # HTTP User-Agent
        search_url_template: Optional[str] = None,  # 搜索 URL 模板
        rules: Optional[Dict[str, Any]] = None,  # 规则配置（selectors、fetch_mode、detail_date 等）
    ):
        """初始化基于规则的 Playwright 适配器。"""
        super().__init__(base_url, timeout_seconds, user_agent)
        self.rules = rules or {}
        self.search_url_template = self.rules.get("search_url") or search_url_template
        self.query_encoding = self.rules.get("query_encoding", "single")
        self.fetch_mode = self.rules.get("fetch_mode", "playwright")
        self.selectors = self.rules.get("selectors", {}) or {}
        self.match_keyword = self.rules.get("match_keyword", True)
        self.match_in_title_only = self.rules.get("match_in_title_only", False)
        self.date_from_item = self.rules.get("date_from_item", False)
        self.link_href_contains = self.rules.get("link_href_contains")
        self.detail_rules = self.rules.get("detail_date", {}) or {}
        self.detail_enabled = self.detail_rules.get("enabled", False)
        self.detail_page_rules = self.rules.get("detail_page", {}) or {}

    def _build_url(self, file_name: str) -> str:
        """构建搜索 URL。"""
        template = self.search_url_template or self.base_url
        if "{query}" not in template:
            return template
        query = _encode_query(file_name, self.query_encoding)
        return template.format(query=query)

    def _fetch_search_html(self, url: str) -> str:
        """获取搜索页 HTML（支持 requests/playwright）。"""
        if self.fetch_mode == "requests":
            headers = {"User-Agent": self.user_agent}
            resp = requests.get(url, headers=headers, timeout=self.timeout_seconds)
            resp.raise_for_status()
            _apply_response_encoding(resp)
            resp.encoding = resp.encoding or "utf-8"
            return resp.text
        timeout_ms = self.timeout_seconds * 1000
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            page = browser.new_page(user_agent=self.user_agent)
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            wait_for = self.selectors.get("wait_for") or self.selectors.get("item")
            if wait_for:
                try:
                    page.wait_for_selector(wait_for, timeout=timeout_ms)
                except PlaywrightTimeoutError:
                    pass
            page.wait_for_timeout(1000)
            html = page.content()
            browser.close()
            return html

    def _parse_items(self, soup: BeautifulSoup) -> Iterable[Any]:
        """解析搜索列表项节点。"""
        item_selector = self.selectors.get("item")
        if item_selector:
            return soup.select(item_selector)
        return soup.find_all("a", href=True)

    def _parse_results(self, html: str, keyword: str, base_url: str) -> List[SearchResult]:
        """从搜索页 HTML 解析候选结果。"""
        soup = BeautifulSoup(html, "html.parser")
        title_selector = self.selectors.get("title", "a")
        date_selector = self.selectors.get("date")
        results: List[SearchResult] = []
        items = list(self._parse_items(soup))
        for item in items:
            link = item if item.name == "a" else item.select_one(title_selector)
            if not link or not link.get("href"):
                continue
            href = link.get("href") or ""
            if self.link_href_contains and self.link_href_contains not in href:
                continue
            title = link.get("title") or link.get_text(" ", strip=True)
            combined_text = item.get_text(" ", strip=True)
            match_text = title if self.match_in_title_only else combined_text
            if self.match_keyword and not _matches_keywords(match_text, keyword):
                continue
            full_url = urljoin(base_url, href)
            publish_time = None
            if date_selector:
                date_node = item.select_one(date_selector)
                if date_node:
                    publish_time = _best_date(date_node.get_text(" ", strip=True))
            if not publish_time and self.date_from_item:
                publish_time = _best_date(combined_text)
            results.append(SearchResult(title=title or keyword, url=full_url, publish_time=publish_time))
        return results

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

    def _fill_detail_date(self, result: SearchResult) -> None:
        """补充详情页发布日期到结果。"""
        if result.publish_time or not self.detail_enabled:
            return
        if result.url.lower().endswith(".pdf"):
            return
        selectors = self.detail_rules.get("selectors", []) or []
        regexes = _compile_regexes(self.detail_rules.get("regexes", []) or DEFAULT_DETAIL_REGEXES)
        fetch_mode = self.detail_rules.get("fetch_mode", "requests")
        html = self._fetch_detail_html(result.url, fetch_mode)
        if not html:
            return
        detail_date = _extract_detail_date(html, selectors, regexes)
        if detail_date:
            result.publish_time = detail_date

    def search(self, file_name: str) -> Iterable[SearchResult]:
        """执行搜索并按需补全日期。"""
        url = self._build_url(file_name)
        html = self._fetch_search_html(url)
        results = self._parse_results(html, file_name, url)
        for result in results:
            try:
                self._fill_detail_date(result)
            except requests.RequestException:
                continue
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

        fetch_mode = rules.get("fetch_mode") or self.fetch_mode
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
        trimmed_html = _build_trimmed_html(html, rules.get("content_extract", {}) or {})
        return DetailInfo(title=title, html=trimmed_html, attachments=attachments)
