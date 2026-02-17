"""
主程序入口与核心流程。

功能概要：
- 读取 YAML 配置、Excel 需爬虫网址汇总
- 初始化站点适配器注册表，按域名选择 GenericHtmlAdapter 或 PlaywrightRuleAdapter
- 对每行执行搜索、筛选发布时间晚于文档时间的候选、下载详情页与附件
- 维护下载索引 JSON 去重，记录成功/失败到 CSV
"""

import argparse
import csv
import hashlib
import json
import logging
import os
import sys
import traceback
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

import pandas as pd
import requests
import yaml
from bs4 import BeautifulSoup, NavigableString, Tag
from dateutil import parser as date_parser

from sites import SiteRegistry
from sites.base import DetailInfo, SearchResult, SiteAdapter


def runtime_base_dir() -> str:
    """返回运行目录（源码/打包环境均可用）。"""
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


def ensure_playwright_env(base_dir: str) -> None:
    """若发布包内存在浏览器目录，设置环境变量供 Playwright 使用。"""
    browser_dir = os.path.join(base_dir, "ms-playwright_browsers")
    if os.path.isdir(browser_dir):
        os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", browser_dir)


def resolve_config_path(base_dir: str, path: str) -> str:
    """相对路径则相对于 base_dir 解析，绝对路径原样返回。"""
    if not path or not isinstance(path, str):
        return path
    path = path.strip()
    if os.path.isabs(path):
        return os.path.normpath(path)
    return os.path.normpath(os.path.join(base_dir, path))


def resolve_config_paths(config: Dict[str, Any], base_dir: str) -> None:
    """将 config 中的路径项解析为绝对路径（相对路径相对于 base_dir）。"""
    path_keys = (
        "excel_path",
        "download_root",
        "log_path",
        "index_path",
        "failures_path",
        "success_path",
    )
    for key in path_keys:
        if key in config and config[key]:
            config[key] = resolve_config_path(base_dir, config[key])


def resolve_excel_input_path(base_dir: str, excel_path: str) -> str:
    """兼容发布包中 Excel 放置位置差异，返回可用路径。"""
    if excel_path and os.path.exists(excel_path):
        return excel_path
    if not excel_path:
        return excel_path

    # 常见场景：用户把 Excel 放在 release 根目录，而不是配置中的子目录。
    fallback = os.path.join(base_dir, os.path.basename(excel_path))
    if os.path.exists(fallback):
        logging.warning("excel_path 不存在，改用根目录同名文件：%s", fallback)
        return fallback
    return excel_path


@dataclass
class RowItem:
    """Excel 中一行的解析结果。"""

    file_name: str  # 文件名/需搜索的关键词
    url: str  # 目标网站地址
    publish_time: datetime  # 文档中记录的发布时间，用于筛选更新


def load_config(path: str) -> Dict[str, Any]:
    """读取并解析 YAML 配置文件。"""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def setup_logging(log_path: str) -> None:
    """初始化日志输出与日志文件目录。"""
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """统一 Excel 列名为：文件名/网址/发布时间。"""
    columns = {c.strip(): c for c in df.columns}
    file_name_col = None
    url_col = None
    time_col = None
    for name in columns:
        if "文件名" in name:
            file_name_col = columns[name]
        if "网址" in name or "网站" in name or "链接" in name:
            url_col = columns[name]
        if "发布" in name or "时间" in name:
            time_col = columns[name]

    if not file_name_col or not url_col or not time_col:
        raise ValueError("Excel缺少必要列：文件名/网址/发布时间")

    return df.rename(
        columns={
            file_name_col: "文件名",
            url_col: "网址",
            time_col: "发布时间",
        }
    )


def parse_excel_date(value: Any) -> Optional[datetime]:
    """解析 Excel 单元格日期为 datetime。"""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, datetime):
        return value
    try:
        if isinstance(value, (int, float)):
            return datetime.fromordinal(datetime(1900, 1, 1).toordinal() + int(value) - 2)
    except (ValueError, OverflowError):
        pass
    try:
        return date_parser.parse(str(value), fuzzy=True)
    except (ValueError, OverflowError):
        return None


def read_rows(excel_path: str) -> List[RowItem]:
    """读取 Excel 行并转换为 RowItem 列表。"""
    df = pd.read_excel(excel_path, engine="openpyxl")
    df = normalize_columns(df)
    rows: List[RowItem] = []
    for _, row in df.iterrows():
        file_name = str(row.get("文件名") or "").strip()
        url = str(row.get("网址") or "").strip()
        publish_time = parse_excel_date(row.get("发布时间"))
        if not file_name or not url or not publish_time:
            logging.warning("跳过空行或时间解析失败：%s", row.to_dict())
            continue
        rows.append(RowItem(file_name=file_name, url=url, publish_time=publish_time))
    return rows


def ensure_index(path: str) -> Dict[str, Any]:
    """读取下载索引，若不存在则返回空结构。"""
    if not os.path.exists(path):
        return {"items": []}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_index(path: str, index_data: Dict[str, Any]) -> None:
    """保存下载索引到 JSON 文件。"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(index_data, f, ensure_ascii=False, indent=2)


def safe_filename(value: str) -> str:
    """清理不合法文件名字符并回退默认名。"""
    return "".join(ch for ch in value if ch not in "\\/:*?\"<>|").strip() or "file"


def extract_filename_from_url(url: str) -> Optional[str]:
    """从 URL 路径中提取文件名。"""
    parsed = urlparse(url)
    name = os.path.basename(parsed.path)
    return name or None


def sha256_file(path: str) -> str:
    """计算文件的 SHA256 哈希。"""
    hasher = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def ensure_unique_path(path: str) -> str:
    """若目标路径已存在，生成不冲突的新路径。"""
    if not os.path.exists(path):
        return path
    base, ext = os.path.splitext(path)
    counter = 1
    while True:
        candidate = f"{base}_{counter}{ext}"
        if not os.path.exists(candidate):
            return candidate
        counter += 1


def build_file_name_from_url(url: str, fallback_prefix: str, index: int) -> str:
    """根据 URL 或兜底前缀生成附件文件名。"""
    name = extract_filename_from_url(url)
    if name:
        return safe_filename(name)
    parsed = urlparse(url)
    ext = os.path.splitext(parsed.path)[1]
    suffix = f"{ext}" if ext else ""
    return safe_filename(f"{fallback_prefix}_{index}{suffix}")


def append_csv(path: str, row: Dict[str, Any]) -> None:
    """向 CSV 追加一行（必要时写表头）。"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    file_exists = os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def _normalize_inline_text(value: str) -> str:
    """压缩行内空白，避免 Markdown 产生多余空格。"""
    return " ".join((value or "").split())


def _inline_to_markdown(node: Any) -> str:
    """将行内 HTML 节点转换为 Markdown。"""
    if isinstance(node, NavigableString):
        return _normalize_inline_text(str(node))
    if not isinstance(node, Tag):
        return ""
    name = (node.name or "").lower()
    if name == "br":
        return "\n"
    children_text = "".join(_inline_to_markdown(child) for child in node.children).strip()
    if name in {"strong", "b"}:
        return f"**{children_text}**" if children_text else ""
    if name in {"em", "i"}:
        return f"*{children_text}*" if children_text else ""
    if name == "a":
        href = (node.get("href") or "").strip()
        label = children_text or _normalize_inline_text(node.get_text(" ", strip=True))
        if href and label:
            return f"[{label}]({href})"
        return label
    return children_text


def _has_block_descendants(node: Tag) -> bool:
    """判断节点是否包含块级子孙节点。"""
    block_tags = {"h1", "h2", "h3", "h4", "h5", "h6", "p", "ul", "ol", "li", "table", "blockquote"}
    for child in node.find_all(True):
        if (child.name or "").lower() in block_tags:
            return True
    return False


def _html_to_markdown(html: str) -> str:
    """将 HTML 内容转换为 Markdown 文本。"""
    soup = BeautifulSoup(html, "html.parser")
    for junk in soup.select("script,style,noscript"):
        junk.decompose()
    root = soup.body or soup

    lines: List[str] = []

    def walk(node: Tag) -> None:
        for child in node.children:
            if isinstance(child, NavigableString):
                text = _normalize_inline_text(str(child))
                if text:
                    lines.append(text)
                continue
            if not isinstance(child, Tag):
                continue
            name = (child.name or "").lower()
            if name in {"script", "style", "noscript"}:
                continue
            if name in {"h1", "h2", "h3", "h4", "h5", "h6"}:
                level = int(name[1])
                text = _normalize_inline_text(child.get_text(" ", strip=True))
                if text:
                    lines.append(f"{'#' * level} {text}")
                continue
            if name == "p":
                text = _inline_to_markdown(child).strip()
                if text:
                    lines.append(text)
                continue
            if name in {"ul", "ol"}:
                ordered = name == "ol"
                idx = 1
                for li in child.find_all("li", recursive=False):
                    item = _inline_to_markdown(li).strip()
                    if not item:
                        continue
                    marker = f"{idx}. " if ordered else "- "
                    lines.append(f"{marker}{item}")
                    idx += 1
                continue
            if name == "blockquote":
                text = _inline_to_markdown(child).strip()
                if text:
                    lines.append(f"> {text}")
                continue
            if name == "div":
                if _has_block_descendants(child):
                    walk(child)
                else:
                    text = _inline_to_markdown(child).strip()
                    if text:
                        lines.append(text)
                continue
            walk(child)

    walk(root)
    return "\n\n".join(line for line in lines if line).strip()


def download_result(
    result: SearchResult,  # 待下载的搜索结果
    adapter: SiteAdapter,  # 站点适配器
    download_root: str,  # 下载根目录
    domain: str,  # 站点域名（用于分目录）
    index_data: Dict[str, Any],  # 下载索引（用于去重）
    timeout_seconds: int,  # 请求超时
    user_agent: str,  # HTTP User-Agent
) -> Optional[Tuple[str, Optional[str]]]:
    """下载搜索结果与附件，并写入索引。"""
    existing_urls = {item.get("url") for item in index_data.get("items", [])}
    existing_hashes = {item.get("sha256") for item in index_data.get("items", [])}
    if result.url in existing_urls:
        logging.info("已下载过，跳过：%s", result.url)
        return None

    detail_info: DetailInfo = adapter.fetch_detail_info(result)
    publish_date = result.publish_time.strftime("%Y%m%d") if result.publish_time else "unknown_date"
    folder_title = safe_filename(detail_info.title or result.title)
    target_dir = os.path.join(download_root, domain, publish_date, folder_title)
    os.makedirs(target_dir, exist_ok=True)

    headers = {"User-Agent": user_agent}

    def record_file(url: str, path: str, file_hash: str, kind: str) -> None:
        """记录下载条目并更新去重集合。"""
        index_data.setdefault("items", []).append(
            {
                "title": detail_info.title or result.title,
                "url": url,
                "publish_time": result.publish_time.isoformat() if result.publish_time else None,
                "path": path,
                "sha256": file_hash,
                "downloaded_at": datetime.now().isoformat(),
                "kind": kind,
            }
        )
        existing_urls.add(url)
        existing_hashes.add(file_hash)

    def download_url(url: str, target_path: str, kind: str) -> Optional[str]:
        """下载指定 URL 到本地路径并去重。"""
        if url in existing_urls:
            logging.info("已下载过，跳过：%s", url)
            return None
        target_path = ensure_unique_path(target_path)
        resp = requests.get(url, headers=headers, timeout=timeout_seconds, stream=True)
        resp.raise_for_status()
        with open(target_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        file_hash = sha256_file(target_path)
        if file_hash in existing_hashes:
            os.remove(target_path)
            logging.info("重复文件hash，已删除：%s", url)
            return None
        record_file(url, target_path, file_hash, kind)
        return target_path

    def write_html(html: str, target_path: str, url: str) -> Optional[str]:
        """写入详情页 HTML 并去重。"""
        target_path = ensure_unique_path(target_path)
        with open(target_path, "w", encoding="utf-8") as f:
            f.write(html)
        file_hash = sha256_file(target_path)
        if file_hash in existing_hashes:
            os.remove(target_path)
            logging.info("重复文件hash，已删除：%s", url)
            return None
        record_file(url, target_path, file_hash, "detail_html")
        return target_path

    def write_markdown(html: str, html_path: str, url: str) -> Optional[str]:
        """将 HTML 转换为 Markdown 并落盘。"""
        markdown = _html_to_markdown(html)
        if not markdown:
            return None
        md_path = ensure_unique_path(f"{os.path.splitext(html_path)[0]}.md")
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(markdown)
        file_hash = sha256_file(md_path)
        if file_hash in existing_hashes:
            os.remove(md_path)
            logging.info("重复Markdown hash，已删除：%s", md_path)
            return None
        record_file(url, md_path, file_hash, "detail_markdown")
        return md_path

    downloaded_any = False
    main_path: Optional[str] = None

    if detail_info.html:
        detail_path = os.path.join(target_dir, f"detail{folder_title}.html")
        saved_path = write_html(detail_info.html, detail_path, result.url)
        if saved_path:
            md_saved_path = write_markdown(detail_info.html, saved_path, result.url)
            downloaded_any = True
            main_path = md_saved_path or saved_path
    else:
        file_name = extract_filename_from_url(result.url) or safe_filename(result.title)
        file_name = safe_filename(file_name)
        target_path = os.path.join(target_dir, file_name)
        saved_path = download_url(result.url, target_path, "direct_file")
        if saved_path:
            downloaded_any = True
            main_path = saved_path

    for idx, attachment in enumerate(detail_info.attachments or [], start=1):
        if isinstance(attachment, dict):
            attachment_url = str(attachment.get("url") or "").strip()
            attachment_name = str(attachment.get("name") or "").strip()
        else:
            attachment_url = str(attachment or "").strip()
            attachment_name = ""
        if not attachment_url:
            continue

        if attachment_name:
            file_name = safe_filename(attachment_name)
            ext = os.path.splitext(file_name)[1]
            if not ext:
                url_ext = os.path.splitext(urlparse(attachment_url).path)[1]
                if url_ext:
                    file_name = f"{file_name}{url_ext}"
        else:
            file_name = build_file_name_from_url(attachment_url, "attachment", idx)

        target_path = os.path.join(target_dir, file_name)
        try:
            saved_path = download_url(attachment_url, target_path, "attachment")
            if saved_path:
                downloaded_any = True
        except Exception as exc:
            logging.error("附件下载失败：%s %s", attachment_url, exc)
            continue

    if not downloaded_any:
        return None
    return target_dir, main_path


def filter_newer_results(results: Iterable[SearchResult], since: datetime) -> List[SearchResult]:
    """过滤出发布时间晚于给定时间的结果。"""
    filtered: List[SearchResult] = []
    for item in results:
        if not item.publish_time:
            continue
        if item.publish_time > since:
            filtered.append(item)
    return filtered


def format_time(value: Optional[datetime]) -> str:
    """将时间格式化为统一字符串。"""
    if not value:
        return "None"
    return value.strftime("%Y-%m-%d %H:%M:%S")


def run(config_path: str, dry_run: Optional[bool]) -> None:
    """执行主流程：搜索、筛选、下载并记录。"""
    config = load_config(config_path)
    base_dir = runtime_base_dir()
    ensure_playwright_env(base_dir)
    resolve_config_paths(config, base_dir)
    config["excel_path"] = resolve_excel_input_path(base_dir, config.get("excel_path", ""))
    dry_run = config.get("dry_run") if dry_run is None else dry_run

    setup_logging(config["log_path"])
    rows = read_rows(config["excel_path"])
    registry = SiteRegistry()

    index_data = ensure_index(config["index_path"])

    overrides = config.get("site_overrides", {})

    for row in rows:
        domain = urlparse(row.url).netloc
        search_template = None
        adapter_name = None
        adapter_config = None
        if domain in overrides:
            override = overrides[domain] or {}
            search_template = override.get("search_url")
            adapter_name = override.get("adapter")
            adapter_config = override

        adapter = registry.ensure_generic(
            url=row.url,
            timeout_seconds=config["request_timeout_seconds"],
            user_agent=config["user_agent"],
            search_url_template=search_template,
            adapter_name=adapter_name,
            adapter_config=adapter_config,
        )

        try:
            results = list(adapter.search(row.file_name))
        except Exception as exc:
            logging.error("搜索失败：%s %s", row.url, exc)
            append_csv(
                config["failures_path"],
                {
                    "file_name": row.file_name,
                    "url": row.url,
                    "reason": f"search_error: {exc}",
                    "time": datetime.now().isoformat(),
                },
            )
            continue

        logging.info("-" * 80)
        latest_publish_time = max(
            (item.publish_time for item in results if item.publish_time),
            default=None,
        )
        if results:
            logging.info(
                "搜索到 %d 条候选：%s | 文档时间=%s | 网页最新时间=%s",
                len(results),
                row.file_name,
                format_time(row.publish_time),
                format_time(latest_publish_time),
            )
            for result in results:
                logging.info(
                    "候选时间对比：标题=%s | 发布时间=%s | 文档时间=%s | 链接=%s",
                    result.title,
                    format_time(result.publish_time),
                    format_time(row.publish_time),
                    result.url,
                )
        else:
            logging.info(
                "未搜索到候选：%s | 文档时间=%s | 网页最新时间=%s",
                row.file_name,
                format_time(row.publish_time),
                format_time(latest_publish_time),
            )

        newer = filter_newer_results(results, row.publish_time)
        if not newer:
            logging.info("未找到更晚发布的文件：%s", row.file_name)
            continue

        for result in newer:
            if dry_run:
                logging.info("匹配到候选（dry-run）：%s -> %s", result.title, result.url)
                continue

            try:
                outcome = download_result(
                    result=result,
                    adapter=adapter,
                    download_root=config["download_root"],
                    domain=domain,
                    index_data=index_data,
                    timeout_seconds=config["request_timeout_seconds"],
                    user_agent=config["user_agent"],
                )
                if outcome:
                    folder_path, main_path = outcome
                    logging.info("下载完成：%s", folder_path)
                    append_csv(
                        config["success_path"],
                        {
                            "file_name": row.file_name,
                            "url": result.url,
                            "path": main_path,
                            "folder_path": folder_path,
                            "main_path": main_path,
                            "publish_time": result.publish_time.isoformat()
                            if result.publish_time
                            else None,
                            "time": datetime.now().isoformat(),
                        },
                    )
            except Exception as exc:
                logging.error("下载失败：%s %s", result.url, exc)
                append_csv(
                    config["failures_path"],
                    {
                        "file_name": row.file_name,
                        "url": result.url,
                        "reason": f"download_error: {exc}",
                        "time": datetime.now().isoformat(),
                    },
                )

    save_index(config["index_path"], index_data)


def main() -> None:
    """命令行入口。"""
    parser = argparse.ArgumentParser(description="自动检测并下载网站更新文件")
    parser.add_argument(
        "--config",
        default=os.path.join(runtime_base_dir(), "config.yaml"),
        help="配置文件路径",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="只检索不下载",
    )
    args = parser.parse_args()
    run(args.config, dry_run=True if args.dry_run else None)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        if getattr(sys, "frozen", False):
            try:
                input("\n程序异常退出，按回车键关闭窗口...")
            except EOFError:
                pass
        raise
