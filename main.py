import argparse
import csv
import hashlib
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

import pandas as pd
import requests
import yaml
from dateutil import parser as date_parser

from sites import SiteRegistry
from sites.base import DetailInfo, SearchResult, SiteAdapter


@dataclass
class RowItem:
    file_name: str
    url: str
    publish_time: datetime


def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def setup_logging(log_path: str) -> None:
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
    if not os.path.exists(path):
        return {"items": []}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_index(path: str, index_data: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(index_data, f, ensure_ascii=False, indent=2)


def safe_filename(value: str) -> str:
    return "".join(ch for ch in value if ch not in "\\/:*?\"<>|").strip() or "file"


def extract_filename_from_url(url: str) -> Optional[str]:
    parsed = urlparse(url)
    name = os.path.basename(parsed.path)
    return name or None


def sha256_file(path: str) -> str:
    hasher = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def ensure_unique_path(path: str) -> str:
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
    name = extract_filename_from_url(url)
    if name:
        return safe_filename(name)
    parsed = urlparse(url)
    ext = os.path.splitext(parsed.path)[1]
    suffix = f"{ext}" if ext else ""
    return safe_filename(f"{fallback_prefix}_{index}{suffix}")


def append_csv(path: str, row: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    file_exists = os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def download_result(
    result: SearchResult,
    adapter: SiteAdapter,
    download_root: str,
    domain: str,
    index_data: Dict[str, Any],
    timeout_seconds: int,
    user_agent: str,
) -> Optional[Tuple[str, Optional[str]]]:
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

    downloaded_any = False
    main_path: Optional[str] = None

    if detail_info.html:
        detail_path = os.path.join(target_dir, "detail.html")
        saved_path = write_html(detail_info.html, detail_path, result.url)
        if saved_path:
            downloaded_any = True
            main_path = saved_path
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
    filtered: List[SearchResult] = []
    for item in results:
        if not item.publish_time:
            continue
        if item.publish_time > since:
            filtered.append(item)
    return filtered


def format_time(value: Optional[datetime]) -> str:
    if not value:
        return "None"
    return value.strftime("%Y-%m-%d %H:%M:%S")


def run(config_path: str, dry_run: Optional[bool]) -> None:
    config = load_config(config_path)
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
    parser = argparse.ArgumentParser(description="自动检测并下载网站更新文件")
    parser.add_argument(
        "--config",
        default="f:/SDXH_Work_Proj/auto_detect_download/config.yaml",
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
    main()
