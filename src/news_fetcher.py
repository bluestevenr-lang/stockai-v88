"""
news_fetcher.py - RSS 新闻抓取模块
读取 rss_sources.json → 抓取全部 RSS → 输出 data/news_raw.json
"""

import calendar
import json
import logging
import os
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import feedparser
import requests
from dateutil import parser as dateparser
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "config" / "rss_sources.json"
OUTPUT_PATH = BASE_DIR / "data" / "news_raw.json"
LOG_PATH = BASE_DIR / "logs" / "app.log"

load_dotenv(BASE_DIR / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("news_fetcher")

# 外媒代理：从 .env 读取，默认指向 VPS 上的 WARP SOCKS5
RSS_PROXY = os.getenv("RSS_PROXY", "socks5h://127.0.0.1:40000")

# 真实 UA 轮换，降低被拦截概率
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Feedly/1.0 (+http://www.feedly.com/fetcher.html; like FeedFetcher-Google)",
]


def load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def parsed_time_to_utc(struct_time) -> datetime | None:
    """将 feedparser 返回的 time.struct_time（已是 UTC）转为 datetime。"""
    if struct_time is None:
        return None
    try:
        ts = calendar.timegm(struct_time)   # struct_time → Unix timestamp（UTC）
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception:
        return None


def parse_date_string(raw: str) -> datetime | None:
    """用 dateutil 解析任意格式日期字符串，结果统一转 UTC。"""
    if not raw:
        return None
    try:
        dt = dateparser.parse(raw)
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)   # 无时区信息一律当 UTC
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def is_fresh(pub_utc: datetime | None, max_age_hours: int = 48,
             title: str = "") -> bool:
    """判断新闻是否在时间窗口内，输出 Debug 日志供验证。"""
    if pub_utc is None:
        return True
    now_utc = datetime.now(timezone.utc)
    age = now_utc - pub_utc
    fresh = age <= timedelta(hours=max_age_hours)
    if not fresh:
        logger.debug(
            f"过滤旧新闻 | 发布UTC: {pub_utc.isoformat()} | "
            f"当前UTC: {now_utc.isoformat()} | 时差: {age} | 标题: {title[:60]}"
        )
    return fresh


def fetch_single_source(source: dict, settings: dict, force_direct: bool = False) -> list[dict]:
    """抓取单个 RSS 源，失败重试，外媒走代理。
    force_direct=True：无视代理与环境变量强制直连——本机代理瞬断时全部源都会
    Connection reset（连"直连"中文源也因 requests trust_env 走了坏代理），此为兜底通道。"""
    name = source["name"]
    url = source["url"]
    language = source.get("language", "en")
    timeout = 10   # 固定 10s
    max_retries = settings.get("max_retries", 2)

    # 外媒（en）走代理；国内源（zh）直连；源配置里的 "proxy" 字段可显式覆盖
    # （如 Google News 中文源虽是 zh 但必须走代理）
    proxies = None
    use_proxy = source.get("proxy", language == "en")
    if use_proxy and RSS_PROXY and not force_direct:
        proxies = {"http": RSS_PROXY, "https": RSS_PROXY}
        logger.debug(f"[{name}] 使用代理: {RSS_PROXY}")

    _http = requests
    if force_direct:
        _http = requests.Session()
        _http.trust_env = False  # 忽略 http_proxy 等环境变量，真直连
        proxies = None

    ua = random.choice(USER_AGENTS)
    headers = {
        "User-Agent": ua,
        "Accept": "application/rss+xml, application/xml, text/xml, */*",
    }

    for attempt in range(1, max_retries + 1):
        try:
            resp = _http.get(url, timeout=timeout, headers=headers, proxies=proxies)
            resp.raise_for_status()
            feed = feedparser.parse(resp.content)

            if feed.bozo and not feed.entries:
                logger.warning(f"[{name}] 解析异常: {feed.bozo_exception}")
                if attempt < max_retries:
                    time.sleep(2 * attempt)
                continue

            news_items = []
            for entry in feed.entries:
                # ── 时区修复：优先用 feedparser 已解析的 UTC struct_time ──
                pub_utc = parsed_time_to_utc(entry.get("published_parsed") or entry.get("updated_parsed"))
                if pub_utc is None:
                    raw_date = entry.get("published") or entry.get("updated") or ""
                    pub_utc = parse_date_string(raw_date)
                pub_iso = pub_utc.isoformat() if pub_utc else ""

                summary = entry.get("summary") or entry.get("description") or ""
                if len(summary) > 2000:
                    summary = summary[:2000]

                tags = entry.get("tags", [])
                category = tags[0].get("term", "") if tags else ""

                news_items.append({
                    "source": name,
                    "source_category": source.get("category", ""),
                    "language": language,
                    "title": entry.get("title", "").strip(),
                    "link": entry.get("link", "").strip(),
                    "published_time": pub_iso,
                    "pub_utc": pub_utc,   # 暂存 datetime 对象，fetch_all 里用完即丢
                    "summary": summary.strip(),
                    "category": category,
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                })

            logger.info(f"[{name}] 抓取成功: {len(news_items)} 条")
            return news_items

        except requests.RequestException as e:
            logger.warning(f"[{name}] 第 {attempt} 次抓取失败: {e}")
            if attempt < max_retries:
                time.sleep(2 * attempt)

    logger.error(f"[{name}] 全部重试失败，跳过（不影响其他源）")
    return []


def _fetch_and_filter(source: dict, settings: dict, max_age_hours: int,
                      force_direct: bool = False) -> tuple[str, list[dict]]:
    """单源抓取 + 时效过滤，供线程池调用，返回 (source_name, fresh_items)。"""
    name = source["name"]
    items = fetch_single_source(source, settings, force_direct=force_direct)
    if not items:
        return name, []

    fresh, stale_count = [], 0
    for item in items:
        pub_utc = item.pop("pub_utc", None)
        if is_fresh(pub_utc, max_age_hours, title=item["title"]):
            fresh.append(item)
        else:
            stale_count += 1

    if stale_count:
        logger.warning(f"[{name}] 过滤 {stale_count} 条超过 {max_age_hours}h 的旧新闻")
    return name, fresh


def fetch_all(max_workers: int = 8) -> dict:
    """并发抓取全部 RSS 源，最大并发数 max_workers。"""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    config = load_config()
    sources = config["sources"]
    settings = config.get("settings", {})
    max_age_hours = settings.get("max_news_age_hours", 48)

    logger.info(
        f"并发抓取 {len(sources)} 个 RSS 源 "
        f"（max_workers={max_workers}，外媒代理: {RSS_PROXY or '未配置'}）"
    )

    all_news = []
    failed_sources = []
    start = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_to_source = {
            pool.submit(_fetch_and_filter, src, settings, max_age_hours): src["name"]
            for src in sources
        }
        for future in as_completed(future_to_source):
            name = future_to_source[future]
            try:
                _, fresh_items = future.result()
                if fresh_items:
                    all_news.extend(fresh_items)
                else:
                    failed_sources.append(name)
            except Exception as e:
                logger.error(f"[{name}] 抓取任务异常: {e}")
                failed_sources.append(name)

    # 【2026-07-09 兜底】全部源 0 条 = 本机代理瞬断把所有请求（含"直连"中文源，
    # requests trust_env 会吃 http_proxy 环境变量）都带崩了 → 强制直连重试一轮。
    # 中文源直连必通，外媒直连在海外节点也通；宁可少收外媒，绝不 0 条污染下游。
    if not all_news:
        logger.warning("⚠️ 第一轮抓取 0 条（疑似代理瞬断），强制直连重试全部源...")
        failed_sources = []
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            _f2 = {pool.submit(_fetch_and_filter, src, settings, max_age_hours, True): src["name"]
                   for src in sources}
            for future in as_completed(_f2):
                name = _f2[future]
                try:
                    _, fresh_items = future.result()
                    if fresh_items:
                        all_news.extend(fresh_items)
                    else:
                        failed_sources.append(name)
                except Exception as e:
                    logger.error(f"[{name}] 直连重试异常: {e}")
                    failed_sources.append(name)
        logger.info(f"直连重试结果: {len(all_news)} 条")

    elapsed = round(time.time() - start, 1)
    stats = {
        "rss_count": len(sources),
        "fetched_news_count": len(all_news),
        "failed_source_count": len(failed_sources),
        "failed_sources": failed_sources,
        "elapsed_seconds": elapsed,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump({"stats": stats, "news": all_news}, f, ensure_ascii=False, indent=2)

    logger.info(
        f"抓取完成: {stats['fetched_news_count']} 条新闻, "
        f"{stats['failed_source_count']} 个源失败, 耗时 {elapsed}s"
    )
    return stats


if __name__ == "__main__":
    stats = fetch_all()
    print(json.dumps(stats, ensure_ascii=False, indent=2))
