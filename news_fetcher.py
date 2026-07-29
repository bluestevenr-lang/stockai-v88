import os
import logging
import json
import feedparser
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

GUARDIAN_API_KEY = os.environ.get("GUARDIAN_API_KEY")
NEWS_API_KEY     = os.environ.get("NEWS_API_KEY")   # 保留，auto_reporter.py 可能导入

# ─────────────────────────────────────────────────────────────────────────────
# Guardian API 话题列表
# ─────────────────────────────────────────────────────────────────────────────
GUARDIAN_TOPICS = [
    # 美股宏观
    "Federal Reserve interest rates",
    "stock market wall street",
    "earnings results quarterly",
    "geopolitical conflict war",
    "oil price energy OPEC",
    "artificial intelligence technology",
    "inflation consumer prices",
    "semiconductor chips",
    # 中港专项
    "China economy trade policy",
    "Hong Kong stock market Hang Seng",
    "China technology Tencent Alibaba",
    "China property real estate",
    "China PBOC monetary policy yuan",
    # 中港深度
    "Hang Seng index Hong Kong stocks weekly",
    "China A shares Shanghai Shenzhen market",
    "Tencent Alibaba Meituan earnings results",
    "China electric vehicle BYD CATL battery",
    # 日经亚洲（新增）
    "Nikkei Asia China economy stocks",
    "Hong Kong Hang Seng index finance",
]

# ─────────────────────────────────────────────────────────────────────────────
# RSS 源列表
# ─────────────────────────────────────────────────────────────────────────────
RSS_FEEDS = [
    # 彭博（财经核心）
    "https://feeds.bloomberg.com/markets/news.rss",
    # MarketWatch
    "https://feeds.marketwatch.com/marketwatch/topstories/",
    "https://feeds.marketwatch.com/marketwatch/marketpulse/",
    # Yahoo Finance
    "https://finance.yahoo.com/news/rssindex",
    # CNBC
    "https://www.cnbc.com/id/100003114/device/rss/rss.html",
    "https://www.cnbc.com/id/10001147/device/rss/rss.html",
    # Seeking Alpha
    "https://seekingalpha.com/feed.xml",
    # FT
    "https://www.ft.com/rss/home/uk",
    # 南华早报（港股/中国，4个分类）
    "https://www.scmp.com/rss/5/feed",    # Business
    "https://www.scmp.com/rss/4/feed",    # Hong Kong
    "https://www.scmp.com/rss/2/feed",    # China
    "https://www.scmp.com/rss/91/feed",   # Economy
    # 日经亚洲（亚洲财经权威，专注中港）
    "https://asia.nikkei.com/rss/feed/nar",
    # 注：东方财富/新浪财经/证券时报 RSS 在 VPS（美国IP）无法访问，已移除
]

# RSS_SOURCES 保留空字典，供 auto_reporter.py 导入时不报错
RSS_SOURCES:    dict = {}
NEWSAPI_TOPICS: list = []   # 已停用，保留供旧代码导入


# ─────────────────────────────────────────────────────────────────────────────
# Guardian API 采集
# ─────────────────────────────────────────────────────────────────────────────
def fetch_guardian(topic: str, api_key: str) -> list:
    if not api_key:
        return []
    try:
        resp = requests.get(
            "https://content.guardianapis.com/search",
            params={
                "q":           topic,
                "api-key":     api_key,
                "show-fields": "headline,trailText,webUrl",
                "order-by":    "newest",
                "page-size":   5,
            },
            timeout=15,
        )
        results = resp.json().get("response", {}).get("results", [])
        return [
            {
                "title":        r.get("webTitle", ""),
                "source":       "The Guardian",
                "url":          r.get("webUrl", ""),
                "published_at": r.get("webPublicationDate", ""),
                "description":  (r.get("fields") or {}).get("trailText", ""),
                "category":     topic,
            }
            for r in results
            if r.get("webTitle")
        ]
    except Exception as e:
        logger.error(f"Guardian API 失败 [{topic}]: {e}")
        return []


# ─────────────────────────────────────────────────────────────────────────────
# RSS 全量采集
# ─────────────────────────────────────────────────────────────────────────────
_RSS_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


def fetch_rss_feed() -> list:
    """
    用 requests 下载 RSS 内容再交给 feedparser 解析，
    避免 feedparser 内部 urllib 被网络策略拦截。
    """
    results = []
    for url in RSS_FEEDS:
        try:
            resp = requests.get(url, headers=_RSS_HEADERS, timeout=10)
            resp.raise_for_status()
            feed = feedparser.parse(resp.content)
            src  = feed.feed.get("title", url)
            for entry in feed.entries[:4]:
                title = entry.get("title", "")
                if not title or "[Removed]" in title:
                    continue
                results.append({
                    "title":        title,
                    "source":       src,
                    "url":          entry.get("link", ""),
                    "published_at": entry.get("published", ""),
                    "description":  entry.get("summary", "")[:200],
                    "category":     "rss",
                })
        except Exception as e:
            logger.error(f"RSS 失败 [{url}]: {e}")
    logger.info(f"  RSS 总计: {len(results)} 条")
    return results


# ─────────────────────────────────────────────────────────────────────────────
# yfinance 持仓标的专属新闻
# ─────────────────────────────────────────────────────────────────────────────
def fetch_yfinance_news(watchlist: list) -> list:
    """
    通过 yfinance Ticker.news 获取每只持仓标的的最新新闻。
    绕过中国 IP 限制，可覆盖港股（0700.HK）和A股（600519.SS）等标的。
    """
    import yfinance as yf
    results = []
    for symbol in watchlist:
        try:
            news = yf.Ticker(symbol).news
            for item in (news or [])[:5]:
                content = item.get("content", {})
                title   = content.get("title", "")
                if not title or "[Removed]" in title:
                    continue
                provider = (content.get("provider") or {}).get("displayName", "Yahoo Finance")
                url      = (content.get("canonicalUrl") or {}).get("url", "")
                pub_date = content.get("pubDate", "")
                summary  = content.get("summary", "")
                results.append({
                    "title":        title,
                    "source":       provider,
                    "url":          url,
                    "published_at": pub_date,
                    "description":  summary[:200],
                    "category":     f"持仓标的:{symbol}",
                    "symbol":       symbol,
                })
        except Exception as e:
            logger.error(f"yfinance news 失败 [{symbol}]: {e}")
    logger.info(f"  yfinance 持仓新闻：{len(results)} 条（{len(watchlist)}只标的）")
    return results


# ─────────────────────────────────────────────────────────────────────────────
# 兼容接口（auto_reporter.py 可能导入，保留空实现）
# ─────────────────────────────────────────────────────────────────────────────
def fetch_rss(category: str) -> list:
    """保留接口，已由 fetch_rss_feed() 替代，始终返回空列表。"""
    return []


def fetch_newsapi(topic: str, date_str: str) -> list:
    """保留接口，NewsAPI 已替换为 Guardian+RSS，始终返回空列表。"""
    return []


# ─────────────────────────────────────────────────────────────────────────────
# 股票基本面 & 财报日期
# ─────────────────────────────────────────────────────────────────────────────
def fetch_stock_data(symbol: str) -> dict:
    import yfinance as yf
    base = {
        "symbol":          symbol,
        "price":           None,
        "forward_pe":      None,
        "trailing_pe":     None,
        "forward_eps":     None,
        "trailing_eps":    None,
        "dividend_yield":  None,
        "revenue_growth":  None,
        "earnings_growth": None,
        "analyst_target":  None,
        "recommendation":  None,
        "52w_high":        None,
        "52w_low":         None,
        "updated_at":      datetime.now().strftime("%Y-%m-%d %H:%M"),
    }
    try:
        info = yf.Ticker(symbol).info
        base.update({
            "price":           info.get("currentPrice") or info.get("regularMarketPrice"),
            "forward_pe":      info.get("forwardPE"),
            "trailing_pe":     info.get("trailingPE"),
            "forward_eps":     info.get("forwardEps"),
            "trailing_eps":    info.get("trailingEps"),
            "dividend_yield":  info.get("dividendYield"),
            "revenue_growth":  info.get("revenueGrowth"),
            "earnings_growth": info.get("earningsGrowth"),
            "analyst_target":  info.get("targetMeanPrice"),
            "recommendation":  info.get("recommendationKey"),
            "52w_high":        info.get("fiftyTwoWeekHigh"),
            "52w_low":         info.get("fiftyTwoWeekLow"),
        })
    except Exception as e:
        logger.error(f"fetch_stock_data 失败 [{symbol}]: {e}")
    return base


def fetch_earnings_date(symbol: str) -> str:
    import yfinance as yf
    try:
        cal = yf.Ticker(symbol).calendar
        if cal is not None and "Earnings Date" in cal:
            return str(cal["Earnings Date"][0])
    except Exception as e:
        logger.error(f"fetch_earnings_date 失败 [{symbol}]: {e}")
    return "暂无数据"


# ─────────────────────────────────────────────────────────────────────────────
# 主采集函数（Guardian + RSS 双源）
# ─────────────────────────────────────────────────────────────────────────────
def build_report_data(watchlist: list, date_str: str) -> dict:
    logger.info(
        f"开始采集数据：Guardian({len(GUARDIAN_TOPICS)}话题) + "
        f"RSS({len(RSS_FEEDS)}源) / {len(watchlist)}只标的"
    )

    # 第一步：yfinance 持仓新闻（优先保留，覆盖港股/A股）
    yf_news = fetch_yfinance_news(watchlist)

    # 第二步：Guardian + RSS 作为宏观新闻池
    macro_news: list = []
    for topic in GUARDIAN_TOPICS:
        articles = fetch_guardian(topic, GUARDIAN_API_KEY)
        macro_news.extend(articles)
        logger.info(f"  Guardian [{topic}]：{len(articles)} 条")

    rss_articles = fetch_rss_feed()
    macro_news.extend(rss_articles)

    # 第三步：宏观新闻去重，保留最新 40 条
    seen: set = set()
    macro_deduped: list = []
    for a in macro_news:
        ttl = a.get("title", "")
        if ttl and ttl not in seen:
            seen.add(ttl)
            macro_deduped.append(a)
    macro_deduped.sort(key=lambda x: x.get("published_at", ""), reverse=True)
    macro_deduped = macro_deduped[:40]

    # 第四步：yfinance 持仓新闻去重（排除已在宏观池中的标题），保留最新 30 条
    seen2: set = set(a["title"] for a in macro_deduped)
    yf_deduped: list = []
    for a in yf_news:
        ttl = a.get("title", "")
        if ttl and ttl not in seen2:
            seen2.add(ttl)
            yf_deduped.append(a)
    yf_deduped.sort(key=lambda x: x.get("published_at", ""), reverse=True)
    yf_deduped = yf_deduped[:30]

    # 第五步：合并，持仓新闻放前面（AI 优先读到）
    all_news = yf_deduped + macro_deduped

    logger.info(
        f"新闻汇总：持仓专属{len(yf_deduped)}条 + 宏观{len(macro_deduped)}条 "
        f"= 共{len(all_news)}条"
    )

    # 标的财务数据
    stocks: dict = {}
    for symbol in watchlist:
        stocks[symbol] = fetch_stock_data(symbol)
        logger.info(
            f"  [{symbol}] price={stocks[symbol]['price']} "
            f"forward_eps={stocks[symbol]['forward_eps']}"
        )

    # 6. 财报日期
    earnings = {s: fetch_earnings_date(s) for s in watchlist}

    logger.info(f"采集完成：{len(all_news)} 条新闻 / {len(stocks)} 只标的")

    return {
        "news":         all_news,
        "stocks":       stocks,
        "earnings":     earnings,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
    }


# ─────────────────────────────────────────────────────────────────────────────
# 本地测试
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    WATCHLIST = [
        "ABBV", "NVDA", "NVO", "LLY", "TSM", "BRK-B", "PM", "VOO", "QQQM", "GOOG",
        "0700.HK", "0883.HK", "1299.HK", "0941.HK",
        "600519.SS", "688981.SS", "601899.SS", "688008.SS",
        "600941.SS", "000333.SZ", "000001.SZ", "601669.SS",
    ]
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    data = build_report_data(watchlist=WATCHLIST, date_str=yesterday)

    # 打印前3条新闻验证（真实来源，非模型推断）
    print("\n" + "="*60)
    print("✅ 前3条新闻验证（来源必须是真实媒体）")
    print("="*60)
    for i, article in enumerate(data["news"][:3], 1):
        print(f"\n[{i}] 标题：{article['title']}")
        print(f"    来源：{article['source']}")
        print(f"    链接：{article['url']}")
        print(f"    时间：{article['published_at'][:16]}")

    # 打印持仓标的新闻（港股/A股验证）
    hk_cn_news = [a for a in data["news"] if "HK" in a.get("category","") or ".SS" in a.get("category","") or ".SZ" in a.get("category","")]
    print(f"\n{'='*60}")
    print(f"🇭🇰🇨🇳 港股/A股持仓标的新闻（前3条）共 {len(hk_cn_news)} 条")
    print("="*60)
    for i, article in enumerate(hk_cn_news[:3], 1):
        print(f"\n[{i}] 标的：{article.get('symbol','')}  来源：{article['source']}")
        print(f"    标题：{article['title']}")
        print(f"    链接：{article['url']}")
        print(f"    时间：{article['published_at'][:16]}")

    print(f"\n总计：{len(data['news'])} 条新闻 / {len(data['stocks'])} 只标的")
