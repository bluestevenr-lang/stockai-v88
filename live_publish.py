"""
live_publish.py — 云端实时数据发布（每小时，由本仓 .github/workflows/live.yml 驱动）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
产出并推送到本仓 data 分支 pub/：
  market_snapshot.json  实时大盘快照（温度/指数/板块，src/market_snapshot.py 真实行情计算）
  news_live.json        热点新闻流（10个RSS源，每条带实际发生时间，北京时间）
  meta.json             各数据文件的生成时间（云端查看器据此显示新鲜度与时段）
时段体系：一天三时段（北京时间）——时段一 00-08 / 时段二 08-16 / 时段三 16-24。
完整日报仍由私有仓 GitHub Actions 每时段一次（07:00/14:00/21:00）生成；
本脚本只负责"实时层"（快照+新闻），无 LLM、无密钥依赖（GITHUB_TOKEN 即可）。
src/market_snapshot.py、src/news_fetcher.py、config/rss_sources.json 为私有仓同步副本。
"""
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timezone, timedelta
from pathlib import Path

BASE = Path(__file__).resolve().parent
# 顺序关键：src 必须排在仓库根之前——根目录有旧版同名 news_fetcher.py 会遮蔽同步副本；
# 根目录仍需在 path 里（market_snapshot 要 import 根目录的 cloud_engine 拐点识别）
sys.path.insert(0, str(BASE))
sys.path.insert(0, str(BASE / "src"))
# 同步副本模块（market_snapshot/news_fetcher）需要这两个目录才能落盘/写日志
for _d in ("data", "logs"):
    (BASE / _d).mkdir(exist_ok=True)

BJT = timezone(timedelta(hours=8))
PUB_REPO = "bluestevenr-lang/stockai-v88"
PUB_BRANCH = "data"




_TOPIC_WORDS = ["AI", "人工智能", "芯片", "半导体", "存储", "算力", "英伟达", "台积电", "苹果", "特斯拉",
                "微软", "谷歌", "Meta", "亚马逊", "美联储", "降息", "加息", "通胀", "CPI", "关税", "财报",
                "IPO", "并购", "原油", "油价", "黄金", "比特币", "加密", "美元", "国债", "收益率", "地缘",
                "伊朗", "俄乌", "中东", "房地产", "新能源", "光伏", "锂电", "汽车", "机器人", "医药", "创新药",
                "白酒", "消费", "银行", "券商", "恒指", "纳指", "标普", "科创", "汇率", "人民币", "出口",
                "PMI", "GDP", "Fed", "tariff", "earnings", "chip", "semiconductor", "oil", "gold",
                "bitcoin", "inflation", "rate cut", "Nvidia", "Apple", "Tesla"]
_TOPIC_MERGE = {"人工智能": "AI", "chip": "芯片", "semiconductor": "芯片", "存储": "芯片", "算力": "AI",
                "Fed": "美联储", "rate cut": "降息", "tariff": "关税", "oil": "原油", "油价": "原油",
                "gold": "黄金", "bitcoin": "比特币", "加密": "比特币", "earnings": "财报",
                "inflation": "通胀", "Nvidia": "英伟达", "Apple": "苹果", "Tesla": "特斯拉"}


def _hot_topics(items, top=8):
    """【V88·热点主题榜】确定性关键词聚类：标题扫词→计数→同义合并→Top8（无LLM，可复算）"""
    from collections import Counter
    cnt = Counter()
    for it in items:
        t = str(it.get("t") or it.get("title") or "")
        tl = t.lower()
        for w in _TOPIC_WORDS:
            if (w.lower() in tl) if w.isascii() else (w in t):
                cnt[w] += 1
    merged = Counter()
    for w, n in cnt.items():
        merged[_TOPIC_MERGE.get(w, w)] += n
    return [{"w": w, "n": n} for w, n in merged.most_common(top) if n >= 2]



def _translate_titles(items):
    """【V88·新闻中文化】英文标题→中文：复用上一版已译结果(按url)，只译新增；
    DeepSeek 单次批量调用，失败保留原文。译文放 t，原文存 t_en。"""
    import os
    import requests as _rq
    prev = {}
    try:
        r = _rq.get("https://raw.githubusercontent.com/bluestevenr-lang/stockai-v88/data/pub/news_live.json",
                    timeout=10)
        if r.status_code == 200:
            for p in r.json().get("items", []):
                if p.get("url") and p.get("zh"):
                    prev[p["url"]] = p["t"]
    except Exception:
        pass
    todo = []
    for it in items:
        t = str(it.get("t", ""))
        if any("\u4e00" <= ch <= "\u9fff" for ch in t):
            continue  # 已是中文
        if it.get("url") in prev:
            it["t_en"], it["t"], it["zh"] = t, prev[it["url"]], 1
        else:
            todo.append(it)
    key = os.getenv("DEEPSEEK_API_KEY", "").strip()
    if not key or not todo:
        return
    todo = todo[:50]
    numbered = "\n".join(f"{i + 1}. {it['t']}" for i, it in enumerate(todo))
    try:
        resp = _rq.post("https://api.deepseek.com/v1/chat/completions",
                        headers={"Authorization": f"Bearer {key}"},
                        json={"model": "deepseek-v4-flash", "temperature": 0.1, "max_tokens": 3000,
                              "messages": [{"role": "user", "content":
                                  "把下面每条财经新闻标题翻译成简洁中文，公司名/指数名保留惯用译名，"
                                  "逐行输出「序号. 译文」，不要任何多余内容：\n" + numbered}]},
                        timeout=75)
        out_map = {}
        for ln in resp.json()["choices"][0]["message"]["content"].strip().splitlines():
            ln = ln.strip()
            if "." in ln[:4]:
                try:
                    num, txt = ln.split(".", 1)
                    out_map[int(num.strip())] = txt.strip()
                except Exception:
                    continue
        for i, it in enumerate(todo):
            if out_map.get(i + 1):
                it["t_en"], it["t"], it["zh"] = it["t"], out_map[i + 1][:140], 1
    except Exception as e:
        print(f"[translate] 翻译失败(保留原文): {str(e)[:80]}")



_STK_IDX = None

def _stock_index():
    """【C1·新闻映射】公司名→代码索引：NAME_MAP精选 + 全市场名录(名字≥3字防误匹配)"""
    global _STK_IDX
    if _STK_IDX is None:
        idx = {}
        try:
            try:
                from cloud_engine import NAME_MAP
            except ImportError:
                from src.cloud_engine import NAME_MAP
            for n, c in NAME_MAP.items():
                if len(n) >= 2 and not n.isascii():
                    idx[n] = c
        except Exception:
            pass
        try:
            import json as _j
            from pathlib import Path as _P
            _f = _P(__file__).resolve().parent / "stock_names.json"
            if not _f.exists():
                _f = _P(__file__).resolve().parent.parent / "stock_names.json"
            for e in _j.loads(_f.read_text(encoding="utf-8")):
                if len(e["n"]) >= 3:
                    idx.setdefault(e["n"], e["c"])
        except Exception:
            pass
        _STK_IDX = idx
    return _STK_IDX


def _match_stocks(title):
    """标题→相关标的（最多3个）"""
    out = []
    try:
        for n, c in _stock_index().items():
            if n in title:
                out.append({"n": n, "c": c})
                if len(out) >= 3:
                    break
    except Exception:
        pass
    return out

def _now_bjt_str():
    return datetime.now(BJT).strftime("%Y-%m-%d %H:%M")


def build_snapshot() -> str:
    """跑真实行情快照，返回 data/market_snapshot.json 内容（失败返回空）"""
    try:
        (BASE / "data").mkdir(exist_ok=True)  # runner 上无此目录，快照写盘前先建
        from market_snapshot import generate_market_snapshot
        generate_market_snapshot()
        fp = BASE / "data" / "market_snapshot.json"
        return fp.read_text(encoding="utf-8") if fp.exists() else ""
    except Exception as e:
        print(f"[live] snapshot 失败: {e}")
        return ""


def build_news() -> str:
    """抓全部 RSS 源，产出热点新闻流 JSON（每条带北京时间的实际发生时间）"""
    try:
        from news_fetcher import fetch_all
        res = fetch_all(max_workers=8)
        items = res.get("news") if isinstance(res, dict) else None
        if items is None:  # fetch_all 可能只落盘不返回明细
            raw = json.loads((BASE / "data" / "news_raw.json").read_text(encoding="utf-8"))
            items = raw.get("news", [])
        out = []
        for it in items:
            ts = str(it.get("published_time", ""))
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                tb = dt.astimezone(BJT).strftime("%Y-%m-%d %H:%M")
                sort_key = dt.timestamp()
            except Exception:
                tb, sort_key = "", 0
            out.append({"t": str(it.get("title", ""))[:140],
                        "stk": _match_stocks(str(it.get("title", ""))),
                        "s": it.get("source", ""),
                        "url": it.get("link", ""),
                        "time": tb, "cat": it.get("source_category", ""),
                        "_k": sort_key})
        out.sort(key=lambda x: -x["_k"])
        for o in out:
            o.pop("_k", None)
        if not out:
            print("[live] 新闻 0 条，跳过发布（保留云端上一版新闻流）")
            return ""
        _translate_titles(out[:60])  # 英文标题→中文（发布的前60条）
        return json.dumps({"generated_at": _now_bjt_str(), "count": len(out),
                           "topics": _hot_topics(out), "items": out[:60]},
                          ensure_ascii=False, indent=1)
    except Exception as e:
        print(f"[live] news 失败: {e}")
        return ""


def _token() -> str:
    t = os.getenv("GITHUB_TOKEN", "").strip() or os.getenv("PUBLISH_TOKEN", "").strip()
    if not t:
        try:
            t = subprocess.run(["gh", "auth", "token"], capture_output=True,
                               text=True, timeout=10).stdout.strip()
        except Exception:
            t = ""
    return t


def publish(files: dict) -> bool:
    """把 files{name:content} 写入 data 分支 pub/ 并推送；meta.json 合并保留其它键"""
    token = _token()
    if not token or not files:
        print(f"[live] 无token或无文件（token={'有' if token else '无'}, files={list(files)})")
        return False
    with tempfile.TemporaryDirectory() as td:
        url = f"https://x-access-token:{token}@github.com/{PUB_REPO}.git"
        try:
            subprocess.run(["git", "clone", "-q", "--depth", "1", "-b", PUB_BRANCH, url, td],
                           check=True, capture_output=True, timeout=90)
            pub = Path(td) / "pub"
            pub.mkdir(exist_ok=True)
            # meta 合并：保留日报等其它键，只更新本次涉及的
            meta = {}
            mf = pub / "meta.json"
            if mf.exists():
                try:
                    meta = json.loads(mf.read_text(encoding="utf-8"))
                except Exception:
                    meta = {}
            now = _now_bjt_str()
            for name, content in files.items():
                (pub / name).write_text(content, encoding="utf-8")
                meta[name.replace(".json", "").replace(".md", "") + "_ts"] = now
                if name == "market_snapshot.json":
                    try:
                        meta["live_snapshot_id"] = json.loads(content).get("snapshot_id", "")
                    except Exception:
                        pass
            mf.write_text(json.dumps(meta, ensure_ascii=False, indent=1), encoding="utf-8")
            env = {**os.environ, "GIT_AUTHOR_NAME": "v88live", "GIT_AUTHOR_EMAIL": "v88@live",
                   "GIT_COMMITTER_NAME": "v88live", "GIT_COMMITTER_EMAIL": "v88@live"}
            subprocess.run(["git", "-C", td, "add", "-A"], check=True, capture_output=True)
            r = subprocess.run(["git", "-C", td, "commit", "-m", f"live publish {now}"],
                               capture_output=True, env=env)
            if r.returncode != 0:
                print("[live] 无变化，跳过推送")
                return True
            # 与其它发布方（V88本地 scan_latest）并发时重试一次
            for _try in (1, 2):
                p = subprocess.run(["git", "-C", td, "push", "-q", "origin", PUB_BRANCH],
                                   capture_output=True, timeout=90)
                if p.returncode == 0:
                    print(f"[live] ✅ 已发布: {list(files)}")
                    return True
                subprocess.run(["git", "-C", td, "pull", "--rebase", "-q", "origin", PUB_BRANCH],
                               capture_output=True, timeout=60)
            print(f"[live] 推送失败: {(p.stderr or b'').decode()[:200]}")
            return False
        except Exception as e:
            print(f"[live] 发布异常: {str(e)[:200]}")
            return False


if __name__ == "__main__":
    files = {}
    snap = build_snapshot()
    if snap:
        files["market_snapshot.json"] = snap
    news = build_news()
    if news:
        files["news_live.json"] = news
    ok = publish(files)
    sys.exit(0 if ok else 1)
