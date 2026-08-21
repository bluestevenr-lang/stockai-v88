# -*- coding: utf-8 -*-
# k3_ask.py — 历史兼容入口；仅调用 Kimi Code 订阅 K3-256K
# v2（2026-08-20）：自动注入 V88 工作区脱敏快照（overview + 提到的个股 + 相关小模块），
#   K3 不再裸答；答复尾部自带模型/数据签名，避免与接待员页脚混淆。
# 飞书主会话已直接使用 K3-256K，不应再由 AGENTS.md 调用本脚本。
import csv
import json
import re
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from kimi_subscription import chat_completion, message_text

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

LEDGER = Path.home() / ".openclaw" / "k3_usage.csv"
WS = Path.home() / ".openclaw" / "workspaces" / "v88-mobile"
CTX = WS / "context"

SYSTEM = (
    "你是V88三方会审体系的首席分析师Kimi（角色：证据官/反对票/漏审检查）。"
    "回答纪律："
    "1) 先报数据时间和来源；"
    "2) 结论必须有依据，无依据就直说'无评级数据不推荐'；"
    "3) 严格区分'不否定'与'通过'；"
    "4) 涉及买卖必须给出入场区间、止损/失效条件，给不出就标注'暂不可执行'；"
    "5) 绝不编造任何数字或记录。"
    "6) 随附的《V88 快照数据》是你的唯一事实来源：只允许使用其中的数字，"
    "每项引用必须带上对应 generated_at；快照里没有的字段（例如今晚美股盘面、实时价）"
    "必须明说'快照无此数据'，禁止凭记忆编数。"
    "7) 若 kimi 快照的 source_times 距今超过 1 个交易日，开头红字标注数据偏旧。"
)

# 模块注入规则：默认带明日计划与三方会审；其余按关键词触发，单文件截断 8KB
MODULE_ALWAYS = ["tomorrow_plan_pub.json", "three_way_pub.json"]
MODULE_BY_KEYWORD = {
    "hot_theme_pub.json": ["热点", "题材", "板块", "主线"],
    "dragon_board_pub.json": ["龙虎榜", "游资", "打板"],
    "opportunity_scan_pub.json": ["机会", "扫描", "雷达", "选股"],
    "claude_standard_pub.json": ["书理", "纪律", "经典"],
    "fund_flow_pub.json": ["资金", "流向", "主力", "北向", "南向"],
    "trend_quality_pub.json": ["趋势", "质量"],
    "ai_cert_pub.json": ["AI", "认证"],
    "portfolio_pub.json": ["持仓", "买了", "买入", "卖了", "卖出", "加仓", "减仓", "仓位", "成本", "盈亏", "我的股"],
    "watchlist.json": ["自选", "关注", "重点", "盯着", "观察名单"],
}
MODULE_MAX_BYTES = 8192
STOCK_MAX = 4


def load_json(p: Path):
    try:
        return json.load(open(p, encoding="utf-8"))
    except Exception:
        return None


def find_stocks(q: str, name_index: dict):
    """从问题里识别个股：精确代码 > 名称全词 > 核心名互含。最多 STOCK_MAX 只。"""
    found = []
    seen = set()

    def add(code):
        code = code.strip()
        if code and code not in seen and (CTX / "stocks" / f"{code}.json").exists():
            seen.add(code)
            found.append(code)

    # 1) 显式代码：A股 600519.SS / 港股 1810.HK / 美股 AAPL
    for m in re.findall(r"\b(\d{6})\.(ss|sz|SS|SZ)\b", q):
        add(f"{m[0]}.{m[1].upper()}")
    for m in re.findall(r"\b(\d{4,5})\.HK\b", q, re.I):
        add(f"{m}.HK")
    for m in re.findall(r"\b([A-Z]{2,5})\b", q):
        add(m)

    # 2) 名称索引：key 直接出现在问题里（长名优先）
    keys = sorted(name_index.keys(), key=len, reverse=True)
    for k in keys:
        if len(found) >= STOCK_MAX:
            break
        if len(k) >= 2 and k in q:
            for c in (name_index[k] if isinstance(name_index[k], list) else [name_index[k]]):
                add(c)

    # 3) 二字滑窗互含（如问题里的"小米" ⊂ "小米集团-W" 的核心名）
    def bigrams(text):
        runs = re.findall(r"[一-鿿]+", text)
        grams = set()
        for r in runs:
            for i in range(len(r) - 1):
                grams.add(r[i:i + 2])
        return grams

    qgrams = bigrams(q)
    if qgrams:
        for k in keys:
            if len(found) >= STOCK_MAX:
                break
            core = re.sub(r"[-－][A-Z]+$", "", k)
            if len(core) >= 2 and (core in q or any(g in core for g in qgrams)):
                for c in (name_index[k] if isinstance(name_index[k], list) else [name_index[k]]):
                    add(c)

    # 4) 兜底：名称索引缺别名时，直接扫个股文件的 name 字段（条件：还没找满）
    STOPGRAMS = {"港股", "美股", "股票", "股价", "个股", "大盘", "指数", "基金", "今晚", "今天", "明天", "现在", "可以"}
    qgrams = {g for g in qgrams if g not in STOPGRAMS}
    if len(found) < STOCK_MAX and qgrams:
        stocks_dir = CTX / "stocks"
        for p in sorted(stocks_dir.glob("*.json")):
            if len(found) >= STOCK_MAX:
                break
            d = load_json(p)
            nm = (d or {}).get("name", "")
            core = re.sub(r"[-－][A-Z]+$", "", nm)
            if len(core) >= 2 and any(g in core for g in qgrams):
                add(p.stem)
    return found[:STOCK_MAX]


def build_context_block(q: str):
    parts = []
    freshness = None

    ov = load_json(CTX / "overview.json")
    if ov:
        freshness = ov.get("projection_generated_at")
        brief = {
            "projection_generated_at": freshness,
            "decision_semantics": ov.get("decision_semantics"),
            "sources": ov.get("sources"),
            "stock_count": ov.get("stock_count"),
        }
        parts.append("### overview（市场总览/语义/各源时间）\n" + json.dumps(brief, ensure_ascii=False, indent=1))

    ni = load_json(CTX / "name_index.json") or {}
    codes = find_stocks(q, ni) if ni else []
    for code in codes:
        d = load_json(CTX / "stocks" / f"{code}.json")
        if d:
            parts.append(f"### 个股快照 {code}（{d.get('name','')}）\n" + json.dumps(d, ensure_ascii=False, indent=1))

    mods = list(MODULE_ALWAYS)
    for fname, kws in MODULE_BY_KEYWORD.items():
        if any(kw in q for kw in kws):
            mods.append(fname)
    for fname in mods:
        p = CTX / "modules" / fname
        if p.exists() and fname not in ("\n".join(parts)):
            raw = p.read_bytes()[:MODULE_MAX_BYTES]
            try:
                d = json.loads(raw.decode("utf-8", "replace"))
                txt = json.dumps(d, ensure_ascii=False, indent=1)
            except Exception:
                txt = raw.decode("utf-8", "replace") + "\n…(截断)"
            parts.append(f"### 模块 {fname}\n" + txt)

    if not parts:
        return "", freshness, []
    return "\n\n".join(parts), freshness, codes


def main():
    q = " ".join(sys.argv[1:]).strip()
    if not q:
        print("用法: k3ask <问题>")
        sys.exit(2)

    ctx_block, freshness, codes = build_context_block(q)
    user = q
    if ctx_block:
        user += (
            "\n\n## V88 快照数据（脱敏，不含账户与持仓数量；时间均为北京时间）\n"
            + ctx_block
            + "\n\n请只基于以上快照与你的书理分析作答；快照没有的数据明说「快照无此数据」。"
        )

    try:
        data = chat_completion(
            messages=[
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": user},
            ],
            model="k3-256k",
            temperature=1,
            timeout=180,
        )
        answer = message_text(data)
        sig = ("\n\n—\n答复模型: k3-256k（Kimi Code订阅兼容入口）｜数据快照: "
               + (freshness or "无") + "｜命中个股: " + ("、".join(codes) if codes else "无")
               + "\n注：飞书主会话已经直接使用该模型，通常无需调用本脚本。")
        print(answer + sig)
        # 记账：把本次调用的 token 用量写入本地账本（供 k3_quota.py 统计）
        try:
            usage = data.get("usage", {}) or {}
            new_file = not LEDGER.exists()
            with open(LEDGER, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                if new_file:
                    w.writerow(["ts", "model", "prompt_tokens", "completion_tokens", "question"])
                w.writerow([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            data.get("model", "k3-256k"),
                            usage.get("prompt_tokens", 0),
                            usage.get("completion_tokens", 0),
                            q[:80]])
        except Exception:
            pass  # 记账失败不影响回答
    except Exception as e:
        print("ERROR: %s" % e)
        sys.exit(1)


main()
