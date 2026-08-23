#!/usr/bin/env python3
"""Build a privacy-minimized V88 workspace for the Windows OpenClaw agent."""

from __future__ import annotations

import argparse
import json
import os
import re
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

# 北京时间固定 UTC+8；不用 zoneinfo，避免部分 Python 环境缺 tzdata 直接崩
CST = timezone(timedelta(hours=8))


PUBLIC_MODULES = (
    "ai_cert_pub.json",
    "claude_standard_pub.json",
    "dragon_board_pub.json",
    "fund_flow_pub.json",
    "health_gate_pub.json",
    "hot_theme_pub.json",
    "opportunity_scan_pub.json",
    "three_way_pub.json",
    "tomorrow_plan_pub.json",
    "trend_quality_pub.json",
    "trend_shift_pub.json",
    "turning_forecast_pub.json",
    "universe_scan_pub.json",
    "value_zone_pub.json",
    "why_buy_pub.json",
)

GPT_FIELDS = (
    "name",
    "verdict",
    "why",
    "ts",
    "tier_at_verify",
    "fresh_for_strong_days",
)
KIMI_FIELDS = ("verdict", "book_verdict", "why", "ts")
CLASSICS_FIELDS = (
    "code",
    "name",
    "tier",
    "when",
    "school",
    "school_why",
    "checks",
    "pass_n",
    "miss_n",
    "fail_n",
    "verdict",
    "rules_gate",
    "anomaly_frozen",
    "anomaly_note",
    "fund",
    "pos52",
)

# Exact privacy-bearing field names only. Technical labels such as pos52 remain valid.
FORBIDDEN_KEYS = {
    "account",
    "account_id",
    "assets",
    "balance",
    "cash",
    "cost",
    "cost_basis",
    "held",
    "holding",
    "holdings",
    "market_value",
    "positions",
    "qty",
    "quantity",
    "shares",
    "total_assets",
}


def read_json(path: Path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return default


def rows_map(doc):
    rows = doc.get("rows", {}) if isinstance(doc, dict) else {}
    if isinstance(rows, dict):
        return rows
    if isinstance(rows, list):
        return {
            str(row.get("code")): row
            for row in rows
            if isinstance(row, dict) and row.get("code")
        }
    return {}


def lookup_code(rows, code):
    """按代码读取记录，兼容港股前导零差异。"""
    if not isinstance(rows, dict):
        return {}
    if code in rows:
        return rows.get(code) or {}
    target = norm_code(code)
    for row_code, row in rows.items():
        if norm_code(row_code) == target:
            return row or {}
    return {}


def pick(row, fields):
    if not isinstance(row, dict):
        return {}
    return {key: row[key] for key in fields if key in row}


def safe_code(code: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]", "_", code)
    return cleaned or "UNKNOWN"


def assert_private_keys_absent(value, path="root") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            is_ticker_record = (
                isinstance(child, dict)
                and str(child.get("code", "")).upper() == str(key).upper()
            )
            if str(key).lower() in FORBIDDEN_KEYS and not is_ticker_record:
                raise ValueError(f"private key blocked at {path}.{key}")
            assert_private_keys_absent(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            assert_private_keys_absent(child, f"{path}[{index}]")


def atomic_json(path: Path, value) -> None:
    rendered = json.dumps(value, ensure_ascii=False, indent=2) + "\n"
    try:
        if path.read_text(encoding="utf-8") == rendered:
            return
    except OSError:
        pass
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(rendered)
        os.replace(temp_name, path)
    finally:
        if os.path.exists(temp_name):
            os.unlink(temp_name)


def norm_code(code: str) -> str:
    """归一代码：港股去前导零（01810.HK→1810.HK），其余原样大写。"""
    c = str(code).strip().upper()
    m = re.match(r"^(\d+)\.(HK|SS|SZ)$", c)
    if m and m.group(2) == "HK":
        return f"{int(m.group(1))}.HK"
    return c


def build_portfolio_index(positions):
    """只保留回答持仓管理所需的最小信息，不暴露账户、数量和成本。"""
    index = {}
    if not isinstance(positions, dict):
        return index
    accounts = positions.get("accounts") or {}
    if not isinstance(accounts, dict):
        return index
    for info in accounts.values():
        if not isinstance(info, dict):
            continue
        for row in info.get("holdings") or []:
            if not isinstance(row, dict) or not row.get("code"):
                continue
            code = str(row["code"]).strip().upper()
            key = norm_code(code)
            item = index.setdefault(
                key,
                {
                    "code": code,
                    "name": row.get("name") or code,
                    "pnl_pct_values": [],
                },
            )
            value = row.get("pnl_pct")
            if isinstance(value, (int, float)) and value not in item["pnl_pct_values"]:
                item["pnl_pct_values"].append(value)
    return index


def public_portfolio_item(item):
    """把同一代码的多账户记录压平，不暴露账户边界。"""
    if not isinstance(item, dict):
        return None
    result = {"code": item.get("code"), "name": item.get("name")}
    values = sorted(item.get("pnl_pct_values") or [])
    if len(values) == 1:
        result["pnl_pct"] = values[0]
    elif len(values) > 1:
        result["pnl_pct_range"] = [values[0], values[-1]]
    return result


def collect_code_matches(value, code, path="root", limit=4):
    """从公开模块中提取与单只股票直接相关的记录，避免让模型自行全库搜索。"""
    target = norm_code(code)
    matches = []
    fingerprints = set()

    def add(match_path, data):
        if len(matches) >= limit:
            return
        try:
            fingerprint = json.dumps(data, ensure_ascii=False, sort_keys=True)
        except (TypeError, ValueError):
            return
        if fingerprint in fingerprints:
            return
        fingerprints.add(fingerprint)
        matches.append({"path": match_path, "data": data})

    def visit(node, node_path):
        if len(matches) >= limit:
            return
        if isinstance(node, dict):
            row_code = node.get("code")
            if row_code and norm_code(row_code) == target:
                add(node_path, node)
                return
            for key, child in node.items():
                child_path = f"{node_path}.{key}"
                if norm_code(key) == target and isinstance(child, (dict, list)):
                    add(child_path, child)
                    continue
                visit(child, child_path)
        elif isinstance(node, list):
            for index, child in enumerate(node):
                visit(child, f"{node_path}[{index}]")

    visit(value, path)
    return matches


def index_public_module(value, codes, path="root", limit=4):
    """单次遍历公开模块，为所有目标代码建索引，避免逐股重复扫全库。"""
    targets = {norm_code(code) for code in codes}
    index = {target: [] for target in targets}
    fingerprints = {target: set() for target in targets}

    def add(target, match_path, data):
        if target not in index or len(index[target]) >= limit:
            return
        try:
            fingerprint = json.dumps(data, ensure_ascii=False, sort_keys=True)
        except (TypeError, ValueError):
            return
        if fingerprint in fingerprints[target]:
            return
        fingerprints[target].add(fingerprint)
        index[target].append({"path": match_path, "data": data})

    def visit(node, node_path):
        if isinstance(node, dict):
            row_code = node.get("code")
            row_target = norm_code(row_code) if row_code else None
            if row_target in targets:
                add(row_target, node_path, node)
                return
            for key, child in node.items():
                child_path = f"{node_path}.{key}"
                key_target = norm_code(key)
                if key_target in targets and isinstance(child, (dict, list)):
                    add(key_target, child_path, child)
                    continue
                visit(child, child_path)
        elif isinstance(node, list):
            for item_index, child in enumerate(node):
                visit(child, f"{node_path}[{item_index}]")

    visit(value, path)
    return {target: matches for target, matches in index.items() if matches}


def first_match(matches, prefixes=()):
    if not matches:
        return None
    for match in matches:
        path = str(match.get("path", ""))
        if any(path.startswith(prefix) for prefix in prefixes):
            return match
    return matches[0]


def action_contract(why_buy, code, matches=None):
    matches = matches or collect_code_matches(why_buy, code, "why_buy", limit=6)
    match = first_match(
        matches,
        ("why_buy.sells.", "why_buy.holds.", "why_buy.rows."),
    )
    if not match or not isinstance(match.get("data"), dict):
        return {}
    row = match["data"]
    cycle = row.get("cycle") if isinstance(row.get("cycle"), dict) else {}
    technical = (
        row.get("stop_layered")
        if isinstance(row.get("stop_layered"), dict)
        else {}
    )
    return {
        "source": "why_buy_pub.json",
        "source_path": match.get("path"),
        "source_generated_at": why_buy.get("generated_at"),
        "signal": row.get("kind"),
        "why_now": row.get("why_now"),
        "guidance": row.get("hold"),
        "invalidation": row.get("fail") or cycle.get("invalid"),
        "layer_summary": row.get("layer_line"),
        "cycle": pick(cycle, ("stance", "grade", "why", "on_break", "invalid")),
        "technical": pick(
            technical,
            ("action", "tech_action", "layer", "why", "review_only", "invalid"),
        ),
    }


def latest_exit_index(ledger):
    result = {"date": None, "ruleset": None, "rows": {}}
    days = ledger.get("days") if isinstance(ledger, dict) else None
    if not isinstance(days, dict) or not days:
        return result
    latest_date = sorted(days)[-1]
    latest = days.get(latest_date) or {}
    lines = latest.get("lines") if isinstance(latest, dict) else None
    result["date"] = latest_date
    result["ruleset"] = latest.get("ruleset") if isinstance(latest, dict) else None
    if isinstance(lines, dict):
        for code, row in lines.items():
            if isinstance(row, dict):
                result["rows"][norm_code(code)] = pick(
                    row,
                    ("code", "name", "scope", "last", "take", "stop"),
                )
    return result


def load_stock_names():
    """从仓库根 stock_names.json 加载 主名/别名 映射（归一代码为键）。"""
    names_path = Path(__file__).resolve().parents[2] / "stock_names.json"
    primary, aliases = {}, {}
    for entry in read_json(names_path, []):
        if not isinstance(entry, dict):
            continue
        n, c = str(entry.get("n", "")).strip(), entry.get("c", "")
        if not n or not c:
            continue
        key = norm_code(c)
        aliases.setdefault(key, [])
        if n not in aliases[key]:
            aliases[key].append(n)
    for key, names in aliases.items():
        # 主名取最长（"小米集团-W" 优于 "小米"），其余全留作别名
        primary[key] = max(names, key=len)
    return primary, aliases


def build(source: Path, destination: Path) -> int:
    required = ("gpt_verify.json", "kimi_verify.json", "three_way_pub.json")
    missing = [name for name in required if not (source / name).is_file()]
    if missing:
        raise FileNotFoundError(f"missing V88 source files: {', '.join(missing)}")

    gpt = read_json(source / "gpt_verify.json", {})
    kimi = read_json(source / "kimi_verify.json", {})
    triad = read_json(source / "three_way_pub.json", {})
    classics = read_json(source / "classics_lens.json", {})
    health = read_json(source / "health_gate_pub.json", {})
    release = read_json(source / "release_check.json", {})
    positions = read_json(source.parent / "positions.json", None)
    portfolio_index = build_portfolio_index(positions)
    exit_ledger = read_json(source / "exit_ledger.json", {})
    exit_index = latest_exit_index(exit_ledger)
    public_docs = {
        filename: value
        for filename in PUBLIC_MODULES
        if (value := read_json(source / filename, None)) is not None
    }

    gpt_rows = rows_map(gpt)
    kimi_rows = rows_map(kimi)
    triad_rows = rows_map(triad)
    classics_rows = rows_map(classics)
    code_by_norm = {}
    for code in sorted(set(gpt_rows) | set(kimi_rows) | set(triad_rows) | set(classics_rows)):
        code_by_norm.setdefault(norm_code(code), code)
    for item in portfolio_index.values():
        code = item.get("code")
        if code:
            code_by_norm.setdefault(norm_code(code), code)
    codes = sorted(code_by_norm.values())
    module_indexes = {
        filename: index_public_module(value, codes, filename[:-5], limit=6)
        for filename, value in public_docs.items()
    }
    generated = datetime.now(CST).strftime(
        "%Y-%m-%d %H:%M:%S（北京时间）"
    )

    overview = {
        "projection_generated_at": generated,
        "privacy": "不含账户、资产、持仓数量、成本、现金或交易凭据",
        "decision_semantics": {
            "gpt_role": "51%终审与综合判断；不得越过硬闸或2A/3A双审",
            "kimi_role": "49%证据官、反对票与漏审检查",
            "classics_role": "纪律校正，只能否决或降级",
            "fail_closed": "过期、分歧、证据不足或执行阻断均不可执行",
        },
        "sources": {
            "gpt": {
                "generated_at": gpt.get("generated_at"),
                "factpack_id": gpt.get("factpack_id"),
                "model": gpt.get("model"),
                "reviewer": gpt.get("reviewer"),
                "stats": gpt.get("stats"),
            },
            "kimi": {
                "generated_at": kimi.get("generated_at"),
                "factpack_id": kimi.get("factpack_id"),
                "reviewer": kimi.get("reviewer"),
                "coverage": kimi.get("coverage"),
            },
            "triad": {
                "generated_at": triad.get("generated_at"),
                "counts": triad.get("counts"),
            },
            "health": health,
            "release": {
                "checked_at": release.get("checked_at"),
                "pass": release.get("pass"),
                "n_fails": release.get("n_fails"),
                "warnings": release.get("warnings", []),
            },
        },
        "stock_count": len(codes),
    }

    name_index = {}
    stock_documents = {}
    names_primary, names_aliases = load_stock_names()
    for code in codes:
        gpt_row = pick(lookup_code(gpt_rows, code), GPT_FIELDS)
        kimi_row = pick(lookup_code(kimi_rows, code), KIMI_FIELDS)
        triad_raw = lookup_code(triad_rows, code)
        triad_row = triad_raw if isinstance(triad_raw, dict) else {}
        classics_row = pick(lookup_code(classics_rows, code), CLASSICS_FIELDS)
        portfolio_item = public_portfolio_item(portfolio_index.get(norm_code(code)))
        evidence = {}
        for filename, module_index in module_indexes.items():
            matches = module_index.get(norm_code(code), [])
            if matches:
                evidence[filename[:-5]] = matches
        cert_matches = evidence.get("ai_cert_pub", [])
        cert_match = first_match(cert_matches, ("ai_cert_pub.by_code.",))
        cert = cert_match.get("data", {}) if cert_match else {}
        risk_line = exit_index["rows"].get(norm_code(code), {})
        position_mode = "持仓管理" if portfolio_item else "未确认持仓·按新开仓审查"
        decision_snapshot = {
            "position_mode": position_mode,
            "is_held": bool(portfolio_item),
            "portfolio_asof": positions.get("updated_at") if isinstance(positions, dict) else None,
            "portfolio_fact": portfolio_item or {},
            "action_contract": action_contract(
                public_docs.get("why_buy_pub.json", {}),
                code,
                module_indexes.get("why_buy_pub.json", {}).get(norm_code(code), []),
            ),
            "risk_line": {
                "source": "exit_ledger.json",
                "date": exit_index.get("date"),
                "ruleset": exit_index.get("ruleset"),
                **risk_line,
            }
            if risk_line
            else {},
            "ai_cert": cert if isinstance(cert, dict) else {},
            "module_evidence": evidence,
        }
        name = (
            triad_row.get("name")
            or gpt_row.get("name")
            or classics_row.get("name")
            or (portfolio_item or {}).get("name")
            or names_primary.get(norm_code(code))
            or code
        )
        name_index.setdefault(str(name), []).append(code)
        # 别名也进索引（如 "小米"→1810.HK），避免快照缺名时搜不到
        for alias in names_aliases.get(norm_code(code), []):
            if alias != name:
                name_index.setdefault(alias, [])
                if code not in name_index[alias]:
                    name_index[alias].append(code)
        stock_documents[code] = {
            "code": code,
            "name": name,
            "projection_generated_at": generated,
            "source_times": {
                "gpt": gpt.get("generated_at"),
                "kimi": kimi.get("generated_at"),
                "triad": triad.get("generated_at"),
            },
            "gpt_factpack_id": gpt.get("factpack_id"),
            "kimi_factpack_id": kimi.get("factpack_id"),
            "gpt": gpt_row,
            "kimi": kimi_row,
            "triad": triad_row,
            "classics": classics_row,
            "decision_snapshot": decision_snapshot,
        }

    assert_private_keys_absent(overview)
    assert_private_keys_absent(name_index)
    for document in stock_documents.values():
        assert_private_keys_absent(document)

    stocks_dir = destination / "stocks"
    modules_dir = destination / "modules"
    stocks_dir.mkdir(parents=True, exist_ok=True)
    modules_dir.mkdir(parents=True, exist_ok=True)
    atomic_json(destination / "overview.json", overview)
    atomic_json(destination / "name_index.json", name_index)

    expected_stocks = set()
    for code, document in stock_documents.items():
        filename = f"{safe_code(code)}.json"
        expected_stocks.add(filename)
        atomic_json(stocks_dir / filename, document)
    for path in stocks_dir.glob("*.json"):
        if path.name not in expected_stocks:
            path.unlink()

    expected_modules = set()
    for filename, value in public_docs.items():
        assert_private_keys_absent(value, f"module.{filename}")
        expected_modules.add(filename)
        atomic_json(modules_dir / filename, value)

    # 派生模块①：持仓名单 portfolio_pub.json
    # 隐私红线：快照会随提问发给模型 API，股数/成本/市值/账户金额一律不进投影；
    # 只放行 名称+代码+盈亏% 与最新止盈止损线（公开市价字段），够回答"买了啥/卖了啥/重点盯啥"。
    if isinstance(positions, dict):
        portfolio_pub = {
            "updated_at": positions.get("updated_at"),
            "note": "V88 脱敏持仓名单：已删除账户、数量、成本和金额。个股问答必须先区分持仓管理与新开仓。",
            "items": [
                public_portfolio_item(item)
                for _, item in sorted(portfolio_index.items())
                if public_portfolio_item(item)
            ],
        }
        # 最新一天的止盈止损线（exit_ledger.json 在 data 内，字段均为公开市价）
        if exit_index["rows"]:
            portfolio_pub["exit_lines"] = {
                "date": exit_index.get("date"),
                "ruleset": exit_index.get("ruleset"),
                "note": "止盈/止损线；last 接近 stop 的即为需要重点关注对象。",
                "lines": list(exit_index["rows"].values()),
            }
        assert_private_keys_absent(portfolio_pub, "module.portfolio_pub.json")
        expected_modules.add("portfolio_pub.json")
        atomic_json(modules_dir / "portfolio_pub.json", portfolio_pub)

    # 派生模块②：自选/重点关注清单（仓库根 watchlist.json，按市场分组 [代码, 名称]）
    watchlist = read_json(Path(__file__).resolve().parents[2] / "watchlist.json", None)
    if isinstance(watchlist, dict):
        assert_private_keys_absent(watchlist, "module.watchlist.json")
        expected_modules.add("watchlist.json")
        atomic_json(modules_dir / "watchlist.json", watchlist)

    for path in modules_dir.glob("*.json"):
        if path.name not in expected_modules:
            path.unlink()

    return len(codes)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--dest", type=Path, required=True)
    args = parser.parse_args()
    count = build(args.source.resolve(), args.dest.resolve())
    print(f"projection ok: {count} stocks")


if __name__ == "__main__":
    main()
