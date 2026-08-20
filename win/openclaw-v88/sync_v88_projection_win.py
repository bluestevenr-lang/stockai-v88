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

    gpt_rows = rows_map(gpt)
    kimi_rows = rows_map(kimi)
    triad_rows = rows_map(triad)
    classics_rows = rows_map(classics)
    codes = sorted(set(gpt_rows) | set(kimi_rows) | set(triad_rows) | set(classics_rows))
    generated = datetime.now(CST).strftime(
        "%Y-%m-%d %H:%M:%S（北京时间）"
    )

    overview = {
        "projection_generated_at": generated,
        "privacy": "不含账户、资产、持仓数量、成本、现金或交易凭据",
        "decision_semantics": {
            "gpt_role": "终审",
            "kimi_role": "独立评审快照",
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
        gpt_row = pick(gpt_rows.get(code), GPT_FIELDS)
        kimi_row = pick(kimi_rows.get(code), KIMI_FIELDS)
        triad_row = triad_rows.get(code) if isinstance(triad_rows.get(code), dict) else {}
        classics_row = pick(classics_rows.get(code), CLASSICS_FIELDS)
        name = (
            triad_row.get("name")
            or gpt_row.get("name")
            or classics_row.get("name")
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
    for filename in PUBLIC_MODULES:
        value = read_json(source / filename, None)
        if value is None:
            continue
        assert_private_keys_absent(value, f"module.{filename}")
        expected_modules.add(filename)
        atomic_json(modules_dir / filename, value)
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
