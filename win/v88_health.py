# -*- coding: utf-8 -*-
# v88_health.py — V88 体系每周体检（查漏补缺·确定性脚本·零 token 消耗）
# 用法: python v88_health.py [私仓路径]
# 输出: 分级体检报告（✅正常 / ⚠️警告 / ❌断更），供蓝一转述+台账更新依据。
import json
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

# (文件, 时间字段候选, 最大允许天数) — 交易日≈自然日放宽到4天（跨周末）
FILES = [
    ("data/market_pool.json",        ["generated_at"], 4),
    ("data/market_snapshot.json",    ["asof", "generated_at"], 4),
    ("data/bottom_turn_pool.json",   ["generated_at"], 4),
    ("data/value_zone.json",         ["generated_at"], 4),
    ("data/trend_shift.json",        ["generated_at"], 4),
    ("data/rotation_forecast.json",  ["analysis_time", "generated_at"], 4),
    ("data/intraday_decisions.json", ["generated_at"], 4),
    ("data/classics_lens.json",      ["generated_at"], 4),
    ("data/gpt_verify.json",         ["generated_at"], 4),
    ("data/tomorrow_plan.json",      ["generated_at"], 4),
    ("data/fable_plan.json",         ["generated_at", "asof"], 999),  # 只查字段存在性
]

TS_RE = re.compile(r"(20\d{2}-\d{2}-\d{2})(?:[ T](\d{2}:\d{2}(?::\d{2})?))?")


def find_repo():
    cands = []
    if len(sys.argv) > 1:
        cands.append(Path(sys.argv[1]))
    here = Path(__file__).resolve()
    cands += [
        here.parent.parent / "ai-daily-report-v2",          # 仓库内 win/ 的兄弟目录
        Path.home() / "Desktop" / "ai-daily-report-v2",
        Path("C:/Users/admin/Desktop/ai-daily-report-v2"),
        Path("/Users/bluesteven/Desktop/ai-daily-report-v2"),
    ]
    for c in cands:
        if (c / "data" / "market_pool.json").exists():
            return c
    return None


def parse_ts(obj, keys):
    for k in keys:
        v = obj.get(k)
        if isinstance(v, str):
            m = TS_RE.search(v)
            if m:
                return m.group(1) + " " + (m.group(2) or "00:00")[:5], m.group(0)
    return None, None


def git(repo, *args):
    try:
        r = subprocess.run(["git", "-C", str(repo)] + list(args),
                           capture_output=True, text=True, timeout=30,
                           encoding="utf-8", errors="replace")
        return (r.stdout or "").strip()
    except Exception as e:
        return "ERR:%s" % e



def main():
    now = datetime.now()
    ok = warn = bad = 0
    lines = ["【V88体系体检】%s" % now.strftime("%Y-%m-%d %H:%M")]

    repo = find_repo()
    if not repo:
        print("\n".join(lines + ["❌ 找不到私仓 ai-daily-report-v2（试了常见路径）"]))
        sys.exit(1)
    lines.append("私仓: %s" % repo)

    # 1) 数据新鲜度
    for rel, keys, max_days in FILES:
        p = repo / rel
        if not p.exists():
            lines.append("❌ %s 文件缺失" % rel)
            bad += 1
            continue
        try:
            obj = json.load(open(p, encoding="utf-8"))
        except Exception as e:
            lines.append("❌ %s 解析失败: %s" % (rel, str(e)[:40]))
            bad += 1
            continue
        dt, raw = parse_ts(obj, keys)
        if not dt:
            lines.append("⚠️ %s 无时间戳字段（无法判断新鲜度）" % rel)
            warn += 1
            continue
        age = (now - datetime.strptime(dt, "%Y-%m-%d %H:%M")).days
        rows = obj.get("rows")
        n = len(rows) if isinstance(rows, (list, dict)) else "-"
        tag = "✅" if age <= max_days else "❌"
        if age > max_days:
            bad += 1
        else:
            ok += 1
        lines.append("%s %s: %s（%d天前, 行数%s）" % (tag, rel.split("/")[-1], raw, age, n))

    # 2) Current model binding; stale records remain unusable.
    try:
        doc = json.loads((repo / "data" / "gpt_verify.json").read_text(encoding="utf-8"))
        valid = doc.get("model") == "gpt-6-astra"
        lines.append("✅ GPT-6审核模型已绑定" if valid else "❌ 缺少当前GPT-6审核，保持待复核")
        ok += int(valid)
        bad += int(not valid)
    except (OSError, ValueError):
        lines.append("❌ GPT-6审核文件未就绪")
        bad += 1

    # 3) git 健康
    unmerged = git(repo, "ls-files", "-u")
    n_un = len([l for l in unmerged.splitlines() if l.strip()]) if not unmerged.startswith("ERR") else -1
    stash = git(repo, "stash", "list")
    n_stash = len([l for l in stash.splitlines() if l.strip()]) if not stash.startswith("ERR") else -1
    sb = git(repo, "status", "-sb").splitlines()[0] if not git(repo, "status", "-sb").startswith("ERR") else "?"
    if n_un > 0:
        lines.append("❌ git 有 %d 个未解决冲突（上次卡死5天就是这种）" % n_un)
        bad += 1
    elif n_un == 0:
        lines.append("✅ git 无冲突 | %s | stash堆积 %d 个%s" % (sb, n_stash, "（建议清理）" if n_stash > 5 else ""))
        ok += 1

    lines.append("订阅：GPT-6/Codex；不调用按量余额接口")

    lines.append("—— 汇总: ✅%d ⚠️%d ❌%d ——" % (ok, warn, bad))
    print("\n".join(lines))


main()
