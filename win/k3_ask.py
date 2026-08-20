# -*- coding: utf-8 -*-
# k3_ask.py — 调用 Moonshot K3 回答一个问题（顺滑层"K3回复"关键词的核心脚本）
# 密钥运行时从本机 ~/.openclaw/openclaw.json 读取，本文件不含任何密钥，可安全入库。
import json
import re
import sys
import urllib.request
import urllib.error
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

SYSTEM = (
    "你是V88三方会审体系的首席分析师Kimi（角色：证据官/反对票/漏审检查）。"
    "回答纪律："
    "1) 先报数据时间和来源；"
    "2) 结论必须有依据，无依据就直说'无评级数据不推荐'；"
    "3) 严格区分'不否定'与'通过'；"
    "4) 涉及买卖必须给出入场区间、止损/失效条件，给不出就标注'暂不可执行'；"
    "5) 绝不编造任何数字或记录。"
)


def find_key(cfg: dict):
    mp = cfg.get("models", {}).get("providers", {}).get("moonshot", {})
    if isinstance(mp, dict):
        for cand in (mp.get("apiKey"), (mp.get("auth") or {}).get("apiKey") if isinstance(mp.get("auth"), dict) else None):
            if isinstance(cand, str) and cand.strip().startswith("sk-"):
                return cand.strip()
    m = re.search(r"sk-[A-Za-z0-9]{16,}", json.dumps(cfg, ensure_ascii=False))
    return m.group(0) if m else None


def find_base(cfg: dict):
    mp = cfg.get("models", {}).get("providers", {}).get("moonshot", {})
    if isinstance(mp, dict):
        base = mp.get("baseUrl") or mp.get("baseURL")
        if isinstance(base, str) and base.startswith("http"):
            return base.rstrip("/")
    return "https://api.moonshot.cn/v1"


def main():
    q = " ".join(sys.argv[1:]).strip()
    if not q:
        print("用法: py -3 k3_ask.py <问题>")
        sys.exit(2)

    cfg_path = Path.home() / ".openclaw" / "openclaw.json"
    cfg = json.load(open(cfg_path, encoding="utf-8"))
    key = find_key(cfg)
    if not key:
        print("ERROR: 未在 openclaw.json 中找到 moonshot 密钥")
        sys.exit(1)
    base = find_base(cfg)

    body = json.dumps({
        "model": "kimi-k3",
        "messages": [
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": q},
        ],
        "temperature": 0.3,
    }).encode("utf-8")

    req = urllib.request.Request(
        base + "/chat/completions",
        data=body,
        headers={"Authorization": "Bearer " + key, "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            data = json.load(r)
        print(data["choices"][0]["message"]["content"])
    except urllib.error.HTTPError as e:
        print("ERROR: HTTP %s — %s" % (e.code, e.read().decode("utf-8", "replace")[:500]))
        sys.exit(1)
    except Exception as e:
        print("ERROR: %s" % e)
        sys.exit(1)


main()
