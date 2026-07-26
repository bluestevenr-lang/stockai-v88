# -*- coding: utf-8 -*-
"""V88·分页版入口(2026-07-26 用户拍板"开始做 立刻")

单一事实源架构: 不复制任何业务代码——按 app_v88_integrated.py 内的
`# ===V88_PAGE_BREAK:XXX===` 标记把同一份源码切成 A/B/C/D 四段,按所选页执行:
  A(总是) = 概览+作战卡+行动中心+作战板      → 🏠总览页只跑这3.9k行=秒开
  B=LISTS  双门/关注中心/自选/持仓决策/搜索/机构/公告/三榜
  C=RADAR  打新/接力/全行业/触底
  D=RESEARCH 旧简报/台账/深度分析/自省/预算
页映射(后段依赖前段定义→执行前缀链): 总览=A ｜ 名单·决策=A+B ｜ 雷达=A+B+C ｜ 研究·系统=A+B+D
单页版 8501 不受影响(标记是纯注释);本入口跑 8503 供验收。深链(?q=&focus=deep)自动切研究页。
"""
from pathlib import Path

_SRC_PATH = Path(__file__).parent / "app_v88_integrated.py"
_SRC = _SRC_PATH.read_text(encoding="utf-8")

_segs = {}
_cur, _buf = "A", []
for _ln in _SRC.splitlines():
    if _ln.startswith("# ===V88_PAGE_BREAK:"):
        _segs[_cur] = "\n".join(_buf)
        _cur, _buf = _ln.split(":", 1)[1].rstrip("=").rstrip("="), []
        _cur = _cur.replace("=", "")
        continue
    _buf.append(_ln)
_segs[_cur] = "\n".join(_buf)

PAGES = {"🏠 总览作战台": ("A",),
         "📋 名单·决策": ("A", "LISTS"),
         "🛰️ 机会雷达": ("A", "LISTS", "RADAR"),
         "📚 研究·系统": ("A", "LISTS", "RESEARCH")}

import streamlit as _st_nav

_qp = dict(_st_nav.query_params)
_page_default = "🏠 总览作战台"
if _qp.get("q") and str(_qp.get("focus")) == "deep":
    _page_default = "📚 研究·系统"   # 深链直达深度分析(位于研究段)
_page = _st_nav.session_state.get("v88_page_nav", _page_default)
if _qp.get("q") and str(_qp.get("focus")) == "deep":
    _page = "📚 研究·系统"
if _page not in PAGES:
    _page = _page_default

_G = {"__name__": "__main__", "__file__": str(_SRC_PATH)}
for _seg_name in PAGES[_page]:
    _code = compile(_segs[_seg_name], f"v88_seg_{_seg_name}", "exec")
    exec(_code, _G)

# 侧边导航(sidebar位置固定,不受渲染顺序影响;切换→rerun→按新页执行前缀链)
with _st_nav.sidebar:
    _st_nav.markdown("### 🧭 V88 分页导航")
    _st_nav.radio("页面", list(PAGES.keys()), key="v88_page_nav",
                  index=list(PAGES.keys()).index(_page))
    _st_nav.caption("总览=秒开(只跑3.9k行)\n其余页含依赖段·首开较慢\n单页版仍在8501·数据同源")
