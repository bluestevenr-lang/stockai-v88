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
import streamlit as _st_boot

_SRC_PATH = Path(__file__).parent / "app_v88_integrated.py"


@_st_boot.cache_resource(show_spinner=False)
def _load_segments(mtime: float):
    # ⚠️参数名不可下划线开头: Streamlit缓存把 _xxx 参数视为"不参与哈希",
    # 会导致缓存永不失效(源码改了还跑旧代码)/不同参数命中同一份缓存。2026-07-27踩坑修复。
    """切段+预编译一次缓存(mtime变=源码更新才重做)——省每次rerun的重复读盘/切分/compile。"""
    src = _SRC_PATH.read_text(encoding="utf-8")
    segs, cur, buf = {}, "A", []
    for ln in src.splitlines():
        if ln.startswith("# ===V88_PAGE_BREAK:"):
            segs[cur] = "\n".join(buf)
            cur, buf = ln.split(":", 1)[1].strip("="), []
            continue
        buf.append(ln)
    segs[cur] = "\n".join(buf)
    return segs


_segs = dict(_load_segments(_SRC_PATH.stat().st_mtime))

# B段公共定义抽取(第二阶段提速): 只取top-level函数def+字面量常量赋值——
# 函数体惰性执行零副作用;雷达页借此跳过B段13k行的渲染与扫描。
import ast as _ast_pg

def _extract_defs(seg_src: str) -> str:
    try:
        tree = _ast_pg.parse(seg_src)
    except SyntaxError:
        return ""
    keep = []
    for node in tree.body:
        if isinstance(node, (_ast_pg.FunctionDef, _ast_pg.AsyncFunctionDef, _ast_pg.ClassDef,
                             _ast_pg.Import, _ast_pg.ImportFrom)):
            keep.append(node)
        elif isinstance(node, _ast_pg.Assign) and all(isinstance(t, _ast_pg.Name) for t in node.targets):
            try:
                _ast_pg.literal_eval(node.value)   # 只收字面量赋值(常量池RAW_US等),防执行副作用
                keep.append(node)
            except Exception:
                continue
    mod = _ast_pg.Module(body=keep, type_ignores=[])
    return _ast_pg.unparse(mod)

@_st_boot.cache_resource(show_spinner=False)
def _bdefs_cached(mtime: float):
    return _extract_defs(_load_segments(mtime).get("LISTS", ""))


_segs["B_DEFS"] = _bdefs_cached(_SRC_PATH.stat().st_mtime)
# RAW池初始化是调用式赋值(init_stock_pools=本地硬编码池,轻且无网络副作用)——原文切片补进B_DEFS
_lists_lines = _segs.get("LISTS", "").splitlines()
for _i, _l in enumerate(_lists_lines):
    if _l.startswith("RAW_US, RAW_HK, RAW_CN_TOP = init_stock_pools()"):
        _raw_block = []
        for _j in range(_i, min(_i + 60, len(_lists_lines))):
            _lj = _lists_lines[_j]
            if _j > _i and _lj.startswith(("def ", "class ", "# ═", "with st.", "st.")):
                break
            _raw_block.append(_lj)
        _segs["B_DEFS"] += "\n" + "\n".join(_raw_block)
        break

PAGES = {"🏠 总览作战台": ("A",),
         "📋 名单·决策": ("A", "LISTS"),
         "🛰️ 机会雷达": ("A", "B_DEFS", "RADAR"),
         "📚 研究·系统": ("A", "B_DEFS", "RESEARCH")}

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
# 深链(?q=&focus=deep)必须走全链: 深链消费与搜索逻辑在LISTS段(15025/4270区),
# 快链会让点名跳转无声失效——保真优先,深链慢一点也要对。
_chain = (("A", "LISTS", "RESEARCH") if (_qp.get("q") and str(_qp.get("focus")) == "deep")
          else PAGES[_page])
@_st_boot.cache_resource(show_spinner=False)
def _compiled(seg_name: str, mtime: float):
    # ⚠️同上:原写法 _seg_name/_mtime 两个参数都不参与哈希 → 首次编译的A段code被
    # 后续所有段命中,LISTS/RESEARCH实际重复执行A段 → 重复key崩页(切页Connection error真因)
    return compile(_segs[seg_name], f"v88_seg_{seg_name}", "exec")


_mt = _SRC_PATH.stat().st_mtime
for _i_seg, _seg_name in enumerate(_chain):
    if _i_seg >= 1:
        # 长段执行给前端反馈(名单页首扫约1分钟)——防"看起来卡死"
        with _st_boot.spinner(f"加载 {_seg_name} 段…名单页首次含全池扫描约1分钟,之后30分钟内秒开"):
            exec(_compiled(_seg_name, _mt), _G)
    else:
        exec(_compiled(_seg_name, _mt), _G)

# 侧边导航(sidebar位置固定,不受渲染顺序影响;切换→rerun→按新页执行前缀链)
with _st_nav.sidebar:
    _st_nav.markdown("### 🧭 V88 分页导航")
    _st_nav.radio("页面", list(PAGES.keys()), key="v88_page_nav",
                  index=list(PAGES.keys()).index(_page))
    _st_nav.caption("总览=秒开(只跑3.9k行)\n其余页含依赖段·首开较慢\n单页版仍在8501·数据同源")
