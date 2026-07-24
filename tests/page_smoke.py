"""V88 页面级冒烟回测：真实执行整个 Streamlit 脚本（含全部UI渲染路径），
捕获任何运行时异常/页面错误框。改动后必须跑通此测试才允许说"修复完成"。

【V88·全局自检 2026-07-18 用户问"为什么发现不了"】异常数=0 不等于页面健康——
今日导航挂掉/自选台消失/持仓表全空洞时异常也是0（被静默except吞掉）。
故加内容级断言清单：关键模块必须真的在页面上、坏味道字样不许出现。
新增模块时把它的标志文案加进 MUST_HAVE。"""
import sys
from streamlit.testing.v1 import AppTest

at = AppTest.from_file("app_v88_integrated.py", default_timeout=600)
at.run()

errs = [str(e.value)[:200] for e in at.exception] if at.exception else []
err_boxes = [str(b.value)[:160] for b in at.error] if hasattr(at, "error") else []
print("页面异常数:", len(errs))
for e in errs:
    print("  EXC:", e)
suspicious = [b for b in err_boxes if "not defined" in b or "Error" in b or "Traceback" in b]
for b in suspicious[:5]:
    print("  ERRBOX:", b)

# ── 内容级自检：把页面全部文本拼起来查关键标志 ────────────────────────────
_texts = []
for _attr in ("markdown", "caption", "expander", "info", "warning", "error", "subheader", "header"):
    try:
        for _el in getattr(at, _attr):
            _texts.append(str(getattr(_el, "value", "") or getattr(_el, "label", "")))
    except Exception:
        pass
_page = "\n".join(_texts)

MUST_HAVE = [
    "我的股票池",              # 一池归一·持仓+自选合并模块头(金名=双重身份)
    "决策中心 · 概率卡",       # 持仓决策中心
    "关注中心",                # 三档双向(Cursor 2026-07-20更名,原推荐中心)
    "公告事件雷达",            # 事件雷达
    "打新雷达",                # 打新
    "机构风向标",              # 机构
]
# AppTest对st.empty槽内大HTML有采集盲区(实测总决断在真浏览器第一屏正常,AppTest抓不到)
# ——这些项只警告不判死,真验证走浏览器JS(scrollHeight/innerText探针)。
WARN_ONLY = ["今日总决断"]
MUST_NOT = [
    "暂不可用",                # 顶部搜索/今日导航挂掉的降级文案
    "⚠️渲染异常",              # 渲染兜底提示
]
missing = [m for m in MUST_HAVE if m not in _page]
for _w in WARN_ONLY:
    if _w not in _page:
        print(f"内容自检: ⚠️ 槽内项未采集到(AppTest盲区,请以浏览器实测为准): {_w}")
bad = [m for m in MUST_NOT if m in _page]
# 【V88·呈现层巡检 2026-07-24 用户抓"Neutral没中文,这该自愈系统发现"】
# 裸英文状态词后面必须跟中文括注——出现即坏味道,不等用户抓。
import re as _re_ui
_naked = []
for _w in ("Neutral", "Risk On", "Risk Off"):
    if _re_ui.search(_re_ui.escape(_w) + r"(?!（|<span|\s*（)", _page):
        # 该词至少出现一次未带括注(宽松:只要存在任一带括注实例则视为已处理)
        if (_w + "（") not in _page and (_w + '<span') not in _page:
            _naked.append(_w)
if _naked:
    bad.append(f"裸英文状态词无中文括注:{'、'.join(_naked)}")

# "待下轮日报"少量正常(个别新持仓),泛滥=日报解析挂了(manifest质检被拒等)
_pending_n = _page.count("待下轮日报")
flood = _pending_n >= 12

print("内容自检: 缺失模块:", missing or "无 ✅")
print("内容自检: 坏味道:", bad or "无 ✅", f"| 待下轮日报×{_pending_n}" + ("（泛滥❌）" if flood else ""))

sys.exit(1 if (errs or suspicious or missing or bad or flood) else 0)
