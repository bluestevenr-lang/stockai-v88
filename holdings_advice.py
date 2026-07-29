"""
holdings_advice.py — 读取 positions.json，用 V88 真引擎逐只给持仓建议
输出「我的持仓建议」Markdown（接入 V88 / 飞书 / 轻量版三端）。
评分与桌面版完全一致（headless 复用 app_v88_integrated 引擎）。
用法: python3 holdings_advice.py [markets]   markets 可选 A/US，默认全部
"""
import os, sys, json, types, importlib.util
from pathlib import Path

STOCKAI = Path(__file__).resolve().parent
# 持仓唯一权威版在云端仓 ai-daily-report-v2/positions.json（用户要求全部云端）
POS = Path.home() / "Desktop" / "ai-daily-report-v2" / "positions.json"


# ── streamlit 桩（同 v88_lite），headless 载引擎 ──
class _Ctx:
    def __enter__(s): return s
    def __exit__(s,*a): return False
    def __call__(s,*a,**k): return s
    def __iter__(s): return iter([_Ctx() for _ in range(4)])
    def __getattr__(s,n): return _Ctx()
class _SS(dict):
    def __getattr__(s,n): return s.get(n)
    def __setattr__(s,n,v): s[n]=v
def _at(f): f.clear=lambda *a,**k:None; return f
class _Cache:
    def __call__(s,*a,**k):
        if len(a)==1 and callable(a[0]) and not k: return _at(a[0])
        return lambda f:_at(f)
    def clear(s,*a,**k): return None
class _ST(types.ModuleType):
    def __init__(s):
        super().__init__("streamlit"); s.session_state=_SS()
        s.columns=lambda spec,*a,**k:[_Ctx() for _ in range(spec if isinstance(spec,int) else len(spec))]
        s.tabs=lambda l,*a,**k:[_Ctx() for _ in range(len(l))]
        s.cache_data=_Cache(); s.cache_resource=_Cache(); s.fragment=_Cache(); s.secrets={}
        for n in ("text_input","text_area","chat_input"): setattr(s,n,lambda *a,**k:"")
        for n in ("number_input","slider"): setattr(s,n,lambda *a,**k:k.get("value") or 0)
        for n in ("checkbox","toggle","button","form_submit_button","download_button"): setattr(s,n,lambda *a,**k:False)
        s.multiselect=lambda *a,**k:k.get("default") or []
        s.radio=s.selectbox=lambda label=None,options=None,*a,**k:(list(options)[k.get("index",0)] if options else None)
    def __getattr__(s,n): return _Ctx()

_ENG=None
def engine():
    global _ENG
    if _ENG: return _ENG
    real=sys.modules.get("streamlit"); sys.modules["streamlit"]=_ST()
    os.environ["V88_ENGINE_ONLY"]="1"; sys.path.insert(0,str(STOCKAI))
    try:
        spec=importlib.util.spec_from_file_location("v88eng",STOCKAI/"app_v88_integrated.py")
        m=importlib.util.module_from_spec(spec); sys.modules["v88eng"]=m
        try: spec.loader.exec_module(m)
        except Exception as e:
            if type(e).__name__!="_V88EngineReady": raise
        _ENG=m; return m
    finally:
        if real is not None: sys.modules["streamlit"]=real


def advise_holding(eng, h):
    """单只持仓 → 建议 dict。"""
    code=str(h["code"])
    if "⚠" in code or code in ("SPCX",) and False:
        pass
    out={"name":h["name"],"code":code,"shares":h.get("shares"),"cost":h.get("cost")}
    try:
        c=eng.to_yf_cn_code(code)
        df=eng.fetch_stock_data(c)
        if df is None or len(df)<20:
            out["advice"]="⚠️ 取不到行情"; return out
        m=eng.calculate_metrics_all(df,c)
        from v88_decision_core import evaluate_decision
        decision=evaluate_decision(
            df,m.get("trend_full") or {},holding=h,action_hint="持有",
            name=h.get("name",code),code=code)
        last=float(m["last_price"]); score=int(decision["unified_score"])
        cost=h.get("cost"); sh=h.get("shares")
        pnl_pct=(last-cost)/cost*100 if cost else None
        pnl_amt=(last-cost)*sh if (cost and sh) else None
        try:
            l=float(m["df"]["Low"].tail(250).min()); hi=float(m["df"]["High"].tail(250).max())
            pos=(last-l)/(hi-l)*100 if hi>l else 50
        except Exception: pos=50
        act=decision["action"]
        st=(f"防守{decision.get('stop') or '—'}→压力{decision.get('resistance') or '—'}"
            f"（盈亏比{decision.get('rr',0):.2f}）")
        out.update({"last":last,"score":score,"rs20":m.get("rs20"),"pnl_pct":pnl_pct,
                    "pnl_amt":pnl_amt,"action":act,"stop_target":st,"decision":decision})
    except Exception as e:
        out["advice"]=f"⚠️ {type(e).__name__}"
    return out


def render_md(markets=None):
    d=json.load(open(POS,encoding="utf-8"))
    eng=engine()
    lines=["## 📌 我的持仓建议",""]
    for acc,info in d["accounts"].items():
        is_a = "A股" in info.get("type","")
        if markets=="A" and not is_a: continue
        if markets=="US" and is_a: continue
        lines.append(f"### {acc}")
        lines.append("| 名称 | 统一分(短/中/长) | 上/下估计 | 盈亏比·期望 | 现价 | 持仓盈亏 | 操作建议 | 止损/目标 |")
        lines.append("|---|---|---|---|---|---|---|---|")
        for h in info["holdings"]:
            r=advise_holding(eng,h)
            if "action" not in r:
                lines.append(f"| {r['name']} | — | — | — | — | — | {r.get('advice','—')} | — |"); continue
            pnl=f"{r['pnl_pct']:+.1f}%" if r['pnl_pct'] is not None else "—"
            if r.get("pnl_amt") is not None: pnl+=f" ({r['pnl_amt']:+.0f})"
            d=r["decision"]
            lines.append(f"| {r['name']} | {r['score']} ({d['short_score']}/{d['medium_score']}/{d['long_score']}) | "
                         f"{d['p_up']}%/{d['p_down']}% | {d['rr']:.2f}·{d['expected_pct']:+.1f}% | "
                         f"{r['last']:.2f} | {pnl} | {r['action']} | {r['stop_target']} |")
        lines.append("")
    return "\n".join(lines)


if __name__=="__main__":
    mk=sys.argv[1] if len(sys.argv)>1 else None
    print(render_md(mk))
