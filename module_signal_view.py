"""Legacy discovery can contribute names, never cached buy authority."""
from html import escape
import re


def legacy_review_text(value):
    text = str(value or '').replace('**','')
    # Do not relabel an old model approval as a GPT-6 approval.
    if re.search(r'kimi|gemini|deepseek',text,re.I):
        return '旧版审核记录已停用；需按当前 GPT-6 与经典巨著协议重新核验。'
    return escape(text)


def current_buy_codes():
    from recommendation_gate import current_publishable, canonical_code, _load
    from stock_reference import reference
    selection=_load('triad_selection.json')
    rows=current_publishable(selection)
    return CurrentBuyCodes({canonical_code(r.get('code')):reference(selection,r) for r in rows})


class CurrentBuyCodes(set):
    def __init__(self,references):
        super().__init__(references)
        self.references=references


def bound(row,code,codes):
    from stock_reference import canonical, compare
    ref=getattr(codes,'references',{}).get(canonical(code))
    if not ref or row.get('master_ref') != ref:return False
    if row.get('tier') in ('1A','2A','3A') and row['tier'] != ref['tier']:return False
    return compare(ref,{**row,'code':code})['ok']


def admitted(row, codes):
    from recommendation_gate import canonical_code
    return bool(row.get('push_eligible') and canonical_code(row.get('code')) in codes and bound(row,row.get('code'),codes))


def admitted_rank(row, code, codes):
    from recommendation_gate import canonical_code
    return bool(row.get('tier') == '3A' and row.get('listable') is True
                and row.get('formal_recommendation') is True
                and canonical_code(code) in codes and bound(row,code,codes))


def discovery_note(name, code, link):
    return f'<div class="v88-operational-note">{link(name,code)} · 技术线索，当前评级与进场条件见上方中央列表。</div>'
