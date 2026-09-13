import sys
from pathlib import Path
from datetime import datetime,timezone,timedelta
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from astra_plan_view import render
class UI:
    def __init__(self):self.calls=[]
    def __getattr__(self,name):return lambda value,**kwargs:self.calls.append((name,str(value)))
    def expander(self,value,**kwargs):self.calls.append(('expander',str(value)));return self
    def __enter__(self):return self
    def __exit__(self,*args):return False

def current_document(**values):
    now=datetime.now(timezone(timedelta(hours=8)))
    return {'month':now.strftime('%Y-%m'),'generated_at':now.isoformat(),'factpack_id':'current-pack',
            'policy':{'monthly_target':200},'research_report':{'factpack_id':'current-pack',
                'month':now.strftime('%Y-%m'),'generated_at':now.isoformat()},**values}

def test_unreconciled_state_never_formats_unknown_profit_or_stops_as_zero():
    ui=UI();render(ui,current_document(month_state={'state':'LEDGER_UNVERIFIED','reason':'本月成交台账尚未对账'}))
    text='\n'.join(v for _,v in ui.calls)
    assert '未对账不按0收益计算' in text and '待成交对账' in text
    assert '进度 0.0%' not in text and '0次亏损止损' not in text
    assert '专业投机原理' in text and '萨普' in text

def test_one_r_review_is_visible_and_scenario_remains_unfilled():
    ui=UI();render(ui,current_document(protection_reviews=[{'code':'TEST','status':'REVIEW_DUE','reason':'已达1R，复核保护'}],
                     monthly_contracts=[{'code':'TEST','target_fit':{'remaining_target':125,'uncovered_target':75,'reason':'不是实际收益'}}]))
    assert any('已达1R' in value for _,value in ui.calls)
    text='\n'.join(v for _,v in ui.calls)
    assert '尚未成交，不计进度' in text and '剩余差额 75' in text

def test_public_view_omits_private_monthly_profit_state():
    ui=UI();render(ui,{'private_redacted':True,'month_state':{'realized_net_pnl':123456789}})
    text='\n'.join(v for _,v in ui.calls)
    assert '123456789' not in text and '实际账户仅私域显示' in text
