from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from research_validation_view import render


class View:
    def __init__(self): self.lines = []
    def __getattr__(self, _): return lambda text: self.lines.append(str(text))


def test_empty_error_and_zero_are_distinct():
    s = View(); render(s, {})
    assert '尚未生成' in ''.join(s.lines)
    s = View(); render(s, {'error': 'disk', 'status': '验证刷新失败'})
    assert '刷新失败' in ''.join(s.lines) and '已结算 0' not in ''.join(s.lines)


def test_no_percentage_or_fake_broker_performance_and_table_unchanged():
    s = View(); render(s, {'snapshots': {'unique_codes': 680, 'verified_market_series': 541},
                         'contracts': {'total': 15, 'states': {'WAITING_ENTRY': 15}},
                         'remaining': ['原始财报']})
    text = ''.join(s.lines)
    assert '15 份' in text and '未入场 15' in text and '已结算 0' in text
    assert '审核分不当作上涨概率' in text and '真实成交 未核实' in text


def test_missing_counts_are_unknown_but_explicit_zero_is_preserved():
    s=View();render(s,{'generated_at':'source','broker_fills_verified':0})
    text=''.join(s.lines)
    assert '冻结 未核实' in text and '模拟已结算 未核实' in text
    assert '真实成交 0' in text and 'PIT回放 未核实' in text


def test_original_contract_data_failures_remain_visible():
    s=View();render(s,{'contracts':{'total':2,'states':{'WAITING_DATA':2}},
        'tracking_status':'DEGRADED_DATA','status':'预测能力尚未证明',
        'settlement_data_errors':[{'code':'A'},{'code':'B'}]})
    text=''.join(s.lines)
    assert '原模拟合同数据待修复 2 份' in text and '预测能力尚未证明' in text
    assert '模拟已结算 0' in text
