"""Model inputs keep original constraints, not the locally rendered K-lines."""
import ast
from copy import deepcopy
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

from deep_prompt_context import VERSION, annual_summary, build, model_context


def sample():
    plan = {'entry_range': [90., 95.], 'take_profit_range': [110., 115.], 'stop': 85.,
            'promotion_trigger': '等待量能确认，未确认不可执行', 'invalidation': '收盘触及85即先处理风险',
            'position_cap': '仅中央3A允许开仓', 'horizon': 'short',
            'profit_contract': {'thesis_deadline': '2026-09-30T16:00:00+08:00',
                                'exit_rule': '到期不得自动延长', 'cost_assumption': '买卖各0.5%',
                                'net_reward_risk': 2., 'guaranteed': False},
            'history': [{'Close': 99}] * 500}
    annual = {'code': 'EXAMPLE', 'headline': '年线向上但先防回撤', 'thesis': '财报缺口未解除，不赋予年度评级',
              'regime': 'up_caution', 'factpack_id': 'f' * 64, 'input_id': 'a' * 64,
              'snapshot_signature': 'b' * 64, 'source_asof': '2026-09-11',
              'metrics': {'ma200': 100., 'annual_bars': 252, 'annual_complete': False,
                          'missing_sessions': ['2025-09-15'], 'source_closes': [90] * 500},
              'phases': [{'phase': f'{i*3}–{(i+1)*3}个月', 'outlook': '条件路径'+str(i),
                          'confirmation': '必须获得新财报与结构确认'+str(i),
                          'invalidation': '末阶段风险仍必须完整保留'+str(i)} for i in range(4)],
              'scenarios': [{'scenario': '下行情景', 'confirmation': '持续创新低', 'invalidation': '反证解决'}],
              'risks': ['不可把历史位置当全年目标', {'risk': '下行', 'condition': '年线转弱', 'points': [1]*500}],
              'gaps': ['缺少当前长期完整双审'],
              'evidence': {'annual_gpt_current': False, 'scope': '短期审核不外推一年', 'evidence_ids': ['receipt']},
              'history': {'series_id': 'c'*64, 'source': 'verified-local', 'source_asof': '2026-09-11',
                          'source_range': {'start': '2024-09-01', 'end': '2026-09-11', 'bars': 500},
                          'gaps': ['MA200部分窗口不足'], 'points': [
                              {'date': str(i), 'close': 100+i/100, 'ma60': 99., 'ma120': 98., 'ma200': 97.}
                              for i in range(252)],
                          'source_closes': [{'date': str(i), 'close': i+10} for i in range(500)]}}
    joint = {'status': '存在分歧', 'review_blocks': ['先复核短线转弱', '缺同包企业支持'],
             'entry_permission': False, 'original_plan': plan, 'input_id': 'j'*64,
             'domains': [{'domain': 'fundamental', 'status': 'limited', 'limitations': ['未核验财报正文'],
                          'reviews': [{'role': 'counteraudit', 'conclusion': '反对', 'reason': '利润未兑现'}],
                          'origins': [{'source_asof': '2026-09-01', 'receipt_hex_groups': ['abcd'],
                                       'source_closes': [100]*500}]}]}
    period = {'version': 'period-consistency-v1', 'status': 'conditional', 'headline': '周期差异需保留',
              'summary': '短中长分别核验', 'conditions': ['不把2周反弹当一年向上'], 'gaps': ['32周样本不足'],
              'source_asof': '2026-09-11', 'snapshot_signature': 'b'*64, 'annual_input_id': 'a'*64,
              'synthesis_input_id': 'j'*64, 'input_id': 'p'*64,
              'observations': [{'window': '32周', 'trading_days': 160, 'complete': False,
                                'score': None, 'meaning': '数据不足', 'points': [1]*500}]}
    cross = {'status': '需复核', 'annual_outlook': annual, 'joint_conclusion': joint,
             'period_consistency': period, 'checks': [{'id': 'invalidation', 'status': 'gap',
                  'detail': '原失效线被触及', 'next_action': '处理原合同风险'}]}
    central = {'tier': '1A', 'executable': False, 'audit_id': 'd'*64, 'trade_plan': plan}
    return annual, joint, cross, central


def keys(value):
    if isinstance(value, dict):
        for key, child in value.items():
            yield key
            yield from keys(child)
    elif isinstance(value, list):
        for child in value:
            yield from keys(child)


def project_sample():
    annual, joint, cross, central = sample()
    return build(annual_outlook=annual, joint_conclusion=joint, cross_validation=cross, central=central)


def test_whitelist_excludes_all_nested_plot_arrays_without_changing_sources():
    annual, joint, cross, central = sample()
    before = deepcopy((annual, joint, cross, central))
    result = build(annual_outlook=annual, joint_conclusion=joint, cross_validation=cross, central=central)
    assert not {'history', 'points', 'source_closes', 'Close', 'Open', 'High', 'Low', 'Volume'} & set(keys(result))
    assert (annual, joint, cross, central) == before
    assert result['annual_outlook']['metrics']['missing_sessions_count'] == 1
    assert result['annual_outlook']['series_evidence']['series_id'] == 'c'*64
    assert len(annual['history']['points']) == 252


def test_all_four_conditions_risks_evidence_and_original_contract_survive():
    annual, joint, cross, central = sample()
    result = project_sample()
    actual = result['annual_outlook']
    for key in ('headline', 'thesis', 'regime', 'phases', 'scenarios', 'gaps', 'evidence', 'input_id', 'snapshot_signature'):
        assert actual[key] == annual[key]
    assert actual['risks'] == ['不可把历史位置当全年目标', {'risk': '下行', 'condition': '年线转弱'}]
    assert result['review_blocks'] == joint['review_blocks']
    assert result['central_constraints']['executable'] is False
    assert result['central_constraints']['trade_plan'] == {k:v for k,v in central['trade_plan'].items() if k != 'history'}
    assert result['joint_conclusion']['domains'][0]['reviews'][0]['conclusion'] == '反对'
    assert result['cross_validation']['checks'] == cross['checks']
    p = result['cross_validation']['period_consistency']
    assert p['conditions'] == cross['period_consistency']['conditions']
    assert p['gaps'] == cross['period_consistency']['gaps']
    assert p['annual_input_id'] == 'a'*64 and p['synthesis_input_id'] == 'j'*64
    assert p['observations'][0]['complete'] is False


def test_complete_structured_context_is_not_truncated_and_is_projected_again():
    projected = project_sample()
    projected['annual_outlook']['history'] = {'points': [99]*500}
    text = model_context(json.dumps(projected, ensure_ascii=False))
    assert len(text) > 1500 and '末阶段风险仍必须完整保留3' in text
    assert 'history' not in set(keys(json.loads(text)))
    assert model_context(text) == text
    assert json.loads(text)['annual_outlook']['series_evidence']['series_id'] == 'c'*64


@pytest.mark.parametrize('text', ['free text', '[]', '{}', '{bad', '{"prompt_context_version":"old"}'])
def test_unknown_context_keeps_callers_ordinary_text_limit(text):
    assert model_context(text) is None


def test_prompt_payload_has_material_reduction_on_full_year_input():
    annual, joint, cross, central = sample()
    before = json.dumps({**central, 'cross_validation': cross, 'joint_conclusion': joint}, ensure_ascii=False)
    after = model_context(json.dumps(project_sample(), ensure_ascii=False))
    assert len(after) < len(before) * .3


def test_both_app_model_boundaries_use_projection_without_raw_five_day_table():
    tree = ast.parse((Path(__file__).resolve().parents[1] / 'app_v88_integrated.py').read_text())
    assignments = {node.targets[0].id: node.value for node in ast.walk(tree)
                   if isinstance(node, ast.Assign) and len(node.targets) == 1
                   and isinstance(node.targets[0], ast.Name)
                   and node.targets[0].id in ('_fwd_ctx', '_deep_central_prompt')}
    assert set(assignments) == {'_fwd_ctx', '_deep_central_prompt'}
    for expr in assignments.values():
        assert any(isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
                   and n.func.id == '_deep_prompt_build' for n in ast.walk(expr))
    assert not any(isinstance(n, ast.Name) and n.id == '_last5' for n in ast.walk(tree))


def test_forward_reason_prompt_reaches_last_condition_without_model_or_network(monkeypatch):
    import stock_horizon
    captured = []
    def fake_completion(messages, **kwargs):
        captured.append(messages[0]['content'])
        return {'usage': {}, 'fake_text': json.dumps({'overall': '仍需复核', 'reasons': {'5日': '企业证据未齐，等条件确认'}})}
    monkeypatch.setattr(stock_horizon, 'gpt_subscription_ready', lambda *a: 'fake-local-ticket')
    monkeypatch.setattr(stock_horizon, 'model_name', lambda: 'fake-no-model')
    monkeypatch.setattr(stock_horizon, '_cache_read', lambda *a: None)
    monkeypatch.setattr(stock_horizon, '_cache_write', lambda *a: None)
    monkeypatch.setattr(stock_horizon, 'chat_completion', fake_completion)
    monkeypatch.setattr(stock_horizon, 'message_text', lambda body: body['fake_text'])
    monkeypatch.setitem(sys.modules, 'v88_ai_budget', SimpleNamespace(
        reserve=lambda *a, **k: {'id': 'fake-local'}, settle=lambda *a, **k: None))
    context = json.dumps(project_sample(), ensure_ascii=False)
    result = stock_horizon.forward_reasons('示例', 'EXAMPLE', {
        'stage': '核验中', 'horizons': [{'label': '5日', 'view': '震荡', 'p_up': 50,
            'upside_pct': 10, 'downside_pct': 5, 'target_price': 110, 'risk_price': 85}]},
        context=context, allow_ai=True)
    assert result['status'] == 'completed' and len(captured) == 1
    prompt = captured[0]
    assert '末阶段风险仍必须完整保留3' in prompt
    assert '到期不得自动延长' in prompt and '32周样本不足' in prompt
    assert '原失效线被触及' in prompt and '先复核短线转弱' in prompt
    assert 'source_closes' not in prompt and '"points"' not in prompt and '"history"' not in prompt
    assert len(stock_horizon._prompt_context('普通文本' * 1000, 1500)) <= 1500


def test_future_paths_keep_branch_conditions_and_counterevidence_without_chart_payloads():
    future = {'input_id':'future-1', 'code':'688002.SS', 'status':'ready',
              'company_adverse':True, 'scale':{'min':-2,'max':2,'meaning':'定性趋势示意'},
              'counterevidence':[{'domain':'fundamental','title':'企业证据','reasons':['现金流待复核']}],
              'points':[{'weeks':8,'date':'2026-11-08','base':-.5,'up':.5,'down':-1,
                         'condition':'基准须修复','invalidation':'结构失守',
                         'branches':{'up':{'label':'改善','condition':'现金流与趋势同时修复','invalidation':'修复失败'},
                                     'down':{'label':'恶化','condition':'跌破结构位','invalidation':'结构收复'}}}],
              'svg':'<svg>bulky</svg>','ohlcv':list(range(1000))}
    out=build(cross_validation={'future_scenario':future})['cross_validation']['future_scenario']
    assert out['points']==future['points']
    assert out['counterevidence']==future['counterevidence'] and out['company_adverse'] is True
    assert out['scale']['meaning']=='定性趋势示意'
    assert 'svg' not in out and 'ohlcv' not in out
