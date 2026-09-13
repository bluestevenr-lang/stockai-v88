from market_overview_ui import cells, legend


def data(value):
    return {'markets':{'港股':{'indices':[{'name':'恒生指数','last':24805.63}],
        'l3':{'probs':[['2周',value]]},'temperature':{'verdict':'🧊 情绪冰点·等待企稳'}}}}


def test_direction_score_not_a_percent_and_exact_engine_bands():
    assert '过去2周档方向 39/100·偏弱' in cells(data(39))
    for score,label in [(41,'偏弱'),(42,'震荡'),(58,'震荡'),(59,'偏强')]:
        assert f'{score}/100·{label}' in cells(data(score))
    assert '39%' not in cells(data(39)) and '方向分≠涨幅/胜率' in legend()


def test_invalid_or_missing_is_unknown_not_neutral_fifty():
    for score in (None,False,float('nan'),101,-1):
        text=cells(data(score))
        assert '○ 过去2周档方向 —·缺证' in text and '50/100' not in text


def test_source_text_is_escaped():
    d=data(39);d['markets']['港股']['indices'][0]['name']='<script>bad</script>'
    assert '<script>' not in cells(d) and '&lt;script&gt;' in cells(d)


def test_direction_index_identity_is_not_taken_from_first_unrelated_quote():
    d=data(39);row=d['markets']['港股'];row['l3']['name']='恒生指数'
    row['indices'].insert(0,{'name':'其他指数','last':999})
    text=cells(d)
    assert '恒生指数</b> 24805.63' in text and '999' not in text
    assert '方向分日期 未单独记录' in text


def test_unreviewed_temperature_action_is_not_displayed_as_market_instruction():
    d=data(59);d['markets']['港股']['temperature']={'label':'满仓买入','verdict':'满仓买入'}
    text=cells(d)
    assert '满仓买入' not in text and '情绪标签待核' in text
