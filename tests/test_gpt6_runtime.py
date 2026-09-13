import sys
from pathlib import Path
from unittest.mock import patch
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import gpt_genai_compat as bridge
import gpt_subscription as subscription
from test_grade_card_safety import central_v2
from grade_card import system_table_html


def test_legacy_sdk_routes_only_gpt6_with_classics_and_counterargument():
    with patch.object(bridge,'api_key',return_value='codex-subscription'),patch.object(bridge,'complete',return_value=('研究结果',{})) as call:
        result=bridge.GenerativeModel('retired-model').generate_content('公开事实')
    assert result.text=='研究结果'
    assert call.call_args.kwargs['model']=='gpt-6-astra'
    assert call.call_args.kwargs['reasoning_effort']=='high'
    assert '斯波朗迪' in call.call_args.args[0] and '失效条件' in call.call_args.args[0]


def test_explicit_other_model_has_no_fallback():
    with pytest.raises(ValueError):subscription.chat_completion([],model='other-model')


@pytest.mark.parametrize('seat,key,value',[('gpt','model','old'),('gpt','at','2000-01-01 00:00'),('gpt','review_scope','sell'),('classics','horizon','short')])
def test_invalid_review_becomes_pending_in_header_and_table(seat,key,value):
    triad=central_v2()
    triad['recommendations'][0]['reviews'][seat][key]=value
    html=system_table_html({'rows':[]},{'rows':[]},{},{},triad=triad)
    assert '3A现买×0' in html and '数据处理与审核队列（2只' in html
    assert '数据处理与审核队列' in html
