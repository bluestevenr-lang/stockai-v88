import numpy as np
import pandas as pd
from prediction_engine import InstitutionalPredictor


def data(close,volume=None):
    close=np.array(close,dtype=float)
    return pd.DataFrame({'High':close+1,'Low':close-1,'Close':close,'Volume':np.full(len(close),100.) if volume is None else volume},index=pd.bdate_range('2025-01-01',periods=len(close)))


def test_zero_volume_window_does_not_reuse_old_vwap():
    df=data(np.linspace(10,12,45),[100.]*25+[0.]*20)
    p=InstitutionalPredictor(df,'TEST')
    assert pd.notna(p.calculate_vwap().iloc[24]) and pd.isna(p.calculate_vwap().iloc[-1])
    result=p.calculate_alpha_factors()
    assert '数据不足' in result['volume_price_divergence']
    assert result['vwap_20'] is None and result['vwap_deviation'] is None and '数据不足' in result['vwap_signal']


def test_low_volatility_percentile_is_not_reversed():
    close=np.concatenate([100+10*np.sin(np.arange(180)), np.full(30,100.)])
    low=InstitutionalPredictor(data(close),'TEST').calculate_alpha_factors()
    high=InstitutionalPredictor(data(np.concatenate([np.full(180,100.),100+20*np.sin(np.arange(30))])),'TEST').calculate_alpha_factors()
    assert '波动收缩' in low['bb_squeeze'] and '波动未落入低分位' in high['bb_squeeze']
    assert '最低' not in low['bb_squeeze']


def test_no_kelly_position_from_daily_up_frequency_or_missing_strategy():
    for close in ([10.]*5,np.linspace(10,100,100),np.linspace(100,10,100)):
        risk=InstitutionalPredictor(data(close),'TEST').calculate_risk_engine()
        assert risk['kelly_position'] is None and '合格结算样本' in risk['position_basis']
        assert 'A级' not in risk['risk_grade']
