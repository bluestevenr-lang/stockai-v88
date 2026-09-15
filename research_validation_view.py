"""Read-only evidence status; keeps the existing stock table intact."""


def _count(value):
    return str(value) if type(value) is int and value>=0 else '未核实'


def render(st, doc):
    if not doc:
        st.warning('前瞻验证报告尚未生成，不能据此宣称已验证预测能力。')
        return
    if doc.get('error'):
        st.error(str(doc.get('status') or '验证报告读取失败')+'：'+str(doc['error']))
        return
    snapshots = doc.get('snapshots') or {}
    contracts = doc.get('contracts') or {}
    states = contracts.get('states') or {}
    settled=states.get('SETTLED',0) if isinstance(contracts.get('states'),dict) else None
    st.info(f"证据闭环快照：冻结 {_count(snapshots.get('unique_codes'))} 只候选/排除档案；"
            f"可核验行情 {_count(snapshots.get('verified_market_series'))} 只；"
            f"前瞻研究合同 {_count(contracts.get('total'))} 份；模拟已结算 {_count(settled)} 份。")
    if doc.get('status'):st.caption(doc['status'])
    st.caption(f"报告生成：{doc.get('generated_at', '未知')}；首份合同：{doc.get('first_contract_at') or '尚无可登记合同'}。"
               "登记从实际捕获时刻开始；旧日期不会变成提前发布的记录。")
    labels = {'WAITING_ENTRY': '未入场', 'OPEN': '模拟持有中', 'WAITING_DATA': '数据待核',
              'AMBIGUOUS': '顺序不明', 'EXPIRED_UNTRIGGERED': '到期未入场',
              'INVALIDATED_BEFORE_ENTRY': '入场前失效', 'SETTLED': '模拟扣费结算'}
    st.write('；'.join(f"{labels.get(k, k)} {_count(v)}" for k, v in states.items()) or
             ('尚无前瞻合同；缺口见下方' if contracts.get('total')==0 else '合同状态未提供；不能据此认定无合同'))
    st.caption('1A/2A的模拟触发不会获得实际买入权限。模拟每股净价差与券商成交、账户收益分别记账。'
               '未入场不计盈亏；同日顺序不明、停牌、缺日和复权改变单列处理。')
    st.caption('分规则、策略、市场、周期、出口及原始分数桶统计。每组不足30笔不显示获利比例；'
               '样本重叠不算独立证据，审核分不当作上涨概率。')
    st.caption(doc.get('pit_scope', ''))
    settlement_errors=doc.get('settlement_data_errors') or []
    if settlement_errors:
        st.warning(f"原模拟合同数据待修复 {len(settlement_errors)} 份；保留原合同，不计为已验证结算。")
    if doc.get('blocked_new_contracts') or doc.get('data_errors'):
        st.warning(f"本轮新合同受阻 {len(doc.get('blocked_new_contracts') or [])} 只；"
                   f"行情凭据错误 {len(doc.get('data_errors') or [])} 只。原始档案保留，不隐藏失败。")
        from grade_card import _market_rows
        for item in _market_rows(doc.get('blocked_new_contracts') or [])[:8]:
            st.caption(f"{item['code']}：{item['reason']}")
    st.caption(f"本快照已核验真实成交 {_count(doc.get('broker_fills_verified'))} 笔；"
               f"合规历史PIT回放 {_count(doc.get('historical_pit_settlements_verified'))} 笔。"
               "当前不能认定已达到机构预测水平。")
    st.caption('仍需补齐：'+'；'.join(doc.get('remaining') or []))
