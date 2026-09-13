"""Problem-led multi-classic crosscheck, derived only from signed atomic checks.

No extra votes/scores/grades. A referenced book is not an implemented algorithm.
Keep this module byte-identical in core/src and desktop.
"""
from collections import Counter
from html import escape
import hashlib
import json

VERSION='classics-framework-v1'
DOMAINS=(
    ('trend','趋势与入场',('trend','entry','volume'),
     ('Sperandeo《专业投机原理》','Edwards与Magee《股市趋势技术分析》','欧奈尔量价方法'),
     ('Weinstein阶段分析：设计参考，待独立规则及样本验证',)),
    ('enterprise','企业与估值',('earnings','cash','quality','valuation','history'),
     ('Lynch《战胜华尔街》','Siegel《股市长线法宝》'),
     ('Graham与Dodd《证券分析》、Graham《聪明的投资者》：安全边际参考，未编码独立估值裁决',)),
    ('capital','风险与退出',('stop','payoff','invalidation','profit','tharp_risk'),
     ('Tharp《通向财务自由之路》','Sperandeo资本保护','欧奈尔止错约束'),()),
    ('validation','策略实证',('tharp_expectancy',),('Tharp净R期望',),
     ('Aronson《循证技术分析》：统计证据要求，未完成独立样本外认证',
      'López de Prado《金融机器学习》：防过拟合参考，未完成交叉验证认证')),
)
POLICY=('按问题选择适用经典；同一证据只计一次，不按书籍数量投票或加分。'
        '风险与事实冲突优先处理，技术不能替代财报，历史收益不能替代样本外验证。'
        '沿用当前真实双审分，新增参考不冒充已通过的规则。')


def evaluate(card):
    book=card.get('books') or {}; checks=book.get('checks') or []
    current=book.get('current') is True and book.get('complete') is True
    counts=Counter(c.get('id') for c in checks)
    by_id={c.get('id'):c for c in checks}
    domains=[];observations=set();appearances=0
    for key,label,ids,implemented,references in DOMAINS:
        selected=[by_id[i] for i in ids if i in by_id]
        duplicate=any(counts[i]>1 for i in ids)
        fail=[c['id'] for c in selected if c.get('ok') is False]
        missing=[c['id'] for c in selected if type(c.get('ok')) is not bool]
        waiting=[]
        for c in selected:
            evidence=c.get('evidence') or {}
            if c['id']=='tharp_expectancy' and c.get('ok') is False:
                if any(evidence.get(k)=='NONPOSITIVE_HISTORY' for k in ('status','raw_status')):continue
                if evidence.get('status') in ('INSUFFICIENT_SAMPLE','NOT_MEASURED','UNSUPPORTED','MISSING_HISTORY') or evidence.get('settled_n') is None:
                    fail.remove(c['id']);missing.append(c['id'])
            if c['id'] in ('entry','volume') and c.get('ok') is False:
                fail.remove(c['id']);waiting.append(c['id'])
        status=('待重核' if not current else '证据冲突' if duplicate else '未覆盖' if not selected else
                '反对' if fail else '缺证' if missing else '入场待确认' if waiting else
                '历史支持·样本外待证' if key=='validation' else '支持')
        for c in selected:
            for field,value in (c.get('evidence') or {}).items():
                if value is None:continue
                raw=json.dumps([field,value],sort_keys=True,ensure_ascii=False,default=str)
                observations.add(hashlib.sha256(raw.encode()).hexdigest());appearances+=1
        domains.append({'id':key,'label':label,'status':status,'check_ids':[c['id'] for c in selected],
                        'failed':fail,'missing':missing,'waiting':waiting,'implemented_basis':list(implemented),
                        'design_references':list(references)})
    return {'version':VERSION,'domains':domains,'current':current,
            'unique_observations':len(observations),'reused_observations':appearances-len(observations),
            'source_independence':'来源独立性须另核；不同书名不是独立数据来源',
            'policy':POLICY,'score_adjustment':0,'no_grade_authority':True,'model_calls':0}


def html(card):
    result=evaluate(card);e=lambda x:escape(str(x))
    colors={'支持':('#15803d','✓'),'反对':('#b45309','!'),'缺证':('#64748b','?'),
            '入场待确认':('#a16207','◷'),'证据冲突':('#b91c1c','⛔'),'历史支持·样本外待证':('#a16207','◷')}
    chips=[];details=[]
    for d in result['domains']:
        color,icon=colors.get(d['status'],('#64748b','○'))
        chips.append(f"<span style='color:{color};margin-right:8px'>{icon} {e(d['label'])}·{e(d['status'])}</span>")
        details.append('<div style="margin:5px 0"><b>'+e(d['label'])+'</b>：'+e('；'.join(d['implemented_basis']))
                       +'<br>已有检查：'+e(' / '.join(d['check_ids']) or '本期未覆盖')
                       +('<br>'+e('；'.join(d['design_references'])) if d['design_references'] else '')+'</div>')
    return ("<details class='v88-classics-framework' style='font-size:11px'><summary>📚 多经典交叉 · "+''.join(chips)
            +'</summary>'+''.join(details)+'<div>'+e(POLICY)+'</div><div>'+e(result['source_independence'])+'</div></details>')
