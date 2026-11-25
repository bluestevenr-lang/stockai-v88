"""Issuer-scoped session notices and original-filing growth periods for screening."""
from datetime import date
from functools import lru_cache
from hashlib import sha256
from io import BytesIO
import re

HALTS = {('002289.SZ','A股'): {'2026-06-25':
    'https://disc.static.szse.cn/download/disc/disk03/finalpage/2026-06-25/af9423d8-f7fe-42f0-a0a5-34683d069519.PDF'}}
CR_10Q = 'https://www.sec.gov/Archives/edgar/data/1944013/000162828026068180/cr-20260930.htm'


def confirmed_halts(code, market, start, end):
    return {day:url for day,url in HALTS.get((code,market), {}).items() if start <= day <= end}


def growth(current, prior):
    return (current - prior) / abs(prior) * 100


def crane_periods():
    # Item 1, condensed consolidated statements of operations, USD millions.
    return {period: {'profit_yoy':growth(profit,old_profit), 'revenue_yoy':growth(sales,old_sales)}
            for period,profit,old_profit,sales,old_sales in (
                ('单季',97.3,91.4,754.6,589.2), ('年初至报告期末',260.3,284.9,2175.7,1724.0))}


def compare(row, periods, url, published, raw_hash=None):
    f = row.get('fundamentals') or {}
    # Bind both independently reported percentages to the SAME interval.
    matches = [scope for scope, values in periods.items() if all(
        isinstance(values,dict) and type(values.get(k)) in (float,int)
        and type(f.get(k)) in (float,int) and abs(f[k]-values[k]) <= .11 for k in ('profit_yoy','revenue_yoy'))]
    return {'code':row['code'], 'period_end':f['period_end'], 'source_url':url, 'published_at':published,
            'sha256':raw_hash, 'periods':periods, 'matched_period':matches[0] if len(matches)==1 else None,
            'status':'matched' if len(matches)==1 else 'conflict_or_ambiguous',
            'fields':['profit_yoy','revenue_yoy']}


def cn_quarter_growth(pdf, code, end):
    from pypdf import PdfReader
    pages = PdfReader(BytesIO(pdf)).pages
    text = '\n'.join(p.extract_text(extraction_mode='layout') or '' for p in pages[:4])
    compact = re.sub(r'\s','',text)
    if ('证券代码：'+code not in compact.replace(':','：') and '股票代码：'+code not in compact.replace(':','：')):
        raise ValueError('filing issuer mismatch')
    quarter = '第三季度' if end.endswith('09-30') else '第一季度'
    if end[:4]+'年'+quarter+'报告' not in compact or '本报告期' not in compact:
        raise ValueError('filing quarter mismatch')
    region = text.split('主要财务数据',1)[-1].split('基本每股收益',1)[0]
    label = re.sub(r'(?<=[\u4e00-\u9fff])[ \t]+(?=[\u4e00-\u9fff])', '', region).replace('％','%').replace('，',',')
    pct = re.compile(r'([-+−]?\d[\d,]*(?:\.\d+)?)%')
    rev = label.find('营业收入')
    profit = label.find('归属于上市公司股东', rev+1)
    if rev < 0 or profit <= rev:
        raise ValueError('unsupported original table')
    block = label[profit:].split('归属于上市公司股东',2)
    if len(block)<2 or '净利润' not in block[1] or '扣除' in block[1]:
        raise ValueError('ambiguous profit label')
    values = [pct.findall(label[rev:profit]),pct.findall(block[1])]
    scopes = ['单季','年初至报告期末'] if end.endswith('09-30') else ['单季']
    if any(len(v)!=len(scopes) for v in values):
        raise ValueError('ambiguous original table columns')
    return {s:{k:float(v[i].replace(',','').replace('−','-')) for k,v in zip(('revenue_yoy','profit_yoy'),values)}
            for i,s in enumerate(scopes)}


@lru_cache(maxsize=256)
def cn_source(code, name, end, through):
    import requests
    from data_access_policy import require_free_source
    require_free_source('cninfo')
    quarter = {'09-30':'三','03-31':'一'}.get(end[5:])
    if not quarter:
        raise ValueError('unsupported announcement period')
    response = requests.post('https://www.cninfo.com.cn/new/hisAnnouncement/query', data={
        'pageNum':1,'pageSize':30,'column':'szse','tabName':'fulltext','category':'',
        'searchkey':name,'seDate':end+'~'+through},timeout=4)
    response.raise_for_status()
    pattern = end[:4]+'年(?:第)?'+quarter+'季度报告'
    options = []
    for a in response.json().get('announcements') or []:
        title = re.sub(r'<[^>]+>|\s','',a.get('announcementTitle',''))
        if a.get('secCode')==code and re.search(pattern,title) and '摘要' not in title:
            path=a.get('adjunctUrl','')
            if re.fullmatch(r'finalpage/\d{4}-\d\d-\d\d/\d+\.PDF',path,re.I):
                options.append(path)
    if not options:
        raise ValueError('no matching issuer report')
    path=max(options)
    published=path.split('/')[1]
    if not end <= published <= through:
        raise ValueError('filing not available')
    url='https://static.cninfo.com.cn/'+path
    response=requests.get(url,timeout=5)
    response.raise_for_status()
    if not response.content.startswith(b'%PDF'):raise ValueError('invalid PDF')
    return cn_quarter_growth(response.content,code,end),url,published,sha256(response.content).hexdigest()


def reconcile(row, now, *, allow_fetch=False):
    end=(row.get('fundamentals') or {}).get('period_end')
    if not end:return None
    code=row.get('code')
    through=now.date().isoformat()
    if code=='CR' and row.get('market')=='美股' and end=='2026-09-30' and through>'2026-10-29':
        return compare(row,crane_periods(),CR_10Q,'2026-10-29')
    if code in ('700.HK','0700.HK') and row.get('market')=='港股' and end=='2026-09-30' and through>'2026-11-18':
        # Tencent's IFRS income statement, RMB million, pp. 17–18.
        amounts = {'单季':(63612,63133,207336,192869),
                   '年初至报告期末':(177727,166582,608579,557395)}
        periods = {k:{'profit_yoy':growth(p,prior_p),'revenue_yoy':growth(r,prior_r)}
                   for k,(p,prior_p,r,prior_r) in amounts.items()}
        return compare(row,periods,'https://www.tencent.com/wp-content/uploads/2026/11/E700_3Qresults.pdf','2026-11-18')
    if allow_fetch and row.get('market')=='美股':
        try:
            return compare(row,*sec_source(code,end,through))
        except (KeyError, ValueError, OSError):
            return None
    if allow_fetch and row.get('market')=='A股' and str(code).endswith(('.SZ','.SS','.BJ')):
        try:
            return compare(row,*cn_source(code[:6],row['name'],end,through))
        except Exception:
            # Unsupported layout or unavailable filing leaves the candidate pending.
            return None
    return None


@lru_cache(maxsize=1)
def sec_symbols():
    import requests
    r=requests.get('https://www.sec.gov/files/company_tickers.json',headers=SEC_HEADERS,timeout=4)
    r.raise_for_status()
    return {v['ticker']:int(v['cik_str']) for v in r.json().values()}


SEC_HEADERS={'User-Agent':'V88 filing reconciliation (github.com/bluestevenr-lang/stockai-v88)'}


@lru_cache(maxsize=256)
def sec_source(code,end,through):
    """Same 10-Q, USD concept and duration; require both prior-year comparative columns."""
    import requests
    from data_access_policy import require_free_source
    require_free_source('sec')
    cik=sec_symbols()[code]
    r=requests.get(f'https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json',headers=SEC_HEADERS,timeout=4)
    r.raise_for_status()
    facts=r.json()['facts']['us-gaap']
    for revenue in ('RevenueFromContractWithCustomerExcludingAssessedTax','SalesRevenueNet','Revenues'):
        streams=[facts.get(tag,{}).get('units',{}).get('USD',[]) for tag in ('NetIncomeLoss',revenue)]
        accessions=[(v['filed'],v['accn']) for v in streams[0] if v.get('end')==end and v.get('form') in ('10-Q','10-Q/A') and v['filed']<=through]
        if not accessions:continue
        filed,acc=max(accessions)
        periods={}
        for k,stream in zip(('profit_yoy','revenue_yoy'),streams):
            rows=[v for v in stream if v.get('accn')==acc and v.get('start')]
            for v in rows:
                if v['end']!=end:continue
                start,finish=map(date.fromisoformat,(v['start'],v['end']))
                length=(finish-start).days
                scope='单季' if 75<=length<=105 else '年初至报告期末' if 155<=length<=290 else None
                if not scope:continue
                prior=[p for p in rows if 358<=(start-date.fromisoformat(p['start'])).days<=373 and
                       358<=(finish-date.fromisoformat(p['end'])).days<=373 and
                       abs((date.fromisoformat(p['end'])-date.fromisoformat(p['start'])).days-length)<=8]
                if len(prior)==1 and prior[0]['val']:
                    periods.setdefault(scope,{})[k]=growth(v['val'],prior[0]['val'])
        complete={k:v for k,v in periods.items() if len(v)==2}
        if complete:
            url=f'https://www.sec.gov/Archives/edgar/data/{cik}/{acc.replace("-","")}/{acc}-index.htm'
            return complete,url,filed,sha256(r.content).hexdigest()
    raise ValueError('no comparable SEC income statement')
