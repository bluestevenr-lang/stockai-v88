#!/usr/bin/env python3
"""
scan_worker.py — 后台全策略扫描进程
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
独立运行（无 Streamlit 依赖），由主 App 通过 subprocess 启动。

功能:
  - 扫描三市场（美/港/A股）× 四策略（趋势/蓄势/拐点/启动）
  - 8 线程并发拉取数据，每市场约 2-3 分钟
  - 每 20 只写一次进度到 scan_progress.json
  - 每 20 只检查一次 scan_heartbeat.json；
    若心跳超过 90 秒未更新（页面关闭），自动退出
  - 结果写入 scan_results.json，有效期 4 小时

使用:
  python scan_worker.py                  # 正常启动
  python scan_worker.py --force          # 忽略现有结果，强制重扫
"""

import os
import sys
import json
import time
import signal
import logging
import warnings
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import yfinance as yf
import requests

warnings.filterwarnings("ignore")

# ── 添加项目路径（用于 sector_map / modules）────────────────────
_SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(_SCRIPT_DIR))

# ── 文件路径 ────────────────────────────────────────────────────
CACHE_DIR       = _SCRIPT_DIR / ".cache_brief"
RESULTS_FILE    = CACHE_DIR / "scan_results.json"
PROGRESS_FILE   = CACHE_DIR / "scan_progress.json"
HEARTBEAT_FILE  = CACHE_DIR / "scan_heartbeat.json"
PID_FILE        = CACHE_DIR / "scan_worker.pid"
POOL_CACHE_FILE = CACHE_DIR / "pool_cache.json"

# ── 参数 ────────────────────────────────────────────────────────
SCAN_TTL          = 4 * 3600   # 结果有效期 4 小时
HEARTBEAT_TIMEOUT = 90          # 心跳超时（秒）
MAX_WORKERS       = 8           # 并发线程数
HEARTBEAT_CHECK_INTERVAL = 20   # 每 N 只检查一次心跳

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [WORKER] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

_FORCE_RESCAN = "--force" in sys.argv
_CLOUD_MODE   = "--cloud" in sys.argv   # GitHub Actions 模式：跳过心跳检查，扫完上传 Gist


# ═══════════════════════════════════════════════════════════════
# 文件 I/O 工具
# ═══════════════════════════════════════════════════════════════

def _heartbeat_alive() -> bool:
    """主页面心跳是否存活（< 90s）"""
    try:
        data = json.loads(HEARTBEAT_FILE.read_text(encoding="utf-8"))
        age = time.time() - data.get("ts", 0)
        return age < HEARTBEAT_TIMEOUT
    except Exception:
        return True  # 文件不存在时宽松处理（刚启动阶段）


def _results_valid() -> bool:
    """结果文件是否仍在有效期内"""
    try:
        data = json.loads(RESULTS_FILE.read_text(encoding="utf-8"))
        age = time.time() - data.get("timestamp", 0)
        return age < SCAN_TTL
    except Exception:
        return False


def _write_progress(pct: int, status: str, detail: str = ""):
    """写进度文件（status: running / done / aborted / error）"""
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        PROGRESS_FILE.write_text(
            json.dumps({"pct": pct, "status": status, "detail": detail, "ts": time.time()},
                       ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════
# 代码转换工具
# ═══════════════════════════════════════════════════════════════

def _to_yf_code(code: str) -> str:
    """将原始代码转换为 yfinance 格式"""
    if not code:
        return code
    code = code.strip().upper()
    if code.endswith(".SH"):
        return code[:-3] + ".SS"
    if "." in code:
        if code.endswith(".HK"):
            try:
                num = int(code.split(".")[0])
                return f"{num}.HK"
            except Exception:
                pass
        return code
    if code.isdigit():
        if len(code) == 6:
            return f"{code}.SS" if code.startswith("6") else f"{code}.SZ"
        if len(code) in (4, 5):
            try:
                num = int(code)
                return f"{num}.HK"
            except Exception:
                pass
    return code


# ═══════════════════════════════════════════════════════════════
# 股票池获取
# ═══════════════════════════════════════════════════════════════

def _fetch_eastmoney(market: str, limit: int) -> list:
    """从东方财富拉取股票池（尝试多个端点，兼容中国/海外 IP）"""
    # 主端点（中国IP可用）+ 备用HTTPS端点
    urls = [
        "https://push2.eastmoney.com/api/qt/clist/get",
        "http://80.push2.eastmoney.com/api/qt/clist/get",
        "https://datacenter.eastmoney.com/securities/api/data/get",
    ]
    fs_map = {
        "us": "m:105,m:106,m:107",
        "hk": "m:128",
        "cn": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23",
    }
    fs = fs_map.get(market)
    if not fs:
        return []

    for url in urls[:2]:   # 只试前两个端点
        all_stocks = []
        pn = 1
        try:
            while len(all_stocks) < limit:
                time.sleep(0.3)
                pz = min(200, limit - len(all_stocks))
                params = {
                    "pn": pn, "pz": pz, "fs": fs,
                    "fields": "f12,f14,f20",
                    "ut": "bd1d9ddb04089700cf9c27f6f7426281",
                    "fid": "f20", "sort": "f20", "type": "rank",
                }
                resp = requests.get(url, params=params, timeout=15, verify=False)
                diff = resp.json().get("data", {}).get("diff", [])
                if isinstance(diff, dict):
                    diff = list(diff.values())
                page = [
                    (x["f12"], x["f14"], _to_yf_code(x["f12"]))
                    for x in diff
                    if isinstance(x, dict) and x.get("f12") and x.get("f14")
                ]
                if not page:
                    break
                all_stocks.extend(page)
                if len(page) < pz:
                    break
                pn += 1
            if len(all_stocks) >= 20:
                log.info(f"EastMoney {market} 获取 {len(all_stocks)} 只 via {url}")
                return all_stocks[:limit]
        except Exception as e:
            log.warning(f"EastMoney {market} 端点 {url} 失败: {e}")
            continue

    return []   # 全部失败，由调用方触发 fallback


def _fallback_us():
    """美股备用池 ~240 只（S&P500 核心 + 纳斯达克成长 + 中概）"""
    return [
        # ── 大型科技 ──
        ("AAPL","苹果","AAPL"),("MSFT","微软","MSFT"),("NVDA","英伟达","NVDA"),
        ("GOOGL","谷歌A","GOOGL"),("GOOG","谷歌C","GOOG"),("META","Meta","META"),
        ("AMZN","亚马逊","AMZN"),("TSLA","特斯拉","TSLA"),("AVGO","博通","AVGO"),
        ("ORCL","甲骨文","ORCL"),("AMD","AMD","AMD"),("INTC","英特尔","INTC"),
        ("QCOM","高通","QCOM"),("AMAT","应用材料","AMAT"),("MU","美光科技","MU"),
        ("LRCX","泛林集团","LRCX"),("KLAC","科磊","KLAC"),("TXN","德州仪器","TXN"),
        ("ADI","亚德诺","ADI"),("NXPI","恩智浦","NXPI"),("ON","安森美","ON"),
        ("MCHP","微芯科技","MCHP"),("STX","希捷","STX"),("WDC","西数","WDC"),
        ("SWKS","Skyworks","SWKS"),("MRVL","迈威科技","MRVL"),("MPWR","芯源系统","MPWR"),
        # ── 软件/云/AI ──
        ("CRM","Salesforce","CRM"),("NOW","ServiceNow","NOW"),("ADBE","Adobe","ADBE"),
        ("INTU","Intuit","INTU"),("PLTR","Palantir","PLTR"),("SNOW","Snowflake","SNOW"),
        ("DDOG","Datadog","DDOG"),("CRWD","CrowdStrike","CRWD"),("ZS","Zscaler","ZS"),
        ("PANW","Palo Alto","PANW"),("NET","Cloudflare","NET"),("MDB","MongoDB","MDB"),
        ("HUBS","HubSpot","HUBS"),("TEAM","Atlassian","TEAM"),("WDAY","Workday","WDAY"),
        ("VEEV","Veeva","VEEV"),("GTLB","GitLab","GTLB"),("PATH","UiPath","PATH"),
        ("AI","C3.ai","AI"),("BILL","Bill.com","BILL"),("DOCN","DigitalOcean","DOCN"),
        ("APP","AppLovin","APP"),("TTD","Trade Desk","TTD"),("RBLX","Roblox","RBLX"),
        ("U","Unity","U"),("GTLB","GitLab","GTLB"),("TOST","Toast","TOST"),
        # ── 消费/零售/媒体 ──
        ("NFLX","奈飞","NFLX"),("DIS","迪士尼","DIS"),("COST","Costco","COST"),
        ("WMT","沃尔玛","WMT"),("TGT","塔吉特","TGT"),("NKE","耐克","NKE"),
        ("SBUX","星巴克","SBUX"),("MCD","麦当劳","MCD"),("AMGN","安进","AMGN"),
        ("CMCSA","康卡斯特","CMCSA"),("CHTR","特许通信","CHTR"),("PARA","派拉蒙","PARA"),
        ("GM","通用汽车","GM"),("F","福特","F"),("RIVN","Rivian","RIVN"),
        ("LCID","Lucid","LCID"),("UBER","优步","UBER"),("LYFT","Lyft","LYFT"),
        ("ABNB","Airbnb","ABNB"),("BKNG","Booking","BKNG"),("EXPE","Expedia","EXPE"),
        ("DASH","DoorDash","DASH"),("SHOP","Shopify","SHOP"),("ETSY","Etsy","ETSY"),
        ("EBAY","eBay","EBAY"),("ZM","Zoom","ZM"),("PTON","Peloton","PTON"),
        ("WBD","华纳兄弟","WBD"),("PARA","派拉蒙","PARAA"),
        # ── 金融 ──
        ("JPM","摩根大通","JPM"),("BAC","美国银行","BAC"),("GS","高盛","GS"),
        ("MS","摩根士丹利","MS"),("BLK","贝莱德","BLK"),("V","Visa","V"),
        ("MA","万事达","MA"),("AXP","美国运通","AXP"),("SPGI","标普全球","SPGI"),
        ("ICE","洲际交易所","ICE"),("CME","芝商所","CME"),("C","花旗","C"),
        ("WFC","富国银行","WFC"),("USB","美合银行","USB"),("PNC","PNC金融","PNC"),
        ("SCHW","嘉信理财","SCHW"),("COF","Capital One","COF"),("SYF","同步金融","SYF"),
        ("PYPL","PayPal","PYPL"),("XYZ","Block","XYZ"),("COIN","Coinbase","COIN"),
        # ── 医疗健康 ──
        ("UNH","联合健康","UNH"),("LLY","礼来","LLY"),("JNJ","强生","JNJ"),
        ("ABBV","艾伯维","ABBV"),("MRK","默克","MRK"),("PFE","辉瑞","PFE"),
        ("TMO","赛默飞","TMO"),("DHR","丹纳赫","DHR"),("ISRG","直觉手术","ISRG"),
        ("REGN","再生元","REGN"),("GILD","吉利德","GILD"),("MRNA","Moderna","MRNA"),
        ("BIIB","百健","BIIB"),("VRTX","Vertex","VRTX"),("ILMN","Illumina","ILMN"),
        ("DXCM","DexCom","DXCM"),("ALGN","爱齐科技","ALGN"),("ZBH","捷迈邦美","ZBH"),
        ("BSX","波士顿科学","BSX"),("EW","爱德华兹","EW"),
        # ── 工业/国防/能源 ──
        ("CAT","卡特彼勒","CAT"),("DE","迪尔","DE"),("HON","霍尼韦尔","HON"),
        ("GE","通用电气","GE"),("RTX","雷神技术","RTX"),("LMT","洛克希德","LMT"),
        ("NOC","诺斯罗普","NOC"),("BA","波音","BA"),("MMM","3M","MMM"),
        ("EMR","艾默生","EMR"),("ETN","伊顿","ETN"),("PH","派克汉尼汾","PH"),
        ("XOM","埃克森美孚","XOM"),("CVX","雪佛龙","CVX"),("COP","康菲石油","COP"),
        ("SLB","斯伦贝谢","SLB"),("OXY","西方石油","OXY"),("NEE","NextEra","NEE"),
        ("DUK","杜克能源","DUK"),("SO","南方公司","SO"),
        # ── 房地产/材料 ──
        ("PLD","普洛斯","PLD"),("AMT","美国铁塔","AMT"),("CCI","皇冠城堡","CCI"),
        ("EQIX","Equinix","EQIX"),("DLR","数字房地产","DLR"),("O","Realty Income","O"),
        ("LIN","林德","LIN"),("APD","空气化工","APD"),("FCX","自由港","FCX"),
        ("NEM","纽蒙特","NEM"),("AA","铝业","AA"),("CF","CF工业","CF"),
        # ── 中概股 ──
        ("BABA","阿里巴巴","BABA"),("BIDU","百度","BIDU"),("PDD","拼多多","PDD"),
        ("JD","京东","JD"),("NIO","蔚来","NIO"),("XPEV","小鹏","XPEV"),
        ("LI","理想","LI"),("BILI","哔哩哔哩","BILI"),("IQ","爱奇艺","IQ"),
        ("TME","腾讯音乐","TME"),("VIPS","唯品会","VIPS"),("TCOM","携程","TCOM"),
        ("EDU","新东方","EDU"),("TAL","好未来","TAL"),("ZTO","中通快递","ZTO"),
        ("YMM","满帮","YMM"),("MNSO","名创优品","MNSO"),("TIGR","老虎证券","TIGR"),
        # ── ETF/指数 ──
        ("SPY","标普500ETF","SPY"),("QQQ","纳斯达克ETF","QQQ"),
        ("IWM","罗素2000ETF","IWM"),("XLK","科技ETF","XLK"),
        ("SOXX","半导体ETF","SOXX"),("ARKK","ARK创新ETF","ARKK"),
        ("GLD","黄金ETF","GLD"),("TLT","长债ETF","TLT"),
    ]


def _fallback_hk():
    """港股备用池 ~150 只（恒指/国企/科技成分）"""
    return [
        # ── 科技/互联网 ──
        ("0700","腾讯控股","0700.HK"),("9988","阿里巴巴","9988.HK"),
        ("1810","小米集团","1810.HK"),("3690","美团","3690.HK"),
        ("9618","京东集团","9618.HK"),("9999","网易","9999.HK"),
        ("9961","携程集团","9961.HK"),("0268","金蝶国际","0268.HK"),
        ("6618","京东健康","6618.HK"),("2382","舜宇光学","2382.HK"),
        ("0992","联想集团","0992.HK"),("9868","小鹏汽车","9868.HK"),
        ("2015","理想汽车","2015.HK"),("9866","蔚来","9866.HK"),
        ("1024","快手科技","1024.HK"),("0020","商汤科技","0020.HK"),
        ("6060","众安在线","6060.HK"),("2013","微盟集团","2013.HK"),
        ("9698","万国数据","9698.HK"),("1347","华虹半导体","1347.HK"),
        ("0285","比亚迪电子","0285.HK"),("3888","金山软件","3888.HK"),
        # ── 金融 ──
        ("0005","汇丰控股","0005.HK"),("0388","香港交易所","0388.HK"),
        ("1299","友邦保险","1299.HK"),("2318","中国平安","2318.HK"),
        ("1398","工商银行","1398.HK"),("3988","中国银行","3988.HK"),
        ("0939","建设银行","0939.HK"),("1288","农业银行","1288.HK"),
        ("2628","中国人寿","2628.HK"),("6881","中国银河","6881.HK"),
        ("0011","恒生银行","0011.HK"),("2388","中银香港","2388.HK"),
        ("2020","安踏体育","2020.HK"),("1336","新华保险","1336.HK"),
        ("0066","港铁公司","0066.HK"),("2888","渣打集团","2888.HK"),
        ("1038","长江基建","1038.HK"),("0083","信和置业","0083.HK"),
        # ── 电信/基础设施 ──
        ("0941","中国移动","0941.HK"),("0728","中国电信","0728.HK"),
        ("0762","中国联通","0762.HK"),("0002","中电控股","0002.HK"),
        ("0003","香港中华煤气","0003.HK"),("0006","电能实业","0006.HK"),
        ("1038","长江基建","1038.HK"),("1997","九龙仓置业","1997.HK"),
        # ── 消费/零售 ──
        ("0291","华润啤酒","0291.HK"),("9633","农夫山泉","9633.HK"),
        ("0168","青岛啤酒","0168.HK"),("2319","蒙牛乳业","2319.HK"),
        ("6862","海底捞","6862.HK"),("0960","龙湖集团","0960.HK"),
        ("0322","康师傅控股","0322.HK"),("1929","周大福","1929.HK"),
        ("0551","裕元集团","0551.HK"),("0175","吉利汽车","0175.HK"),
        ("0027","银河娱乐","0027.HK"),("1128","永利澳门","1128.HK"),
        ("0880","澳博控股","0880.HK"),("1044","恒安国际","1044.HK"),
        ("2331","李宁","2331.HK"),("0288","万洲国际","0288.HK"),
        ("6110","滔搏","6110.HK"),("9896","名创优品","9896.HK"),
        # ── 医疗 ──
        ("2269","药明生物","2269.HK"),("1177","中国生物制药","1177.HK"),
        ("1093","石药集团","1093.HK"),("2196","上海医药","2196.HK"),
        ("1801","信达生物","1801.HK"),("2359","药明康德","2359.HK"),
        ("1833","平安好医生","1833.HK"),("9969","诺辉健康","9969.HK"),
        ("2162","康方生物","2162.HK"),("0867","康哲药业","0867.HK"),
        ("2186","绿叶制药","2186.HK"),("0460","四环医药","0460.HK"),
        # ── 地产/工业 ──
        ("0016","新鸿基地产","0016.HK"),("0823","领展房产基金","0823.HK"),
        ("1211","比亚迪","1211.HK"),("0883","中国海洋石油","0883.HK"),
        ("0857","中国石油","0857.HK"),("2899","紫金矿业","2899.HK"),
        ("0101","恒隆地产","0101.HK"),("0017","新世界发展","0017.HK"),
        ("1109","华润置地","1109.HK"),("3380","龙湖地产","3380.HK"),
        ("0012","恒基地产","0012.HK"),("0688","中国海外发展","0688.HK"),
        ("2007","碧桂园","2007.HK"),("1918","融创中国","1918.HK"),
        ("0914","海螺水泥","0914.HK"),("1316","澳门赛马会","1316.HK"),
        ("0004","九龙仓集团","0004.HK"),("0836","华润电力","0836.HK"),
        # ── 能源/原材料 ──
        ("0386","中国石油化工","0386.HK"),("1088","中国神华","1088.HK"),
        ("0358","江西铜业","0358.HK"),("1171","兖矿能源","1171.HK"),
        ("3993","洛阳钼业","3993.HK"),("0347","鞍钢股份","0347.HK"),
        ("0489","东风集团","0489.HK"),("2202","万科企业","2202.HK"),
        # ── ETF ──
        ("2800","盈富基金","2800.HK"),("3033","南方恒生科技","3033.HK"),
        ("2828","恒生中国企业","2828.HK"),("3032","恒生科技ETF","3032.HK"),
    ]


def _fallback_cn():
    """A股备用池 ~150 只（沪深300 + 科创板核心）"""
    return [
        # ── 白酒/消费 ──
        ("600519","贵州茅台","600519.SS"),("000858","五粮液","000858.SZ"),
        ("000596","古井贡酒","000596.SZ"),("002304","洋河股份","002304.SZ"),
        ("603369","今世缘","603369.SS"),("600779","水井坊","600779.SS"),
        ("600809","山西汾酒","600809.SS"),("000568","泸州老窖","000568.SZ"),
        ("000799","酒鬼酒","000799.SZ"),("603589","口子窖","603589.SS"),
        ("000661","长春高新","000661.SZ"),("300498","温氏股份","300498.SZ"),
        ("002714","牧原股份","002714.SZ"),("601888","中国中免","601888.SS"),
        ("603288","海天味业","603288.SS"),("600887","伊利股份","600887.SS"),
        ("002415","海康威视","002415.SZ"),("000333","美的集团","000333.SZ"),
        ("000651","格力电器","000651.SZ"),("002352","顺丰控股","002352.SZ"),
        # ── 金融 ──
        ("601318","中国平安","601318.SS"),("600036","招商银行","600036.SS"),
        ("601166","兴业银行","601166.SS"),("000001","平安银行","000001.SZ"),
        ("600016","民生银行","600016.SS"),("601328","交通银行","601328.SS"),
        ("601288","农业银行","601288.SS"),("600000","浦发银行","600000.SS"),
        ("601601","中国太保","601601.SS"),("600030","中信证券","600030.SS"),
        ("601688","华泰证券","601688.SS"),("000776","广发证券","000776.SZ"),
        ("601198","东兴证券","601198.SS"),("601995","中金公司","601995.SS"),
        ("000002","万科A","000002.SZ"),("600048","保利发展","600048.SS"),
        ("001979","招商蛇口","001979.SZ"),("600346","恒力石化","600346.SS"),
        # ── 新能源/汽车 ──
        ("300750","宁德时代","300750.SZ"),("002594","比亚迪","002594.SZ"),
        ("002475","立讯精密","002475.SZ"),("600104","上汽集团","600104.SS"),
        ("000625","长安汽车","000625.SZ"),("601238","广汽集团","601238.SS"),
        ("300274","阳光电源","300274.SZ"),("601012","隆基绿能","601012.SS"),
        ("300014","亿纬锂能","300014.SZ"),("688819","天合光能","688819.SS"),
        ("002129","TCL科技","002129.SZ"),("601127","赛力斯","601127.SS"),
        ("002714","牧原股份","002714.SZ"),("600732","爱旭股份","600732.SS"),
        ("603806","福斯特","603806.SS"),("688223","晶科能源","688223.SS"),
        # ── 医药/医疗 ──
        ("600276","恒瑞医药","600276.SS"),("000538","云南白药","000538.SZ"),
        ("600085","同仁堂","600085.SS"),("002007","华兰生物","002007.SZ"),
        ("300015","爱尔眼科","300015.SZ"),("603259","药明康德","603259.SS"),
        ("600196","复星医药","600196.SS"),("300760","迈瑞医疗","300760.SZ"),
        ("688521","奥精医疗","688521.SS"),("002050","三花智控","002050.SZ"),
        ("600763","通策医疗","600763.SS"),("002594","比亚迪","002594.SZ"),
        ("300122","智飞生物","300122.SZ"),("688185","康希诺","688185.SS"),
        # ── 科技/半导体 ──
        ("688981","中芯国际","688981.SS"),("000063","中兴通讯","000063.SZ"),
        ("002230","科大讯飞","002230.SZ"),("300059","东方财富","300059.SZ"),
        ("002049","紫光股份","002049.SZ"),("603501","韦尔股份","603501.SS"),
        ("688012","中微公司","688012.SS"),("688036","传音控股","688036.SS"),
        ("002129","TCL科技","002129.SZ"),("000725","京东方A","000725.SZ"),
        ("688041","海光信息","688041.SS"),("688099","晶晨股份","688099.SS"),
        ("688256","寒武纪","688256.SS"),("688111","金山办公","688111.SS"),
        # ── 工业/原材料/能源 ──
        ("601899","紫金矿业","601899.SS"),("600019","宝钢股份","600019.SS"),
        ("600900","长江电力","600900.SS"),("601766","中国中车","601766.SS"),
        ("601088","中国神华","601088.SS"),("601857","中国石油","601857.SS"),
        ("600028","中国石化","600028.SS"),("601668","中国建筑","601668.SS"),
        ("601800","中国交建","601800.SS"),("600362","江西铜业","600362.SS"),
        ("603993","洛阳钼业","603993.SS"),("002460","赣锋锂业","002460.SZ"),
        ("002009","天齐锂业","002009.SZ"),("600160","巨化股份","600160.SS"),
        ("601919","中远海控","601919.SS"),("600585","海螺水泥","600585.SS"),
        ("000100","TCL集团","000100.SZ"),("600690","海尔智家","600690.SS"),
        # ── ETF ──
        ("510300","沪深300ETF","510300.SS"),("510500","中证500ETF","510500.SS"),
        ("159915","创业板ETF","159915.SZ"),("588000","科创50ETF","588000.SS"),
    ]


# ── Tushare：优先用 ts_helper 共享模块 ──────────────────────────
try:
    from ts_helper import (
        get_pro as _get_ts_pro_ext,
        fetch_daily_tushare as _fetch_daily_ts_ext,
        fetch_cn_stock_pool as _fetch_cn_pool_ext,
        is_cn as _is_cn_ext,
    )
    _USE_TS_HELPER = True
except Exception:
    _USE_TS_HELPER = False

# ── 本地 Tushare 单例（ts_helper 不可用时的备用）────────────────
_ts_pro_instance = None

def _get_ts_pro():
    """获取 Tushare pro_api 单例（懒初始化）"""
    global _ts_pro_instance
    if _ts_pro_instance is None:
        token = os.environ.get("TUSHARE_TOKEN", "")
        if token:
            try:
                import tushare as _ts
                _ts.set_token(token)
                _ts_pro_instance = _ts.pro_api()
                log.info("Tushare 初始化成功")
            except Exception as e:
                log.warning(f"Tushare 初始化失败: {e}")
    return _ts_pro_instance


def _fetch_cn_pool_tushare(limit: int = 300) -> list:
    if _USE_TS_HELPER:
        return _fetch_cn_pool_ext(limit)
    # 以下为本地实现兜底
    """
    Tushare 获取 A 股股票池（主板+中小板+创业板+科创板，上市状态）
    返回 [(code6, name, yf_code), ...]
    """
    pro = _get_ts_pro()
    if pro is None:
        return []
    try:
        df = pro.stock_basic(exchange="", list_status="L",
                             fields="ts_code,name,market")
        if df is None or len(df) == 0:
            return []
        # 只保留沪深主流市场，排除北交所
        df = df[df["market"].isin(["主板", "中小板", "创业板", "科创板"])]
        pool = []
        for _, row in df.iterrows():
            ts_code = str(row["ts_code"])   # 600519.SH / 000858.SZ
            name    = str(row["name"])
            yf_code = (ts_code[:-3] + ".SS") if ts_code.endswith(".SH") else ts_code
            pool.append((ts_code[:6], name, yf_code))
        log.info(f"Tushare CN 股票池获取: {len(pool)} 只，取前 {limit} 只")
        return pool[:limit]
    except Exception as e:
        log.warning(f"Tushare CN 股票池失败: {e}")
        return []


def _fetch_df_tushare(yf_code: str):
    if _USE_TS_HELPER:
        return _fetch_daily_ts_ext(yf_code, days=400)
    # 以下为本地实现兜底
    """
    A股专用数据获取（Tushare）
    yf_code: 600519.SS 或 000858.SZ
    返回标准化 DataFrame [Open/High/Low/Close/Volume]
    """
    pro = _get_ts_pro()
    if pro is None:
        return None
    if not (yf_code.endswith(".SS") or yf_code.endswith(".SZ")):
        return None
    try:
        # yfinance .SS → Tushare .SH
        ts_code = (yf_code[:-3] + ".SH") if yf_code.endswith(".SS") else yf_code
        from datetime import datetime as _dt2, timedelta as _td2
        end_d   = _dt2.now().strftime("%Y%m%d")
        start_d = (_dt2.now() - _td2(days=400)).strftime("%Y%m%d")
        df = pro.daily(ts_code=ts_code, start_date=start_d, end_date=end_d,
                       fields="trade_date,open,high,low,close,vol")
        if df is None or len(df) < 30:
            return None
        df = df.sort_values("trade_date").reset_index(drop=True)
        df.index = pd.to_datetime(df["trade_date"], format="%Y%m%d")
        df = df.rename(columns={"open": "Open", "high": "High", "low": "Low",
                                 "close": "Close", "vol": "Volume"})
        return df[["Open", "High", "Low", "Close", "Volume"]].astype(float)
    except Exception as e:
        log.debug(f"Tushare daily {yf_code}: {e}")
        return None


def _load_pool_or_fetch() -> tuple:
    """获取股票池：缓存 → Tushare(CN) / 东财(US/HK) → 内置备用池"""
    try:
        if POOL_CACHE_FILE.exists():
            data = json.loads(POOL_CACHE_FILE.read_text(encoding="utf-8"))
            if time.time() - data.get("ts", 0) < 6 * 3600:
                us = [tuple(x) for x in data["US"]]
                hk = [tuple(x) for x in data["HK"]]
                cn = [tuple(x) for x in data["CN"]]
                log.info(f"池缓存命中: US={len(us)} HK={len(hk)} CN={len(cn)}")
                return us, hk, cn
    except Exception:
        pass

    _write_progress(2, "running", "正在拉取股票池...")

    # US/HK：东财（中国IP有效）→ 内置备用池
    us = _fetch_eastmoney("us", 350) or _fallback_us()
    hk = _fetch_eastmoney("hk", 200) or _fallback_hk()

    # CN：优先 Tushare（全球可用，覆盖率高）→ 东财 → 内置备用池
    cn = _fetch_cn_pool_tushare(300)
    if len(cn) < 50:
        log.info("Tushare CN 池不足，尝试东财...")
        cn = _fetch_eastmoney("cn", 250)
    if len(cn) < 50:
        log.info("东财 CN 池不足，使用内置备用池")
        cn = _fallback_cn()

    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        POOL_CACHE_FILE.write_text(
            json.dumps({"ts": time.time(), "US": us, "HK": hk, "CN": cn}, ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception:
        pass
    log.info(f"股票池获取完成: US={len(us)} HK={len(hk)} CN={len(cn)}")
    return us, hk, cn


# ═══════════════════════════════════════════════════════════════
# 数据获取
# ═══════════════════════════════════════════════════════════════

def _fetch_df(yf_code: str):
    """
    拉取最近 350 天日线数据。
    A股（.SS/.SZ）：优先 Tushare → 失败再 yfinance
    其他市场：直接 yfinance
    """
    # ── A股：Tushare 为主 ────────────────────────────────────────
    if yf_code.endswith(".SS") or yf_code.endswith(".SZ"):
        df = _fetch_df_tushare(yf_code)
        if df is not None and len(df) >= 30:
            return df
        log.debug(f"Tushare 失败，降级 yfinance: {yf_code}")

    # ── 其余市场 / Tushare 失败兜底：yfinance ────────────────────
    try:
        df = yf.download(yf_code, period="350d", progress=False, auto_adjust=True)
        if df is None or len(df) < 30:
            return None
        if hasattr(df.columns, "levels"):
            df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
        for col in ("Close", "Open", "High", "Low", "Volume"):
            if col not in df.columns:
                return None
        return df
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════
# 评分函数（从 app_v88_integrated.py 复制，已移除 Streamlit 依赖）
# ═══════════════════════════════════════════════════════════════

def _score_top(df) -> dict | None:
    """趋势强势：均线系统 + RSI + 动量 + 成交量"""
    if df is None or len(df) < 50:
        return None
    try:
        close  = df["Close"].astype(float)
        volume = df["Volume"].astype(float)
        high   = df["High"].astype(float)
        last_c = float(close.iloc[-1])

        ma20  = float(close.rolling(20).mean().iloc[-1])
        ma50  = float(close.rolling(50).mean().iloc[-1])
        ma200 = float(close.rolling(200).mean().iloc[-1]) if len(df) >= 200 else 0

        delta = close.diff()
        gain  = delta.where(delta > 0, 0).fillna(0)
        loss  = (-delta.where(delta < 0, 0)).fillna(0)
        rsi   = float(100 - 100 / (1 + gain.ewm(com=13).mean().iloc[-1] /
                                   (loss.ewm(com=13).mean().iloc[-1] + 1e-10)))

        ret20 = float(close.iloc[-1] / close.iloc[-21] - 1) * 100 if len(close) >= 21 else 0
        ret60 = float(close.iloc[-1] / close.iloc[-61] - 1) * 100 if len(close) >= 61 else 0

        avg_v20   = float(volume.tail(20).mean())
        last_v    = float(volume.iloc[-1])
        vol_surge = last_v > avg_v20 * 1.2

        # 52 周水位（距高点）
        h52w   = float(high.tail(252).max()) if len(df) >= 252 else float(high.max())
        dist_h = (last_c / h52w - 1) * 100 if h52w > 0 else 0

        score = 0
        signals = []
        if last_c > ma20:                  score += 20; signals.append("✅ 站MA20")
        if last_c > ma50:                  score += 15; signals.append("✅ 站MA50")
        if ma200 > 0 and last_c > ma200:   score += 15; signals.append("🏔 站MA200")
        if 50 < rsi < 75:                  score += 15; signals.append(f"RSI{rsi:.0f}")
        if ret20 > 5:                      score += 15; signals.append(f"20日+{ret20:.1f}%")
        if ret60 > 10:                     score += 10; signals.append(f"60日+{ret60:.1f}%")
        if vol_surge:                      score += 10; signals.append("🔥 放量")

        if score < 40:
            return None
        setup = "强势" if score >= 70 else "偏强"
        return {"score": min(100, score), "signals": signals, "setup": setup,
                "dist_h": dist_h, "rsi": rsi}
    except Exception:
        return None


def _score_coil(df) -> dict | None:
    """蓄势潜伏：量缩价稳 + ATR 收缩 + 贴近均线"""
    if df is None or len(df) < 60:
        return None
    try:
        close  = df["Close"].astype(float)
        volume = df["Volume"].astype(float)
        high   = df["High"].astype(float)
        low    = df["Low"].astype(float)

        ma20  = close.rolling(20).mean()
        ma50  = close.rolling(50).mean()
        ma200 = close.rolling(200).mean() if len(df) >= 200 else None

        last_c  = float(close.iloc[-1])
        avg_v60 = float(volume.tail(60).mean())
        atr10   = float((high - low).tail(10).mean())
        atr60   = float((high - low).tail(60).mean())

        atr_contracting = atr10 < atr60 * 0.70
        vol_drying      = float(volume.tail(10).mean()) < avg_v60 * 0.75

        near_ma20    = abs(last_c / float(ma20.iloc[-1]) - 1) < 0.03
        ma20_flat_up = float(ma20.iloc[-1]) >= float(ma20.iloc[-5]) if len(ma20) >= 5 else False
        above_ma50   = last_c > float(ma50.iloc[-1])
        above_ma200  = ma200 is not None and last_c > float(ma200.iloc[-1])

        range60 = float(high.tail(60).max() - low.tail(60).min())
        range20 = float(high.tail(20).max() - low.tail(20).min())
        range_contracting = range20 < range60 * 0.60 if range60 > 0 else False

        h60        = float(high.tail(60).max())
        price_zone = h60 * 0.75 <= last_c <= h60 * 0.95 if h60 > 0 else False

        score = 0
        signals = []
        if atr_contracting:              score += 20; signals.append("🔇 波动收缩")
        if vol_drying:                   score += 20; signals.append("📉 量能萎缩")
        if near_ma20 and ma20_flat_up:   score += 15; signals.append("📐 贴近MA20")
        if above_ma50:                   score += 15; signals.append("✅ 站上MA50")
        if above_ma200:                  score += 15; signals.append("🏔 站上MA200")
        if range_contracting:            score += 10; signals.append("🎯 区间收窄")
        if price_zone:                   score += 5;  signals.append("📍 蓄势区")

        setup = "强蓄势" if score >= 70 else ("蓄势中" if score >= 45 else "弱蓄势")
        return {"score": min(100, score), "signals": signals, "setup": setup}
    except Exception:
        return None


def _score_inflection(df) -> dict | None:
    """拐点通道（赔率）：三关全中才入池"""
    if df is None or len(df) < 40:
        return None
    try:
        close  = df["Close"].astype(float)
        volume = df["Volume"].astype(float)
        high   = df["High"].astype(float)
        low    = df["Low"].astype(float)

        delta = close.diff()
        gain  = delta.where(delta > 0, 0).fillna(0)
        loss  = (-delta.where(delta < 0, 0)).fillna(0)
        rsi   = float(100 - 100 / (1 + gain.ewm(com=13).mean().iloc[-1] /
                                   (loss.ewm(com=13).mean().iloc[-1] + 1e-10)))

        last_c = float(close.iloc[-1])
        period = min(126, len(df))
        h6m    = float(high.tail(period).max())
        l6m    = float(low.tail(period).min())
        range6m = h6m - l6m
        pos6m   = (last_c - l6m) / range6m if range6m > 0 else 0.5
        in_bottom_40 = pos6m <= 0.40

        ret5  = float(close.iloc[-1] / close.iloc[-6]  - 1) * 100 if len(close) >= 6  else 0
        ret20 = float(close.iloc[-1] / close.iloc[-21] - 1) * 100 if len(close) >= 21 else 0

        rsi_divergence = (float(close.tail(20).min()) <= last_c * 1.02) and (rsi > 40)
        rebound_signal = (ret5 > 0) and (ret20 < -5)
        gate1 = in_bottom_40 and (rsi_divergence or rebound_signal)

        if len(low) >= 20:
            higher_lows = float(low.iloc[-10:].min()) > float(low.iloc[-20:-10].min())
        else:
            higher_lows = False
        not_new_low = last_c > float(close.tail(20).min()) * 0.99
        gate2 = higher_lows and not_new_low

        recent_10  = df.tail(10).copy()
        up_days    = recent_10[recent_10["Close"] >= recent_10["Open"]]
        down_days  = recent_10[recent_10["Close"] <  recent_10["Open"]]
        avg_vol_up   = float(up_days["Volume"].mean())   if len(up_days)   > 0 else 0
        avg_vol_down = float(down_days["Volume"].mean()) if len(down_days) > 0 else 1
        gate3 = avg_vol_up > avg_vol_down

        if not (gate1 and gate2 and gate3):
            return None

        score = 0
        signals = []
        bottom_score = int((0.40 - pos6m) / 0.40 * 30) if pos6m <= 0.40 else 0
        score += bottom_score
        signals.append(f"📍 底部{pos6m*100:.0f}%位")

        if rebound_signal:  score += 20; signals.append(f"↩️ 5日+{ret5:.1f}%")
        if rsi_divergence:  score += 15; signals.append(f"📈 RSI底背离{rsi:.0f}")
        if higher_lows:     score += 20; signals.append("🔼 高低点抬升")
        vol_ratio = avg_vol_up / avg_vol_down if avg_vol_down > 0 else 1
        score += min(15, int(vol_ratio * 5))
        signals.append(f"💰 买/卖量={vol_ratio:.1f}x")

        setup = "强拐点" if score >= 65 else ("拐点中" if score >= 45 else "弱拐点")
        return {
            "score": min(100, score), "signals": signals, "setup": setup,
            "pos6m": pos6m, "ret5": ret5, "ret20": ret20, "rsi": rsi, "gate3": gate3,
        }
    except Exception:
        return None


def _score_breakout_v2(df, bm_ret5: float = 0.0) -> dict | None:
    """启动通道（胜率）：三信号满足 ≥ 2/3 才入池"""
    if df is None or len(df) < 25:
        return None
    try:
        close  = df["Close"].astype(float)
        volume = df["Volume"].astype(float)
        high   = df["High"].astype(float)
        low    = df["Low"].astype(float)

        last_c  = float(close.iloc[-1])
        last_v  = float(volume.iloc[-1])
        avg_v20 = float(volume.tail(20).mean())

        delta = close.diff()
        gain  = delta.where(delta > 0, 0).fillna(0)
        loss  = (-delta.where(delta < 0, 0)).fillna(0)
        rsi   = float(100 - 100 / (1 + gain.ewm(com=13).mean().iloc[-1] /
                                   (loss.ewm(com=13).mean().iloc[-1] + 1e-10)))

        high20_prev = float(close.iloc[-21:-1].max()) if len(close) >= 21 else float(close.iloc[:-1].max())
        s1_breakout = last_c > high20_prev
        s1_margin   = (last_c / high20_prev - 1) * 100 if high20_prev > 0 else 0

        s2_volume = last_v > avg_v20 * 1.5
        s2_ratio  = last_v / avg_v20 if avg_v20 > 0 else 1

        ret5  = float((close.iloc[-1] / close.iloc[-6] - 1) * 100) if len(close) >= 6 else 0
        s3_rs = ret5 > bm_ret5 + 2.0

        met = sum([s1_breakout, s2_volume, s3_rs])
        if met < 2:
            return None

        daily_range  = float(high.iloc[-1] - low.iloc[-1])
        strong_close = ((last_c - float(low.iloc[-1])) / daily_range > 0.70) if daily_range > 0 else False
        rsi_ok = 50 <= rsi <= 78

        score = 0
        signals = []
        if s1_breakout:   score += 35; signals.append(f"🚀 突破+{s1_margin:.1f}%")
        if s2_volume:     score += 30; signals.append(f"🔥 量{s2_ratio:.1f}x")
        if s3_rs:         score += 25; signals.append(f"💪 RS+{ret5-bm_ret5:.1f}%")
        if strong_close:  score += 5;  signals.append("⬆️ 强收盘")
        if rsi_ok:        score += 5;  signals.append(f"RSI{rsi:.0f}")

        setup = "强启动" if score >= 70 else ("启动中" if score >= 50 else "弱启动")
        return {
            "score": min(100, score), "signals": signals, "setup": setup,
            "s1": s1_breakout, "s2": s2_volume, "s3": s3_rs,
            "met": met, "ret5": ret5, "rsi": rsi,
        }
    except Exception:
        return None


def _gen_rationale(df, channel: str, result: dict) -> str:
    """生成一行理由：变量 → 预期差 → 价格位置 → 验证窗口"""
    try:
        close  = df["Close"].astype(float)
        last_c = float(close.iloc[-1])
        ma20   = float(close.rolling(20).mean().iloc[-1])
        h52w   = float(df["High"].tail(252).max()) if len(df) >= 252 else float(df["High"].max())
        dist_h = (last_c / h52w - 1) * 100 if h52w > 0 else 0

        if channel == "INFLECTION":
            pos6m = result.get("pos6m", 0.5)
            ret5  = result.get("ret5", 0)
            rsi   = result.get("rsi", 50)
            var_part = "量能回升+低点抬高" if result.get("gate3") else "结构企稳"
            exp_part = (f"市场仍恐慌RSI{rsi:.0f}，买量已占优"
                        if rsi < 45 else f"底部{pos6m*100:.0f}%位反弹{ret5:+.1f}%")
            return (f"变量:{var_part} → 预期差:{exp_part} → "
                    f"价格:{last_c:.2f}距MA20{(last_c/ma20-1)*100:+.1f}% → 验证:3-5日站MA20")
        else:
            ret5 = result.get("ret5", 0)
            rsi  = result.get("rsi", 60)
            met  = result.get("met", 2)
            sig_n = "三信号共振" if met == 3 else "双信号确认"
            exp_part = f"未追距52W高{dist_h:.1f}%" if dist_h < -5 else "历史高位突破"
            return (f"变量:{sig_n}放量突破 → 预期差:{exp_part} → "
                    f"价格:{last_c:.2f} RSI{rsi:.0f} → 验证:48h维持突破位")
    except Exception:
        return "计算中"


def _get_bm_return(ticker: str, days: int = 5) -> float:
    """拉取基准指数 N 日收益率"""
    try:
        df = yf.download(ticker, period="30d", progress=False, auto_adjust=True)
        if df is None or len(df) < days + 1:
            return 0.0
        closes = df["Close"].dropna()
        if len(closes) < days + 1:
            return 0.0
        return float((closes.iloc[-1] / closes.iloc[-(days + 1)] - 1) * 100)
    except Exception:
        return 0.0


# ═══════════════════════════════════════════════════════════════
# 单股评分（给 ThreadPoolExecutor 调用）
# ═══════════════════════════════════════════════════════════════

def _score_one_stock(args: tuple):
    """拉取数据并运行四策略评分，返回结果 dict 或 None"""
    item, bm_ret5, sector_fn = args
    code    = item[0]
    name    = item[1] if len(item) > 1 else code
    yf_code = item[2] if len(item) > 2 else code

    df = _fetch_df(yf_code)
    if df is None or len(df) < 30:
        return None
    try:
        price = float(df["Close"].iloc[-1])
        if not (0 < price < 1_000_000):
            return None
    except Exception:
        return None

    sector    = sector_fn(code, name) if sector_fn else "其他"
    price_str = f"{price:.2f}"

    out = {}

    top_r  = _score_top(df)
    coil_r = _score_coil(df)
    inf_r  = _score_inflection(df)
    bo_r   = _score_breakout_v2(df, bm_ret5)

    base = {"股票": name, "代码": code, "行业": sector, "现价": price_str}

    if top_r:
        out["top"] = {**base,
                      "得分": top_r["score"], "形态": top_r["setup"],
                      "信号": " ".join(top_r["signals"][:3]),
                      "理由": f"趋势多头·{top_r['setup']}",
                      "建议": top_r["setup"]}

    if coil_r and coil_r["score"] >= 45:
        out["coil"] = {**base,
                       "得分": coil_r["score"], "形态": coil_r["setup"],
                       "信号": " ".join(coil_r["signals"][:3]),
                       "理由": f"蓄势待发·{coil_r['setup']}",
                       "建议": coil_r["setup"]}

    if inf_r:
        out["inflection"] = {**base,
                             "得分": inf_r["score"], "形态": inf_r["setup"],
                             "信号": " ".join(inf_r["signals"][:3]),
                             "理由": _gen_rationale(df, "INFLECTION", inf_r),
                             "建议": inf_r["setup"]}

    if bo_r:
        out["breakout"] = {**base,
                           "得分": bo_r["score"], "形态": bo_r["setup"],
                           "信号": " ".join(bo_r["signals"][:3]),
                           "理由": _gen_rationale(df, "BREAKOUT", bo_r),
                           "建议": bo_r["setup"]}

    return out if out else None


# ═══════════════════════════════════════════════════════════════
# 单市场扫描
# ═══════════════════════════════════════════════════════════════

def _scan_market(pool: list, market_key: str, bm_ticker: str,
                 pct_start: int, pct_end: int) -> dict | None:
    """扫描单一市场，返回四策略 Top 列表，或心跳超时时返回 None"""
    bm_ret5 = _get_bm_return(bm_ticker)
    log.info(f"[{market_key}] 扫描 {len(pool)} 只  基准5日 {bm_ret5:+.2f}%")

    sector_fn = None
    try:
        from modules.sector_map import get_sector  # noqa: PLC0415
        sector_fn = get_sector
    except Exception:
        pass

    top_pool = []
    coil_pool = []
    inf_pool  = []
    bo_pool   = []

    args_list  = [(item, bm_ret5, sector_fn) for item in pool]
    total      = len(pool)
    done_count = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(_score_one_stock, a): i for i, a in enumerate(args_list)}
        for fut in as_completed(futures):
            done_count += 1

            if done_count % HEARTBEAT_CHECK_INTERVAL == 0 or done_count == total:
                # 写进度
                pct = int(pct_start + (pct_end - pct_start) * done_count / total)
                _write_progress(pct, "running",
                                f"[{market_key}] {done_count}/{total} 只...")
                # 心跳检查
                if not _heartbeat_alive():
                    log.warning(f"[{market_key}] 心跳超时，取消扫描")
                    ex.shutdown(wait=False, cancel_futures=True)
                    return None

            result = fut.result()
            if result is None:
                continue
            if "top"        in result: top_pool.append(result["top"])
            if "coil"       in result: coil_pool.append(result["coil"])
            if "inflection" in result: inf_pool.append(result["inflection"])
            if "breakout"   in result: bo_pool.append(result["breakout"])

    top30  = sorted(top_pool,  key=lambda x: x["得分"], reverse=True)[:30]
    coil30 = sorted(coil_pool, key=lambda x: x["得分"], reverse=True)[:30]
    inf10  = sorted(inf_pool,  key=lambda x: x["得分"], reverse=True)[:10]
    bo10   = sorted(bo_pool,   key=lambda x: x["得分"], reverse=True)[:10]

    log.info(f"[{market_key}] 完成 趋势={len(top30)} 蓄势={len(coil30)} "
             f"拐点={len(inf10)} 启动={len(bo10)}")
    return {"top": top30, "coil": coil30, "inflection": inf10,
            "breakout": bo10, "bm_ret5": bm_ret5}


# ═══════════════════════════════════════════════════════════════
# GitHub Gist 上传（云端模式专用）
# ═══════════════════════════════════════════════════════════════

def _upload_to_gist(json_str: str) -> bool:
    """把扫描结果上传/更新到 GitHub Gist（供 Streamlit Cloud 读取）"""
    gist_token = os.environ.get("GIST_TOKEN", "")
    gist_id    = os.environ.get("GIST_ID", "")
    if not gist_token or not gist_id:
        log.warning("GIST_TOKEN / GIST_ID 未配置，跳过上传")
        return False
    try:
        resp = requests.patch(
            f"https://api.github.com/gists/{gist_id}",
            headers={
                "Authorization": f"token {gist_token}",
                "Accept": "application/vnd.github+json",
            },
            json={"files": {"scan_results.json": {"content": json_str}}},
            timeout=30,
        )
        if resp.status_code == 200:
            log.info(f"✅ 结果已上传到 Gist: {gist_id}")
            return True
        else:
            log.error(f"Gist 上传失败: {resp.status_code} {resp.text[:200]}")
            return False
    except Exception as e:
        log.error(f"Gist 上传异常: {e}")
        return False


# ═══════════════════════════════════════════════════════════════
# 主流程
# ═══════════════════════════════════════════════════════════════

def main():
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    log.info("scan_worker 启动")

    # 结果仍然有效且不强制重扫时直接退出
    if not _FORCE_RESCAN and _results_valid():
        log.info("结果仍在有效期（4h），无需重扫")
        _write_progress(100, "done", "已有有效缓存")
        return

    # 检查是否已有另一个进程在跑
    if PID_FILE.exists():
        try:
            old_pid = int(PID_FILE.read_text().strip())
            if old_pid != os.getpid():
                os.kill(old_pid, 0)     # 不抛说明进程存活
                log.info(f"进程 {old_pid} 已在运行，退出")
                return
        except (ProcessLookupError, ValueError):
            pass   # 旧进程已死，继续
        except Exception:
            pass

    # 写入 PID
    PID_FILE.write_text(str(os.getpid()), encoding="utf-8")

    # 信号处理：清理 PID 文件
    def _cleanup(signum=None, frame=None):
        PID_FILE.unlink(missing_ok=True)
        _write_progress(0, "aborted", "进程被中断")
        log.info("scan_worker 退出（信号中断）")
        sys.exit(0)

    signal.signal(signal.SIGTERM, _cleanup)
    signal.signal(signal.SIGINT,  _cleanup)

    _write_progress(1, "running", "初始化中...")

    try:
        # 获取股票池
        us_pool, hk_pool, cn_pool = _load_pool_or_fetch()
        log.info(f"股票池就绪: US={len(us_pool)} HK={len(hk_pool)} CN={len(cn_pool)}")

        results = {}
        markets = [
            ("US", us_pool, "SPY",        5,  35),
            ("HK", hk_pool, "^HSI",       35, 65),
            ("CN", cn_pool, "000300.SS",  65, 95),
        ]

        for mkt_key, pool, bm_ticker, pct_start, pct_end in markets:
            if not _CLOUD_MODE and not _heartbeat_alive():
                log.warning("心跳超时，终止")
                _cleanup()
                return

            _write_progress(pct_start, "running",
                            f"正在扫描 {mkt_key} ({len(pool)} 只)...")
            mkt_result = _scan_market(pool, mkt_key, bm_ticker, pct_start, pct_end)

            if mkt_result is None:
                _cleanup()
                return

            results[mkt_key] = mkt_result

        # 写入最终结果
        final = {"timestamp": time.time(), **results}
        result_json = json.dumps(final, ensure_ascii=False, default=str)
        RESULTS_FILE.write_text(result_json, encoding="utf-8")
        log.info(f"✅ 扫描完成，结果已写入 {RESULTS_FILE}")
        _write_progress(100, "done", "全市场扫描完成 ✅")

        # 云端模式：上传结果到 GitHub Gist
        if _CLOUD_MODE:
            _upload_to_gist(result_json)

    except Exception as e:
        log.error(f"扫描异常: {e}", exc_info=True)
        _write_progress(0, "error", str(e)[:120])
    finally:
        PID_FILE.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
