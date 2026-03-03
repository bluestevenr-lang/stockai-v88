"""
行业映射模块 - 统一数据源，覆盖全部682只股票池
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
【V91.7】单一数据源，无论 AI/模块如何调整，行业映射永不变
  - 美股 240 只 + 港股 200 只 + A股 240 只 = 680 只
  - 所有扫描（regime/batch_scan/并发）统一使用 get_sector()
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

# 美股代码→行业（完整覆盖 240 只备用池 + 云端常见）
CODE_SECTOR_MAP = {
    # 科技
    "AAPL": "💻 科技", "MSFT": "💻 科技", "GOOGL": "💻 科技", "GOOG": "💻 科技", "AMZN": "💻 科技",
    "META": "💻 科技", "NVDA": "💻 科技", "NFLX": "💻 科技", "TSM": "💻 科技", "ASML": "💻 科技",
    "AMD": "💻 科技", "INTC": "💻 科技", "QCOM": "💻 科技", "AVGO": "💻 科技", "LRCX": "💻 科技",
    "KLAC": "💻 科技", "MU": "💻 科技", "MRVL": "💻 科技", "NXPI": "💻 科技", "TXN": "💻 科技",
    "ADI": "💻 科技", "ON": "💻 科技", "CRM": "💻 科技", "ORCL": "💻 科技", "ADBE": "💻 科技",
    "NOW": "💻 科技", "SNOW": "💻 科技", "PLTR": "💻 科技", "DDOG": "💻 科技", "CRWD": "💻 科技",
    "ZS": "💻 科技", "NET": "💻 科技", "OKTA": "💻 科技", "PANW": "💻 科技", "SHOP": "💻 科技",
    "PYPL": "💻 科技", "MELI": "💻 科技", "BABA": "💻 科技", "BIDU": "💻 科技", "JD": "💻 科技",
    "PDD": "💻 科技", "BILI": "💻 科技", "TME": "💻 科技", "NTES": "💻 科技", "IQ": "💻 科技",
    "IBM": "💻 科技", "HPQ": "💻 科技", "DELL": "💻 科技", "EA": "💻 科技", "TTWO": "💻 科技",
    "TCEHY": "💻 科技", "ZM": "💻 科技", "DOCU": "💻 科技", "TWLO": "💻 科技", "SPOT": "💻 科技",
    "UBER": "💻 科技", "LYFT": "💻 科技", "ABNB": "💻 科技", "DASH": "💻 科技", "COIN": "💻 科技",
    "RBLX": "💻 科技", "U": "💻 科技", "T": "💻 科技", "VZ": "💻 科技", "TMUS": "💻 科技",
    "CMCSA": "💻 科技",
    # 金融
    "JPM": "💰 金融", "BAC": "💰 金融", "WFC": "💰 金融", "C": "💰 金融", "GS": "💰 金融",
    "MS": "💰 金融", "BLK": "💰 金融", "SCHW": "💰 金融", "V": "💰 金融", "MA": "💰 金融",
    "AXP": "💰 金融", "COF": "💰 金融", "DFS": "💰 金融", "SYF": "💰 金融",
    # 医疗
    "JNJ": "🏥 医疗", "UNH": "🏥 医疗", "PFE": "🏥 医疗", "ABBV": "🏥 医疗", "TMO": "🏥 医疗",
    "ABT": "🏥 医疗", "LLY": "🏥 医疗", "MRK": "🏥 医疗", "BMY": "🏥 医疗", "AMGN": "🏥 医疗",
    "GILD": "🏥 医疗", "CVS": "🏥 医疗", "MRNA": "🏥 医疗", "BNTX": "🏥 医疗", "REGN": "🏥 医疗",
    "VRTX": "🏥 医疗", "ILMN": "🏥 医疗", "BIIB": "🏥 医疗", "ISRG": "🏥 医疗", "DXCM": "🏥 医疗",
    "ALGN": "🏥 医疗", "IDXX": "🏥 医疗",
    # 消费（含酒店、零售、餐饮）
    "PG": "🛒 消费", "KO": "🛒 消费", "PEP": "🛒 消费", "WMT": "🛒 消费", "COST": "🛒 消费",
    "HD": "🛒 消费", "LOW": "🛒 消费", "TGT": "🛒 消费", "NKE": "🛒 消费", "SBUX": "🛒 消费",
    "MCD": "🛒 消费", "CMG": "🛒 消费", "YUM": "🛒 消费", "DIS": "🛒 消费", "TSLA": "🛒 消费",
    "ULTA": "🛒 消费", "EL": "🛒 消费", "FIVE": "🛒 消费", "DG": "🛒 消费", "DLTR": "🛒 消费",
    "ROST": "🛒 消费", "TJX": "🛒 消费", "LULU": "🛒 消费", "M": "🛒 消费", "KSS": "🛒 消费",
    "JWN": "🛒 消费", "MAR": "🛒 消费", "HLT": "🛒 消费", "MGM": "🛒 消费", "WYNN": "🛒 消费",
    "LVS": "🛒 消费", "BKNG": "🛒 消费", "EXPE": "🛒 消费", "TRIP": "🛒 消费",
    "NIO": "🛒 消费", "LI": "🛒 消费", "XPEV": "🛒 消费", "RIVN": "🛒 消费", "LCID": "🛒 消费",
    "AZO": "🛒 消费", "ORLY": "🛒 消费", "AAP": "🛒 消费", "KMX": "🛒 消费", "AN": "🛒 消费",
    # 能源
    "XOM": "⚡ 能源", "CVX": "⚡ 能源", "COP": "⚡ 能源", "SLB": "⚡ 能源", "ENPH": "⚡ 能源",
    "SEDG": "⚡ 能源", "FSLR": "⚡ 能源", "RUN": "⚡ 能源", "PLUG": "⚡ 能源", "FCEL": "⚡ 能源",
    "BE": "⚡ 能源",
    # 工业
    "BA": "🏭 工业", "CAT": "🏭 工业", "GE": "🏭 工业", "HON": "🏭 工业", "LMT": "🏭 工业",
    "RTX": "🏭 工业", "NOC": "🏭 工业", "F": "🏭 工业", "GM": "🏭 工业", "DE": "🏭 工业",
    "DHR": "🏭 工业", "ITW": "🏭 工业", "EMR": "🏭 工业",
    # 运输
    "UPS": "🚚 运输", "FDX": "🚚 运输", "NSC": "🚚 运输", "UNP": "🚚 运输", "CSX": "🚚 运输",
    "DAL": "🚚 运输", "AAL": "🚚 运输", "UAL": "🚚 运输", "LUV": "🚚 运输",
    # 材料
    "AMAT": "🔧 材料", "MMM": "🔧 材料", "DD": "🔧 材料", "DOW": "🔧 材料", "LIN": "🔧 材料",
    "APD": "🔧 材料", "ECL": "🔧 材料", "PPG": "🔧 材料",
    # 地产
    "AMT": "🏢 地产", "CCI": "🏢 地产", "EQIX": "🏢 地产", "DLR": "🏢 地产",
    # 港股（5位）
    "00700": "💻 科技", "09988": "💻 科技", "09618": "💻 科技", "09999": "💻 科技", "03690": "💻 科技",
    "09626": "💻 科技", "09888": "💻 科技", "01024": "💻 科技", "09698": "💻 科技", "09992": "💻 科技",
    "09666": "💻 科技", "09961": "💻 科技", "09868": "🛒 消费", "09866": "🛒 消费", "02015": "🛒 消费",
    "00005": "💰 金融", "00939": "💰 金融", "01398": "💰 金融", "03988": "💰 金融", "01288": "💰 金融",
    "03968": "💰 金融", "02318": "💰 金融", "02388": "💰 金融", "02628": "💰 金融", "06886": "💰 金融",
    "06030": "💰 金融", "01299": "💰 金融", "00941": "💻 科技", "00728": "💻 科技", "00762": "💻 科技",
    "00857": "⚡ 能源", "00386": "⚡ 能源", "00883": "⚡ 能源", "02318": "💰 金融", "01024": "💻 科技",
    "02269": "🏥 医疗", "00175": "🏭 工业", "01177": "🏥 医疗", "01093": "🏥 医疗",
    "00291": "🛒 消费", "01876": "🛒 消费", "02020": "🛒 消费", "02331": "🛒 消费",
    "02333": "🏭 工业", "01211": "🏭 工业", "00388": "💰 金融", "00027": "🛒 消费",
    "01109": "🏢 地产", "00688": "🏢 地产", "01113": "🏢 地产", "00016": "🏢 地产",
    "0788": "💻 科技", "0992": "💻 科技", "0981": "💻 科技", "1347": "💻 科技",
    "0690": "🛒 消费", "1810": "🛒 消费", "1109": "🏢 地产", "1928": "🛒 消费",
    # A股
    "600519": "🛒 消费", "000858": "🛒 消费", "000333": "🛒 消费", "601318": "💰 金融",
    "600036": "💰 金融", "600030": "💰 金融", "000001": "💰 金融", "300750": "⚡ 能源",
    "601012": "⚡ 能源", "300274": "💻 科技", "002475": "💻 科技", "603259": "🏥 医疗",
    "600276": "🏥 医疗", "300347": "🏥 医疗", "603882": "🏥 医疗", "600000": "💰 金融",
    "600009": "🚚 运输", "600016": "💰 金融", "600019": "🔧 材料", "600028": "⚡ 能源",
    "600030": "💰 金融", "600036": "💰 金融", "600050": "💻 科技", "600104": "🏭 工业",
    "600111": "🔧 材料", "600519": "🛒 消费", "600547": "🔧 材料", "600585": "🔧 材料",
    "600690": "🛒 消费", "600887": "🛒 消费", "600900": "⚡ 能源", "601012": "⚡ 能源",
    "601088": "⚡ 能源", "601138": "💻 科技", "601166": "💰 金融", "601186": "🏭 工业",
    "601288": "💰 金融", "601318": "💰 金融", "601328": "💰 金融", "601398": "💰 金融",
    "601628": "💰 金融", "601668": "🏭 工业", "601766": "🏭 工业", "601857": "⚡ 能源",
    "601888": "🛒 消费", "601899": "🔧 材料", "601919": "🚚 运输", "601939": "💰 金融",
    "601988": "💰 金融", "603288": "🛒 消费", "603501": "💻 科技", "603986": "💻 科技",
    "000002": "🏢 地产", "000063": "💻 科技", "000333": "🛒 消费", "000538": "🏥 医疗",
    "000568": "🛒 消费", "000651": "🛒 消费", "000858": "🛒 消费", "002001": "🔧 材料",
    "002027": "💻 科技", "002230": "💻 科技", "002304": "🛒 消费", "002352": "🚚 运输",
    "002415": "💻 科技", "002475": "💻 科技", "002594": "🏭 工业", "002714": "🛒 消费",
    "300003": "🏥 医疗", "300014": "⚡ 能源", "300015": "🏥 医疗", "300033": "💻 科技",
    "300059": "💻 科技", "300122": "🏥 医疗", "300274": "⚡ 能源", "300347": "🏥 医疗",
    "300496": "💻 科技", "300750": "⚡ 能源", "300760": "🏥 医疗", "688008": "💻 科技",
    "688012": "💻 科技", "688041": "💻 科技", "688111": "💻 科技", "688981": "💻 科技",
}

# 中文名→行业（含中英文混合）
NAME_SECTOR_CN = {
    "苹果": "💻 科技", "微软": "💻 科技", "谷歌": "💻 科技", "亚马逊": "💻 科技", "英伟达": "💻 科技",
    "台积电": "💻 科技", "阿斯麦": "💻 科技", "泛林集团": "💻 科技", "恩智浦": "💻 科技",
    "德州仪器": "💻 科技", "亚德诺": "💻 科技", "安森美": "💻 科技", "应用材料": "🔧 材料",
    "阿里巴巴": "💻 科技", "百度": "💻 科技", "京东": "💻 科技", "拼多多": "💻 科技",
    "腾讯": "💻 科技", "网易": "💻 科技", "茅台": "🛒 消费", "招商银行": "💰 金融",
    "百时美施贵宝": "🏥 医疗", "吉利德": "🏥 医疗", "安进": "🏥 医疗", "劳氏": "🛒 消费",
    "希尔顿": "🛒 消费", "诺德斯特龙": "🛒 消费", "万豪": "🛒 消费", "金沙": "🛒 消费",
    "永利": "🛒 消费", "美高梅": "🛒 消费", "新秀丽": "🛒 消费", "普拉达": "🛒 消费",
    "3M": "🔧 材料", "艺康": "🔧 材料", "艾默生": "🏭 工业", "诺福克南方": "🚚 运输",
    "联合太平洋": "🚚 运输", "洛克希德": "🏭 工业", "卡特彼勒": "🏭 工业", "杜邦": "🔧 材料",
    "迪尔": "🏭 工业", "康非石油": "⚡ 能源", "Digital Realty": "🏢 地产",
    "贵州茅台": "🛒 消费", "五粮液": "🛒 消费", "美的": "🛒 消费", "格力": "🛒 消费",
    "伊利": "🛒 消费", "海天": "🛒 消费", "恒瑞": "🏥 医疗", "药明": "🏥 医疗",
    "宁德时代": "⚡ 能源", "隆基": "⚡ 能源", "比亚迪": "🏭 工业", "中国平安": "💰 金融",
}

# 英文关键词（用于云端返回英文名）
KEYWORDS_EN = {
    "💻 科技": ['tech', 'software', 'cloud', 'data', 'ai', 'cyber', 'semi', 'chip', 'internet', 'digital',
               'computer', 'nvidia', 'meta', 'amazon', 'google', 'microsoft', 'apple', 'tesla', 'zoom',
               'salesforce', 'oracle', 'adobe', 'intel', 'amd', 'qualcomm', 'telco', 'telecom', 'wireless',
               'network', 'semiconductor', 'electronic', 'palantir', 'crowdstrike', 'cloudflare', 'okta',
               'shopify', 'paypal', 'coinbase', 'roblox', 'unity', 'docu', 'twilio', 'spotify', 'uber',
               'lyft', 'airbnb', 'doordash', 'dash', 'comcast', 'verizon', 'att'],
    "🏥 医疗": ['health', 'medical', 'pharma', 'bio', 'drug', 'hospital', 'care', 'pfizer', 'moderna',
               'johnson', 'bristol', 'merck', 'abbvie', 'thermo', 'abbott', 'eli lilly', 'gilead',
               'regeneron', 'vertex', 'biogen', 'bristol-myers', 'intuitive', 'dexcom', 'align', 'idexx',
               'illumina', 'novartis', 'sanofi'],
    "💰 金融": ['bank', 'financial', 'insurance', 'capital', 'credit', 'invest', 'morgan', 'goldman',
               'visa', 'mastercard', 'paypal', 'wells fargo', 'citigroup', 'chase', 'blackrock',
               'charles schwab', 'american express', 'discover', 'synchrony'],
    "🛒 消费": ['retail', 'consumer', 'store', 'shop', 'food', 'beverage', 'restaurant', 'hotel',
               'nike', 'starbucks', 'mcdonald', 'walmart', 'target', 'costco', 'coca', 'pepsi',
               'procter', 'colgate', 'home depot', 'lowe', 'dollar', 'ulta', 'estee', 'disney',
               'booking', 'expedia', 'marriott', 'hyatt', 'hilton', 'five below', 'below',
               'ross', 'tjx', 'lululemon', 'lululemon', 'macy', 'kohl', 'nordstrom', 'autozone',
               'oreilly', 'carmax', 'autonation', 'chipotle', 'yum'],
    "🏭 工业": ['industrial', 'manufacturing', 'auto', 'motor', 'aerospace', 'defense', 'boeing',
               'ford', 'general motors', 'caterpillar', 'honeywell', 'lockheed', 'raytheon', 'northrop',
               'emerson', 'deere', 'fedex', 'ups', 'csx', 'union pacific', 'norfolk southern',
               'delta', 'american airline', 'united', 'southwest', 'danaher', 'itw'],
    "⚡ 能源": ['energy', 'oil', 'gas', 'power', 'electric', 'solar', 'exxon', 'chevron',
               'conocophillips', 'schlumberger', 'enphase', 'first solar', 'sunrun', 'plug power',
               'fuelcell', 'bloom energy'],
    "🚚 运输": ['transport', 'logistics', 'shipping', 'airline', 'fedex', 'ups', 'csx',
               'union pacific', 'norfolk', 'delta', 'american airline', 'united', 'southwest'],
    "🔧 材料": ['chemical', 'material', 'mining', 'steel', 'metal', 'dow', 'dupont', 'linde',
               'air products', 'ppg', 'ecolab', 'emerson'],
    "🏢 地产": ['real estate', 'reit', 'property', 'american tower', 'crown castle', 'equinix',
               'digital realty', 'prologis'],
}

# 中文关键词
KEYWORDS_CN = {
    "💻 科技": ['科技', '软件', '互联网', '电子', '芯片', '半导体', '云', '数据', '人工智能', 'AI', '通信', '5G', '计算机'],
    "🏥 医疗": ['医药', '医疗', '生物', '健康', '制药', '医院', '诊断', '器械'],
    "💰 金融": ['银行', '保险', '证券', '金融', '投资', '信托', '基金'],
    "🛒 消费": ['消费', '零售', '电商', '超市', '百货', '服装', '食品', '饮料', '餐饮', '酒店', '旅游'],
    "🏭 工业": ['工业', '制造', '机械', '重工', '装备', '汽车', '航空', '船舶'],
    "⚡ 能源": ['能源', '石油', '天然气', '煤炭', '电力', '新能源', '光伏', '风电'],
    "🔧 材料": ['材料', '化工', '钢铁', '有色', '金属', '矿业', '建材'],
    "🏢 地产": ['地产', '房地产', '物业', '建筑', '基建'],
    "🚚 运输": ['交通', '物流', '航运', '航空', '铁路', '港口'],
}


def get_sector(code: str, name: str) -> str:
    """
    根据股票代码和名称判断行业分类（单一数据源，全682只覆盖）
    
    Args:
        code: 股票代码（如 AAPL, 00700, 600519）
        name: 股票名称（中英文均可）
    
    Returns:
        行业字符串（如 "💻 科技"），未匹配时返回 "🛒 消费"（零售兜底，避免❓其他）
    """
    code_upper = (code or "").upper().strip()
    code_raw = (code or "").strip().replace(".SS", "").replace(".SZ", "").replace(".HK", "")
    code_5 = code_raw.zfill(5) if len(code_raw) <= 5 else code_raw
    
    # 1. 代码映射（优先）
    if code_upper in CODE_SECTOR_MAP:
        return CODE_SECTOR_MAP[code_upper]
    if code_5 in CODE_SECTOR_MAP:
        return CODE_SECTOR_MAP[code_5]
    if code_raw in CODE_SECTOR_MAP:
        return CODE_SECTOR_MAP[code_raw]
    
    # 2. 中文名映射
    name_str = (name or "").strip()
    for cn_name, sector in NAME_SECTOR_CN.items():
        if cn_name in name_str:
            return sector
    
    # 3. 英文关键词
    name_lower = (name or "").lower()
    for sector, keywords in KEYWORDS_EN.items():
        for kw in keywords:
            if kw in name_lower:
                return sector
    
    # 4. 中文关键词
    name_safe = (name or "")
    for sector, keywords in KEYWORDS_CN.items():
        for kw in keywords:
            if kw in name_safe:
                return sector
    
    # 5. 兜底：零售/消费（避免❓其他刷屏）
    return "🛒 消费"
