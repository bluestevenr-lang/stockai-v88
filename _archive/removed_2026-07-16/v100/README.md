# StockAI V100

**使命：** 成为一个干净、可信的个股深度研究工作台——输入任意代码，60秒内得到技术面诊断、交易计划和AI研判。

## 目录结构

```
v100/
├── ai/client.py         # 所有AI调用的唯一入口
├── data/
│   ├── cache.py         # 统一缓存（单目录，按命名空间TTL管理）
│   ├── fetcher.py       # 数据获取：akshare(A/HK) + yfinance(US)
│   └── validator.py     # 数据验证：价格合理性 + 双源对账
├── core/
│   ├── metrics.py       # 技术指标：MA/RSI/MACD/布林/ATR
│   ├── analysis.py      # 深度分析主流程（协调所有模块）
│   ├── trade_plan.py    # 交易计划：入场/止损/目标/仓位
│   ├── archive.py       # 分析历史归档（本地JSON）
│   └── discipline.py    # 纪律门：入场前置过滤条件
├── config/
│   ├── settings.toml    # 全局配置（代理/AI模型/缓存TTL）
│   └── watchlist.csv    # 自选股列表
├── ui/app.py            # Streamlit主入口（2个Tab）
└── ci_check.py          # 强制规则检查
```

## 强制规则（CI执行）

- 每个 `.py` 文件 ≤ 300 行
- 每个函数 ≤ 50 行
- 禁止版本号注释
- AI调用必须经过 `ai/client.py`
- 数据必须经过 `data/fetcher.py`

## 启动

```bash
cd ~/Desktop/StockAI
python -m streamlit run v100/ui/app.py
```

## 不在V100范围内（V101再议）

- 每日推送报告（report.py）
- 持仓异动监控（monitor.py）
- 多市场扫描引擎
