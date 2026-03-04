# 🤖 AI股市早晚报 - 钉钉自动推送系统

## 📖 系统简介

基于 GitHub Actions 的全自动股市分析推送系统，每天两次推送专业的 AI 分析报告到钉钉群。

### ✨ 核心特性

- ⏰ **每天两次自动推送**
  - 🌅 上午 7:00：港股+A股开盘前瞻
  - 🌙 晚上 19:00：美股+次日A股预测
  
- 🤖 **AI 驱动分析**
  - 使用 Gemini 2.5 Flash 生成专业分析
  - 实时市场数据（yfinance）
  - 技术面+基本面综合研判
  
- 💰 **完全免费**
  - GitHub Actions 免费额度充足
  - Gemini API 免费版
  - 钉钉机器人免费
  
- 🔒 **安全可靠**
  - GitHub Secrets 加密存储密钥
  - Private 仓库保护代码
  - 加签验证保证推送安全

---

## 🚀 快速开始（5步部署）

### 第1步：准备钉钉机器人 ⏱️ 5分钟

**操作指南：** 详见 `docs/钉钉机器人设置教程.md`

**需要获取：**
- Webhook URL
- Secret 密钥

---

### 第2步：本地测试（可选但推荐）⏱️ 3分钟

在推送到 GitHub 前，先在本地测试功能是否正常：

```bash
cd /Users/bluesteven/Desktop/StockAI

# 编辑测试脚本，填写你的配置
# 填写 GEMINI_API_KEY_TEST、DINGTALK_WEBHOOK_TEST、DINGTALK_SECRET_TEST
nano test_dingtalk.py

# 安装依赖
pip3 install yfinance google-generativeai

# 运行测试
python3 test_dingtalk.py
```

如果钉钉群收到消息，说明配置正确！

---

### 第3步：推送代码到 GitHub ⏱️ 5分钟

```bash
# 初始化 Git（如果还没有）
git init
git add .
git commit -m "feat: 添加AI股市早晚报功能"

# 在 GitHub 创建私有仓库（网页操作）
# https://github.com/new
# 仓库名：StockAI，选择 Private

# 关联并推送（替换你的用户名）
git remote add origin https://github.com/你的用户名/StockAI.git
git branch -M main
git push -u origin main
```

---

### 第4步：配置 GitHub Secrets ⏱️ 3分钟

在 GitHub 仓库页面：

1. 点击「Settings」→「Secrets and variables」→「Actions」
2. 点击「New repository secret」，依次添加：

| Name | Value | 说明 |
|------|-------|------|
| `GEMINI_API_KEY` | `AIza...` | 你的 Gemini API Key |
| `DINGTALK_WEBHOOK` | `https://oapi.dingtalk.com/...` | 钉钉 Webhook URL |
| `DINGTALK_SECRET` | `SEC...` | 钉钉加签密钥 |

---

### 第5步：启用并测试 ⏱️ 2分钟

1. 在 GitHub 仓库点击「Actions」标签
2. 点击「AI股市早晚报」workflow
3. 点击「Enable workflow」启用
4. 点击「Run workflow」手动测试
5. 查看执行日志，确认成功
6. 检查钉钉群是否收到消息

**✅ 完成！** 从明天开始，每天自动推送！

---

## 📁 项目结构

```
StockAI/
├── auto_reporter.py              # 核心推送脚本
├── test_dingtalk.py              # 本地测试脚本
├── .github/
│   └── workflows/
│       └── daily-report.yml      # GitHub Actions 配置
├── docs/
│   ├── 钉钉机器人设置教程.md
│   └── GitHub自动推送部署教程.md
└── README_钉钉推送.md            # 本文件
```

---

## ⏰ 推送时间表

| 时间 | 类型 | 内容 | 覆盖市场 |
|------|------|------|----------|
| 07:00 | 早报 | 开盘前瞻 | 港股 + A股 |
| 19:00 | 晚报 | 复盘+预测 | 美股 + 明日A股 |

**时区说明：** 所有时间均为北京时间（UTC+8）

---

## 📊 报告内容示例

### 🌅 早报（7:00）- 港股+A股

```markdown
# 🌅 AI股市早报 - 港股+A股

> 📅 2026-02-10 周一
> 🤖 AI驱动 · 数据支撑 · 专业分析

---

## 📊 今日港股+A股开盘前瞻

### 市场概况
恒生指数隔夜收于27183点（+0.5%），位于MA20上方，呈现强势...
上证指数昨日收于4128点，成交量温和放大...

### 今日关注点
1. 政策面：国务院发布XX政策，利好科技板块
2. 资金面：北向资金昨日净流入50亿，连续三日流入
3. 行业热点：新能源、半导体、消费电子

### 操作建议
- 今日仓位：激进型70%，稳健型50%
- 重点关注：科技股、新能源板块
- 风险提示：注意美联储议息会议

---

💡 免责声明：仅供参考，不构成投资建议
```

### 🌙 晚报（19:00）- 美股+明日A股

```markdown
# 🌙 AI股市晚报 - 美股+明日A股

> 📅 2026-02-10 周一
> 🤖 AI驱动 · 数据支撑 · 专业分析

---

## 🌙 今日美股+明日A股研判

### 今日美股开盘情况
标普500开盘上涨0.3%，纳指涨0.5%，科技股领涨...
NVDA涨2%，带动芯片板块...

### 明日A股预测
1. 开盘预判：高开0.3%左右（受美股提振）
2. 全天走势：震荡上行，关键点位4150-4200
3. 热点板块：半导体、人工智能、新能源汽车

### 操作策略
- 明日仓位：可适当加仓至60-70%
- 潜在机会：芯片股（XXX、XXX）、新能源（XXX）
- 风险提示：外围不确定性仍存，控制好仓位

---

💡 免责声明：仅供参考，不构成投资建议
```

---

## 🔧 高级配置

### 修改推送时间

编辑 `.github/workflows/daily-report.yml`：

```yaml
schedule:
  # 早报：改为 8:00（UTC 0:00）
  - cron: '0 0 * * *'
  # 晚报：改为 20:00（UTC 12:00）
  - cron: '0 12 * * *'
```

### 添加更多股票

编辑 `auto_reporter.py` 的 `markets` 列表：

```python
markets = [
    ("^HSI", "恒生指数"),
    ("000001.SS", "上证指数"),
    ("AAPL", "苹果"),  # 新增
    # ... 添加更多
]
```

### 自定义报告模板

编辑 `auto_reporter.py` 的 `generate_ai_report()` 函数中的 `prompt` 变量。

---

## 🐛 故障排查

### 问题1：GitHub Actions 运行失败

**解决方案：**
1. 检查 Secrets 配置是否正确
2. 查看 Actions 运行日志，找到具体错误
3. 确认 API Key 有效且有额度

### 问题2：钉钉没收到消息

**解决方案：**
1. 检查 Webhook URL 和 Secret 是否正确
2. 确认机器人安全设置为「加签」方式
3. 本地运行 `test_dingtalk.py` 测试

### 问题3：AI 分析内容不理想

**解决方案：**
1. 编辑 `auto_reporter.py` 中的 AI prompt
2. 调整提示词，让 AI 输出更符合需求的内容
3. 可以切换为 `gemini-2.0-flash-exp` 模型

---

## 📈 使用成本

| 项目 | 成本 | 额度 |
|------|------|------|
| GitHub Actions | 免费 | 2000分钟/月（Private仓库）|
| Gemini API | 免费 | 每天1500次调用 |
| 钉钉机器人 | 免费 | 无限制 |
| **总计** | **¥0** | **完全免费** |

**月度用量预估：**
- Actions 运行时间：60-120分钟/月
- Gemini API 调用：60次/月
- 钉钉消息：60条/月

**完全在免费额度内！**

---

## 🎯 后续规划

- [ ] 添加节假日判断（非交易日跳过）
- [ ] 支持多个钉钉群推送
- [ ] 加入重要财经事件提醒
- [ ] 生成图表（K线图、资金流向图）
- [ ] 支持微信推送（Server酱/企业微信）

---

## 📞 技术支持

如遇问题，请查阅：
- `docs/钉钉机器人设置教程.md`
- `docs/GitHub自动推送部署教程.md`

---

## 📄 许可证

MIT License - 自由使用和修改

---

**Made with ❤️ by AI + Human**
