# ⚡ 5分钟快速启动指南（V2优化版）

> 🆕 **V2更新**：省钱不降质！Token消耗减少70%，输出更聚焦

## 📋 你需要准备的信息

在开始前，请准备好以下三个信息：

1. **Gemini API Key**（你已经有了）
   - 格式：`AIza...` 开头
   
2. **钉钉 Webhook URL**（需要创建机器人获取）
   - 格式：`https://oapi.dingtalk.com/robot/send?access_token=...`
   
3. **钉钉 Secret 密钥**（创建机器人时一起生成）
   - 格式：`SEC...` 开头

---

## 🚀 操作步骤（按顺序执行）

### ✅ 第1步：创建钉钉机器人（3分钟）

```bash
1. 打开钉钉，创建一个群聊（或用现有群）
2. 群设置 → 智能群助手 → 添加机器人 → 自定义
3. 机器人名称：AI股市分析助手
4. 安全设置：选择「加签」
5. 复制保存：
   - Webhook URL
   - Secret 密钥
```

**详细教程：** `docs/钉钉机器人设置教程.md`

---

### ✅ 第2步：本地测试（可选，3分钟）

```bash
cd /Users/bluesteven/Desktop/StockAI

# 安装依赖
pip3 install yfinance google-generativeai

# 编辑测试脚本（填写你的三个密钥）
nano test_dingtalk.py

# 运行测试
python3 test_dingtalk.py
```

**期望结果：** 钉钉群收到一条 AI 分析消息

---

### ✅ 第3步：推送到 GitHub（5分钟）

#### 3.1 在 GitHub 创建仓库
```
1. 访问 https://github.com/new
2. 仓库名：StockAI
3. 选择：Private（私有）
4. 点击：Create repository
```

#### 3.2 推送代码
```bash
cd /Users/bluesteven/Desktop/StockAI

# 初始化（如果还没有）
git init
git add .
git commit -m "feat: 添加AI股市早晚报"

# 关联远程仓库（替换你的用户名）
git remote add origin https://github.com/你的用户名/StockAI.git

# 推送
git branch -M main
git push -u origin main
```

---

### ✅ 第4步：配置 GitHub Secrets（2分钟）

```
1. 打开你的 GitHub 仓库页面
2. Settings → Secrets and variables → Actions
3. New repository secret，依次添加三个：

   名称: GEMINI_API_KEY
   值: 你的 Gemini API Key
   
   名称: DINGTALK_WEBHOOK
   值: 你的钉钉 Webhook URL
   
   名称: DINGTALK_SECRET
   值: 你的钉钉 Secret 密钥
```

---

### ✅ 第5步：启用自动任务（1分钟）

```
1. GitHub 仓库 → Actions 标签
2. 点击「AI股市早晚报」
3. Enable workflow（启用）
4. Run workflow → 手动测试一次
5. 查看日志，确认成功
6. 检查钉钉群是否收到消息
```

---

## ✅ 完成！自动推送已启动

从明天开始：
- 🌅 每天 **7:00** → 港股+A股开盘前瞻
- 🌙 每天 **19:00** → 美股+次日A股预测

---

## 🔍 快速检查清单

部署完成后，确认：

- [ ] 钉钉机器人已创建，保存了 Webhook 和 Secret
- [ ] GitHub 仓库已创建（Private）
- [ ] 代码已成功推送到 GitHub
- [ ] 三个 Secrets 已正确配置
- [ ] GitHub Actions 已启用
- [ ] 手动测试运行成功（绿色✓）
- [ ] 钉钉群收到了测试消息

---

## ❓ 遇到问题？

### 常见问题速查

**Q: Actions 运行失败？**
→ 检查 Secrets 是否正确配置，查看日志定位错误

**Q: 钉钉没收到消息？**
→ 确认机器人安全设置选的是「加签」，检查 Webhook 和 Secret

**Q: Gemini API 失败？**
→ 确认 API Key 正确，检查是否有额度

**Q: 时间不准？**
→ 已配置北京时间，GitHub Actions 会自动转换

---

## 📚 详细文档

- 完整教程：`README_钉钉推送.md`
- 钉钉配置：`docs/钉钉机器人设置教程.md`
- GitHub 部署：`docs/GitHub自动推送部署教程.md`

---

## 🎉 恭喜完成！

你现在拥有了一个：
✅ 完全自动化的 AI 股市分析系统
✅ 每天两次精准推送
✅ 完全免费运行
✅ 专业数据+AI 驱动

**享受自动化投资分析的便利吧！** 🚀
