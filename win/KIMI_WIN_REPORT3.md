# Win-Kimi 任务书 #3 执行报告

> 执行时间：2026-08-20（任务书时间 2026-08-20 14:10）  
> 执行环境：Windows 10 / Git Bash / OpenClaw 2026.7.1-2 / 工作目录 `C:\Users\admin\Desktop\StockAI`

---

## 1. 代码更新

```bash
cd /c/Users/admin/Desktop/StockAI
git pull
```

结果：当前分支 `main` 已是最新；`data` 分支有远端更新但未影响当前工作树。

---

## 2. K2 可用型号核实

### 2.1 OpenClaw 模型列表

```text
moonshot/kimi-k3                           text+image 1049k       no    yes   default,configured
moonshot/kimi-k2-thinking                  text       262k        no    yes
moonshot/kimi-k2-thinking-turbo            text       262k        no    yes
moonshot/kimi-k2-turbo                     text       256k        no    yes
moonshot/kimi-k2.5                         text+image 262k        no    yes
moonshot/kimi-k2.6                         text+image 262k        no    yes
moonshot/kimi-k2.7-code                    text+image 262k        no    yes
```

### 2.2 开放平台模型列表

```bash
KEY=$(grep -o '"apiKey"[^,}]*' ~/.openclaw/openclaw.json | head -1 | grep -o 'sk-[A-Za-z0-9]*')
curl -s https://api.moonshot.cn/v1/models -H "Authorization: Bearer $KEY" | grep -o '"id":"[^"]*"'
```

返回：

```text
"id":"kimi-k2.6"
"id":"kimi-k3"
"id":"kimi-k2.7-code-highspeed"
"id":"kimi-k2.7-code"
```

### 2.3 最终选定的 K2 型号

任务书要求优先找 `kimi-k2.7`；OpenClaw 列表中无精确的 `kimi-k2.7`，最接近且已配置可用的是：

**`<K2> = moonshot/kimi-k2.7-code`**

---

## 3. 默认模型切换到 K2（保留 K3 fallback）

### 3.1 备份

```bash
cp ~/.openclaw/openclaw.json ~/.openclaw/openclaw.json.bak-m3
```

### 3.2 修改后的 `v88-mobile` model 段（完整原文）

```json
{
  "id": "v88-mobile",
  "name": "v88-mobile",
  "workspace": "C:\\Users\\admin\\.openclaw\\workspaces\\v88-mobile",
  "agentDir": "C:\\Users\\admin\\.openclaw\\agents\\v88-mobile\\agent",
  "model": {
    "primary": "moonshot/kimi-k2.7-code",
    "fallbacks": [
      "moonshot/kimi-k3"
    ]
  },
  ...
}
```

### 3.3 验证与网关重启

- `openclaw config validate`：通过。
- `openclaw gateway restart` 因 taskkill 无法终止旧 PID 失败；改用 PowerShell 重启计划任务：

```powershell
Stop-ScheduledTask -TaskName 'OpenClaw Gateway'
Start-Sleep -Seconds 3
Start-ScheduledTask -TaskName 'OpenClaw Gateway'
```

重启后任务状态：`Running`。`openclaw config get agents` 确认 `v88-mobile` 的 `model` 已改为对象形式。

---

## 4. K3 顺滑直达层

### 4.1 脚本可用性验证

脚本路径：`win/k3_ask.py`

首次运行报：

```text
ERROR: HTTP 400 — {"error":{"message":"invalid temperature: only 1 is allowed for this model","type":"invalid_request_error"}}
```

修复：将脚本中的 `"temperature": 0.3` 改为 `"temperature": 1`（Kimi K3 当前仅支持 `temperature=1`）。

修复后测试：

```bash
py -3 win/k3_ask.py "用一句话确认你是kimi-k3"
```

输出：

```text
我是Kimi，但当前对话未提供可核验的“kimi-k3”型号信息，因此不能确认该具体型号。
```

脚本可正常调用，返回 Kimi 风格回答。

### 4.2 脚本最终可用路径

由于 `v88-mobile` 的 `AGENTS.md` 原有限制只能运行 `<仓库>\win\v88ctl.ps1`，为稳妥起见，脚本同时部署到工作区：

- 仓库路径：`C:\Users\admin\Desktop\StockAI\win\k3_ask.py`
- 工作区路径：**`C:\Users\admin\.openclaw\workspaces\v88-mobile\k3_ask.py`**（AGENTS.md 中引用此路径）

### 4.3 Moonshot baseUrl

从 `~/.openclaw/openclaw.json` 读取：`https://api.moonshot.cn/v1`

### 4.4 AGENTS.md 追加规则

文件：`C:\Users\admin\.openclaw\workspaces\v88-mobile\AGENTS.md`

已追加「K3 直达规则（2026-08-20 起，最高优先级）」，核心要点：

- 默认 K2.7 接待员模式。
- 用户消息含"K3"二字即触发 `py -3 C:\Users\admin\.openclaw\workspaces\v88-mobile\k3_ask.py "<原问题>"`。
- 输出原样转述，开头加 `【K3 首席分析师】`。
- 脚本报错时发错误原文，严禁冒充 K3。
- 普通消息不主动调用 K3。

---

## 5. 每日收盘三方会审 + 破位预警

### 5.1 系统时区

```powershell
Get-TimeZone
```

结果：

```text
StandardName : 中国标准时间
DisplayName  : (UTC+08:00) 北京，重庆，香港特别行政区，乌鲁木齐
BaseUtcOffset: 08:00:00
```

与 cron 表达式 `47 15 * * 1-5`（北京时间 A 股收盘后）一致。

### 5.2 Cron 任务创建

```bash
openclaw cron add \
  --name "v88-daily-close-review" \
  --display-name "V88每日收盘三方会审" \
  --cron "47 15 * * 1-5" \
  --agent v88-mobile \
  --model moonshot/kimi-k3 \
  --channel feishu \
  --account default \
  --to "ou_8759f7dbabcd38d084f8dacd444375bb" \
  --expect-final \
  --timeout-seconds 300 \
  --message '<提示词原文>'
```

创建结果（JSON 摘要）：

```json
{
  "id": "10aede83-73df-4871-a2b4-b604f1e4fa52",
  "name": "v88-daily-close-review",
  "schedule": { "kind": "cron", "expr": "47 15 * * 1-5" },
  "payload": {
    "kind": "agentTurn",
    "model": "moonshot/kimi-k3",
    "timeoutSeconds": 300
  },
  "delivery": {
    "mode": "announce",
    "channel": "feishu",
    "to": "ou_8759f7dbabcd38d084f8dacd444375bb",
    "accountId": "default"
  }
}
```

### 5.3 Cron 列表

```text
ID                                   Declaration  Name              Schedule                  Next   Last   Status  Target    Delivery                                                   Agent ID    Model
10aede83-73df-4871-a2b4-b604f1e4fa52 -            V88每日收盘三方会审  cron 47 15 * * 1-5 (exact)  in 53m  2m ago ok      isolated  announce -> feishu:ou_8759f7dbabcd38d084f8dacd444375bb (explicit)  v88-mobile  moonshot/kimi-k3
```

### 5.4 手动触发测试

```bash
openclaw cron run --expect-final --wait 10aede83-73df-4871-a2b4-b604f1e4fa52
```

结果摘要：

- `ok: true`, `enqueued: true`, `completed: true`, `status: ok`
- 实际调用模型：`model: "kimi-k3"`, `provider: "moonshot"`
- 投递状态：`delivered: true`, `deliveryStatus: "delivered"`
- 目标用户：`ou_8759f7dbabcd38d084f8dacd444375bb`
- 生成标题：《V88每日收盘三方会审》2026-08-20

**测试推送已发出，手机端应已收到一条会审消息——这是验收信号之一。**

---

## 6. 顺手收尾

### 6.1 `win/v88_mobile_config_patch.json`

- 文件存在，已确认**不含 `sk-` 字符串**。
- 该文件当前已在 `main` 分支跟踪，无需新增提交。

### 6.2 电源设置

```powershell
powercfg /q SCHEME_CURRENT SUB_SLEEP STANDBYIDLE
```

结果：

- 当前交流电源设置索引：`0x00000000`
- 当前直流电源设置索引：`0x00000000`

符合任务书要求（AC 为 `0x00000000`）。

---

## 7. 验收（需用户在飞书端实测）

请在飞书"OpenClaw ai 助手"里依次发送：

1. `今天天气怎么样`
   - 预期：快速应答，底部 Model 签名应为 `moonshot/kimi-k2.7-code`（或类似 K2.7 型号）。
2. `K3回复 苹果现在能买吗`
   - 预期：回复以 `【K3 首席分析师】` 开头，内容由 `k3_ask.py` 输出。
3. 确认是否收到刚才手动触发的《V88每日收盘三方会审》推送。

实测后请把三条消息的原始输出反馈，再补充到本报告。

---

## 8. 遗留问题 / 注意事项

1. **网关重启方式**：`openclaw gateway restart` 因无法 kill 旧 PID 报错，已改用 Windows 计划任务重启。后续若再出现类似问题，可继续用 PowerShell 重启 `OpenClaw Gateway` 任务。
2. **K3 脚本 temperature**：Kimi K3 当前只接受 `temperature=1`，已将 `win/k3_ask.py` 从 `0.3` 改为 `1`；该改动需随报告一起提交。
3. **AGENTS.md 中旧规则冲突**：原 AGENTS.md "允许的遥控" 一节限制"只能运行 `<仓库>\win\v88ctl.ps1"；新增的 K3 直达规则已声明"最高优先级"，但如 agent 仍拒绝执行，可能需要进一步放宽该限制。

---

## 9. 提交记录

```bash
cd /c/Users/admin/Desktop/StockAI
git add win/KIMI_WIN_REPORT3.md win/k3_ask.py
git commit -m "win-kimi: 任务书3报告——双模型切换+每日自动会审"
git push
```

---

*报告结束。*
