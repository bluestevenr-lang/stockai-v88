# Win-Kimi 任务书 #3：双模型切换 + 每日自动会审（2026-08-20 14:10）

执行者：Win 上的 Kimi Code（yolo 会话）。本任务书整体幂等，中断可重跑。
纪律沿用：密钥永不入 git；bat 纯 ASCII；ps1 带 BOM；不动 data/；openclaw CLI 用绝对路径 `C:\Users\admin\AppData\Roaming\npm\openclaw.cmd`。

背景（已完成，不用复查）：OpenClaw 网关在跑、v88-mobile 绑定 feishu:default、配对已批准（用户 ou_8759f7dbabcd38d084f8dacd444375bb）、问答链路已通、当前默认模型 moonshot/kimi-k3。

用户定的规则（原话）：**在飞书里说"K3回复"= 点名 K3 上；否则一律默认 K2.7。**

---

## 1. 核实 K2 可用型号（先做，后面都依赖它）

```bash
OC="/c/Users/admin/AppData/Roaming/npm/openclaw.cmd"
"$OC" models list --all | grep -i "k2\|k3"
```

再用本地配置里的 moonshot key 直接问开放平台（二选一验证）：

```bash
KEY=$(grep -o '"apiKey"[^,}]*' ~/.openclaw/openclaw.json | head -1 | grep -o 'sk-[A-Za-z0-9]*')
curl -s https://api.moonshot.cn/v1/models -H "Authorization: Bearer $KEY" | grep -o '"id":"[^"]*"'
```

- 优先找 `kimi-k2.7`；没有就用返回列表里最新的 K2 系（如 kimi-k2-0905-preview 之类），**把你最终选的型号名记进报告**。
- 下文用 `<K2>` 指代你选定的型号。

## 2. 默认模型切到 K2（保底层）

把 v88-mobile 的主模型改成 K2，K3 留作 fallback（K2 挂了不至于失联）：

```bash
OC="/c/Users/admin/AppData/Roaming/npm/openclaw.cmd"
"$OC" config get agents > /tmp/agents_before.json 2>/dev/null || cat ~/.openclaw/openclaw.json > /tmp/agents_before.json
# 用 config patch 把 v88-mobile 的 model 改为对象形式：
"$OC" config patch --stdin <<EOF
{"agents":{"list":[{"id":"v88-mobile","model":{"primary":"moonshot/<K2>","fallbacks":["moonshot/kimi-k3"]}}]}}
EOF
"$OC" config validate
"$OC" gateway restart
```

注意：patch 合并语义如果不符合预期（比如把 agents.list 整个覆盖了），就改为直接编辑 `~/.openclaw/openclaw.json` 里 v88-mobile 那一项，改前备份原文件到 `~/.openclaw/openclaw.json.bak-m3`。**改完必须确认文件里其他 agent/通道配置完好**，把改后 v88-mobile 的 model 段原文贴进报告。

## 3. 顺滑层：关键词"K3"直达（核心需求）

脚本我已写好并入库：`win/k3_ask.py`（运行时从本机配置读密钥，文件本身无密钥）。你先验证它能跑：

```bash
cd /c/Users/admin/Desktop/StockAI
py -3 win/k3_ask.py "用一句话确认你是kimi-k3并报告当前时间"
```

- 输出正常回答 = 脚本通。若 401/404，检查 `~/.openclaw/openclaw.json` 里 moonshot 的 baseUrl，把真实 baseUrl 记进报告（脚本会自动读，但你要知道是什么）。
- 脚本需要拷一份到网关工作区还是直接用仓库路径，取决于 v88-mobile 的 exec 工作目录限制——如果 agent 跑不了仓库路径，就把脚本复制到 v88-mobile 的工作区目录再试，把最终可用路径记进报告。

然后给 v88-mobile 的行为准则文件（任务书#1 里你建的那个 AGENTS.md，找到它）**追加**以下规则，原文照抄：

```markdown
## K3 直达规则（2026-08-20 起，最高优先级）
- 默认你是 K2.7 接待员：快速应答、查数据、报状态。
- 用户消息里只要出现"K3"两个字符（如"K3回复""K3分析""K3会审"），必须：
  1) 用 exec 运行：py -3 <最终可用路径>\k3_ask.py "<用户完整问题原样传入>"
  2) 把脚本输出原样转述给用户，开头加一行"【K3 首席分析师】"。
  3) 脚本报错就把错误原文发给用户并建议检查密钥/网络，严禁自己冒充 K3 回答。
- 普通消息绝不主动调用 K3（省额度）。
```

## 4. 每日收盘三方会审 + 破位预警（自动推飞书）

OpenClaw 有 cron 能力（`"$OC" cron --help` 先看用法）。建一个任务：

- **时间**：cron 表达式 `47 15 * * 1-5`（北京时间，周一至周五，A股收盘后）。先 `Get-TimeZone`（powershell）确认系统时区是中国标准时间，记进报告。
- **模型**：该任务指定 `moonshot/kimi-k3`（cron 支持 per-job model；不支持就让任务内 agent 用 k3_ask.py 兜底，或直接接受用默认模型——按 cron 帮助实际支持的来，记进报告）。
- **提示词**（原文照抄）：

```
生成《V88每日收盘三方会审》。步骤：
1) 先执行"同步V88"刷新投影数据（按AGENTS.md里的方式）；
2) 读工作区投影与持仓数据，输出：①今日A股/港股/美股市场小结 ②每只持仓股对照防线/止损位逐个检查，破位的用【破位】开头标红并给处置建议 ③当前3A候选股的三席状态（GPT/Kimi/书理，报各记录时间） ④明日操作清单；
3) 结论纪律：无三方通过记录的个股一律写"暂不可执行"，不编造；
4) 全文控制在600字内，适合手机阅读。
```

- **投递**：优先用 cron 自带的 deliver/channel 能力发到飞书单聊（target 用户 ou_8759f7dbabcd38d084f8dacd444375bb，渠道 feishu:default）；如果 cron 不支持投递，就在提示词末尾加"完成后用 message 工具把全文发到 feishu 单聊"。
- **建完立刻手动触发一次**（`cron run` 之类，按实际命令），让用户手机马上收到一条测试推送——这是验收信号。
- 把 `cron list` 输出和手动触发结果记进报告。

## 5. 顺手收尾（两件小事）

1. `win/v88_mobile_config_patch.json`（任务书#2 你修 message 工具的补丁）确认无 `sk-` 字符串后入库提交。
2. 验证电源设置并记录：`powercfg /q SCHEME_CURRENT SUB_SLEEP STANDBYIDLE`，Current AC Power Setting Index 应为 `0x00000000`（用户已自行设置，你只核对记录）。

## 6. 验收（需要用户配合的部分，明确告诉他）

全部装完后，让用户在飞书"OpenClaw ai 助手"里依次发：

1. `今天天气怎么样` —— 应快速应答（K2.7，底部签名 Model 应为 K2 型号）
2. `K3回复 苹果现在能买吗` —— 应出现"【K3 首席分析师】"开头的深度回答
3. 测试推送是否已收到（第4步手动触发的）

把用户的实测结果写进 `win/KIMI_WIN_REPORT3.md`（含每步原始输出、选定型号、baseUrl、cron 配置、时区、遗留问题），然后：

```bash
cd /c/Users/admin/Desktop/StockAI
git add win/KIMI_WIN_REPORT3.md win/v88_mobile_config_patch.json
git commit -m "win-kimi: 任务书3报告——双模型切换+每日自动会审"
git push
```

push 成功后对用户说"报告3已提交"。
