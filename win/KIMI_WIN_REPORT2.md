# Win-Kimi 验收报告 #2：飞书切换与最终验收

执行时间：2026-08-20 13:30–13:40（北京时间）
执行依据：`win/KIMI_WIN_MISSION2.md`

---

## 0. 死机恢复自查

- 当前 HEAD：`674e300 win: 任务书2加死机恢复第0步(幂等重跑)`
- `git pull` 已拉取到最新 `674e300`，工作区干净。
- OpenClaw 配置文件 `~/.openclaw/openclaw.json` 中 `moonshot` 与飞书 App ID `cli_a9256edcc93b9bd2` 的 grep 计数为 5，配置完好。
- 网关通过 Windows 任务计划 `OpenClaw Gateway` 运行，重启后状态正常。

---

## 1. 启用飞书通道

OpenClaw CLI 绝对路径：`C:\Users\admin\AppData\Roaming\npm\openclaw.cmd`

执行内容：

1. 因 `feishu` 插件未安装，先执行 `openclaw plugins install clawhub:@openclaw/feishu` 完成安装。
2. 使用 `openclaw config patch --stdin` 写入飞书通道配置（App ID = `cli_a9256edcc93b9bd2`，App Secret 仅写入本地 `~/.openclaw/openclaw.json`，未入 git）。
3. `openclaw plugins enable feishu`
4. `openclaw agents bind --agent v88-mobile --bind feishu:default`
5. `openclaw gateway restart`

`openclaw channels status --probe` 原始输出：

```text
Checking channel status (probe)…
Gateway reachable.
- Feishu default: enabled, configured, running, connected, works

Tip: https://docs.openclaw.ai/cli#status adds gateway health probes to status output (requires a reachable gateway).
```

补充修复：
- 首次测试时 `v88-mobile` 的 `tools.allow` 未包含 `message`，导致飞书消息无法发出；已使用 `win/v88_mobile_config_patch.json` 通过 `openclaw config patch --file` 修复。

---

## 2. 自动完成配对

用户按提示使用手机飞书向蓝一发送“你好”。

网关日志记录到配对请求：

```text
feishu[default]: pairing request sender=ou_8759f7dbabcd38d084f8dacd444375bb
```

配对码从 `~/.openclaw/credentials/feishu-pairing.json` 读出：

```text
FJNZETNZ
```

立即执行批准：

```bash
openclaw pairing approve feishu FJNZETNZ
```

输出：

```text
Approved feishu sender ou_8759f7dbabcd38d084f8dacd444375bb.
```

批准后该用户自动获得命令执行权限。

---

## 3. 问答测试

用户从手机飞书发送（同义重发两次）：

1. `苹果现在能买么`
2. `苹果现在能买吗`

网关日志确认两条消息均已接收并分派到 `agent:v88-mobile:main`：

```text
feishu[default]: received message from ou_8759f7dbabcd38d084f8dacd444375bb in oc_5adde540bf14e69ef2410ba13d4f85b7 (p2p)
feishu[default]: Feishu[default] DM from ou_8759f7dbabcd38d084f8dacd444375bb: 苹果现在能买么
feishu[default]: dispatching to agent (session=agent:v88-mobile:main)
...
feishu[default]: Feishu[default] DM from ou_8759f7dbabcd38d084f8dacd444375bb: 苹果现在能买吗
feishu[default]: dispatching to agent (session=agent:v88-mobile:main)
```

模型调用记录：

```text
[model-fetch] start provider=moonshot api=openai-completions model=kimi-k3 ...
[model-fetch] response provider=moonshot api=openai-completions model=kimi-k3 status=200 elapsedMs=2904 contentType=text/event-stream
[model-fetch] response provider=moonshot api=openai-completions model=kimi-k3 status=200 elapsedMs=5081 contentType=text/event-stream
[model-fetch] response provider=moonshot api=openai-completions model=kimi-k3 status=200 elapsedMs=3867 contentType=text/event-stream
...
[model-fetch] response provider=moonshot api=openai-completions model=kimi-k3 status=200 elapsedMs=4939 contentType=text/event-stream
```

发送阶段日志：

```text
feishu[default]: streaming start failed; using non-streaming card fallback for 60s: Error: Create card request failed with HTTP 400
feishu[default]: dispatch complete (queuedFinal=true, replies=1)
```

说明：两次问答最终都标记为 `dispatch complete (queuedFinal=true, replies=1)`，即 OpenClaw 已成功将回复投递到飞书。但网关日志未打印最终回复文本，**手机端实际显示内容请用户确认**。

---

## 4. V88 隧道验收

执行：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File "C:\Users\admin\Desktop\StockAI\win\v88ctl.ps1" -Command start
powershell -NoProfile -ExecutionPolicy Bypass -File "C:\Users\admin\Desktop\StockAI\win\v88ctl.ps1" -Command url
powershell -NoProfile -ExecutionPolicy Bypass -File "C:\Users\admin\Desktop\StockAI\win\v88ctl.ps1" -Command status
```

生成并确认的 trycloudflare 隧道链接：

```text
https://peaceful-lang-plants-bridge.trycloudflare.com
```

`v88ctl.ps1 -Command status` 输出：

- V88 页面：运行中
- OpenClaw 网关：Running

任务书要求用户**用手机**打开该链接验证全链路通。

---

## 5. 遗留问题

1. **飞书应用权限不足**：日志反复出现 `99991672` 权限错误，提示应用尚未开通以下任一权限：
   - `contact:contact.base:readonly`
   - `contact:contact:access_as_app`
   - `contact:contact:readonly`
   - `contact:contact:readonly_as_app`
   当前 OpenClaw 已忽略该错误并继续运行，但建议后续在飞书开放平台为应用 `cli_a9256edcc93b9bd2` 开通上述权限。

2. **流式卡片发送失败并回退**：`streaming start failed; using non-streaming card fallback for 60s: Error: Create card request failed with HTTP 400`。非流式回退后消息可正常 dispatch，但体验上有延迟。

3. **feishu-dedup 持久化状态错误**：`openKeyedStore is only available for trusted plugins in this release`，当前已回退到内存模式，不影响功能。

4. **网关任务计划需管理员 UAC 弹窗**：`win\注册网关任务-双击我.bat` 需要用户有空时以管理员身份双击执行，属于第 4 步加固，不在本次验收范围。

---

## 6. 提交记录

```bash
cd /c/Users/admin/Desktop/StockAI
git add win/KIMI_WIN_REPORT2.md
git commit -m "win-kimi: 飞书切换与验收报告"
git push
```

---

*密钥说明：飞书 App Secret 仅保存在本地 `~/.openclaw/openclaw.json`，未进入 git。*
