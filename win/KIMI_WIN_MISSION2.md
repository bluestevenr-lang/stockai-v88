# Win-Kimi 任务书 #2：飞书切换与最终验收（2026-08-20 09:40）

前置事实（Mac-Kimi 已完成，不用复查）：
- **Mac 网关的飞书通道已关闭并重启**——不会有双网关抢消息，放心切换（原 enable 脚本里的 CUTOVER 确认条件已满足）
- 你在任务书#1 里已完成：OpenClaw 主体、v88-mobile(K3)、密钥注入、网关在跑
- openclaw CLI 绝对路径：`C:\Users\admin\AppData\Roaming\npm\openclaw.cmd`（PATH 里不一定有，全程用绝对路径最稳；Git Bash 里写 `/c/Users/admin/AppData/Roaming/npm/openclaw.cmd`）

纪律沿用任务书#1（密钥永不入 git；bat 纯 ASCII；ps1 带 BOM；不动 data/）。

## 1. 启用飞书通道（非交互，别跑那个带 Read-Host 的 ps1，会卡死）

参考 `win/enable_openclaw_feishu_win.ps1` 的配置结构，直接执行：

```bash
OC="/c/Users/admin/AppData/Roaming/npm/openclaw.cmd"
"$OC" config patch --stdin <<'EOF'
{"channels":{"feishu":{"enabled":true,"connectionMode":"websocket","domain":"feishu","dmPolicy":"pairing","groupPolicy":"disabled","requireMention":true,"accounts":{"default":{"appId":"cli_a9256edcc93b9bd2","appSecret":"<用户给你的Secret>","enabled":true}}}}}
EOF
"$OC" plugins enable feishu
"$OC" agents bind --agent v88-mobile --bind feishu:default
"$OC" config validate
"$OC" gateway restart
"$OC" channels status --probe
```

App ID = `cli_a9256edcc93b9bd2`。**App Secret 用户在本次会话里已经给你了**（如果没有，才许问一次）。把 channels status 的原始输出记进报告。

## 2. 自动完成配对（关键，别让主人回来点）

1. 告诉用户一句话："**现在用手机飞书给蓝一发'你好'**"，然后等他发
2. 蓝一收到后会生成配对码。你在网关日志里找它：
   - 日志目录 `~/.openclaw/logs/`（按时间排序找最新行，grep -i "pairing\|配对"）
   - 或试 `"$OC" pairing list`
3. 拿到码立刻执行 `"$OC" pairing approve feishu <码>`
4. 再让用户发第二条："**苹果现在能买吗**"——蓝一应该按 AGENTS.md 口径回答（报数据时间+三席结论）。把问答结果记进报告。

## 3. 验收"打开V88"链路

```bash
powershell -NoProfile -ExecutionPolicy Bypass -File "C:\Users\admin\Desktop\StockAI\win\v88ctl.ps1" -Command start
powershell -NoProfile -ExecutionPolicy Bypass -File "C:\Users\admin\Desktop\StockAI\win\v88ctl.ps1" -Command url
```

把生成的 trycloudflare 链接发给用户，让他**用手机**点开（不是电脑上点）。手机能打开 = 全链路通。链接和结果记进报告。
（注意：手机不在局域网也行，隧道就是干这个的。链接即钥匙，提醒用户勿转发。）

## 4. 网关加固（不在本次范围）

`win\注册网关任务-双击我.bat` 需要 UAC 管理员弹窗，你点不了——留给用户有空时双击一次即可，不阻塞本次验收。报告里标注"待用户管理员双击"。

## 5. 收尾

写 `win/KIMI_WIN_REPORT2.md`：每步原始输出、配对码批准记录、蓝一问答截图级记录、隧道链接、遗留问题。然后：

```bash
cd /c/Users/admin/Desktop/StockAI
git add win/KIMI_WIN_REPORT2.md
git commit -m "win-kimi: 飞书切换与验收报告"
git push
```

push 成功后对用户说"报告已提交"。
