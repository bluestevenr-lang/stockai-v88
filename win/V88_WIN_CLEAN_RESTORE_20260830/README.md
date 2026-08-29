# V88 新 SSD 清洁恢复包（2026-08-30）

这个目录是新 Windows 11 的唯一恢复入口。不要运行 `win/` 下的旧安装、K3 API、全权限或 CUTOVER 脚本。

## 为什么 Windows 比 Mac 多几道闸

Mac 的 LaunchAgent 已有稳定登录环境；OpenClaw 2026.7.1-2 在原生 Windows 上使用当前用户的
`LogonTrigger + InteractiveToken` 计划任务。也就是说：**同一用户登录后，锁屏可以继续运行；注销、
重启后停在登录页则不能运行。** 本恢复包只承诺“登录后 7×24”，不会把“机器通电”冒充“机器人在线”。

新盘部署必须从头到尾使用同一个专用 Windows 用户。不要在普通用户和另一个管理员账户之间切换执行；
否则 OAuth、飞书配置和计划任务会落在不同用户目录。若以后要断电重启后无人值守，必须由本人另行选择
安全的自动登录方案；恢复脚本不会静默保存 Windows 密码。

`RESTORE_V88.bat` 会请求管理员权限，以便读取新盘健康计数和注册正式任务。专用 Windows 用户本身必须是
管理员；每个阶段都必须批准为这个同一用户。若 UAC 要求输入另一个管理员账户，立即取消——脚本也会比较
交互登录 SID 与提权 SID，不同就硬停，避免 Gateway 错装到另一个人的登录任务里。

计划任务只提供“Gateway 进程崩溃后 3 次、每次间隔 1 分钟的重启”。RPC 卡死或飞书 WebSocket 断开时，
脱敏健康任务会告警，但不会擅自改模型、凭据或重启主机；因此这里不把它宣传成完整自愈。BIOS 来电自启、
Windows Update 重启策略和安全自动登录也必须在新机现场单独确认。

## 目标

- 唯一飞书接待代理：`v88-gpt`。
- GPT：`openai/gpt-5.6-sol`，官方 ChatGPT/Codex OAuth，`fallbacks=[]`。
- K3：只使用 Kimi Code managed OAuth 的 `kimi-code/k3-256k`，不绑定飞书。
- 工具：只允许读取脱敏工作区，禁止命令执行、写文件、浏览器、主机控制和交易。
- 零新增费用：禁止 API Key、按量付费、Extra Usage 和静默回退。

## 明天的顺序

1. 新 SSD 安装 Windows 11，完成系统更新。先运行
   `preflight_new_ssd_win.ps1 -Phase Host`；它只读检查新盘、坏块/NVMe/NTFS事件、待重启、时间、
   TLS、代理、AC 睡眠/休眠和 18789 端口。`BLOCKED` 不得继续；`MANUAL_REQUIRED` 必须看完
   `%LOCALAPPDATA%\V88CleanRestore\preflight-host.json`。没有“全部放行”开关；确实无法自动证明时，只能按
   报告中的唯一项目逐项确认：`-SsdHealthExternallyVerified`、`-UnscopedEventsReviewed`、
   `-PowerSettingsExternallyVerified`、`-TimeSyncExternallyVerified`、`-PendingRenameReviewed`。
   `-HostPreflightReviewed` 只对应已有 AutoLogon 的安全影响；这些确认及报告哈希只写入受保护的本机状态。
2. 安装 Git、Python 3.10+、ChatGPT/Codex 桌面端和官方 Kimi Code；Node 必须符合固定 OpenClaw 的
   22.22.3–22.x、24.15–24.x 或 25.9–25.x。不要导入旧系统凭据。
3. 清洁克隆公开恢复仓与私有 V88 数据仓，确认本目录和 `Desktop\ai-daily-report-v2\data` 存在；不要把旧 D 盘当作清洁来源。
4. 双击 `RESTORE_V88.bat`，批准管理员提示，默认执行 `Prepare`。若报告要求人工证明，只添加与该项
   一一对应的确认开关；例如只有 AutoLogon 时使用 `RESTORE_V88.bat -Stage Prepare -HostPreflightReviewed`。
   它会再次核验预检、文件哈希、固定版本、
   同一 Windows 用户、只读权限，并真实生成一次脱敏投影。
5. 执行 `RESTORE_V88.bat -Stage OAuth`，在新 Win 上完成 GPT 官方 OAuth。
6. 完成 Kimi Code managed OAuth，再执行 `RESTORE_V88.bat -Stage Kimi`。K3 验收独立记录；K3 未好不会伪装成三方完成。
7. 让 Mac 端先禁用正式 `v88-gpt` 飞书接收账户，避免两个主机抢同一应用。
8. 执行 `RESTORE_V88.bat -Stage Feishu -MacReceiverDisabled`，在新 Win 本机输入正式 App ID/Secret。
9. 手机给正式 V88-GPT 发自然语言；若出现配对码，在新 Win 批准后再发一次，直到收到真实回答。
10. 在新 ChatGPT 桌面端重新配对 Remote，并由 Mac 实际读取这个新任务；不能复用旧主机 ID 或旧任务 ID。
11. 执行 `RESTORE_V88.bat -Stage Verify -MacReceiverDisabled -PhoneAnswerConfirmed -RemoteMacReadConfirmed`。
    脚本会换成永久 Gateway 任务、拒绝 Startup 文件夹降级、加 3 次/每分钟的崩溃重启，再显示一次性口令。
    手机必须把该口令发给正式 V88-GPT，脚本会核验同一飞书会话由 `gpt-5.6-sol` 生成并实际发出回复，且由
    本人输入 `YES` 确认手机看见。旧时间戳或健康通知不能通过。若口令超时，直接重跑 Verify 会续接已安装的
    永久任务，不需要手工清场。随后它才注册投影和脱敏健康监控。
12. 关闭 Codex/ChatGPT 桌面端，再双击运行
    `RESTORE_V88.bat -Stage PostExit -CodexClosedConfirmed`，按提示完成新的手机一次性口令；健康与投影任务
    也必须在本轮开始后各自重新成功一次，历史成功记录无效（最多等待约 6 分钟）。
13. 人工重启 Win，让**同一个专用用户完成登录**，不要先打开 Codex；先从手机问一次。Mac 再实际读取
    新配对的 Win Remote 任务，然后运行
    `RESTORE_V88.bat -Stage PostReboot -RemoteMacReadAfterRebootConfirmed`，再次完成手机一次性口令。
    只有脚本证明本次启动晚于 PostExit 保存的启动标记、Mac Remote 已重新读到且两项任务本次开机后执行，
    才写入最终 `verified=true`。

如果私仓不在默认位置，每个需要数据的阶段都追加：`-V88DataPath "你的路径\data"`。

## 硬闸

- OAuth、App Secret、API Key、Remote 标识、资产、持仓和原始日志不得进入 Git 或迁移包。
- 基础聊天通过不等于 V88 业务认证通过。必须另行完成最新同事实包 GPT/K3 双审、脱敏投影和中央发布检查。
- K3 暂时不可用时，GPT 只读飞书基础服务仍可恢复；但 K 席和所有需要双审/三审的推荐必须明确显示未验证，不得冒充通过。
- 手机往返、退出 Codex 后问答、重启后问答三项缺一项，都不能宣布 7×24 恢复。
- Windows 进入睡眠/休眠、用户注销或重启后停在登录页，Gateway 都不可能保持在线；显示器可以关闭，
  但 AC 供电下系统睡眠和休眠必须为“从不”。
- Gateway 只监听 `127.0.0.1:18789`，不需要开放 Windows 入站防火墙；飞书走出站 WebSocket。
- Gateway 正式任务必须唯一、属于恢复用户并带崩溃重启策略；Startup 文件夹 fallback 不算正式验收。
- 重新执行 Prepare/OAuth/Kimi/Feishu 会使下游认证失效；一旦永久 Gateway 转换已经开始，禁止倒回上游
  覆盖配置，只能续跑 Verify 或使用另行审查的维护/重置流程。

## 回退

- 在 Win 正式验收完成前，Mac 仍是临时主机。
- 切换失败时保持 Win 飞书账户关闭，重新启用 Mac 正式账户；不要同时启用两端。
