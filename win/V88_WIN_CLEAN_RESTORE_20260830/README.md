# V88 新 SSD 清洁恢复包（2026-08-30）

这个目录是新 Windows 11 的唯一恢复入口。不要运行 `win/` 下的旧安装、K3 API、全权限或 CUTOVER 脚本。

## 目标

- 唯一飞书接待代理：`v88-gpt`。
- GPT：`openai/gpt-5.6-sol`，官方 ChatGPT/Codex OAuth，`fallbacks=[]`。
- K3：只使用 Kimi Code managed OAuth 的 `kimi-code/k3-256k`，不绑定飞书。
- 工具：只允许读取脱敏工作区，禁止命令执行、写文件、浏览器、主机控制和交易。
- 零新增费用：禁止 API Key、按量付费、Extra Usage 和静默回退。

## 明天的顺序

1. 新 SSD 安装 Windows 11，完成系统更新和新盘健康基线。
2. 安装 Git、Python 3.10+、ChatGPT/Codex 桌面端和官方 Kimi Code；不要导入旧系统凭据。
3. 清洁克隆公开恢复仓与私有 V88 数据仓，确认本目录和 `Desktop\ai-daily-report-v2\data` 存在；不要把旧 D 盘当作清洁来源。
4. 双击 `RESTORE_V88.bat`，默认执行 `Prepare`。它会核验文件哈希、固定版本、只读权限，并真实生成一次脱敏投影。
5. 执行 `RESTORE_V88.bat -Stage OAuth`，在新 Win 上完成 GPT 官方 OAuth。
6. 完成 Kimi Code managed OAuth，再执行 `RESTORE_V88.bat -Stage Kimi`。K3 验收独立记录；K3 未好不会伪装成三方完成。
7. 让 Mac 端先禁用正式 `v88-gpt` 飞书接收账户，避免两个主机抢同一应用。
8. 执行 `RESTORE_V88.bat -Stage Feishu -MacReceiverDisabled`，在新 Win 本机输入正式 App ID/Secret。
9. 手机给正式 V88-GPT 发自然语言；若出现配对码，在新 Win 批准后再发一次，直到收到真实回答。
10. 在新 ChatGPT 桌面端重新配对 Remote，并由 Mac 实际读取这个新任务；不能复用旧主机 ID 或旧任务 ID。
11. 执行 `RESTORE_V88.bat -Stage Verify -MacReceiverDisabled -PhoneAnswerConfirmed -RemoteMacReadConfirmed`。只有这一步全部通过，脚本才注册 Gateway、脱敏投影和只读健康监控自启动。
12. 退出 Codex 后再用手机问一次；最后人工重启 Win，再问一次。脚本不会自动重启。

如果私仓不在默认位置，每个需要数据的阶段都追加：`-V88DataPath "你的路径\data"`。

## 硬闸

- OAuth、App Secret、API Key、Remote 标识、资产、持仓和原始日志不得进入 Git 或迁移包。
- 基础聊天通过不等于 V88 业务认证通过。必须另行完成最新同事实包 GPT/K3 双审、脱敏投影和中央发布检查。
- K3 暂时不可用时，GPT 只读飞书基础服务仍可恢复；但 K 席和所有需要双审/三审的推荐必须明确显示未验证，不得冒充通过。
- 手机往返、退出 Codex 后问答、重启后问答三项缺一项，都不能宣布 7×24 恢复。

## 回退

- 在 Win 正式验收完成前，Mac 仍是临时主机。
- 切换失败时保持 Win 飞书账户关闭，重新启用 Mac 正式账户；不要同时启用两端。
