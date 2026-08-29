# Mac 临时 V88/OpenClaw 主机状态（2026-08-29）

## 已完成

- Mac 用户 OpenClaw 固定为 `2026.7.1-2`，Gateway 仅监听本机回环地址，由 LaunchAgent 常驻。
- 正式飞书应用以独立账户别名 `v88-gpt` 接入；旧 “OpenClaw ai 助手” 两个账户已禁用。
- 飞书默认账户为 `v88-gpt`，唯一绑定代理为 `v88-gpt`。
- `v88-gpt` 使用 `openai/gpt-5.6-sol`，`fallbacks=[]`。
- GPT 官方 OAuth 已完成真实本机问答；执行轨迹为 OpenAI、无 fallback。
- Kimi Code managed OAuth 已完成真实 K3 问答；K3 不绑定飞书，只作为独立复核席。
- 工具边界为只读工作区：只允许 `read`，禁止写入、命令执行、浏览器、跨会话发送、定时任务和主机控制。
- Google/Moonshot 的旧 API-key 活动入口已隔离；活动认证只剩 OpenAI OAuth。
- V88 脱敏投影任务已启用，每 5 分钟刷新；投影不含账户、资产、持仓数量、成本和交易凭据。

## 当前限制

- 正式飞书应用的“手机发自然语言 -> 机器人回答”仍必须用真实手机消息验收；仅有连接探针不能替代往返证据。
- 当前 V88 投影虽已刷新，但 GPT/K3/三方底层裁决时间早于投影生成时间，且发布检查仍未通过。因此机器人只能解释现有快照并明确标注过期，不得发布新买入名单。
- 正式飞书 App Secret 只保存在 Mac 本机 OpenClaw 配置，未写入 Git、迁移包或本文。

## 新 SSD 切回 Win 的硬闸

1. 新 Win 必须重新完成 Remote 配对、GPT OAuth、Kimi Code managed OAuth 和正式飞书凭据录入；不得复制 Mac/旧 Win 的 OAuth、令牌、Secret、主机 ID 或任务 ID。
2. 新 Win 先保持飞书关闭。只有本机 GPT 哨兵、K3 哨兵、投影测试、健康监控和唯一代理配置通过，才进入切换步骤。
3. 切换时先禁用 Mac 的 `v88-gpt` 飞书账户，再启用 Win 正式账户，避免同一应用两个接收端抢消息。
4. 必须实测：手机自然语言问答、退出 Codex 后仍回答、Gateway 单实例、重启后仍回答。
5. V88 业务层需另行完成最新同事实包 GPT/K3 双审和中央发布检查；基础聊天成功不等于投资业务认证成功。

## 零费用纪律

- 只使用现有 ChatGPT/Codex 与 Kimi Code 订阅。
- 禁止 API Key、按量付费、Extra Usage 和静默模型回退。
