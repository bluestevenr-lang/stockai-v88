# Win GPT OpenClaw automatic recovery report

- generated_at: 2026-08-25 21:10:24 +08:00
- host: DESKTOP-4H6ES39
- cli_found: True
- config_exists: True

## Safe configuration summary
feishu_enabled=True
feishu_account=default; enabled=True; has_app_id=True; has_secret=True
feishu_account=v88-gpt; enabled=True; has_app_id=True; has_secret=True
agent=main; model=
agent=v88-mobile; model={"primary":"kimi-coding/k3-256k","fallbacks":[]}
agent=v88-gpt; model="openai/gpt-5.6-sol"

## Scheduled task before recovery
state=Running; last_result=267009; last_run=2026/8/24 22:52:09; next_run=

## Version
OpenClaw 2026.7.1-2 (0790d9f)
exit_code=0

## Config validation
Config valid: $OPENCLAW_HOME\openclaw.json
exit_code=0

## Gateway restart
Gateway restart failed: Error: taskkill could not terminate gateway process 25344
Tip: openclaw gateway install
Tip: openclaw gateway
Tip: schtasks /Run /TN "OpenClaw Gateway"
exit_code=1

## Deep status after restart
[fetch-timeout] fetch timeout after 2500ms (elapsed 2509ms) operation=fetchWithTimeout url=https://registry.npmjs.org/openclaw/latest
OpenClaw status

Overview
+----------------------+-----------------------------------------------------------------------------------------------+
| Item                 | Value                                                                                         |
+----------------------+-----------------------------------------------------------------------------------------------+
| OS                   | windows 10.0.26200 (x64) 路 node 24.19.0                                                       |
| Dashboard            | http://127.0.0.1:18789/                                                                       |
| Tailscale exposure   | off                                                                                           |
| Channel              | stable (default)                                                                              |
| Update               | npm 路 deps ok                                                                                 |
| Gateway              | local 路 ws://127.0.0.1:18789 (local loopback) 路 reachable 184ms 路 auth token                  |
| Gateway service      | Scheduled Task installed 路 registered 路 unknown                                               |
| Node service         | Scheduled Task not installed                                                                  |
| Agents               | 3 路 1 bootstrap file present 路 sessions 11 路 default main active 25h ago                      |
| Memory               | enabled (plugin memory-core) 路 not checked                                                    |
| Plugin compatibility | none                                                                                          |
| Probes               | enabled                                                                                       |
| Events               | none                                                                                          |
| Tasks                | 0 active 路 0 queued 路 0 running 路 11 issues 路 audit clean 路 36 tracked                        |
| Heartbeat            | 30m (main), disabled (v88-gpt), disabled (v88-mobile)                                         |
| Last heartbeat       | skipped 路 20m ago ago 路 unknown                                                               |
| Sessions             | 11 active 路 default kimi-k3 (262k ctx) 路 3 stores                                             |
+----------------------+-----------------------------------------------------------------------------------------------+

Security audit
Summary: 0 critical 路 5 warn 路 1 info
  WARN Reverse proxy headers are not trusted
    gateway.bind is loopback and gateway.trustedProxies is empty. If you expose the Control UI through a reverse proxy, configure trusted proxies so local-client c鈥?    Fix: Set gateway.trustedProxies to your proxy IPs or keep the Control UI local-only.
  WARN Filesystem tool policy does not make exec read-only
    Found scopes where write/edit/apply_patch are unavailable but exec remains available: - agents.list.v88-mobile.tools: runtime=[exec, process], disabledFs=[writ鈥?    Fix: For read-only agents, deny exec and process too. If shell access is intentional, constrain the filesystem boundary with sandbox mode "all" and workspaceAccess "ro" or "none".
  WARN Extension plugin tools may be reachable under permissive tool policy
    Enabled extension plugins: feishu. Permissive tool policy contexts: - default - agents.list.main - agents.list.v88-gpt
    Fix: Use restrictive profiles (`minimal`/`coding`) or explicit tool allowlists that exclude plugin tools for agents handling untrusted input.
  WARN Plugin index includes unpinned npm specs
    Unpinned plugin index install records: - codex (@openclaw/codex) - moonshot (@openclaw/moonshot-provider)
    Fix: Pin install specs to exact versions (for example, `@scope/pkg@1.2.3`) for higher supply-chain stability.
  WARN Feishu doc create can grant requester permissions
    channels.feishu tools include "doc"; feishu_doc action "create" can grant document access to the trusted requesting Feishu user.
    Fix: Disable channels.feishu.tools.doc when not needed, and restrict tool access for untrusted prompts.
Full report: openclaw security audit
Deep probe: openclaw security audit --deep

Channels
+----------+---------+--------+----------------------------------------------------------------------------------------+
| Channel  | Enabled | State  | Detail                                                                                 |
+----------+---------+--------+----------------------------------------------------------------------------------------+
| Feishu   | ON      | OK     | configured 路 accounts 2/2                                                              |
+----------+---------+--------+----------------------------------------------------------------------------------------+

Sessions
+--------------------------------+--------+---------+--------------+------------------+--------------------------------+
| Key                            | Kind   | Age     | Model        | Runtime          | Tokens                         |
+--------------------------------+--------+---------+--------------+------------------+--------------------------------+
| agent:v88-                     | cron   | 4m ago  | k3-256k      | OpenClaw Default | unknown/262k (?%)              |
| mobile:cron:17ce0d7a-鈥?        |        |         |              |                  |                                |
| agent:v88-gpt:main             | direct | 7m ago  | gpt-5.6-sol  | OpenAI Codex     | 33k/372k (9%) 路 馃梽锔?99% cached  |
| agent:v88-                     | cron   | 5h ago  | k3-256k      | OpenClaw Default | unknown/262k (?%)              |
| mobile:cron:10aede83-鈥?        |        |         |              |                  |                                |
| agent:v88-mobile:main          | direct | 22h ago | k3-256k      | OpenClaw Default | 23k/262k (9%) 路 馃梽锔?72% cached  |
| agent:v88-gpt:general-design-  | direct | 25h ago | gpt-5.6-sol  | OpenAI Codex     | 22k/372k (6%) 路 馃梽锔?46% cached  |
| te鈥?                           |        |         |              |                  |                                |
| agent:main:main                | direct | 25h ago | kimi-k3      | OpenClaw Default | unknown/262k (?%)              |
| agent:v88-                     | direct | 25h ago | kimi-k3      | OpenClaw Default | unknown/262k (?%)              |
| gpt:explicit:gateway-鈥?        |        |         |              |                  |                                |
| agent:v88-                     | cron   | 30h ago | k3-256k      | OpenClaw Default | unknown/262k (?%)              |
| mobile:cron:c1ccfffe-鈥?        |        |         |              |                  |                                |
| agent:v88-mobile:explicit:v88- | direct | 4d ago  | k3-256k      | OpenClaw Default | 7.9k/262k (3%) 路 馃梽锔?48% cached |
| o鈥?                            |        |         |              |                  |                                |
| agent:v88-mobile:explicit:v88- | direct | 5d ago  | k3-256k      | OpenClaw Default | 7.4k/262k (3%)                 |
| k鈥?                            |        |         |              |                  |                                |
+--------------------------------+--------+---------+--------------+------------------+--------------------------------+

Health
+------------+-----------+---------------------------------------------------------------------------------------------+
| Item       | Status    | Detail                                                                                      |
+------------+-----------+---------------------------------------------------------------------------------------------+
| Gateway    | reachable | 307ms                                                                                       |
| Event loop | OK        | healthy 路 max 35ms 路 p99 35ms 路 util 0.029 路 cpu 0.028                                      |
| Feishu     | OK        | configured                                                                                  |
+------------+-----------+---------------------------------------------------------------------------------------------+

FAQ: https://docs.openclaw.ai/faq
Troubleshooting: https://docs.openclaw.ai/troubleshooting
Next steps:
  Need to share?      openclaw status --all
  Need to debug live? openclaw logs --follow
  Need to test channels? openclaw status --deep
exit_code=0

## Feishu channel probe
Checking channel status (probe)鈥?Gateway reachable.
- Feishu default: enabled, configured, running, connected, works
- Feishu v88-gpt (V88-GPT): enabled, configured, running, connected, works

Tip: https://docs.openclaw.ai/cli#status adds gateway health probes to status output (requires a reachable gateway).
exit_code=0

## Agent bindings
[
  {
    "id": "main",
    "workspace": "C:\\Users\\admin\\.openclaw\\.openclaw\\workspace",
    "agentDir": "C:\\Users\\admin\\.openclaw\\agents\\main\\agent",
    "bindings": 0,
    "isDefault": true
  },
  {
    "id": "v88-mobile",
    "name": "v88-mobile",
    "workspace": "C:\\Users\\admin\\.openclaw\\workspaces\\v88-mobile",
    "agentDir": "C:\\Users\\admin\\.openclaw\\agents\\v88-mobile\\agent",
    "model": "kimi-coding/k3-256k",
    "bindings": 1,
    "isDefault": false
  },
  {
    "id": "v88-gpt",
    "name": "v88-gpt",
    "identityName": "V88-GPT 榫欒櫨",
    "identityEmoji": "馃",
    "identitySource": "identity",
    "workspace": "C:\\Users\\admin\\.openclaw\\workspaces\\v88-gpt",
    "agentDir": "C:\\Users\\admin\\.openclaw\\agents\\v88-gpt\\agent",
    "model": "openai/gpt-5.6-sol",
    "bindings": 1,
    "isDefault": false
  }
]
exit_code=0

## Available models
Model                                      Input      Ctx         Local Auth  Tags
moonshot/kimi-k3                           text+image 262k        no    yes   default,configured
kimi-coding/k3-256k                        text+image 262k        no    no    configured
moonshot/kimi-k2-thinking                  text       262k        no    yes   
moonshot/kimi-k2-thinking-turbo            text       262k        no    yes   
moonshot/kimi-k2-turbo                     text       256k        no    yes   
moonshot/kimi-k2.5                         text+image 262k        no    yes   
moonshot/kimi-k2.6                         text+image 262k        no    yes   
moonshot/kimi-k2.7-code                    text+image 262k        no    yes   
moonshot-ai/kimi-k3                        text+image 1049k       no    yes   
moonshotai/kimi-k3                         text+image 1049k       no    yes   
exit_code=0
