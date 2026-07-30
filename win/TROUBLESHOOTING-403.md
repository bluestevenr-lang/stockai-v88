# V88·Win 常驻主机 403 排查记录（2026-07-30 · 已解决）

> 现场：`DESKTOP-4H6ES39`，`claude remote-control` 起不来 / 会话连上就掉。
> 本文只记**已用实验证伪的假设**和**证据链**，避免下次重复走同一批弯路。
> 日志原件：`win\logs\remote_20260730.log`（主）、`rc_debug_20260730.log`（bridge）、
> `rc_debug_20260730-cse_*.log`（每个子会话一份，**403 的真相在这里**）、
> `rc_debug_20260730-newtoken.log` + `manual_newtoken_20260730.log`（换令牌后的验证）。

## 结论先行

**两种 403，完全不同的东西，不要混为一谈：**

| | 注册层 403 | 会话层 403 |
|---|---|---|
| 报文 | `Registration/Poll: Access denied (403): Request not allowed. Check your organization permissions.` | `RemoteIO: transport closed permanently (code 403)` |
| 打在哪 | `work/poll`、`/v1/environments/bridge` | `/v1/code/sessions/*/worker`（+`/events/stream`） |
| 表现 | bridge 直接致命退出 | bridge 正常，子会话连上后 **~2m20s** 掉 |
| 结论 | **上游抖动，已自愈**（持续 3 分钟） | **换令牌解决**（18:48 重新登录后消失） |

会话层 403 的判词由 CLI 自己给出，在子会话日志最后一行：

```
CCRClient: 10 consecutive auth failures with a valid-looking token
        — server-side auth unrecoverable, exiting
```

「valid-looking token」= 令牌**格式与有效期都正常**，是服务端拒绝它 —— 事后证明
这句判词完全准确，**问题就在令牌实例本身**，而不在设备、环境或代理。
那个 ~2m20s 的固定寿命 = `CCRClient` 的 10 次退避重试跑完。

### 根因（2026-07-30 22:52 定案）

**旧令牌实例被服务端在 `/v1/code/sessions/*` 这一层单独拒绝，而同一实例在
`/v1/messages` 推理上照常放行。** 重新登录换取新令牌后立即恢复。

决定性对照 —— 新旧令牌除了令牌值本身，**其余属性完全相同**：

| 属性 | 旧令牌（18:48 前） | 新令牌（18:48 后） |
|---|---|---|
| `scopes` | `user:file_upload,user:inference,user:mcp_servers,user:profile,user:sessions:claude_code` | **完全相同** |
| `subscriptionType` | `max` | `max` |
| 长度 / 前缀 | 108 / `sk-ant-oat01` | 108 / `sk-ant-oat01` |
| `expiresAt` | 2026-07-31 02:07:55（**未过期**） | 2026-07-31 02:48:03 |
| worker 层 | **403 ×10 → 自杀** | **attempt=1 一次成功** |

**故可同时排除**：授权范围不足（scopes 一字不差，且本就含 `user:sessions:claude_code`）、
订阅层级、令牌过期、设备被封、环境损坏、代理。
剩下的唯一解释是**该令牌实例的服务端 session 层授权记录失效**
（合理推测：同账号在 Mac 端的登录刷新了 OAuth 授权，令 Win 侧这份令牌的
session 层 grant 作废，而推理端点仍宽容放行 —— **此为推测，未验证**）。

**运维含义：** 症状特征是「推理正常 + 遥控会话 ~2m20s 必掉 + 日志出现
`valid-looking token`」，则**直接 `/login` 换令牌**，不要再去动 pointer、环境或代理。

## 时间线（本地时间 UTC+8）

### 第零阶段：exit 9009（另一个病，已在前序提交治好）

```
12:17-13:19  remote-control 反复 exit 9009，30s 一轮空转
             根因：.bat 命令行里的中文被 cmd 截断 + 任务计划下 PATH 拿不到 claude
             已治：命令行全 ASCII + claude 绝对路径解析（见 win\遥控常驻V88.bat 注释）
```

### 第一阶段：会话层 403 开始

```
15:36:31  Session failed: RemoteIO 403   cse_01FzZb9jm4f6KdLJxuoVzTRB
15:48:14  Session failed: RemoteIO 403   cse_01Go1U1CpiRpC41G4C4neV1F
16:37:55  Session failed: RemoteIO 403   cse_01XH2nqGPQScP9i1u6Jn2Lxh
16:41:05  Session failed: RemoteIO 403   cse_01CHMhwfVquweUstEfaY4Q3r
```

### 第二阶段：注册层 403（3 分钟，已自愈）

```
16:47:15  1P 遥测端点 403          <- 和环境无关的端点也中招,是关键旁证
16:47:16  Poll: Access denied 403  -> bridge 致命退出
16:47:19  ArchiveSession: 403
16:47:21  bat 重试 -> 16:49:24 退出,Registration: Access denied 403
16:49:54  bat 重试 -> 16:50:18 退出,502
16:50:48  bat 重试 -> 16:50:56 注册成功 env_01A99jV95Erc3j67yviLrY72
          之后 Ready 持续 80 分钟,poll 全 200
16:53:21  Session failed: RemoteIO 403   cse_01Go1U1（注册好了，会话层照旧掉）
```

**判据：连遥测端点一起 403 + 当天全程夹杂大量 502 + 3 分钟后同 env 复用成功
=> 上游鉴权抖动，不是指针/环境的问题。bat 的 30s 重试循环扛过去了，不需要人工干预。**

### 第三阶段：受控复现（证明与环境/指针无关）

```
18:11:53  手动 kill bridge         -> 18:12:34 注册成功,同一个 env(第 2 次证明 env 没坏)
18:12:38  捞回旧会话 cse_01Go1U1   -> 18:14:55 RemoteIO 403 (2m18s)
18:15:52  指针只删 sessionId 字段  -> 18:16:14 注册成 *新* env_01EUaFsv6dnXhYcsKHUnx1r9
18:16:23  全新 env 的全新会话       -> 18:18:44 RemoteIO 403 (2m21s)   <- 换环境无效!
18:21:49  用备份原样还原指针        -> 18:22:28 注册回 env_01A99j,复用恢复
18:22:28  又捞回旧会话             -> 18:24:51 RemoteIO 403 (2m19s)
```

旧令牌下 worker 层的完整死法（`rc_debug_20260730-cse_01Go1U1*.log`，UTC 时间）：

```
10:22:37Z  CCRClient: GET /v1/code/sessions/cse_01Go1U1.../worker returned 403 (attempt 1/10)
   …       PUT worker (init) returned 403   ×9 次交织
10:24:43Z  GET ... returned 403 (attempt 10/10)
10:24:43Z  CCRClient: GET retries exhausted
10:24:51Z  CCRClient: 10 consecutive auth failures with a valid-looking token
                    — server-side auth unrecoverable, exiting
```

### 第四阶段：换令牌 + 受控验证（本次，问题消失）

```
18:48:03  /login 换取新令牌（.credentials.json 重写,有效期 -> 07-31 02:48:03）
18:51:49  schtasks /end 停掉常驻循环 + kill 旧 bridge(PID 4048,18:22 起,握的是旧令牌)
          -> 确认 0 个 remote-control 进程、0 个 bat 循环，清场
18:52:06  新进程起（PID 21368，带 Clash 代理 127.0.0.1:7897，独立 debug 通道）
18:52:06  Found prior environment env_01A99jV95Erc3j67yviLrY72 (ageMs≈29.6min) -> 请求复用
18:52:11  Registered, server environmentId=env_01A99jV95Erc3j67yviLrY72   <- 注册层 OK
18:52:15  CCR v2: registered worker epoch=6 attempt=1                     <- ★决定性★
          Capacity 0/32 -> 1/32（白天一直卡 0/32，预建会话首次成功）
18:53:17  502 抖动 -> CLI 自行重试
18:53:22  Reconnected after 4s                                            <- 自愈
18:54:38  存活 153s（> 2m20s 生死线），worker 403 计数 = 0，auth-failure 退出 = 0
```

**验证判据（三条同时成立才算通过）：**

1. `worker.*returned 403` 计数 **0**（旧令牌下每会话必有 10 次）
2. `consecutive auth failures` 计数 **0**
3. 存活 **> 140s**（旧令牌的固定寿命 ~2m20s）

三条全过。另外 `registered worker … attempt=1` 是最强单点证据：
整天卡死的正是这一个请求，现在一次成功。

## 已证伪的假设（别再试）

| 假设 | 证伪方式 | 结果 |
|---|---|---|
| 指针指向已删环境，复用被拒 | 同一个 `env_01A99j…` 当天成功注册 **4 次** | 证伪 |
| 换全新环境就好 | 新 `env_01EUaFsv…` 里的全新会话照样 403 | 证伪 |
| Clash 代理（`127.0.0.1:7897`）搞的鬼 | 直连与走代理打 `/v1/messages`，返回**一模一样**的 401 JSON；且换令牌后**带同一个代理**跑通 | 证伪（双向） |
| 令牌过期 | 旧 `.credentials.json` 有效期到 07-31 02:07（故障时**未过期**），`max` 订阅，且本机推理正常 | 证伪 |
| 令牌 scope 不含会话权限 | 旧令牌 scopes **已含** `user:sessions:claude_code`，且与新令牌**一字不差** | 证伪 |
| 有冲突的凭据源 | 无 `ANTHROPIC_*` 环境变量（进程/用户/机器三级都查了），settings 无 `apiKeyHelper` | 证伪 |
| CLI 版本被服务端拒 | `2.1.220`（最新），换令牌后**同一版本**跑通 | 证伪（双向） |
| 账号级封锁 | 同账号 Mac 端 worker 全天 200，两个会话持续 Connected | 证伪 |
| 设备/机器被服务端标记 | **同一台机器、同一代理、同一 CLI、同一 env，仅换令牌即通** | 证伪 |

## 指针文件的坑（踩过一次）

`~/.claude/projects/C--Users-admin-Desktop-StockAI/bridge-pointer.json`

```json
{"sessionId":"session_01Go1U1...","environmentId":"env_01A99j...","source":"standalone","pid":...,"procStart":"..."}
```

- `environmentId` 是**手机配对用的固定环境 id**，启动时以 `reuseEnvironmentId` 复用。
- **这个文件只能整份替换，不能改字段。** 实测只删掉 `sessionId`（其余字段原样保留），
  bridge 就不再打印 `Found prior environment`，直接注册出一个新 env
  —— 等于把手机侧的配对链接换掉。原样复制回去后立刻恢复复用。
- 那条 `reconnectSession → 400 Session not found` 是已删会话留下的**无害噪音**。
  注意：换令牌后同一个 `session_01Go1U1` **4 秒就重连上了**，说明白天那 ~2.5 分钟的
  「捞不动旧会话」其实也是 403 的次生症状，不是指针的问题 —— **更没有理由动它**。
- 动它之前先备份到 `win\logs\quarantine\`。

## 安全副产物（本次顺手堵的口子）

`win\logs\` 原先**未被 .gitignore 排除**：第 53 行的 `*.log` 只挡住日志本身，
挡不住 `quarantine\` 里的备份。实测 `git add -An win/logs/` 会 stage 到
`quarantine\credentials.*.json.bak` —— 那是一份**含真实 `accessToken` / `refreshToken`
的凭据文件**，而 StockAI 是**公开仓库**。任何人一次 `git add -A` 就会把令牌推上公网。

已在 `.gitignore` 加 `win/logs/` **整目录排除**。
历史已核查：`git log --all -- 'win/logs/*'` 为空，**从未泄露过**。

> 教训：排障时往 `quarantine\` 里备份凭据是对的，但备份目录必须先确认在 ignore 覆盖内。

## 排障动作清单

```powershell
# 看 bridge 当前是不是活的、注册在哪个 env
Get-CimInstance Win32_Process -Filter "Name='claude.exe'" |
  Where-Object { $_.CommandLine -match 'remote-control' } |
  Select-Object ProcessId,CreationDate

# 会话层 403 的真相永远在子会话日志的最后几行
Get-Content win\logs\rc_debug_<YYYYMMDD>-cse_*.log -Tail 20

# 三条验证判据（换令牌后应全为 0 / 存活 >140s）
Select-String -Path win\logs\rc_debug_*.log -Pattern 'worker.*returned 403' | Measure-Object
Select-String -Path win\logs\rc_debug_*.log -Pattern 'consecutive auth failures' | Measure-Object

# 受控清场（别只 kill bridge，bat 循环 30s 就把它拉回来）
schtasks /end /tn "V88-遥控常驻"
Stop-Process -Id <bridge pid> -Force

# 对比新旧令牌属性（定位「是不是令牌实例的问题」）
# 见本文「根因」表：比 scopes / subscriptionType / expiresAt，别只看有没有过期
```

## 复发时的第一动作

1. 抓子会话日志最后 20 行，确认是否 `valid-looking token`。
2. 若是 → **直接 `/login` 换令牌**，然后按上面「三条验证判据」验收。
3. 若注册层 403（`Access denied … organization permissions`）→ **什么都别做**，
   bat 的 30s 重试循环会扛过去（本次实测 3 分钟自愈）。
4. 两者都不是，才考虑发工单。

## 遗留（与 403 无关，另开）

换令牌后会话能活了，于是**首次暴露**出下游问题：

```
MCP server "github": HTTP Connection failed: Unauthorized
MCP server "Claude_Code_Remote": HTTP Connection failed: Unauthorized
```

这两个 MCP 连接器需要在换令牌后重新授权。白天的日志里查不到这两行，
是因为会话都在 worker 层就死了，从没活到连 MCP 的阶段 —— 属**新暴露的旧问题**，
不影响遥控主链路，另行处理。
