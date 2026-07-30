# V88·Win 常驻主机 403 排查记录（2026-07-30）

> 现场：`DESKTOP-4H6ES39`，`claude remote-control` 起不来 / 会话连上就掉。
> 本文只记**已用实验证伪的假设**和**证据链**，避免下次重复走同一批弯路。
> 日志原件：`win\logs\remote_20260730.log`（主）、`rc_debug_20260730.log`（bridge）、
> `rc_debug_20260730-cse_*.log`（每个子会话一份，**403 的真相在这里**）。

## 结论先行

**两种 403，完全不同的东西，不要混为一谈：**

| | 注册层 403 | 会话层 403 |
|---|---|---|
| 报文 | `Registration/Poll: Access denied (403): Request not allowed. Check your organization permissions.` | `RemoteIO: transport closed permanently (code 403)` |
| 打在哪 | `work/poll`、`/v1/environments/bridge` | `/v1/code/sessions/*/worker`（+`/events/stream`） |
| 表现 | bridge 直接致命退出 | bridge 正常，子会话连上后 **~2m20s** 掉 |
| 本次结论 | **上游抖动，已自愈**（持续 3 分钟） | **未解决**，见下 |

会话层 403 的判词由 CLI 自己给出，在子会话日志最后一行：

```
CCRClient: 10 consecutive auth failures with a valid-looking token
        — server-side auth unrecoverable, exiting
```

「valid-looking token」= 令牌**格式与有效期都正常**，是服务端拒绝它。
那个 ~2m20s 的固定寿命 = `CCRClient` 的 10 次退避重试跑完。

## 时间线（本地时间 UTC+8）

### 第一阶段：会话层 403 开始（未解决）

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
```

**判据：连遥测端点一起 403 + 当天全程夹杂大量 502 + 3 分钟后同 env 复用成功
=> 上游鉴权抖动，不是指针/环境的问题。bat 的 30s 重试循环扛过去了，不需要人工干预。**

### 第三阶段：受控复现（本次排查）

```
18:11:53  手动 kill bridge         -> 18:12:34 注册成功,同一个 env(第 2 次证明 env 没坏)
18:12:38  捞回旧会话 cse_01Go1U1   -> 18:14:55 RemoteIO 403 (2m18s)
18:15:52  指针只删 sessionId 字段  -> 18:16:14 注册成 *新* env_01EUaFsv6dnXhYcsKHUnx1r9
18:16:23  全新 env 的全新会话       -> 18:18:44 RemoteIO 403 (2m21s)   <- 换环境无效!
18:21:49  用备份原样还原指针        -> 18:22:28 注册回 env_01A99j,复用恢复
18:22:28  又捞回旧会话             -> 18:24:51 RemoteIO 403 (2m19s)
```

## 已证伪的假设（别再试）

| 假设 | 证伪方式 | 结果 |
|---|---|---|
| 指针指向已删环境，复用被拒 | 同一个 `env_01A99j…` 当天成功注册 **4 次** | 证伪 |
| 换全新环境就好 | 新 `env_01EUaFsv…` 里的全新会话照样 403 | 证伪 |
| Clash 代理（`127.0.0.1:7897`）搞的鬼 | 直连与走代理打 `/v1/messages`，返回**一模一样**的 401 JSON | 转发透明，证伪 |
| 令牌过期 | `.credentials.json` 有效期到 07-31 02:07，`max` 订阅，且**本机 Claude 推理正常** | 证伪 |
| 有冲突的凭据源 | 无 `ANTHROPIC_*` 环境变量（进程/用户/机器三级都查了），settings 无 `apiKeyHelper` | 证伪 |
| CLI 版本被服务端拒 | `claude update` -> 已是最新 `2.1.220` | 证伪 |
| 账号级封锁 | **同账号 Mac 端 worker 全天 200，两个会话持续 Connected** | 证伪 |

排除上述后，故障面收窄到：**本机这份令牌 / 这台设备的注册状态**被服务端在
`/v1/code/sessions/*` 这一层拒绝。注意同一份凭据在本机跑 `/v1/messages` 推理是好的
—— 所以不是凭据整体失效，是**端点范围内的**拒绝。

## 指针文件的坑（踩过一次）

`~/.claude/projects/C--Users-admin-Desktop-StockAI/bridge-pointer.json`

```json
{"sessionId":"session_01Go1U1...","environmentId":"env_01A99j...","source":"standalone","pid":...,"procStart":"..."}
```

- `environmentId` 是**手机配对用的固定环境 id**，启动时以 `reuseEnvironmentId` 复用。
- **这个文件只能整份替换，不能改字段。** 实测只删掉 `sessionId`（其余字段原样保留），
  bridge 就不再打印 `Found prior environment`，直接注册出一个新 env
  —— 等于把手机侧的配对链接换掉。原样复制回去后立刻恢复复用。
- 那条 `reconnectSession → 400 Session not found` 是已删会话留下的**无害噪音**，
  代价只是每次启动多花 ~2.5 分钟去捞一个捞不动的旧会话，别为它动指针。
- 动它之前先备份到 `win\logs\quarantine\`。

## 排障动作清单

```powershell
# 看 bridge 当前是不是活的、注册在哪个 env
Get-CimInstance Win32_Process -Filter "Name='claude.exe'" |
  Where-Object { $_.CommandLine -match 'remote-control' } |
  Select-Object ProcessId,CreationDate

# 会话层 403 的真相永远在子会话日志的最后几行
Get-Content win\logs\rc_debug_<YYYYMMDD>-cse_*.log -Tail 20

# 受控重启:bat 循环会在 30s 后自动拉起,不用手动起
Stop-Process -Id <bridge pid> -Force

# 代理是否透明(两条应返回一模一样的 401 JSON)
# 见本文「已证伪的假设」表，用 Invoke-WebRequest 打 /v1/messages 对照
```

## 未完成

会话层 403 在换令牌后若仍复现，即为**设备/环境被服务端标记**，
需带本文证据链发工单。下一步与结果续记在本文件。
