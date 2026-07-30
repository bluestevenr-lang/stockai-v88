# V88·Win 常驻主机 403 排查记录（2026-07-30）

> 现场：`DESKTOP-4H6ES39`，`claude remote-control` 的子会话连上后 ~2m20s 必掉 403。
> 本文只记**已用实验证伪的假设**和**证据链**，避免下次重复走同一批弯路。
> 日志原件在 `win\logs\`（该目录已整体 gitignore，见文末「安全副产物」）：
> `remote_20260730.log`（主）、`rc_debug_20260730.log`（bridge）、
> `rc_debug_20260730-cse_*.log`（子会话，**403 的真相在这里**）、
> `rc_debug_20260730-newtoken*.log` / `-retest*.log`（换令牌后的两次交互验证）。

> ## ⚠️ 勘误（本文件已被推翻过两次，两次都记在这里）
>
> **勘误 1（`330fb3f` → `57d11f9`）**：曾断言「根因是令牌实例，换令牌即通，已解决」。
> 被 19:00:21 推翻 —— 用**同一个新令牌**，计划任务那轮（epoch=7）照样 403 而死。
> 错误原因：只跑了一次交互测试就下结论，样本不足。
>
> **勘误 2（`57d11f9` → 本次）**：曾断言「变量是登录令牌类型，S4U 不携带完整凭据材料，
> 改成 `LogonType=Password` 应可修」。**被 19:25 的验收推翻** —— 任务已实际改为
> `LogonType=Password`（已核验），epoch=10 死法与 S4U 完全一样：
> `Bootstrap 403` / `org fast mode 403` / `SSETransport 403(permanent)` /
> `worker 403 ×14`、心跳 0。错误原因：把「唯一还没排除的差异」当成了原因，
> 而 S4U 只是相关、不是因果。**这一步让你白改了一次任务口令，抱歉。**
>
> **勘误 3（本次）**：曾把「任务计划进程没有交互会话 / 桌面」列为待验证假设。
> **已验证不成立** —— 任务改成 `LogonType=InteractiveToken` + 登录时触发后，
> bridge 确实跑在 `SessionId=1`（已核验），epoch=11 照样全线 403 而死。
>
> 现在成立的只是一条**相关性**（下节），机制仍未查明。
> **已经连错三次，所以下文严格区分「实测事实」与「未验证假设」，不再给第四个理论。**

## 结论先行

**三种 403，完全不同的东西，不要混为一谈：**

| | 注册层 403 | 会话层 403 | 上游抖动 |
|---|---|---|---|
| 报文 | `Registration/Poll: Access denied (403): Request not allowed. Check your organization permissions.` | `RemoteIO: transport closed permanently (code 403)` | 夹杂大量 502 |
| 打在哪 | `work/poll`、`/v1/environments/bridge` | `/v1/code/sessions/*/worker`（+`/events/stream`） | 各端点 |
| 表现 | bridge 直接致命退出 | bridge 正常，**子会话** ~2m20s 掉 | 自行重试后恢复 |
| 结论 | **上游抖动，已自愈**（16:47 起 3 分钟） | **未解决**，但已定位到启动方式，见下 | CLI 自己扛 |

### 目前成立的结论：与令牌无关；「被任务计划启动」必死，交互启动必通（机制未明）

**同一份令牌、同一台机器、同一个 `env_01A99j…`、同一个代理、同一份 `.bat`、
同一个 `USERPROFILE`，唯一差异是「谁启动的」，结果就分成两组 —— 10 轮零例外：**

| epoch | 时刻 | 启动方式 | 令牌 | Bootstrap / org fast mode | 心跳 | worker 403 | 结局 |
|---|---|---|---|---|---|---|---|
| 1,2,3 | 15:45–16:51 | 计划任务（S4U） | 旧 | 403 | **0** | 19~113 | 死 |
| 4,5 | 18:12,18:22 | 计划任务（S4U） | 旧 | 403 | **0** | 多 | 死（2m18s / 2m19s）|
| **6** | **18:52** | **交互 spawn** | **新** | — | **16** | **0** | **活 296s**（人为终止）|
| 7 | 18:58 | 计划任务（S4U） | **新** | **403** | **0** | 多 | **死（138s）** |
| **8** | **19:04** | **交互 spawn** | **新** | **ok** | **7** | **0** | **活 160s**（人为终止）|
| **9** | **19:07** | **交互跑同一个 .bat**（`CLAUDE*` 变量已剥净） | **新** | **ok** | **13** | **0** | **活 276s** |
| 10 | 19:25 | 计划任务（**已改 `LogonType=Password`**） | 新 | **403** | **0** | 14 | **死** |
| 11 | 19:35 | 计划任务（**已改 `InteractiveToken`，`SessionId=1` 已核验**） | 新 | **403** | **0** | 多 | **死** |
| **12** | **19:37** | **交互 spawn**（决胜局，紧跟 epoch 11 失败之后 1 分钟） | **新** | **ok** | **6** | **0** | **活**（过线）|

**汇总：任务计划启动全死（`S4U` / `Password` / `InteractiveToken` 三种登录方式都试过），
交互 shell 启动 4/4 全通。**

**epoch 11→12 是最干净的一组对照**：相隔 1 分钟、同一台机器、同一 `SessionId=1`、
同一份令牌、同一 session id —— 计划任务那个死，交互那个活。
这同时排掉了「那三次成功只是 18:52–19:12 的上游好窗口」的怀疑。

**判据：`Heartbeat sent` 是会话健康的铁证。**
死的那几轮 `hb=0`（注册后 1 秒就全线 403，从没活到发心跳）；
活的那几轮 `w403=0` 且心跳每 20 秒一次不断。**没有任何一轮是中间态。**

**epoch 7 是最关键的一轮**：它用的是新令牌，却和旧令牌时代死得一模一样 ——
一句话否掉「换令牌即通」。
**epoch 9 是第二关键的一轮**：它跑的是**同一个 `遥控常驻V88.bat`**，
且我事先把 `CLAUDECODE` / `CLAUDE_CODE_CHILD_SESSION` / `CLAUDE_CODE_ENTRYPOINT` /
`CLAUDE_CODE_SESSION_ID` / `CLAUDE_PID` 全部删掉，只是由交互 shell 启动 —— 通了。
**这同时否掉了「bat 内容有问题」和「我的测试因继承 Claude Code 环境变量而作弊」。**

### 已排除：登录类型不是原因，环境变量也不是

**登录类型 / 会话号不是原因** —— 三种登录方式都实测过，都死：

```
LogonType = S4U              (epoch 1-5, 7)  -> 死
LogonType = Password         (epoch 10)      -> 死   ★推翻 S4U 假设★
LogonType = InteractiveToken (epoch 11)      -> 死   ★推翻「无交互会话」假设★
   ↑ 这一轮 bridge 实测跑在 SessionId=1，与成功轮完全一样，仍然 403
```

**环境变量不是原因** —— 用一次性计划任务 dump 了任务计划进程的完整环境
（`schtasks /create` 跑一个只做 `set > file` 的 .bat），与成功轮的交互 shell 逐项比：

- **凭据相关路径完全一致**：`USERPROFILE=C:\Users\admin`、
  `APPDATA` / `LOCALAPPDATA` / `HOMEPATH` / `HOMEDRIVE` / `USERNAME` 两边相同。
- 只在交互侧存在的变量，逐个都不成立：`CLAUDE*` 五个
  （**epoch=9 已剥净后仍通，证伪**）、`HTTP_PROXY`/`HTTPS_PROXY`/`GIT_*`
  （**bat 自己就会设，不构成差异**）、`NO_COLOR`/`PYTHONIOENCODING`/
  `PSEXECUTIONPOLICYPREFERENCE`/`NODEFAULTCURRENTDIRECTORYINEXEPATH`/
  `COREPACK_ENABLE_AUTO_PIN`（与鉴权无关）。
- 不存在 `C:\Windows\System32\config\systemprofile\.claude`；
  用户级 / 机器级均无 `ANTHROPIC_*` / `CLAUDE_*`。

**变量值也不是原因** —— 第二轮 dump 改用 `InteractiveToken` 主体（与真实任务同主体），
与交互 shell 做**值级**逐项比对，全部 60+ 个变量里只有两个不同：

```
PATHEXT       交互侧多一个 .CPL
PSMODULEPATH  交互侧多了 用户 Documents\WindowsPowerShell\Modules
```

两者与鉴权毫无关系。**至此环境这条线彻底排除。**

**剩下唯一还没查的可测差异：父进程链。**

```
通 (4/4)：explorer.exe -> powershell -> claude(我的会话) -> powershell -> Start-Process -> claude.exe
死 (全部)：svchost.exe(Schedule) -> cmd.exe -> claude.exe
```

**这是观察，不是结论** —— 我没有验证父进程链为什么会影响 `/v1/code/sessions/*/worker`
的鉴权结果。但它给出了一个**可以照抄的方向**：让启动者变成 `explorer.exe`
（登录时由「启动」文件夹 / `HKCU\...\Run` 拉起），而不是任务计划。

一条仍然重要的形态证据：失败轮里 **bridge 层 `work/poll` 返回 200，子会话层全线 403**
（`Bootstrap` / `org fast mode` / `claudeai-mcp` / `worker` / 1P 遥测一起 403）。
即**同一进程树内两条鉴权路径表现不同** —— 符合「子会话进程取不到可用凭据」，
**不符合**「账号 / 设备 / 平台被服务端封禁」。

## 下一步（**均未执行**，需要你选）

**A. 弃用任务计划，改由 explorer 在登录时拉起（唯一还没试过的启动方式）**
把启动器放进「启动」文件夹或写 `HKCU\Software\Microsoft\Windows\CurrentVersion\Run`。
这两者都由 **`explorer.exe`** 在登录时拉起 —— 正是 4/4 成功轮的那条父进程链。
配合你要开的 netplwiz 自动登录，闭环是：

```
断电恢复 / 开机 -> 自动登录(netplwiz) -> explorer 启动
              -> Run 键拉起 遥控常驻V88.bat -> bridge 跑在会话 1
              -> 随后锁屏(rundll32 user32.dll,LockWorkStation)
```

- **这不是新理论**，是照抄已实测可用的条件（换掉唯一还剩的那个差异：启动者）。
- 代价：需要保持登录态（自动登录 + 锁屏来兜安全）。
- **锁屏必须在 bat 之前或与之并列**，不能追加成任务计划的第二个操作 ——
  bat 是死循环永不返回，顺序执行的第二个操作永远等不到。
- **仍未验证的点**：bridge 在**锁屏状态下**能否持续存活。必须单独测，
  别和自动登录一起上（今天已经吃过「一次改两个变量」的亏）。

**B. 直接发工单**
本地能查的都查完了：令牌、scope、指针、环境 id、代理、CLI 版本、
**登录类型（三种）**、**环境变量（名与值）**、配置路径 —— 全部排除。
文末证据清单已按端点 / UTC 时刻 / 账号组织标识备好。

**建议 A 和 B 并行**：A 有实测依据、成本低；B 不依赖 A 的结果。
无论 A 成不成，`/v1/code/sessions/*/worker` 对同一份令牌因启动方式不同而 403，
本身就值得让官方看一眼。

**当前状态**：`V88-遥控常驻` 已 `schtasks /end` 停掉；`LogonType` 现为
`InteractiveToken`、触发器为「登录时」（本次为验收所改，**已证明无效**）。
它下次登录会自己起来并继续空转刷日志 —— 若不马上做 A，建议先
`schtasks /change /tn "V88-遥控常驻" /disable`。

遥控当前**可用**：交互 shell 起的 bridge（见「排障动作清单」），已 4/4 验证。

**当前状态：`V88-遥控常驻` 已被我 `schtasks /end` 停掉，且刻意没有重新 `/run`。**
理由：它在 S4U 下每 30 秒拉起一个必死的会话，纯粹刷日志、毫无产出。
需要遥控时，先用交互方式手工起（见「排障动作清单」），那条路径已验证可用。

## 验收判据（缺一不可）

1. 子会话日志里 `Heartbeat sent` **> 0**（死的轮次恒为 0）
2. `worker.*returned 403` 与 `PUT worker (init) returned 403` **计数 0**
3. bridge 存活 **> 140s**（旧死法的固定寿命 ~2m20s = 10 次退避重试跑完）

## 时间线（本地时间 UTC+8）

### 第零阶段：exit 9009（另一个病，已在前序提交治好）

```
12:17-13:19  remote-control 反复 exit 9009，30s 一轮空转
             根因：.bat 命令行里的中文被 cmd 截断 + 任务计划下 PATH 拿不到 claude
             已治：命令行全 ASCII + claude 绝对路径解析（见 win\遥控常驻V88.bat 注释）
```

### 第一阶段：会话层 403 开始（计划任务 / 旧令牌）

```
15:36:31  Session failed: RemoteIO 403   cse_01FzZb9jm4f6KdLJxuoVzTRB
15:48:14  Session failed: RemoteIO 403   cse_01Go1U1CpiRpC41G4C4neV1F
16:37:55  Session failed: RemoteIO 403   cse_01XH2nqGPQScP9i1u6Jn2Lxh
16:41:05  Session failed: RemoteIO 403   cse_01CHMhwfVquweUstEfaY4Q3r
```

### 第二阶段：注册层 403（3 分钟，已自愈，与主线无关）

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
=> 上游鉴权抖动。bat 的 30s 重试循环扛过去了，不需要人工干预。**

### 第三阶段：受控复现（证明与环境/指针无关）

```
18:11:53  手动 kill bridge         -> 18:12:34 注册成功,同一个 env(第 2 次证明 env 没坏)
18:12:38  捞回旧会话 cse_01Go1U1   -> 18:14:55 RemoteIO 403 (2m18s)
18:15:52  指针只删 sessionId 字段  -> 18:16:14 注册成 *新* env_01EUaFsv6dnXhYcsKHUnx1r9
18:16:23  全新 env 的全新会话       -> 18:18:44 RemoteIO 403 (2m21s)   <- 换环境无效!
18:21:49  用备份原样还原指针        -> 18:22:28 注册回 env_01A99j,复用恢复
18:22:28  又捞回旧会话             -> 18:24:51 RemoteIO 403 (2m19s)
```

旧死法的完整形态（`rc_debug_20260730-cse_01Go1U1*.log`，UTC 时间）：

```
10:22:37Z  CCRClient: GET /v1/code/sessions/cse_01Go1U1.../worker returned 403 (attempt 1/10)
   …       PUT worker (init) returned 403   ×9 次交织
10:24:43Z  GET ... returned 403 (attempt 10/10)
10:24:43Z  CCRClient: GET retries exhausted
10:24:51Z  CCRClient: 10 consecutive auth failures with a valid-looking token
                    — server-side auth unrecoverable, exiting
```

「valid-looking token」= 令牌**格式与有效期都正常**。事后看这句判词是准确的：
令牌确实没问题，问题在**这个进程拿不到能用的凭据**。

### 第四阶段：换令牌 + 四轮对照（本次，定位到启动方式）

```
18:48:03  /login 换新令牌（.credentials.json 重写；旧令牌当时【尚未过期】）
18:51:49  schtasks /end + kill 旧 bridge(PID 4048，18:22 起，握旧令牌) -> 清场
18:52:06  【交互】起 PID 21368，带 Clash 代理，独立 debug 通道
18:52:15    registered worker epoch=6 attempt=1；Capacity 0/32 -> 1/32
18:53:17    502 抖动 -> 18:53:22 Reconnected after 4s（自愈）
18:57:38    心跳 16 次、worker 403 = 0、存活 296s  => 通
18:57:45  kill 21368；18:57:48 schtasks /run 恢复计划任务
18:57:52  【S4U】bridge PID 2916 起；18:58:03 registered worker epoch=7 attempt=1
18:58:04    org fast mode 403 / claudeai-mcp 403 / Bootstrap 403 / SSETransport 403(permanent)
            / worker GET 403 (1/10) —— 注册后 1 秒全线 403，全程 0 心跳
19:00:21    10 consecutive auth failures -> exiting（138s）  => 新令牌照样死！
19:03:50  停任务 + 清场
19:04:01  【交互】起 PID 20912；19:04:11 epoch=8 attempt=1
19:06:40    心跳 7 次、worker 403 = 0、存活 160s（过 149s 线）  => 通
19:07:13  【交互跑同一个 .bat】，先删掉全部 CLAUDE* 环境变量；bridge PID 6168
19:07:27    epoch=9 attempt=1
19:07:28    Bootstrap Fetch ok / Org fast mode: disabled(extra_usage_disabled)  <- 200!
19:11:53    心跳 13 次、worker 403 = 0、存活 276s  => 通（同一个 bat，只换启动者）
```

### 第五阶段：改 LogonType=Password 后验收 —— 失败

```
19:2x     用户按 schtasks /change /ru /rp * 改任务口令，LogonType: S4U -> Password（已核验）
19:24     kill 掉交互轮（bat 循环 11028 + bridge 6168），清场
19:25:01  schtasks /run；Last Result 267009（正在运行，非登录失败）
19:25:05  bridge PID 14100 起；19:25:11 Registered env_01A99j；
19:25:15    registered worker epoch=10 attempt=1
19:25:17    Bootstrap Fetch failed: 403 / org fast mode 403
19:25:19    SSETransport: HTTP 403 (permanent)
19:25:20    worker returned 403 (attempt 1/10) …一路打到 7/10
19:26:23  我在它跑满 10/10 前手动停掉；本轮 心跳 0、worker 403 = 14
          => Password 与 S4U 死法完全一致，S4U 假设被推翻
19:26+    dump 任务计划进程环境与交互 shell 逐项对比 => 凭据路径一致，无变量可解释
```

### 第六阶段：改 InteractiveToken + 登录时触发 —— 也失败，随后决胜局

```
19:34     任务改 LogonType=Interactive、触发器 BootTrigger -> LogonTrigger（备份见 quarantine）
19:35:04  schtasks /run；19:35:08 bridge PID 20148，实测 SessionId=1
19:35:17    registered worker epoch=11 attempt=1
19:35:19    org fast mode 403 / Bootstrap 403 / SSETransport 403(permanent)
19:35:21    worker 403 (attempt 1/10) … 一路打到 9/10，心跳 0   => 死
          ==> 「无交互会话」假设被推翻：会话号一样，照样死
19:37:29  【决胜局】交互 Start-Process 起 PID 18328（SessionId=1，与上一轮同）
19:37:42    registered worker epoch=12 attempt=1
19:37:44    Org fast mode: disabled / Bootstrap Fetch ok        <- 200
19:40:25    心跳 6+ 次不断、worker 403 = 0、已过 140s 线          => 通
          ==> 相隔 1 分钟、条件全同、仅启动者不同 -> 启动方式差异可复现
19:38     用 InteractiveToken 同主体再 dump 环境做【值级】对比
          => 仅 PATHEXT / PSMODULEPATH 不同，环境这条线彻底排除
```

## 已证伪的假设（别再试）

| 假设 | 证伪方式 | 结果 |
|---|---|---|
| **令牌实例被服务端拒**（本文前一版的结论） | **epoch=7 用新令牌，死法与旧令牌一模一样** | **证伪** |
| 令牌过期 | 旧 `.credentials.json` 有效期到 07-31 02:07，故障时**未过期**，`max` 订阅，推理正常 | 证伪 |
| 令牌 scope 不含会话权限 | 旧令牌 scopes **已含** `user:sessions:claude_code`，且与新令牌**逐项相同**（含 `subscriptionType=max`、长度 108、前缀 `sk-ant-oat01`） | 证伪 |
| `.bat` 内容有问题 | **交互跑同一个 .bat（epoch=9）直接通** | 证伪 |
| 我的测试沾了 Claude Code 注入的凭据 | 进程环境里**只有** `CLAUDECODE=1` 等标记，**无** `ANTHROPIC_API_KEY`/`OAUTH_TOKEN`；且 epoch=9 已把 `CLAUDE*` 全删仍通 | 证伪 |
| Clash 代理（`127.0.0.1:7897`）搞的鬼 | 直连与走代理打 `/v1/messages` 返回**一模一样**的 401 JSON；且成功轮次**带同一个代理** | 证伪（双向） |
| 指针指向已删环境，复用被拒 | 同一个 `env_01A99j…` 当天成功注册 **9 次** | 证伪 |
| 换全新环境就好 | 新 `env_01EUaFsv…` 里的全新会话照样 403 | 证伪 |
| 会话 id 被污染 | 同一个 `cse_01Go1U1…` 在 epoch 6/8/9 健康、在 1-5/7 必死 | 证伪 |
| 有冲突的凭据源 | 无 `ANTHROPIC_*` 环境变量（进程/用户/机器三级都查了），settings 无 `apiKeyHelper` | 证伪 |
| CLI 版本被服务端拒 | `2.1.220`（最新），成功与失败轮次**同一版本** | 证伪（双向） |
| 账号级封锁 | 同账号 Mac 端 worker 全天 200，两个会话持续 Connected | 证伪 |
| 设备/机器/平台被服务端标记 | **同机同令牌同 env，交互启动 3/3 通、S4U 5/5 死** | 证伪 |
| `USERPROFILE` / 配置目录在后台不同 | bat 每轮 `diag USERPROFILE=C:\Users\admin`，两轮相同；无 systemprofile\.claude | 证伪 |
| **S4U 登录令牌不带凭据材料**（勘误 1 后的结论） | **改成 `LogonType=Password` 后 epoch=10 死法完全一样** | **证伪** |
| **任务计划进程没有交互会话 / 桌面**（勘误 2 后的假设） | **改成 `InteractiveToken`、bridge 实测 `SessionId=1`，epoch=11 照样死** | **证伪** |
| 任务计划环境缺了某个关键变量 | dump 完整环境比**变量名**：凭据路径全同，差异项逐个证伪 | 证伪 |
| 任务计划环境某个变量**值**不同 | 用同主体（`InteractiveToken`）再 dump 一次比**值**：只有 `PATHEXT`(.CPL) 与 `PSMODULEPATH` 不同，均与鉴权无关 | 证伪 |
| 那三次成功只是 18:52–19:12 的上游好窗口 | epoch 11（死）与 epoch 12（活）相隔 **1 分钟** | 证伪 |

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
  注意：健康轮次里同一个 `session_01Go1U1` **4 秒就重连上了**，说明白天那 ~2.5 分钟
  「捞不动旧会话」也是 403 的次生症状，不是指针的问题 —— **更没有理由动它**。
- 动它之前先备份到 `win\logs\quarantine\`。

## 安全副产物（本次顺手堵的口子）

`win\logs\` 原先**未被 .gitignore 排除**：`*.log` 只挡住日志本身，
挡不住 `quarantine\` 里的备份。实测 `git add -An win/logs/` 会 stage 到
`quarantine\credentials.*.json.bak` —— 那是一份**含真实 `accessToken` / `refreshToken`
的凭据文件**，而 StockAI 是**公开仓库**。任何人一次 `git add -A` 就会把令牌推上公网。

已在 `.gitignore` 加 `win/logs/` **整目录排除**。
历史已核查：`git log --all -- 'win/logs/*'` 为空，**从未泄露过**。

> 教训：排障时往 `quarantine\` 里备份凭据是对的，但备份目录必须先确认在 ignore 覆盖内。

## 排障动作清单

```powershell
# 三条验收判据
$f = "win\logs\rc_debug_<日期>-cse_*.log"
(Select-String -Path $f -Pattern 'Heartbeat sent').Count                      # 要 > 0
(Select-String -Path $f -Pattern 'worker.*returned 403|PUT worker').Count     # 要 = 0
Get-CimInstance Win32_Process -Filter "Name='claude.exe'" |
  Where-Object { $_.CommandLine -match 'remote-control' } |
  Select-Object ProcessId,CreationDate                                        # 存活要 > 140s

# 受控清场（别只 kill bridge，bat 循环 30s 就把它拉回来）
schtasks /end /tn "V88-遥控常驻"
Stop-Process -Id <bridge pid> -Force

# 【当前可用的遥控方式】交互起（已验证 3/3 通）
$env:https_proxy="http://127.0.0.1:7897"; $env:http_proxy=$env:https_proxy
Start-Process "$env:USERPROFILE\.local\bin\claude.exe" `
  -ArgumentList 'remote-control','--spawn=same-dir','--name','V88-Win-Host','--verbose' `
  -WorkingDirectory "$env:USERPROFILE\Desktop\StockAI"

# 看任务的登录方式（本次定位的关键一项）
([xml](schtasks /query /tn "V88-遥控常驻" /xml ONE)).Task.Principals.Principal
```

## 复发 / 新现场的第一动作

1. 抓子会话日志：有 `Heartbeat sent` 吗？**没有** = 会话层 403，**有** = 别的病。
2. 确认进程的启动方式。**任务计划启动 = 已知必死**（`S4U` / `Password` /
   `InteractiveToken` 三种登录方式都实测过，全死），先用交互方式起来救急。
   一键核对三条判据：`powershell -NoProfile -ExecutionPolicy Bypass -File win\verify-remote-403.ps1`
3. 若注册层 403（`Access denied … organization permissions`）→ **什么都别做**，
   bat 的 30s 重试循环会自愈（本次实测 3 分钟恢复）。
4. **不要**再去换令牌 / 动指针 / 换环境 / 查代理 —— 上表全部证伪过。

## 发工单要用的证据清单（若 `LogonType=Password` 仍 403 再走这条）

**账号 / 环境标识**

| 项 | 值 |
|---|---|
| 账号 | `bluestevener@gmail.com`，accountUuid `490d4a6e-57bb-425d-8592-66c58aebe0b4` |
| 组织 | organizationUuid `20f39d52-2b65-4d81-9d5a-cf66ef77ed75`，`claude_max`，本人 `organizationRole=admin` |
| 速率层 | `default_claude_max_5x`，`billingType=apple_subscription` |
| 机器 | `DESKTOP-4H6ES39`，Windows 11 企业版 10.0.26200 |
| CLI | `2.1.220`（claude.exe，`%USERPROFILE%\.local\bin`） |
| environmentId | `env_01A99jV95Erc3j67yviLrY72`（全天复用成功 9 次） |
| sessionId | `session_01Go1U1CpiRpC41G4C4neV1F` / worker 侧 `cse_01Go1U1CpiRpC41G4C4neV1F` |
| 代理 | Clash 混合端口 `127.0.0.1:7897`（已证明与故障无关） |

**被拒端点**

```
GET  /v1/code/sessions/{cse}/worker                 -> 403（每会话 10 次退避后放弃）
PUT  /v1/code/sessions/{cse}/worker  (init)         -> 403
GET  /v1/code/sessions/{cse}/worker/events/stream   -> 403 (permanent)
Bootstrap / org fast mode status / claudeai-mcp     -> 403
1P event logging（遥测）                             -> 403
——同时——
GET  /v1/environments/bridge, work/poll             -> 200   ★同一进程树内★
POST /v1/messages（本机推理）                        -> 200
```

**失败时刻（UTC，全部 `10 consecutive auth failures … exiting`）**

```
07:48:14  08:37:55  08:41:05  08:53:21  10:14:55  10:18:44  10:24:51  11:00:21
11:25:15 起的那轮（LogonType=Password）打到 attempt 7/10 时被人为终止，形态相同
```

**成功时刻（UTC，同一份令牌、同一 sessionId，交互启动）**

```
10:52:15 (epoch 6, 心跳 16, 存活 296s)
11:04:11 (epoch 8, 心跳 7,  存活 160s)
11:07:27 (epoch 9, 心跳 13, 存活 276s)
```

**关键对照（请对方重点看这三条）**

- **最干净的一组：`11:35:17Z` 死 vs `11:37:42Z` 活，相隔 1 分钟**，
  同一台机器、**同一 `SessionId=1`**、同一份 OAuth 令牌、同一 environmentId、
  同一 sessionId、同一 CLI 2.1.220、同一代理、同一 `USERPROFILE`、
  环境变量值级比对仅差 `PATHEXT`/`PSMODULEPATH`。
  唯一差异：前者由**任务计划**启动，后者由**交互 shell** 启动。
- 三种登录令牌类型（`S4U` / `Password` / `InteractiveToken`）经任务计划启动**全部失败**；
  经交互 shell 启动 **4/4 全部成功**（`10:52:15Z`、`11:04:11Z`、`11:07:27Z`、`11:37:42Z`）。
- **同一进程树内两条鉴权路径表现不同**：bridge 层 `work/poll` 与
  `/v1/environments/bridge` 返回 **200**，而它 spawn 出的子会话进程对
  `/v1/code/sessions/*/worker` 全部 **403**。
- 同账号 Mac 端全天 worker 200，两个会话持续 Connected。

**日志文件**（本地，未入仓）

```
win\logs\rc_debug_20260730-cse_01Go1U1CpiRpC41G4C4neV1F.log      失败轮（w403=113, hb=0）
win\logs\rc_debug_20260730-newtoken-cse_01Go1U1...log            成功轮（w403=0,  hb=16）
win\logs\rc_debug_20260730-retest-cse_01Go1U1...log              成功轮（w403=0,  hb=7）
win\logs\rc_debug_20260730.log / remote_20260730.log             bridge 层与主日志
```

> **注意**：CLI 的 debug 日志**不记录 worker 调用的 `request-id`**（只有遥测那条带
> `x-client-request-id`）。所以给对方的关联键只能是**上面的 UTC 时间戳 + sessionId**。

## 遗留（与 403 无关，另开）

会话能活之后**首次暴露**出下游问题：

```
MCP server "github": HTTP Connection failed: Unauthorized
MCP server "Claude_Code_Remote": HTTP Connection failed: Unauthorized
```

这两个 MCP 连接器需要在换令牌后重新授权。白天日志里查不到这两行，
是因为会话都在 worker 层就死了，从没活到连 MCP 的阶段 —— 属**新暴露的旧问题**，
不影响遥控主链路。
