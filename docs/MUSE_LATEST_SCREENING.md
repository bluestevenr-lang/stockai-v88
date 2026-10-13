# Muse 最新筛选只读入口

私有仓库 `bluestevenr-lang/v88-daily-report`，分支 `review-exchange`，固定文件 `latest/screening.json`。

GitHub Contents API：

```
GET /repos/bluestevenr-lang/v88-daily-report/contents/latest/screening.json?ref=review-exchange
Accept: application/vnd.github.raw+json
```

沿用核心仓库 Contents 只读权限，不需新增写权限。最新名单读此文件；飞书投递是否成功、最终发送正文和当时采用的裁决仍读原冻结回执。此接口不触发发送、不产生Muse认证、不改变系统评分和止损。

## 内容

- `kind=latest-screening`，`schema_version=1`。
- `snapshot_id`、`snapshot_sha256`：整轮快照身份。sha为规范JSON内容哈希，排除 `snapshot_id`、`snapshot_sha256`、`published_at`；编码同 muse_review.digest。
- `source_run`：云端工作流run id和attempt；首次补发布以原私有运行包的提交号溯源。`source_commit`是生产任务环境提供的Git提交；云端为工作流所在仓库的触发提交，不冒称核心代码或行情来源的提交。
- `source_screened_at`：3A与Astra原计算时间。`screened_at`为二者较晚时间；`published_at`单独表示写入交换区时间，不能替代计算或行情时间。
- `counts`：模块×cn/hk/us计数，零候选明确为0；`astra_cycle_id`为现有Astra轮次。
- `items`仅当前正式3A榜单与Astra研究名单，不额外混入候补或历史持仓。

每项复用飞书草案 `muse_review.item` 的原有结构：`item_id/module/market/code/name/system_state/plan_anchor/facts/item_sha256`。`facts`及其哈希与同一事实的草案一致，不重新评分。为方便只读分析，额外提供如下原值别名：

| 字段 | 含义 |
|---|---|
| `score` | 原统一综合分 |
| `state` | 原系统状态（研究/等待不等于买入） |
| `entry_range` | 原入场带 |
| `first_target` | 3A当前首段结构目标；Astra相对固定计划复核的当前首段目标，缺时明确回退原计划目标 |
| `first_target_basis` | 目标依据；Astra风险倍数情景会显式保留该标签，不冒充结构阻力 |
| `take_profit_range` | 原计划止盈带，不能与首段结构空间混用 |
| `stop_range` | 原止损带 |
| `screened_at` | 原市场计算时间；有保留市场版本时保留该市场原时间 |
| `last/currency/quote_asof` | 原报价、币种及报价时间，不以写入时间替换 |

字段无依据时用null，不能用0、目标情景或当前系统时间填补。3A空间与RR仍在 `facts.opportunity`，完整财务/风险依据沿用 `facts`。

## 发布和故障语义

完整筛选与半小时筛选共用 `autonomous_runtime`：只有本轮strategy_board和astra_cycle两个子任务都成功，且两份原计算时间属于本轮，才发布。筛选失败或退回旧数据时保留上一份成功文件，并在本轮autonomous_status的muse_screening子项标明失败/跳过；不能把旧结果重新盖新筛选时间。

覆盖通过GitHub Contents API的SHA条件更新，冲突时重新读取最新文件再判断，最多3次。任一模块比已发布版本更老都不覆盖。每次成功覆盖保存在该分支Git提交历史中，不删除历史回执/计划。发布子进程45秒预算，失败可见但不阻塞原正常飞书发送。

`latest/screening.json`不是时段草案，没有虚构delivery_id。Muse可据此做最新独立分析；若要进入既有飞书认证，仍须匹配该次草案delivery_id、draft_sha256、item_sha256及截止窗口。不能将新筛选裁决冒充旧飞书版本的认证。


## 2026-10-13 按需验证协议 screening-review-v1

保留三时段正式自动验证；用户说“验证最新V88”时，Muse另读本文件。V88的120秒收集只读取已有裁决，不调用模型、不自动让Muse反复分析。

最新快照新增`review_protocol`、`criteria_version`、`verdict_repository`、`verdict_branch`、`verdict_file`、`archive_template`。完整原快照同时不可变保存于核心review-exchange的`screenings/<snapshot_id>.json`，原published_at不因重试刷新。

Muse只需覆盖写独立私有仓库`bluestevenr-lang/v88-muse-verdicts`、main分支的`screening-verdicts/latest.json`。JSON结构如下（值须从实际快照复制，示例不构成裁决）：

```json
{
  "schema_version": 1, "kind": "latest-screening",
  "snapshot_id": "<本次快照ID>", "snapshot_sha256": "<本次快照完整SHA256>",
  "criteria_version": "muse-shadow-v1",
  "items": [{
    "item_id": "<原item_id>", "module": "3A", "market": "cn",
    "code": "<原代码>", "name": "<原名称>", "system_state": "<原状态>",
    "item_sha256": "<原item_sha256>", "verdict": "pass",
    "reason": "单行，不超过40字", "checked_at": "<真实ISO带时区时间>",
    "evidence_ref": "<可追溯证据，单行不超过500字>"
  }]
}
```

- verdict只允许pass/fail/unverified；同股不同模块分别绑定。买入状态通过=认可买入条件；重点观察通过=认可重点观察，不代表可买。证据不足提交unverified，不能为取得绿标编造依据。
- 实际收集时间不得早于原published_at，且不晚于该轮screened_at后24小时；checked_at必须介于实际发布与接收时间之间。这个24小时是按需回执最长受理期，不表示报价可交易或旧徽章可以认证新事实。
- 收集验证快照/条目哈希、状态、标准、真实Git提交和blob。独立不可变回执写`screening-reviews/<snapshot_id>/<verdict_blob_sha>.json`；当前投影`latest/screening-review.json`。回执保存理由/证据/核查时间/首次received_at/提交号/原快照/accepted_items/review_sha256。无效新裁决不能继续沿用前一张绿标；历史回执不回改。
- 网页只读回执及对应快照；当前snapshot_id、snapshot_sha256和item_sha256全匹配才可展示该次按需认证。新行情、新价带或事实改版后等待本版裁决；旧版保留历史可追溯，不自动套给当前。正式发送历史仍以正式投递回执为准。
- `comparison`列出新增/移出/事实变化/仅来源指纹变化及字段路径，帮助Muse少读重复内容。`provenance_changed`不证明底层来源内容等价，不能据此自动复用通过；需要按该次来源证据作出新裁决。系统不为通过核查反向调整评分。
- 按需回执不修改飞书outbox、不触发重发。下一正式时段仍只收其草案verdict_file上的独立正式裁决，不把latest/screening.json冒充已定版报告。

### 当前投影与网页同步补充（2026-10-14 凌晨）

- 当前裁决投影以实际 observed_at 做并发次序，首次真实 received_at 保留不动。无效新裁决或删除 latest 裁决文件使当前徽章回到未验证，历史回执不变；重新提交同一有效版本仍校验原核查与接收时间，不借恢复操作延长期限。
- 网页逐股使用原始事实 item_sha256＋原筛选时点匹配；显示层状态文字不重新签事实。整份筛选 SHA 和裁决回执 SHA 同时校验。桌面额外合并的私人日历历史不会被误当作股票事实改版。行情或条件确实变化后认证失效，正式已发送版另行冻结展示。
- 最新快照、最新裁决和正式投递台账后台并行只读，复用25秒总预算、不阻塞页面；网络不可用时不沿用当前通过徽章。
