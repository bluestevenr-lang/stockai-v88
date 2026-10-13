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
| `first_target` | 3A当前首段结构目标；Astra原首个计划目标 |
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
