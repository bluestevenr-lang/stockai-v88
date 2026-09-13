# V88 Windows 同步与启动（2026-09-13）

Windows 继续使用同一份 StockAI 界面和 ai-daily-report-v2 后台代码。

1. 关闭正在运行的 V88 服务窗口。
2. 双击桌面“同步V88”。新版入口为 `StockAI\win\同步V88.bat`。
3. 出现 `UPDATE VERIFIED` 后，双击“V88”或 `StockAI\win\启动V88.bat`。

首次更新会安装新增依赖，需要等待；后续依赖未变化会跳过安装。
同步不自动提交或推送任何本地文件，不调用模型、不发送消息、不启动后台推送任务。
Git 冲突、缺依赖、数据包校验失败都会停止，并显示 `UPDATE FAILED`。

程序优先使用 `%USERPROFILE%\v88env\Scripts\python.exe`，否则使用 `py -3`。
要求 Python 3.12+、Git for Windows，以及私有仓库的 GitHub 登录权限。
保留现有 `.env`、代理设置、持仓、Astra 录单和历史数据库；不再要求 Tushare/Kimi 密钥。

本次附带只读页面数据快照，源时间不会改成下载时间。首次导入前备份原文件至
`ai-daily-report-v2\.v88-work\windows-backups`；同一包不重复覆盖更新后的数据。
大型行情/研究历史数据库不在 Git 发行包内，继续保留 Windows 已有历史；缺失会明确显示，
个股深度分析按原免费数据路径补取。快照有效期仍由中央规则校验，更新程序不授予评级。

桌面旧快捷方式如果没有出现 `UPDATE VERIFIED`，直接运行新版 `StockAI\win\启动V88.bat`，
它同样会完成依赖和数据更新。同步失败请保留报错内容，不要强制覆盖本地分支。
