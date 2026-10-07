# v0.1.0

公开仓库：[watcher47/hpc-mcp](https://github.com/watcher47/hpc-mcp)。[版本发布页](https://github.com/watcher47/hpc-mcp/releases/tag/v0.1.0) 提供 wheel、源码归档、Windows 安装 ZIP 和 `SHA256SUMS`。准备阶段资产可能尚不存在，安装前必须确认实际发布并验证下载文件。包名 `hpc-codex-mcp`，版本 `0.1.0`，MIT；不承诺 PyPI 分发。

Windows ZIP 包含 wheel、本地 masked 配置 helper、安装指南与许可证。helper 作为外部脚本运行，不是 wheel 中的 CLI；源码分发包含脚本。运行时仍提供 24 MCP 工具，SSH/Slurm 安全边界、原生队列交接和安装回退逻辑保持原样。

官方完整 Python 的 tkinter 是新建配置窗口的必要条件。Codex app-server 为实验协议，需对当前安装版本执行 `doctor`/schema 检查；不能假设任意版本兼容。当前本地发布检查环境可包含 Codex `0.162.0-alpha.2`，这不构成其他版本承诺。桌面重载、SSH 账号/权限、真实作业及用户实际 GUI 操作均需在目标环境验证。本地自动化测试不等于新用户真实部署验收。

轮询不调用模型；结果分析回合消耗 token。队列确认不保证结果收集完成，协议不提供全局绝对 exactly-once。升级和卸载保留用户配置、数据库、任务监控和旧 wheel；详见 [回退说明](windows-setup.md)。
