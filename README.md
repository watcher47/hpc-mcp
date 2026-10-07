# hpc-mcp

让 Codex 在 Windows 电脑上通过 SSH/SFTP 管理已授权的 HPC 项目与 Slurm 作业，并在结束后把结果交回原任务。运行方式是本地 STDIO；集群不需要安装 MCP 服务。

## 交给 Codex 安装

> 帮我安装 https://github.com/watcher47/hpc-mcp ，先阅读 AGENTS.md 和 docs/install-with-codex.md，按发布版流程执行。询问我自己的 SSH 用户名和允许使用的本地项目目录；密码只在本机遮蔽输入窗口填写。

完整流程见 [Codex 安装契约](docs/install-with-codex.md)。推荐下载 [v0.1.0 官方发布资产](https://github.com/watcher47/hpc-mcp/releases/tag/v0.1.0)，验证 SHA-256，再用隔离环境安装。项目不承诺 PyPI 包可用。

`10.29.114.128` 是明确的站点预设，本站用户无需重新设置 IP；其他集群可以覆盖。它不是 SSH 运行时通用默认值。必须使用你自己的 SSH 用户名，不能根据 Windows 用户或仓库作者推断。密码只保存在用户本机 `servers.json`，不进入聊天、命令参数、环境变量或日志；本配置流程不使用钥匙串。新建窗口不会覆盖已有配置。

要求 Windows、Python 3.11+、带 tkinter 的官方完整 Python、`uv`、Codex CLI/Desktop。集群需要 SSH/SFTP、Python 3、`sbatch`、`squeue` 和 `sacct`；远程文本搜索需要 `rg`。Windows 是本版安装目标；其他平台没有完整部署支持承诺。GUI 取消或不可用意味着配置未完成。安装不会自动提交 Slurm 作业。

## 能力与边界

24 个 MCP 工具覆盖连接检查、项目映射/同步、远程文件、受限 SSH 命令、Slurm 预览/提交/查询/日志/结果与监控。凭据不出现在工具参数或返回值中。项目路径限制、命令策略、TOFU 主机信任与请求 UUID 提交防重保持生效。授权目录和策略见 [配置说明](docs/configuration.md)。

独立 `hpc-agent` 每约 45–55 秒查询作业，等待轮询不消耗模型 token；后续分析回合会消耗。它通过原生队列通知原任务，队列 ACK 不证明结果已收集。原生协议不提供全局绝对 exactly-once 保证；证据不足时保留不确定状态，详见 [队列交接](docs/native-queue-handoff.md) 与 [诊断](docs/agent-delivery-diagnostics.md)。

[Windows 安装、升级与回退](docs/windows-setup.md)保留用户配置、数据库、监控记录和旧发布环境。真实集群/桌面验收需要独立授权，本地测试不能替代它。

## 开发验证

```powershell
uv sync --extra test --locked
uv run pytest -m "not real_cluster" --cov=hpc_mcp --cov-report=term-missing --cov-fail-under=85
uv build
```

版本 `0.1.0`，许可证 [MIT](LICENSE)。见 [发布说明](docs/release-v0.1.0.md)。
