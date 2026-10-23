# hpc-mcp

让 Codex 在 Windows 电脑上通过 SSH/SFTP 管理已授权的 HPC 项目与 Slurm 作业，并在结束后把结果交回原任务。运行方式是本地 STDIO；集群不需要安装 MCP 服务。

## 交给 Codex 安装

> 帮我安装 https://github.com/watcher47/hpc-mcp ，先阅读 AGENTS.md 和 docs/install-with-codex.md，按发布版流程执行。询问我自己的 SSH 用户名和允许使用的本地项目目录；密码只在本机遮蔽输入窗口填写。

已发布稳定版为 [v0.1.0](https://github.com/watcher47/hpc-mcp/releases/tag/v0.1.0)，安装时使用[该版本的 Codex 安装契约](https://github.com/watcher47/hpc-mcp/blob/v0.1.0/docs/install-with-codex.md)，验证 SHA-256，再用隔离环境安装。项目不承诺 PyPI 包可用。

当前 `main` 是包含后续修复的 `0.2.0` 源码，尚未发布对应安装资产。当前能力和[安装契约](docs/install-with-codex.md)随源码更新；源码提交不表示受管安装已升级。

`10.29.114.128` 是明确的站点预设，本站用户无需重新设置 IP；其他集群可以覆盖。它不是 SSH 运行时通用默认值。必须使用你自己的 SSH 用户名，不能根据 Windows 用户或仓库作者推断。密码只保存在用户本机 `servers.json`，不进入聊天、命令参数、环境变量或日志；本配置流程不使用钥匙串。新建窗口不会覆盖已有配置。

要求 Windows、Python 3.11+、带 tkinter 的官方完整 Python、`uv`、Codex CLI/Desktop。集群需要 SSH/SFTP、Python 3、`sbatch`、`squeue` 和 `sacct`；远程文本搜索需要 `rg`。Windows 是本版安装目标；其他平台没有完整部署支持承诺。GUI 取消或不可用意味着配置未完成。安装不会自动提交 Slurm 作业。

## 能力与边界

当前源码的 26 个 MCP 工具覆盖连接检查、项目映射/同步、远程文件、受限 SSH 命令、GPU 清单与选卡、Slurm 预览/提交/取消/查询/日志/结果与监控。凭据不出现在工具参数或返回值中。项目路径限制、命令策略、TOFU 主机信任与请求 UUID 提交防重保持生效。授权目录和策略见 [配置说明](docs/configuration.md)，按明确 JobID 停止本人作业见 [取消接口](docs/slurm-cancellation.md)。

可按物理 GPU index、UUID 或单卡 GRES 精确选卡，也可自动选择指定数量。启用该功能的提交会复查物理占用，并在业务启动前核验实际分配；同一 UUID 重试恢复原选择。首版要求直连计算节点、单节点 Bash 和每卡唯一 GRES，详见 [GPU 选择](docs/gpu-selection.md)。

独立 `hpc-agent` 每约 45–55 秒查询作业，等待轮询不消耗模型 token；后续分析回合会消耗。它通过原生队列通知原任务，队列 ACK 不证明结果已收集。原生协议不提供全局绝对 exactly-once 保证；证据不足时保留不确定状态，详见 [队列交接](docs/native-queue-handoff.md) 与 [诊断](docs/agent-delivery-diagnostics.md)。

[Windows 安装、升级与回退](docs/windows-setup.md)保留用户配置、数据库、监控记录和旧发布环境。真实集群/桌面验收需要独立授权，本地测试不能替代它。

## 开发验证

```powershell
uv sync --extra test --locked
uv run pytest -m "not real_cluster" --cov=hpc_mcp --cov-report=term-missing --cov-fail-under=85
uv build
```

当前源码版本 `0.2.0`，许可证 [MIT](LICENSE)。见 [变更说明](docs/release-v0.2.0.md)。

复用已安装服务完成最小 CPU 作业提交、监控和结果回收，见 [无人值守 CPU 流程与恢复说明](docs/cpu-smoke-workflow.md)。真实提交需要明确授权；该流程保留 UUID 和验证证据，不自动提交科研仿真。
