# 本机 hpc-mcp 无人值守 CPU 流程

该流程复用本机已经安装的 hpc-mcp、现有配置、SSH 身份和主机信任，不要求任务直接绑定原生 MCP 工具。Python 客户端通过 `installation.json` 中的 `managed_command` 启动原 STDIO 服务，所有远端操作仍经过它的项目路径、命令和幂等性检查。不要把配置文件内容、密码或其他 MCP 的启动参数打印出来。

## 授权新的 CPU 测试

只有明确授权新的 CPU 测试时，才运行完整入口。先核实已有服务器映射，阅读 `tests/fixtures/cpu_smoke/AGENTS.md`，再把下列占位值替换为本次核实的信息，在仓库目录的原 Codex PowerShell 终端中执行：

```powershell
.\scripts\Invoke-CpuSmokeWorkflow.ps1 `
  -RecordPath '.superpowers/cpu-smoke-RUN_LABEL' `
  -Server 'VERIFIED_ALIAS' -ExpectedHost 'VERIFIED_HOST' `
  -ExpectedHostname 'VERIFIED_HOSTNAME' -ExpectedUser 'AUTHORIZED_SSH_USER' `
  -Partition 'AUTHORIZED_PARTITION' -WaitSeconds 300
```

**记录目录没有提交收据时，这个完整入口会提交一个新作业。** 新运行使用独立记录目录；恢复未知提交结果时必须保留原目录、UUID 和输入，不能为了重试而换目录。已有匹配收据时，入口跳过提交并继续监控和收集。该脚本只运行本仓库 `tests/fixtures/cpu_smoke` 的固定 CPU 测试，不能用于任意科研脚本或资源值。

入口依次执行：

1. 通过 `server_list` 核实唯一 alias、IP 和用户；`server_test_connection` 核实 SSH 与 sbatch/squeue/sacct；`ssh_execute` 分别执行允许的 whoami、hostname 和 sinfo 只读查询。
2. `project_ensure`、`project_status` 核实 fixture 的真实映射；先 `project_list_remote_changes` 再 `project_sync`；发现冲突立即停止。`.hpcignore` 只允许三个输入文件，逐个 `remote_read` 核对 SHA-256。
3. `slurm_preview` 验证固定资源：1 节点、1 任务、1 CPU、128M、2 分钟、无 GPU。UUID 与输入哈希在 `slurm_submit` 前落盘。重试只复用这一个 UUID；有收据时跳过提交。
4. 在原始终端上下文、fixture 目录中执行服务原样返回的 `hpc-agent watch`，再以 `slurm_watch_job` 验证。仅临时把原发布版 Scripts 加入当前进程 PATH，不修改持久设置。缺少真实 CODEX_THREAD_ID/CODEX_SESSION_ID 时停止，绝不补造。
5. `slurm_get_job` 等到终态或等待预算到期。必须是确切主 JobID、`COMPLETED`、`0:0`；失败先保存 bounded stdout/stderr 诊断，不收成功结果、不自动重提。
6. `slurm_list_artifacts` 与精确 `remote_list` 验证预期文件；`remote_download` 下载三份文件到 fixture 的 `collected/<UUID>/`，核对长度与 SHA-256，并验证日志、结果、身份、资源和简单计算一致。已有本地文件只校验，绝不默默覆盖。

## 仅收集已有作业

恢复时使用原记录目录。下面的命令要求已有提交收据，仅执行 `collect`，不调用完整入口或 `submit`：

```powershell
$recordPath = (Resolve-Path '.superpowers/EXISTING_RUN_RECORD').Path
$run = Get-Content -LiteralPath (Join-Path $recordPath 'run.json') -Raw -Encoding UTF8 | ConvertFrom-Json
if (-not $run.receipt.job_id) { throw 'Existing submission receipt is required.' }
$installation = Get-Content -LiteralPath "$env:USERPROFILE\.hpc-codex-mcp\state\installation.json" -Raw -Encoding UTF8 | ConvertFrom-Json
$installedPython = $installation.managed_command[0]
$common = @('--record', $recordPath,
  '--server', $run.intent.server, '--expected-host', $run.intent.host,
  '--expected-hostname', $run.intent.hostname, '--expected-user', $run.intent.user,
  '--partition', $run.intent.partition)
& $installedPython scripts/cpu_smoke_workflow.py collect @common --wait-seconds 0
```

`run.json` 保存 UUID、输入哈希、映射、收据、会计、监控和传输回执；`calls.jsonl` 保存实际 MCP 请求/返回，`tool-schema.json` 保存实时 schema。它们在 ignored `.superpowers` 下；本地输出也被忽略。不要提交配置、数据库、凭据或真实服务器验收记录。

| 情况 | 处理 |
| --- | --- |
| 普通 exec 在进程启动前报 MXC `0x80070020` | 使用受支持的 `require_escalated` 请求和正常审核。拒绝时报告具体拒绝，不能修改安全设置或绕开审核。 |
| 当前任务没有 hpc MCP 原生绑定 | 使用本入口的已安装 STDIO 服务；不能据“已安装”声称原生工具已绑定。 |
| SSH 认证失败或主机指纹变化 | 保留错误并停止；本人认证/主机身份核实不能自动代批。不要创建新凭据、改信任或扩大权限。 |
| COMMAND_BLOCKED | 检查允许的命令参数，不扩大命令策略，不用通用 SSH 绕过提交记录。 |
| 同步冲突 | 停止上传，给出冲突文件供明确处理，不自动覆盖/删除。 |
| slurm_submit 超时、SUBMISSION_UNKNOWN 或本地登记失败 | 保留 request UUID 和输入；恢复同一请求，不制造新 UUID。已知 JobID 时先查询，不因 watch 故障重提。 |
| 作业仍排队/运行或 accounting 为 UNKNOWN | 只重跑 collect，保留原 JobID；等待预算到期不表示失败，也不取消。 |
| 作业失败、超时、非零退出 | 保存日志和会计，解释原因；调整资源或新运行需要对应的用户授权。 |
| 下载完成但回执未落盘、发现无回执的现存文件 | 保留文件并核实远端哈希；入口会停止，不能默默覆盖。 |
| watch预检报CODEX_THREAD_UNAVAILABLE / thread/read `-32600` | 当前终端标识可能属于durable执行任务；不能当作本机原生任务。修复版在持久化前拒绝错误绑定。使用真实local原生任务；旧的从未发送完成事件可在明确授权下，以recover-watch进行有审计、受发送栅栏/租约保护的迁移。任何已发送/未知事件不可重置或重放。 |

## 自动后续处理

电脑需要在线、供电，并具有可用的用户会话和已验证的 agent 启动配置。原生事件只触发原任务授权范围内的后续处理；队列 ACK 不表示结果已收集。必须核验真实事件、目标任务、主作业会计和产物，最终完成还需要该输入自身的原生回合会计。

跨主机执行器不等于本机原生任务。对于错误绑定且从未发送的事件，只能在明确授权后通过 `recover-watch` 迁移到正确的本机任务，并保留审计。已有发送意图、回执、租约或不唯一的事件会被拒绝，不能重置或重放。

普通顺序收集与真正事件触发的自动处理分别记录。流程不会授权新的科研仿真、替换认证、批准安全请求或调整持久审核设置。没有弹窗也不表示已获得额外授权。

正式仿真需要先固定：真实本地项目根及 AGENTS、server alias/用户名、授权科研目标与数据集、入口脚本/参数/随机种子、依赖或已审查运行环境、分区/account/QOS、节点/任务/CPU/内存/时限、GPU 数量及精确选卡策略、输入/预期产物清单和本地保存目录、科学验收指标、重试/费用/资源上限。明确这些后才在该科研项目内走同样映射、差异、同步、预览、UUID 提交、监控和收集步骤。

## 本地验证

```powershell
uv sync --extra test --locked
uv run pytest -m "not real_cluster" --cov=hpc_mcp --cov-report=term-missing --cov-fail-under=85
uv build
```

新增回归覆盖 UUID 恢复、意图变化拒绝、已有收据不重提、列表响应解析、主作业成功/失败会计和下载哈希。真实服务器验收独立记录，不能由这些测试替代。
