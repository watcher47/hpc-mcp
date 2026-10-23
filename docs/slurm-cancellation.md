# 按 JobID 正常停止本人作业

当前源码增加原生 MCP 工具 `slurm_cancel_job`。它通过现有 SSH 身份工作，不需要新凭据、额外服务器权限或修改通用命令配置。

调用参数为：

```json
{
  "job_id": "<本人需要停止的明确 JobID>",
  "request_id": "<为此次取消生成并保存的 UUID>",
  "server_alias": "<现有配置别名>"
}
```

已在本机唯一注册的作业可省略 `server_alias`；未注册或跨服务器 JobID 重复时必须明确选择现有别名。支持一个数字 JobID，或一个 `123_4` 形式的数组元素。数组父 JobID 表示该作业的全部元素。拒绝列表、范围、步骤、用户名选择、任意信号、shell 和全部作业取消。

`cancellation_requested` 只表示 `scancel` 已接受正常取消请求；继续用 `slurm_get_job` 确认终态、退出码与日志。会计状态可能保留 `CANCELLED by <uid>`，核对主状态时取首个词并保留原始值；取消验收不要求退出码为非零。作业已离开队列时返回 `not_active`，不能据此宣称已取消。未注册作业可通过既有只读 SSH `sacct` 能力确认状态。

仓库客户端脚本同样只调用 MCP：

```powershell
python scripts/cancel_job.py --job-id ACTUAL_JOB_ID --request-id SAVED_UUID --server EXISTING_ALIAS
```

在含已安装依赖的 Python 环境运行；脚本校验受管安装元数据后连接实际 STDIO 服务。新工具需要连接包含此修复的新发行目录，已连接旧服务的客户端需重载 MCP。当前源码提供 26 个工具；公开稳定版为 v0.1.0，v0.2.0 安装资产尚未发布。本地修复 wheel 由 SHA-256 区分，不表示已发布新的公开版本。

## 身份、安全与审计

专用服务只发送本包内精确匹配的 stdlib helper 和固定参数。远端以 `os.getuid()` 取得 SSH 实际身份，用 `squeue --array --jobs=ID --format=%i|%U|%T` 核实所有返回元素的 UID；字段与数组展开语义见 [squeue 官方说明](https://slurm.schedmd.com/squeue.html)。任何所有权不符、异常响应或输出超限都会阻止取消，包括 root 登录时的他人作业。

所有权核实后只执行 `scancel --user=UID -- ID`，用户过滤在调度器再次约束所有者。删除继承的 `SCANCEL_*`、`SQUEUE_*`、`SLURM_CLUSTERS` 环境选择，固定英文输出；每个子命令限时 10 秒，每路输出最多 64 KiB。正常取消使用 Slurm 默认整作业终止语义，见 [scancel 官方说明](https://slurm.schedmd.com/scancel.html)。通用 `remote_execute` 与 `ssh_execute` 的调度器限制继续生效。

本机 `AppPaths.data_directory/cancellations.sqlite3` 保存请求 UUID、服务器别名、JobID、UTC 时间和结果。先持久化意图再发送，结果含实际 UID、已核实所有者、原状态与错误码，不保存凭据或原始命令输出。同 UUID 重放只返回原结果，换目标报 `CANCELLATION_REQUEST_CONFLICT`。

## 失败恢复

- `JOB_NOT_OWNED`：没有发送取消，不改身份或权限来重试。
- `SLURM_QUERY_FAILED` / `SLURM_QUERY_INVALID`：没有发送取消；先诊断只读队列查询。
- `CANCELLATION_AUDIT_FAILED`：发送前审计不可写，没有发送取消；修复本机存储问题后可用同 UUID 恢复。
- `CANCELLATION_UNCERTAIN`：可能已发送；保留 UUID、审计意图与结果，先查队列/会计。原 UUID 被持久化隔离，不会再次发送。只有确认作业仍活动且原取消未生效后，才能在原有明确取消授权范围内创建新的请求，不自动盲重试。
- 请求被拒绝、已结束或已经接受时，同 UUID 重放也不产生新的取消操作。若本机进程在发送中崩溃，未完成的审计意图同样按结果不明处理。

## 缺陷来源与测试遗漏

`a646546db111c52188e8ff07f07a33608f25f468`（2026-09-15 02:12:37 +08:00，`fix: harden remote execution and project roots`）把公开 `remote_execute` 限制为无参数 `pwd`。`24fb5ec8c202e31d2eba98edf3e94d08f070f90b`（2026-09-20 17:15:46 +08:00，`feat: add controlled general SSH and SFTP tools`）为通用 SSH 添加包括 `scancel`、`scontrol` 的硬禁用列表。`8da2c4ad` 的后续注释说明调度器修改应由专用服务处理。

这些提交没有更详细的提交正文；从提交名、实现和注释可确认是通用执行边界的收紧。遗漏是提交已有专用服务，正常取消却从未实现，实际 25-tool schema 也没有取消入口。原测试覆盖通用 `scancel` 拒绝及提交/查询，没有覆盖停止作业的完整生命周期。`sstat` / `scontrol` 的只读诊断路由是另一个能力范围，本修复不扩展它们。

新增回归覆盖本人/他人/root 身份、单 ID/数组、参数注入拒绝、查询异常、输出/超时边界、环境过滤、持久审计、同 UUID 去重、冲突、发送结果不明与真实 MCP schema。真实验收仅使用 `tests/fixtures/cpu_smoke/slurm/cancel_smoke.sbatch` 新建的短时 CPU 作业；保留实际 JobID、返回、终态、退出码与日志作为独立证据。
