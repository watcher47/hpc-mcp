# 指定 GPU 提交作业

v0.2.0 提供 `slurm_gpu_inventory`，并为 `slurm_preview`、`slurm_submit` 增加可选的 `gpu_selection`。先查看当前连接节点的实际清单，再选择显卡，不能沿用上一次启动时的 NVIDIA 编号或把 Slurm `IDX` 当成 NVIDIA index。

## 使用方式

完成项目映射和同步后，调用：

```python
slurm_gpu_inventory(project_id)
```

清单包含物理 GPU 的 NVIDIA index、UUID、设备 minor、单卡 GRES 类型和占用判定。缺少身份或进程归属证据时会返回 `unknown`，不能据此宣称显卡空闲。

选择方式如下；`exact` 模式的三个选择字段互斥：

```json
{"mode": "exact", "indices": [0, 2]}
```

```json
{"mode": "exact", "uuids": ["GPU-<实际清单中的UUID>"]}
```

```json
{"mode": "exact", "gres_types": ["<实际清单中的单卡类型>"]}
```

```json
{"mode": "auto", "count": 2}
```

向预览和提交传入同一个选择对象，提交仍使用一个固定请求 UUID：

```python
slurm_preview(project_id, script_path, gpu_selection)
slurm_submit(project_id, script_path, request_id, gpu_selection)
```

精确模式指定的卡不能使用时返回原因，不会自动改用别的卡。自动模式优先选择通过物理检查且 Slurm 未占用的卡，再选择由 Slurm 占用、可以排队的卡。提交前会再次探测；最终选择保存在请求记录中。相同请求 UUID 的重试恢复原作业和原选择，不会重新自动选卡。改变选择要求或脚本内容时须使用一个经用户批准的新请求，不能用重试绕过冲突。

## 脚本要求

启用选卡的首版支持 SSH 直连计算节点上的单节点 Bash 作业。采用节点内每卡唯一的 typed GRES；同型号多卡共用一个类型、MIG/MPS、跨节点作业、数组及不能安全保留语义的脚本形式不在此版精确选卡范围。

当前账号需要能读取 Slurm 节点/GRES 信息、`/etc/slurm/gres.conf`、NVIDIA 身份与进程信息，并通过 `scontrol pidinfo` 确认进程归属。权限或设备可见性限制导致映射证据不完整时返回 `unknown`，不会按显卡数量猜测身份。

推荐从脚本中移除 GPU 数量指令，由 `gpu_selection` 统一声明。保留的 GPU 请求必须与最终单卡 GRES 一致，不能同时声明冲突的 `--gres`、`--gpus` 或 `--gpus-per-*`。其余 CPU、内存、时间和项目内日志路径仍按项目规则填写。

选卡提交不继承可能覆盖调度头的 `SBATCH_*` 环境设置；所需账户、分区及其他资源参数应明确写入脚本。

MCP 不修改原脚本：它生成请求专属提交脚本，保留有效的调度头和业务代码，在业务启动前插入 GPU guard，并向 `sbatch` 传入已核验的节点和 typed GRES。使用明确的项目绝对路径；不要依赖 `$0` 或 `BASH_SOURCE` 寻找上传的脚本位置，因为 Slurm 实际执行的是 spooled copy。

## 状态与失败

- `ready`：选择通过预检，可以提交；不代表作业立即运行。
- `queueable`：资源由 Slurm 管理的作业占用，可以提交排队。
- `busy`：指定资源存在已确认的占用冲突，不能提交。
- `unknown`：必要证据缺失或映射不明确，不能提交。

Slurm 外的计算进程会使对应卡不可选。Slurm 空闲卡需要三次采样均无计算进程、显存使用不超过 512 MiB、利用率不超过 5%。CPU/RAM 瞬时压力不会新增一个物理准入门槛。

作业分配后，guard 会核对实际分配的节点、GRES 与 GPU UUID，再检查占用。冲突退出 `75`，证据不完整退出 `76`，业务程序不会启动。通过返回的 guard 诊断路径和现有项目文件工具读取报告。失败沿用原有诊断接力，不自动换卡、取消或重提。

不会覆盖 Slurm 的 `CUDA_VISIBLE_DEVICES`。双重检查不能阻止检查之后新增的 Slurm 外进程；没有设备 cgroup 时不能将其称为硬隔离。Slurm typed GRES 的配置与调度语义见 [官方说明](https://slurm.schedmd.com/gres.html)。

省略 `gpu_selection` 的原有调用保留原来的脚本提交流程，不会自动获得本页描述的物理选卡检查。新 GPU 工作流应显式传入选择对象；CPU 作业无需传入。
