# v0.2.0 源码变更（待发布）

当前提交更新 GitHub 源码；`v0.2.0` 标签和安装资产尚未发布。已发布稳定版为 `v0.1.0`。

新增物理 GPU 选择：通过 NVIDIA index、UUID、单卡 GRES 类型精确选择，或按数量自动选择。`slurm_gpu_inventory` 提供动态身份及占用清单；`slurm_preview` 和 `slurm_submit` 接受相同的可选 `gpu_selection`。

提交使用已核验的节点和 typed GRES，保存请求选择、物理身份及脚本身份。同 UUID 重试恢复原作业，改变请求会报告冲突。修复多个 GPU GRES 条目只计算第一项的问题。

启用选卡时在业务程序前执行 GPU guard。实际分配不符、外部占用或必要证据缺失会阻止业务启动，保留诊断并沿用原有失败接力。不覆盖 `CUDA_VISIBLE_DEVICES`，不承诺无 cgroup 下的硬隔离。

首版支持直连计算节点、单节点 Bash、每卡唯一 GRES 的整张 NVIDIA GPU。旧调用、CPU 作业、旧提交回执和原有监控流程保持兼容。详细用法见 [GPU 选择](gpu-selection.md)。

源码同时包含原生任务上下文验证、安全接续未发送的完成事件、Windows 本机账户编码修复，以及[按明确 JobID 取消本人作业](slurm-cancellation.md)的接口。当前源码提供 26 个 MCP 工具。

后续发布计划提供 wheel、源码归档、Windows 安装 ZIP 和 `SHA256SUMS`。发布后按 [Codex 安装指南](install-with-codex.md)安装或受管升级，保留用户配置、数据库和旧版本供回滚。`v0.1.0` 发布资产继续保留。

本版本的自动化验证使用合成 GPU/Slurm 证据和离线模拟提交；未通过发布动作自动提交真实集群作业，也未替换作者电脑上正在运行的受管 MCP。实际集群运行应按项目授权执行。
