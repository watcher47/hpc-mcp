# 本机配置

新用户使用 [引导安装](install-with-codex.md) 的本地窗口。窗口只新建配置，拒绝任何已有目标，包括链接；没有 SSH 验证、提交作业或密码命令行参数。密码以明文保存在本机 JSON 中，由文件系统 ACL 限制读取，特殊字符原样保存；它不是加密存储。取消输出 `cancelled`（退出码 2），完成输出 `completed`（0），运行时/GUI/权限失败输出 `incomplete`（1）。创建失败不会发布不完整的配置文件或回退为开放权限；本配置流程不使用钥匙串。

路径优先级：`--config-file`、`HPC_MCP_CONFIG`、平台用户配置目录。Windows 默认 `%LOCALAPPDATA%\hpc-codex-mcp\servers.json`；实际路径以 `resolve_servers_path()` 为准，禁止 Git 项目和 OneDrive。不要读取配置文件到聊天。默认新 profile 为 `default`，站点预设 `10.29.114.128:22`，必须提供自己的 SSH 用户名、非空密码、已存在的绝对本地项目目录。

本站 `/home/<你确认的SSH用户名>/codex-projects` 是窗口显示并要求确认的 home 约定，不是对所有集群的推断。非标准 home 或其他集群必须提供实际 `--remote-root`，窗口可修改。该绝对目录同时设置 `remoteProjectsRoot` 与 `allowedRemotePaths`，本地目录设置 `allowedLocalPaths`。首次项目映射前远程根必须已存在、归该用户所有且不是链接；需要时由管理员或用户通过另行授权的 SSH/SFTP 预建，安装不会自动远程写入。通过 `server_test_connection` 返回的实际 home 核对约定，再以 `project_ensure` 返回映射为准，不自行拼接路径。禁止把文件系统根目录作为授权项目根。

配置沿用 `ClusterConfig` 结构和既有验证。`transportMode` 固定 `exec`；`hostKeyPolicy` 为 `tofu`。第一次认证成功才保存主机指纹，后续变化失败并保留证据。命令允许规则包含受控 `squeue`/`sacct`，拒绝 `rm`/`shutdown`；通用 SSH 执行/上传/下载还受授权目录、既有策略和输出上限约束。项目工具另有项目范围校验。

已有配置应通过 `hpc-setup validate-config` 检查；不要运行新建 helper 覆盖它。对象和数组 profile 格式均支持，密码或私钥认证沿用运行时；代理、交互 shell、MFA、命令模板不受支持并明确拒绝。超时、keepalive 与输出限制见源模型，`maxOutputBytes` 必须为 1 至 1,073,741,824 的整数。不要把配置、数据库或主机信任记录加入 Git。
