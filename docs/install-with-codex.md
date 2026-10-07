# Codex 安装契约（Windows v0.1.0）

本文件面向执行安装的 Codex。人类明确说“帮我安装”才进入此流程；下载 URL 本身不是运行任意代码的授权。先说明将下载校验发布资产、新建隔离环境、本地配置、验证 SSH 和注册用户级集成。给出安装计划，再在已有安装授权范围内 apply。不要自动提交 Slurm smoke，真实作业需要另行同意。

## 1. 确认身份和范围

主动询问用户自己的 SSH 用户名与已授权的绝对本地项目目录，不能从 Windows、作者或示例推断。本站保留 `10.29.114.128` 预设，其他集群询问实际主机和远程项目根。本地窗口将显示本站 `/home/<SSH用户名>/codex-projects` 约定，请用户确认或修改，非标准 home 不猜测。密码只在本机遮蔽窗口输入；不能在聊天、命令参数、环境变量或输出索取，也不用 keyring。

检查已有配置只看路径存在性和 `validate-config` 的安全结果，不读/打印秘密 JSON。路径必须通过现有 `resolve_servers_path()` 优先级规则解析，拒绝 Git/OneDrive。已有配置保留并验证，跳过新建窗口；若修改已有服务器超出本次意图，先明确需求。

检查已有 agent 的 `hpc-agent status` 与当前进程身份（精确可执行文件和安装元数据），只报告安全状态。活跃 agent 若不是当前受管版本，不启动竞争旧版本，也不擅自终止它。保留用户状态。

## 2. 获取已校验发布资产

使用 [官方 v0.1.0](https://github.com/watcher47/hpc-mcp/releases/tag/v0.1.0)，下载 `hpc_codex_mcp-0.1.0-py3-none-any.whl`、Windows ZIP 与 `SHA256SUMS` 到用户本地临时目录，所有命令中的路径使用该实际目录。按 manifest 中准确文件名比较 `Get-FileHash -Algorithm SHA256`，任何缺失/不一致立即停止。ZIP 包含 `scripts/configure_windows.py` 与指南；helper 不在 wheel 中。先检查 ZIP 条目无绝对路径、`..` 或链接再解压，解压后的 wheel 必须与已校验 wheel 一致；执行脚本前确认来源版本。SHA256SUMS 通过同一官方发布来源提供一致性校验，不声称独立签名。

发布页不存在时如实报告。可使用固定 `v0.1.0` tag 的源码 fallback（标签尚未存在也必须停止），不能换成滚动 main 或假称 PyPI 已发布。检查对应提交与来源、只在临时 checkout 内 `uv sync --locked` / `uv build`；构建 wheel，记录其 SHA-256，与官方 manifest 比较（构建差异必须解释并得到对源码安装的明确授权），之后同样使用 `install --wheel`。不跳过官方 wheel 的完整性步骤。

## 3. 隔离 bootstrap 与 GUI

确认 `uv` 在 PATH，Python 3.11+ 与 Codex 可用。在 bootstrap 中使用 [python.org Windows 官方完整安装器](https://www.python.org/downloads/windows/) 安装的 Python，并启用 Tcl/Tk；不要把缺失 tkinter 的 uv 下载运行时默认为 GUI 可用。不满足时使用官方完整 Python 安装 fallback；无 GUI 环境则说明配置未完成，无 console 密码 fallback。

下面是变量模板，先把值替换为实际路径与用户确认的非秘密信息，不使用任何密码变量：

```powershell
$bootstrap = Join-Path $env:LOCALAPPDATA 'hpc-mcp-bootstrap-v0.1.0'
$fullPython = 'ACTUAL_FULL_PYTHON_PATH'
$wheel = 'ACTUAL_VERIFIED_WHEEL_PATH'
$helper = 'ACTUAL_EXTRACTED_ZIP_PATH\scripts\configure_windows.py'
uv venv --python $fullPython $bootstrap
$python = Join-Path $bootstrap 'Scripts\python.exe'
uv pip install --python $python $wheel
& $python -c "import tkinter; print('tkinter available')"
```

若 bootstrap 已存在，先检查其身份和版本，不能静默覆盖。以上只是准备；新建配置时 Codex 打开实际 helper 的本机窗口：

```powershell
& $python $helper --username 'USER_CONFIRMED_SSH_USERNAME' --local-root 'USER_AUTHORIZED_ABSOLUTE_DIRECTORY'
# 其他集群/非标准 home 额外传 --host 和 --remote-root；不传 password 参数。
```

使用可见交互窗口是本请求的必要部分，等待进程结果，不能只启动后宣称成功。用户在 masked Entry 内输入密码并确认远程根。helper 输出 `completed`/0 才表示新建完成；`cancelled`/2 或 `incomplete`/1 均是未完成。权限在写入秘密字节之前限制为当前用户；失败不回退开放权限，也不覆盖现有目标。窗口不连接 SSH。

## 4. 验证并注册

所有步骤使用同一个 bootstrap 可执行文件及解析后的 config 路径（显式 override 则每条命令带 `--config-file`）：

```powershell
$setup = Join-Path $bootstrap 'Scripts\hpc-setup.exe'
& $setup validate-config
& $setup doctor
& $setup doctor --check-ssh
& $setup install --wheel $wheel
& $setup install --wheel $wheel --apply
```

先读 plan 输出，再 apply。`doctor` 检查当前 Codex schema，不启动模型；SSH 检查会认证、建立本机 TOFU 并检查 Slurm 命令，不提交作业。缺失能力必须报告，不能忽略失败。首次密码错误导致 SSH doctor 失败时，配置/安装不能报告成功；不要反复触发 PAM 登录尝试，也不要读取秘密 JSON 或删除/覆盖现有配置以重新运行窗口。此 helper 只支持新建，已有配置的本地修正需要用户明确批准并自行在本机安全修正，之后再验证。取消、修正或 schema 不兼容可能需要用户操作，不承诺完全自动。安装创建独立 release 环境、用户级 STDIO 注册、SID 范围登录任务与 `hpc-slurm-workflow` skill。无需管理员。`--rollback-wheel` 只供明确受管升级回退，不授权接管其他注册。

## 5. 当前会话启动与客户端发现

再次检查 agent status、进程身份；已有当前 agent 则继续复用。没有活跃 agent 时，从规范安装元数据验证精确任务再启动，不直接运行任意 PATH 上的旧 `hpc-agent run`，不打印 Codex 原始配置或元数据。可在 bootstrap Python 内使用本版本适配器：

```python
from hpc_mcp.config.paths import resolve_servers_path
from hpc_mcp.setup.install import Installer
from hpc_mcp.setup.windows import run_command

installer = Installer(resolve_servers_path())  # explicit config override must match install
record = installer._load()  # validates canonical metadata and fingerprint
if record is None:
    raise RuntimeError("No managed installation")
if not installer.startup.verify([record["agent_command"]]):
    raise RuntimeError("Managed task absent")
result = run_command(["schtasks", "/Run", "/TN", installer.startup.name])
if result.returncode:
    raise RuntimeError("Managed task start failed")
print("Managed current-session startup requested")
```

适配器验证当前用户 SID、所有者标记和与元数据相符的精确命令。启动请求不等于 agent 已运行，随后用元数据对应 release 的 CLI 检查 `status` 和进程身份。上述内部方法适用于固定 v0.1.0；不要假设其他版本 API 不变。

`codex mcp list` 仅证明注册。检查当前 Codex 连接后的工具列表，必须发现 24 个工具，否则如实报告“需要客户端重载”并让用户重载后复核。验证用户 skills 目录已安装 `hpc-slurm-workflow/SKILL.md`（installer 已负责复制，避免另装冲突）。MCP/GUI 完成、schema/SSH 检查、agent 状态、24-tool discovery 分别记录，不用一个成功替代其他证据。

完成时给出版本、验证结果、配置路径（无内容）和待用户重载步骤。后续先用 `server_test_connection` 返回的实际 home 核对窗口约定；远程项目根必须已存在、用户拥有且不是链接，首次映射需要时由管理员或用户在另外授权的 SSH/SFTP 操作中预建，安装流程不创建它。`project_ensure` 以返回映射为准，再决定上传；不自行猜远程路径。升级/卸载须先计划，并保留配置、数据库、watch/event 与旧 release；见 [Windows 回退](windows-setup.md)。
