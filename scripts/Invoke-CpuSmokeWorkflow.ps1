<#
.SYNOPSIS
Run the authorized CPU smoke through the existing installed hpc-mcp, with recovery.
No persistent settings, credential prompts or new permissions are created here.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$RecordPath,
    [Parameter(Mandatory)][string]$Server,
    [Parameter(Mandatory)][string]$ExpectedHost,
    [Parameter(Mandatory)][string]$ExpectedHostname,
    [Parameter(Mandatory)][string]$ExpectedUser,
    [Parameter(Mandatory)][string]$Partition,
    [ValidateRange(0, 3600)][int]$WaitSeconds = 300
)
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$recordRoot = [System.IO.Path]::GetFullPath($RecordPath)
$metadataPath = Join-Path $env:USERPROFILE '.hpc-codex-mcp\state\installation.json'
$installation = Get-Content -LiteralPath $metadataPath -Raw -Encoding UTF8 | ConvertFrom-Json
$installedPython = $installation.managed_command[0]
$driver = Join-Path $PSScriptRoot 'cpu_smoke_workflow.py'
$common = @('--record', $recordRoot, '--server', $Server,
    '--expected-host', $ExpectedHost, '--expected-hostname', $ExpectedHostname,
    '--expected-user', $ExpectedUser, '--partition', $Partition)
& $installedPython $driver submit @common
if ($LASTEXITCODE -ne 0) { throw 'MCP submission stopped; preserve the record and UUID.' }
$run = Get-Content -LiteralPath (Join-Path $recordRoot 'run.json') -Raw -Encoding UTF8 | ConvertFrom-Json

# Execute the exact returned official command in its originating terminal context.
# PATH changes affect this process only and select this release's hpc-agent.
$previousPath = $env:PATH
try {
    $env:PATH = (Split-Path -Parent $installedPython) + [System.IO.Path]::PathSeparator + $env:PATH
    Push-Location -LiteralPath (Join-Path $repoRoot 'tests\fixtures\cpu_smoke')
    try {
        $watchOutput = Invoke-Expression $run.receipt.watch_command
        if ($LASTEXITCODE -ne 0) { throw 'Official watch registration failed; never resubmit to repair monitoring.' }
        $watchOutput | Set-Content -LiteralPath (Join-Path $recordRoot 'watch-registration.json') -Encoding UTF8
    } finally { Pop-Location }
} finally { $env:PATH = $previousPath }
& $installedPython $driver watch-status @common
if ($LASTEXITCODE -ne 0) { throw 'MCP could not verify the registered watch.' }
& $installedPython $driver collect @common --wait-seconds $WaitSeconds
if ($LASTEXITCODE -ne 0) { throw 'Collection is pending or failed; retain diagnostics and rerun collect with the same record.' }
