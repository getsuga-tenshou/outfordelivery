[CmdletBinding()]
param(
    [ValidateSet('Install', 'SetInterval', 'Status', 'Disable', 'Enable')]
    [string]$Action = 'Install',
    [string]$Config = 'data/scheduler/config.json',
    [ValidateSet(15, 60, 1440)]
    [int]$IntervalMinutes = 1440,
    [string]$TaskName = 'OutForDelivery-Analytics',
    [string]$Python = 'venv/Scripts/pythonw.exe'
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$taskPath = '\'

function Resolve-RepoPath([string]$Value) {
    if ([IO.Path]::IsPathRooted($Value)) { return (Resolve-Path -LiteralPath $Value).Path }
    return (Resolve-Path -LiteralPath (Join-Path $repoRoot $Value)).Path
}

if ($Action -in @('Install', 'SetInterval')) {
    # Absolute paths and an argument string keep spaces in checkout paths intact.
    $configPath = Resolve-RepoPath $Config
    $pythonPath = Resolve-RepoPath $Python
    $scriptPath = Join-Path $PSScriptRoot 'schedule_sync.py'
    foreach ($value in @($configPath, $pythonPath, $scriptPath)) {
        if ($value.Contains('"')) { throw 'Task paths cannot contain double quotes.' }
    }
    $existingTask = Get-ScheduledTask -TaskName $TaskName -TaskPath $taskPath -ErrorAction SilentlyContinue
    if ($Action -eq 'Install' -and $existingTask) {
        throw 'Task already exists. Inspect it; use Enable/Disable instead of replacing it.'
    }
    $consolePython = Join-Path (Split-Path -Parent $pythonPath) 'python.exe'
    & $consolePython $scriptPath status --config $configPath | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Scheduler configuration/state check failed.' }
    $arguments = '"{0}" tick --config "{1}"' -f $scriptPath, $configPath
    $trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes($IntervalMinutes) `
        -RepetitionInterval (New-TimeSpan -Minutes $IntervalMinutes)
    if ($Action -eq 'SetInterval') {
        if (-not $existingTask -or $existingTask.Actions.Count -ne 1 -or
            $existingTask.Actions.Execute -ne $pythonPath -or $existingTask.Actions.Arguments -ne $arguments) {
            throw 'Existing task does not match this scheduler and configuration; no trigger changed.'
        }
        Set-ScheduledTask -TaskName $TaskName -TaskPath $taskPath -Trigger $trigger | Out-Null
    } else {
        $taskAction = New-ScheduledTaskAction -Execute $pythonPath -Argument $arguments -WorkingDirectory $repoRoot
        $settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -StartWhenAvailable `
            -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit ([TimeSpan]::Zero)
        $identity = [Security.Principal.WindowsIdentity]::GetCurrent().Name
        $principal = New-ScheduledTaskPrincipal -UserId $identity -LogonType Interactive -RunLevel Limited
        $task = New-ScheduledTask -Action $taskAction -Trigger $trigger -Settings $settings -Principal $principal `
            -Description 'Process new completed actual-stream batches; retain run identity and retry unfinished transfers. Requires this user to be logged in and the device available.'
        Register-ScheduledTask -TaskName $TaskName -TaskPath $taskPath -InputObject $task | Out-Null
    }
} elseif ($Action -eq 'Disable') {
    Disable-ScheduledTask -TaskName $TaskName -TaskPath $taskPath | Out-Null
} elseif ($Action -eq 'Enable') {
    Enable-ScheduledTask -TaskName $TaskName -TaskPath $taskPath | Out-Null
}

Get-ScheduledTask -TaskName $TaskName -TaskPath $taskPath |
    Select-Object TaskName, TaskPath, State
Get-ScheduledTaskInfo -TaskName $TaskName -TaskPath $taskPath |
    Select-Object LastRunTime, LastTaskResult, NextRunTime
