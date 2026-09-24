$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$pidFile = Join-Path $root 'quant.pid'
if (-not (Test-Path $pidFile)) { Write-Output 'OpenClaw Quant is not running.'; exit 0 }
$processId = [int](Get-Content $pidFile -Raw)
$process = Get-Process -Id $processId -ErrorAction SilentlyContinue
if ($process) { Stop-Process -Id $processId -Force; Write-Output "Stopped OpenClaw Quant PID $processId" } else { Write-Output 'Process was already stopped.' }
Remove-Item -LiteralPath $pidFile -Force
