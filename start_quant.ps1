param([int]$Port = 8765)
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$python = Join-Path $env:LOCALAPPDATA 'OpenClaw\deps\python\python.exe'
if (-not (Test-Path $python)) {
  $pythonCommand = Get-Command python -ErrorAction SilentlyContinue
  if ($pythonCommand) { $python = $pythonCommand.Source }
}
if (-not (Test-Path $python)) { throw 'Bundled Python runtime not found.' }
$pidFile = Join-Path $root 'quant.pid'
if (Test-Path $pidFile) {
  $oldPid = [int](Get-Content $pidFile -Raw)
  if (Get-Process -Id $oldPid -ErrorAction SilentlyContinue) { Write-Output "Already running with PID $oldPid"; exit 0 }
}
$proc = Start-Process -FilePath $python -ArgumentList @((Join-Path $root 'app.py'), '--port', $Port) -WorkingDirectory $root -WindowStyle Hidden -PassThru
$proc.Id | Set-Content -Path $pidFile -Encoding ascii
Write-Output "OpenClaw Quant PAPER service started: http://127.0.0.1:$Port (PID $($proc.Id))"
