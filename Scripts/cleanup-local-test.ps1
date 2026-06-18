param(
    [switch]$KeepLogs  # 로그 파일 삭제 건너뜀
)

$ErrorActionPreference = "Stop"

# 프로젝트 루트 경로 결정
$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

# docker CLI 경로 확인 (PATH에 없으면 Docker Desktop 기본 경로 사용)
$dockerCommand = Get-Command docker -ErrorAction SilentlyContinue
if ($dockerCommand) {
    $docker = $dockerCommand.Source
}
else {
    $docker = "C:\Program Files\Docker\Docker\resources\bin\docker.exe"
    if (-not (Test-Path $docker)) {
        throw "Docker CLI was not found. Add docker.exe to PATH or install Docker Desktop."
    }
}

# 이 프로젝트와 관련된 서버 프로세스 및 Aspire 프로세스 종료
$processes = Get-CimInstance Win32_Process | Where-Object {
    (
        $_.Name -in @("dotnet.exe", "GatewayServer.exe", "WorldServer.exe", "DummyClient.exe") -and
        $_.CommandLine -like "*$root*"
    ) -or (
        $_.Name -eq "dcp.exe" -and
        $_.CommandLine -like "*aspire-dcp*"
    )
}

foreach ($process in $processes) {
    Stop-Process -Id $process.ProcessId -Force -ErrorAction SilentlyContinue
}

# Aspire 세션 컨테이너 중 redis/dynamodb-local 이름 패턴과 일치하는 것만 선별해 제거
$containerIds = & $docker ps -aq --filter "label=com.microsoft.developer.usvc-dev.persistent=false"
$targetContainerIds = @()
foreach ($containerId in $containerIds) {
    $name = & $docker inspect --format "{{.Name}}" $containerId 2>$null
    if ($name -match "^/(redis|redis-eventbus|dynamodb-local)-") {
        $targetContainerIds += $containerId
    }
}

if ($targetContainerIds.Count -gt 0) {
    & $docker rm -f @targetContainerIds | Out-Null
}

# 1000명 테스트 실행 추적용 임시 로그 파일 삭제
if (-not $KeepLogs) {
    Remove-Item -LiteralPath (Join-Path $root ".runlogs\dummy-1000-current.pid") -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath (Join-Path $root ".runlogs\dummy-1000-current.outpath") -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath (Join-Path $root ".runlogs\dummy-1000-current.errpath") -Force -ErrorAction SilentlyContinue
}

Write-Output "Local test cleanup complete. Removed containers: $($targetContainerIds.Count)."
