<#
.SYNOPSIS
    End-to-end verification of the Kafka dead-letter path.

.DESCRIPTION
    1. Preflight: Kafka must be reachable on localhost:9092.
    2. Compile PoisonInjector + DltReader against the project's own dependencies.
    3. Snapshot the delivery broadcast counter.
    4. Inject a malformed record into binance-raw-trades.
    5. Wait for the 3 attempts (1s backoff) plus the DLT publish.
    6. Dump the DLT record: raw payload + kafka_dlt-* headers.
    7. Re-read the broadcast counter to prove the consumer kept going.

    The app must already be running (mvn spring-boot:run) for steps 3 and 7 to mean
    anything, but steps 4-6 work against the broker alone.

.EXAMPLE
    docker compose up -d
    mvn spring-boot:run          # separate terminal
    .\tools\dlt-verify\verify-dlt.ps1
#>

[CmdletBinding()]
param(
    [int] $RetryWindowSeconds = 8
)

$ErrorActionPreference = 'Stop'
$repoRoot = Resolve-Path (Join-Path $PSScriptRoot '..\..')
Set-Location $repoRoot

$classes = Join-Path $repoRoot 'target\dlt-verify-classes'
$cpFile  = Join-Path $repoRoot 'target\dlt-verify-cp.txt'

function Get-BroadcastCount {
    try {
        $m = Invoke-RestMethod 'http://127.0.0.1:8080/actuator/metrics/terminal.delivery.trades.broadcast' -TimeoutSec 5
        return ($m.measurements | Where-Object { $_.statistic -eq 'COUNT' }).value
    } catch {
        return $null
    }
}

# ---------------------------------------------------------------- preflight
Write-Host '== preflight ==' -ForegroundColor Cyan
$kafkaUp = $false
try {
    $kafkaUp = (Test-NetConnection -ComputerName 127.0.0.1 -Port 9092 -WarningAction SilentlyContinue).TcpTestSucceeded
} catch { $kafkaUp = $false }

if (-not $kafkaUp) {
    Write-Host 'Kafka is NOT listening on 127.0.0.1:9092.' -ForegroundColor Red
    Write-Host 'Start the stack first:  docker compose up -d ; docker compose ps'
    exit 1
}
Write-Host 'kafka: reachable on :9092'

$before = Get-BroadcastCount
if ($null -eq $before) {
    Write-Host 'app: not running (broadcast counter unavailable) - steps 3/7 will be skipped' -ForegroundColor Yellow
} else {
    Write-Host "app: broadcast counter = $before"
}

# ---------------------------------------------------------------- classpath
Write-Host ''
Write-Host '== resolving project classpath ==' -ForegroundColor Cyan
& mvn -q -B dependency:build-classpath "-Dmdep.outputFile=$cpFile" | Out-Null
if (-not (Test-Path $cpFile)) {
    throw "mvn dependency:build-classpath produced no output at $cpFile"
}
$cp = (Get-Content $cpFile -Raw).Trim()
Write-Host "classpath entries: $((($cp -split ';') | Where-Object { $_ }).Count)"

New-Item -ItemType Directory -Force -Path $classes | Out-Null
& javac -proc:none -cp $cp -d $classes (Join-Path $PSScriptRoot 'PoisonInjector.java') (Join-Path $PSScriptRoot 'DltReader.java')
if ($LASTEXITCODE -ne 0) { throw 'javac failed' }
$runCp = "$cp;$classes"

# ---------------------------------------------------------------- inject
Write-Host ''
Write-Host '== injecting poison record ==' -ForegroundColor Cyan
& java -cp $runCp PoisonInjector

# ---------------------------------------------------------------- wait
Write-Host ''
Write-Host "== waiting ${RetryWindowSeconds}s for 3 attempts (1s backoff) + DLT publish ==" -ForegroundColor Cyan
Start-Sleep -Seconds $RetryWindowSeconds

# ---------------------------------------------------------------- read DLT
Write-Host ''
Write-Host '== reading binance-raw-trades.DLT ==' -ForegroundColor Cyan
& java -cp $runCp DltReader

# ---------------------------------------------------------------- recovery
$after = Get-BroadcastCount
Write-Host ''
Write-Host '== consumer recovery ==' -ForegroundColor Cyan
if ($null -eq $before -or $null -eq $after) {
    Write-Host 'broadcast counter unavailable; check the app log for "Broadcast ... -> /topic/trades" after the poison offset'
} else {
    $delta = $after - $before
    Write-Host "broadcast counter: $before -> $after  (delta = $delta)"
    if ($delta -gt 0) {
        Write-Host 'PASS: live trades kept flowing after the poison record (no poison loop).' -ForegroundColor Green
    } else {
        Write-Host 'WARN: counter did not move - is ingestion running? (check BINANCE_WS_URL / frames.received)' -ForegroundColor Yellow
    }
}
