param(
    [Parameter(Mandatory = $true)]
    [string]$Url,

    [string]$Output = "",

    [string]$Nibiru = "",

    [switch]$Http1Only,

    [ValidateRange(0, 2147483647)]
    [int]$SyncIntervalMiB = 0,

    [string]$LogFile = "",

    [string]$SummaryFile = "",

    [switch]$Adaptive
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($Output)) {
    $Output = Join-Path $PSScriptRoot "windows11-test.iso"
}

if ([string]::IsNullOrWhiteSpace($Nibiru)) {
    $Nibiru = Join-Path $PSScriptRoot "nibiru-xtreme.exe"
    if (-not (Test-Path $Nibiru)) {
        $Nibiru = Join-Path $PSScriptRoot "..\target\release\nibiru-xtreme.exe"
    }
}

if (-not (Test-Path $Nibiru)) {
    throw "Nibiru binary not found: $Nibiru"
}

if ([string]::IsNullOrWhiteSpace($LogFile)) {
    $LogFile = Join-Path $PSScriptRoot "nibiru-download.jsonl"
}
if ([string]::IsNullOrWhiteSpace($SummaryFile)) {
    $SummaryFile = Join-Path $PSScriptRoot "nibiru-benchmark.json"
}

if (Test-Path $Output) {
    Remove-Item -Force $Output
}

Write-Host "Nibiru Xtreme - Windows 11 ISO benchmark"
Write-Host "Binary : $Nibiru"
Write-Host "Output : $Output"
Write-Host "URL    : signed Microsoft URL (not printed to avoid leaking query tokens)"
$version = (& $Nibiru --version | Out-String).Trim()
if ($LASTEXITCODE -ne 0) { throw "Could not read engine version" }
Write-Host ("Version: {0}" -f $version)
Write-Host ("Sync interval: {0} MiB (0 = final only)" -f $SyncIntervalMiB)

$logOffset = 0L
if (Test-Path -LiteralPath $LogFile) { $logOffset = (Get-Item -LiteralPath $LogFile).Length }
$started = [System.Diagnostics.Stopwatch]::StartNew()
$downloadArgs = @("download", $Url, "--out", $Output, "--log", $LogFile, "--sync-interval-mib", "$SyncIntervalMiB")
if ($Http1Only) { $downloadArgs += "--http1-only" }
if ($Adaptive) { $downloadArgs += "--adaptive" }
& $Nibiru @downloadArgs
$started.Stop()
if ($LASTEXITCODE -ne 0) {
    throw "Nibiru exited with code $LASTEXITCODE"
}
$elapsed = $started.Elapsed

$item = Get-Item $Output
$bytes = [double]$item.Length
$seconds = [Math]::Max($elapsed.TotalSeconds, 0.001)
$mbps = ($bytes / 1MB) / $seconds
$decimalMbps = ($bytes / 1e6) / $seconds
$gbps = (($bytes * 8) / 1e9) / $seconds

$hash = Get-FileHash -Algorithm SHA256 $Output
$nativeTimings = [ordered]@{}
$logStream = [System.IO.File]::OpenRead($LogFile)
try {
    $null = $logStream.Seek($logOffset, [System.IO.SeekOrigin]::Begin)
    $reader = [System.IO.StreamReader]::new($logStream, [System.Text.Encoding]::UTF8)
    try { $currentLog = $reader.ReadToEnd() }
    finally { $reader.Dispose() }
}
finally { $logStream.Dispose() }
$records = @($currentLog -split '\r?\n' | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
    ForEach-Object { $_ | ConvertFrom-Json })
$completion = $records |
    Where-Object { $_.event -eq "http.download_completed" } | Select-Object -Last 1
if ($null -ne $completion) {
    foreach ($match in [regex]::Matches($completion.detail, '(?:^| )([a-z_]+_ms|checkpoint_count|checkpoint_sync_ms_sum|adaptive_target_at_end)=([0-9]+)')) {
        $nativeTimings[$match.Groups[1].Value] = [long]$match.Groups[2].Value
    }
}
$adaptiveChanges = 0
foreach ($record in $records) {
    if ($record.event -eq "http.adaptive_decision" -and
        $record.detail -match 'previous_workers=([0-9]+) target_workers=([0-9]+)' -and
        $Matches[1] -ne $Matches[2]) { $adaptiveChanges++ }
}
$summaryPath = $SummaryFile
$summary = [ordered]@{
    completed_utc = [DateTime]::UtcNow.ToString("o")
    engine_version = $version
    http_mode = $(if ($Http1Only) { "http1-only" } else { "auto-h2-window16m" })
    sync_interval_mib = $SyncIntervalMiB
    adaptive = [bool]$Adaptive
    adaptive_started = @($records | Where-Object { $_.event -eq "http.adaptive_started" }).Count -gt 0
    adaptive_decision_count = $adaptiveChanges
    native_timings = $nativeTimings
    output_file = $item.FullName
    size_bytes = $item.Length
    elapsed_seconds = $seconds
    average_mib_per_second = $mbps
    average_mb_per_second = $decimalMbps
    average_gbit_per_second = $gbps
    sha256 = $hash.Hash
    binary_sha256 = (Get-FileHash -Algorithm SHA256 $Nibiru).Hash
    log_file = $LogFile
}
[System.IO.File]::WriteAllText($summaryPath, ($summary | ConvertTo-Json -Depth 4), (New-Object System.Text.UTF8Encoding($false)))

Write-Host ""
Write-Host "Completed"
Write-Host ("Size       : {0:N2} GiB" -f ($bytes / 1GB))
Write-Host ("Elapsed    : {0:N2} s" -f $seconds)
Write-Host ("Average    : {0:N2} MiB/s" -f $mbps)
Write-Host ("Average    : {0:N2} MB/s" -f $decimalMbps)
Write-Host ("Average    : {0:N3} Gbit/s" -f $gbps)
Write-Host ("SHA-256    : {0}" -f $hash.Hash)
Write-Host ("Summary    : {0}" -f $summaryPath)
