param(
    [Parameter(Mandatory = $true)]
    [string]$Url,

    [string]$Output = "",

    [string]$Nibiru = ""
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

if (Test-Path $Output) {
    Remove-Item -Force $Output
}

Write-Host "Nibiru Xtreme - Windows 11 ISO benchmark"
Write-Host "Binary : $Nibiru"
Write-Host "Output : $Output"
Write-Host "URL    : signed Microsoft URL (not printed to avoid leaking query tokens)"

$started = [System.Diagnostics.Stopwatch]::StartNew()
& $Nibiru download $Url --out $Output --log (Join-Path $PSScriptRoot "nibiru-download.jsonl")
$started.Stop()
if ($LASTEXITCODE -ne 0) {
    throw "Nibiru exited with code $LASTEXITCODE"
}
$elapsed = $started.Elapsed

$item = Get-Item $Output
$bytes = [double]$item.Length
$seconds = [Math]::Max($elapsed.TotalSeconds, 0.001)
$mbps = ($bytes / 1MB) / $seconds
$gbps = (($bytes * 8) / 1e9) / $seconds

$hash = Get-FileHash -Algorithm SHA256 $Output
$summaryPath = Join-Path $PSScriptRoot "nibiru-benchmark.json"
$summary = [ordered]@{
    completed_utc = [DateTime]::UtcNow.ToString("o")
    output_file = $item.FullName
    size_bytes = $item.Length
    elapsed_seconds = $seconds
    average_mib_per_second = $mbps
    average_gbit_per_second = $gbps
    sha256 = $hash.Hash
    binary_sha256 = (Get-FileHash -Algorithm SHA256 $Nibiru).Hash
    log_file = Join-Path $PSScriptRoot "nibiru-download.jsonl"
}
[System.IO.File]::WriteAllText($summaryPath, ($summary | ConvertTo-Json), (New-Object System.Text.UTF8Encoding($false)))

Write-Host ""
Write-Host "Completed"
Write-Host ("Size       : {0:N2} GiB" -f ($bytes / 1GB))
Write-Host ("Elapsed    : {0:N2} s" -f $seconds)
Write-Host ("Average    : {0:N2} MiB/s" -f $mbps)
Write-Host ("Average    : {0:N3} Gbit/s" -f $gbps)
Write-Host ("SHA-256    : {0}" -f $hash.Hash)
Write-Host ("Summary    : {0}" -f $summaryPath)
