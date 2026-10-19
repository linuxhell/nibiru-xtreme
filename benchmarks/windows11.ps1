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

$started = Get-Date
& $Nibiru download $Url --out $Output
if ($LASTEXITCODE -ne 0) {
    throw "Nibiru exited with code $LASTEXITCODE"
}
$elapsed = (Get-Date) - $started

$item = Get-Item $Output
$bytes = [double]$item.Length
$seconds = [Math]::Max($elapsed.TotalSeconds, 0.001)
$mbps = ($bytes / 1MB) / $seconds
$gbps = (($bytes * 8) / 1GB) / $seconds

$hash = Get-FileHash -Algorithm SHA256 $Output

Write-Host ""
Write-Host "Completed"
Write-Host ("Size       : {0:N2} GiB" -f ($bytes / 1GB))
Write-Host ("Elapsed    : {0:N2} s" -f $seconds)
Write-Host ("Average    : {0:N2} MiB/s" -f $mbps)
Write-Host ("Average    : {0:N3} Gbit/s" -f $gbps)
Write-Host ("SHA-256    : {0}" -f $hash.Hash)
