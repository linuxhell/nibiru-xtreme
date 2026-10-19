param(
    [Parameter(Mandatory = $true)]
    [string]$Url,

    [ValidateRange(1, 2147483647)]
    [int]$SyncIntervalMiB = 512,

    [ValidateRange(1, 10)]
    [int]$Rounds = 1,

    [switch]$Http1Only,

    [switch]$TestAdaptive
)

$ErrorActionPreference = "Stop"
$runId = [DateTime]::Now.ToString("yyyyMMdd-HHmmss") + "-" + [Guid]::NewGuid().ToString("N").Substring(0, 8)
$prefix = $(if ($TestAdaptive) { "comparacao-adaptativo-" } else { "comparacao-sync-" })
$runRoot = Join-Path $PSScriptRoot ($prefix + $runId)
$zipPath = $runRoot + ".zip"
New-Item -ItemType Directory -Path $runRoot | Out-Null
$results = @()

Write-Host $(if ($TestAdaptive) { "Comparacao: paralelismo fixo versus adaptativo; sincronizacao somente no final" } else { "Comparacao: sincronizacao final versus sincronizacao durante o download" })
Write-Host ("Serao {0} downloads completos. A mesma ISO de teste sera substituida a cada execucao." -f (2 * $Rounds))

try {
    for ($round = 1; $round -le $Rounds; $round++) {
        # Alternate order when more than one round is requested.
        $order = @(0, $SyncIntervalMiB)
        if ($TestAdaptive) { $order = @(0, 1) }
        if (($round % 2) -eq 0) { $order = @($SyncIntervalMiB, 0) }
        if ($TestAdaptive -and ($round % 2) -eq 0) { $order = @(1, 0) }
        foreach ($interval in $order) {
            $label = $(if ($interval -eq 0) { "final" } else { "background" })
            if ($TestAdaptive) { $label = $(if ($interval -eq 0) { "fixed" } else { "adaptive" }) }
            $stem = "round-{0}-{1}" -f $round, $label
            $summaryPath = Join-Path $runRoot ($stem + ".json")
            $parameters = @{
                Url = $Url
                SyncIntervalMiB = $interval
                LogFile = Join-Path $runRoot ($stem + ".jsonl")
                SummaryFile = $summaryPath
                Output = Join-Path $runRoot "windows11-test.iso"
            }
            if ($TestAdaptive) {
                $parameters.SyncIntervalMiB = 0
                $parameters.Adaptive = ($interval -ne 0)
            }
            if ($Http1Only) { $parameters.Http1Only = $true }
            Write-Host ("Round {0}: {1}" -f $round, $label)
            & (Join-Path $PSScriptRoot "windows11.ps1") @parameters
            $result = Get-Content -Raw -LiteralPath $summaryPath | ConvertFrom-Json
            $result | Add-Member -NotePropertyName round -NotePropertyValue $round
            $results += $result
        }
    }

    $hashes = @($results | Select-Object -ExpandProperty sha256 -Unique)
    $sizes = @($results | Select-Object -ExpandProperty size_bytes -Unique)
    $binaries = @($results | Select-Object -ExpandProperty binary_sha256 -Unique)
    if ($hashes.Count -ne 1 -or $sizes.Count -ne 1) {
        throw "Os arquivos diferem em tamanho ou SHA-256. Comparacao invalida."
    }
    if ($binaries.Count -ne 1) { throw "O executavel mudou durante a comparacao." }

    $baseline = @($results | Where-Object { $_.sync_interval_mib -eq 0 })
    $background = @($results | Where-Object { $_.sync_interval_mib -ne 0 })
    if ($TestAdaptive) {
        $baseline = @($results | Where-Object { -not $_.adaptive })
        $background = @($results | Where-Object { $_.adaptive })
    }
    $baselineMean = [double]($baseline | Measure-Object -Property elapsed_seconds -Average).Average
    $backgroundMean = [double]($background | Measure-Object -Property elapsed_seconds -Average).Average
    $exercised = @($background | Where-Object { $_.native_timings.checkpoint_count -gt 0 }).Count -eq $Rounds
    if ($TestAdaptive) { $exercised = @($background | Where-Object { $_.adaptive_started }).Count -eq $Rounds }
    $comparison = [ordered]@{
        completed_utc = [DateTime]::UtcNow.ToString("o")
        rounds = $Rounds
        test_kind = $(if ($TestAdaptive) { "adaptive_concurrency" } else { "background_sync" })
        same_size_and_sha256 = $true
        same_binary_sha256 = $true
        experiment_exercised = $exercised
        adaptive_changed_concurrency = @($background | Where-Object { $_.adaptive_decision_count -gt 0 }).Count -gt 0
        baseline_mean_seconds = $baselineMean
        background_mean_seconds = $backgroundMean
        saved_seconds = $baselineMean - $backgroundMean
        saved_percent = 100 * ($baselineMean - $backgroundMean) / $baselineMean
        results = $results
    }
    $comparisonFile = $(if ($TestAdaptive) { "comparacao-adaptativo.json" } else { "comparacao-sync.json" })
    [System.IO.File]::WriteAllText((Join-Path $runRoot $comparisonFile),
        ($comparison | ConvertTo-Json -Depth 6), (New-Object System.Text.UTF8Encoding($false)))
    Write-Host ("Media padrao       : {0:N3} s" -f $baselineMean)
    Write-Host ("Media experimental : {0:N3} s" -f $backgroundMean)
    Write-Host ("Diferenca        : {0:N3} s ({1:N2}%)" -f $comparison.saved_seconds, $comparison.saved_percent)
    if (-not $exercised) { Write-Host "O recurso experimental nao foi exercitado em algum teste; confira o modo de transferencia no log." }
    if ($TestAdaptive -and -not $comparison.adaptive_changed_concurrency) {
        Write-Host "O controlador foi ativado, mas nao alterou o paralelismo nesta comparacao."
    }
    Write-Host "Uma rodada e indicativa. CDN e cache podem variar; nenhuma configuracao padrao foi alterada."
}
finally {
    # Include diagnostic files only. Never include the multi-GB ISO in the ZIP.
    $diagnostics = @(Get-ChildItem -LiteralPath $runRoot -File | Where-Object { $_.Extension -in @(".json", ".jsonl") })
    if ($diagnostics.Count -gt 0) {
        Compress-Archive -LiteralPath $diagnostics.FullName -DestinationPath $zipPath
        Write-Host ("Envie este ZIP: {0}" -f $zipPath)
    }
}
