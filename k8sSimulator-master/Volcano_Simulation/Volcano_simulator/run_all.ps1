# run_all.ps1 — uruchamia symulacje bin-pack dla wszystkich *.yaml w demos/k8ssim-PW/workloads/
# i zapisuje wyniki w katalogu metrics/.

param(
    # Parametr: liczba węzłów; domyślnie 8
    [int]$Nodes = 8
)

# Ustaw ścieżki
$workloadDir = ".\demos\k8ssim-PW\workloads"
$metricsDir   = ".\metrics"
$runCmd       = ".\demos\k8ssim-PW\cmd\run.go"

# Upewnij się, że katalog metrics istnieje
if (-not (Test-Path $metricsDir)) {
    New-Item -ItemType Directory -Path $metricsDir | Out-Null
}

Write-Host "`n=== Uruchamiam symulacje dla workloadów z $workloadDir, Nodes=$Nodes ===`n"

# Dla każdego pliku *.yaml w katalogu workloadDir
Get-ChildItem -Path $workloadDir -Filter *.yaml | ForEach-Object {
    $yamlPath = $_.FullName
    $baseFile = $_.Name       # np. "burst.yaml"
    Write-Host "----"; Write-Host " workload: $baseFile"; Write-Host "----"

    # Wywołaj Go: go run -mod=mod run.go --workload=<ścieżka> --nodes=<Nodes>
    go run -mod=mod $runCmd --workload=$yamlPath --nodes=$Nodes

    Write-Host "`n"
}

Write-Host "Wszystkie symulacje zakończone. Pliki CSV znajdują się w $metricsDir`n"
