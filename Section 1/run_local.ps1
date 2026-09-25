param(
    [string]$AFile = "matrix_a.txt",
    [string]$BFile = "matrix_b.txt",
    [int]$MapperTasks = 1,
    [string]$OutputFile = "result.txt",
    [string]$BenchmarkFile = "local_benchmark.csv"
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

if ($MapperTasks -lt 1) {
    throw "MapperTasks must be at least 1."
}
if (-not (Test-Path $AFile) -or -not (Test-Path $BFile)) {
    throw "Input files were not found: $AFile and $BFile"
}

$bHeader = (Get-Content $BFile -TotalCount 1) -split '\s+'
$n = [int]$bHeader[0]
$p = [int]$bHeader[1]
$m = @(Get-Content $AFile).Count
$aBytes = (Get-Item $AFile).Length
$bBytes = (Get-Item $BFile).Length
$inputName = Split-Path $AFile -Leaf

$mapperPath = Join-Path $PSScriptRoot "mapper.exe"
$reducerPath = Join-Path $PSScriptRoot "reducer.exe"
g++ -std=c++17 -O2 mapper.cpp -o $mapperPath
g++ -std=c++17 -O2 reducer.cpp -o $reducerPath

$temporaryDirectory = Join-Path ([System.IO.Path]::GetTempPath()) ("hw3-local-" + [System.Guid]::NewGuid())
New-Item -ItemType Directory -Path $temporaryDirectory | Out-Null
$totalWatch = [System.Diagnostics.Stopwatch]::StartNew()

try {
    $chunks = @()
    for ($task = 0; $task -lt $MapperTasks; $task++) {
        $chunkPath = Join-Path $temporaryDirectory ("chunk_{0:D3}.txt" -f $task)
        New-Item -ItemType File -Path $chunkPath | Out-Null
        $chunks += $chunkPath
    }

    $lineNumber = 0
    foreach ($line in Get-Content $AFile) {
        Add-Content -Path $chunks[$lineNumber % $MapperTasks] -Value $line
        $lineNumber++
    }

    $mapperWatch = [System.Diagnostics.Stopwatch]::StartNew()
    $processes = @()
    for ($task = 0; $task -lt $MapperTasks; $task++) {
        $outputPath = Join-Path $temporaryDirectory ("map_{0:D3}.out" -f $task)
        $errorPath = Join-Path $temporaryDirectory ("map_{0:D3}.err" -f $task)
        $command = '/c ""{0}" "{1}" < "{2}" > "{3}" 2> "{4}""' -f `
            $mapperPath, (Resolve-Path $BFile), $chunks[$task], $outputPath, $errorPath
        $process = Start-Process -FilePath $env:ComSpec -ArgumentList $command -PassThru -WindowStyle Hidden
        $processes += [PSCustomObject]@{
            Process = $process
            Output = $outputPath
            Error = $errorPath
        }
    }

    foreach ($entry in $processes) {
        $entry.Process.WaitForExit()
        if ($entry.Process.ExitCode -ne 0) {
            throw "Mapper failed: $(Get-Content $entry.Error -Raw)"
        }
    }
    $mapperWatch.Stop()

    $shuffleWatch = [System.Diagnostics.Stopwatch]::StartNew()
    $sortedOutput = Join-Path $temporaryDirectory "global_sorted.out"
    Get-Content (Join-Path $temporaryDirectory "map_*.out") | Sort-Object | Set-Content $sortedOutput
    $shuffleWatch.Stop()

    $reducerWatch = [System.Diagnostics.Stopwatch]::StartNew()
    Get-Content $sortedOutput |
        & $reducerPath |
        Set-Content $OutputFile
    $reducerWatch.Stop()

    $totalWatch.Stop()
    $totalSeconds = $totalWatch.Elapsed.TotalSeconds
    $throughput = if ($totalSeconds -gt 0) { $m / $totalSeconds } else { 0 }
    $outputBytes = (Get-Item $OutputFile).Length
    $header = "n,m,p,mapper_tasks,input_rows,input_bytes,matrix_b_bytes,mapper_time_s,shuffle_time_s,reducer_time_s,total_time_s,output_bytes,throughput_rows_per_s,input_file"
    if (-not (Test-Path $BenchmarkFile) -or ((Get-Content $BenchmarkFile -TotalCount 1) -ne $header)) {
        $header | Set-Content $BenchmarkFile
    }
    $row = "$n,$m,$p,$MapperTasks,$m,$aBytes,$bBytes,$($mapperWatch.Elapsed.TotalSeconds),$($shuffleWatch.Elapsed.TotalSeconds),$($reducerWatch.Elapsed.TotalSeconds),$totalSeconds,$outputBytes,$throughput,$inputName"
    $row | Add-Content $BenchmarkFile

    Write-Host "Dimensions: n=$n, m=$m, p=$p"
    Write-Host "Mapper tasks: $MapperTasks"
    Write-Host "Mapper seconds: $($mapperWatch.Elapsed.TotalSeconds)"
    Write-Host "Shuffle seconds: $($shuffleWatch.Elapsed.TotalSeconds)"
    Write-Host "Reducer seconds: $($reducerWatch.Elapsed.TotalSeconds)"
    Write-Host "Total seconds: $totalSeconds"
    Write-Host "Result written to $OutputFile"
    Get-Content $OutputFile
}
finally {
    if ($totalWatch.IsRunning) {
        $totalWatch.Stop()
    }
    Remove-Item $temporaryDirectory -Recurse -Force -ErrorAction SilentlyContinue
}
