param(
    [switch]$SkipLarge
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

function Ensure-LargeIdentityTest {
    param(
        [string]$Directory,
        [int]$Size = 1000
    )

    $aPath = Join-Path $Directory "matrix_a.txt"
    $bPath = Join-Path $Directory "matrix_b.txt"
    $expectedPath = Join-Path $Directory "expected.txt"
    if ((Test-Path $aPath) -and (Test-Path $bPath) -and (Test-Path $expectedPath)) {
        return
    }

    New-Item -ItemType Directory -Path $Directory -Force | Out-Null
    $aWriter = New-Object System.IO.StreamWriter $aPath
    $bWriter = New-Object System.IO.StreamWriter $bPath
    $expectedWriter = New-Object System.IO.StreamWriter $expectedPath
    try {
        $bWriter.WriteLine("$Size $Size")
        for ($row = 0; $row -lt $Size; $row++) {
            $aValues = New-Object System.Text.StringBuilder
            $bValues = New-Object System.Text.StringBuilder
            $expectedValues = New-Object System.Text.StringBuilder
            [void]$aValues.Append($row)
            [void]$aValues.Append(' ')
            for ($column = 0; $column -lt $Size; $column++) {
                $value = if ($row -eq $column) { 1 } else { 0 }
                if ($column -gt 0) {
                    [void]$bValues.Append(' ')
                    [void]$expectedValues.Append(' ')
                    [void]$aValues.Append(' ')
                }
                [void]$aValues.Append($value)
                [void]$bValues.Append($value)
                [void]$expectedValues.Append($value)
            }
            $aWriter.WriteLine($aValues.ToString())
            $bWriter.WriteLine($bValues.ToString())
            $expectedWriter.WriteLine($expectedValues.ToString())
        }
    }
    finally {
        $aWriter.Dispose()
        $bWriter.Dispose()
        $expectedWriter.Dispose()
    }
}

$largeDirectory = Join-Path $PSScriptRoot "tests\large-1000x1000"
if (-not $SkipLarge) {
    Write-Host "Preparing 1000 x 1000 identity test data..."
    Ensure-LargeIdentityTest -Directory $largeDirectory
}

$temporaryDirectory = Join-Path ([System.IO.Path]::GetTempPath()) ("hw3-mapreduce-" + [System.Guid]::NewGuid())
New-Item -ItemType Directory -Path $temporaryDirectory | Out-Null
$passed = 0

try {
    $testDirectories = Get-ChildItem (Join-Path $PSScriptRoot "tests") -Directory |
        Where-Object { -not $SkipLarge -or $_.Name -ne "large-1000x1000" } |
        Sort-Object Name

    foreach ($testDirectory in $testDirectories) {
        $mapperCounts = if ($testDirectory.Name -eq "large-1000x1000") { @(1) } else { @(1, 2, 3) }
        foreach ($mapperCount in $mapperCounts) {
            $actualOutput = Join-Path $temporaryDirectory ("{0}-{1}.actual" -f $testDirectory.Name, $mapperCount)

            & (Join-Path $PSScriptRoot "run_local.ps1") `
                -AFile (Join-Path $testDirectory.FullName "matrix_a.txt") `
                -BFile (Join-Path $testDirectory.FullName "matrix_b.txt") `
                -MapperTasks $mapperCount `
                -OutputFile $actualOutput | Out-Null

            $expected = @(Get-Content (Join-Path $testDirectory.FullName "expected.txt") | ForEach-Object { $_.Trim() })
            $actual = @(Get-Content $actualOutput | ForEach-Object { $_.Trim() })

            if (($expected -join "`n") -ne ($actual -join "`n")) {
                Write-Host "FAIL: $($testDirectory.Name) with $mapperCount mapper tasks"
                Write-Host "Expected:"
                $expected | Write-Host
                Write-Host "Actual:"
                $actual | Write-Host
                exit 1
            }

            Write-Host "PASS: $($testDirectory.Name) with $mapperCount mapper tasks"
            $passed++
        }
    }

    Write-Host "All $passed matrix/process test combinations passed."
}
finally {
    Remove-Item $temporaryDirectory -Recurse -Force -ErrorAction SilentlyContinue
}
