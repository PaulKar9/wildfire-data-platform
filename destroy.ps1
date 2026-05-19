<#
.SYNOPSIS
    Tear down a Wildfire Platform environment — drops catalog, workspace folder, and job.
    Does NOT touch the Git Folder (repo link) — that's shared across environments.

.EXAMPLE
    .\destroy.ps1 -WorkspaceHost "https://dbc-xxx.cloud.databricks.com" -Token "dapi..." -EnvName ml
    .\destroy.ps1 ... -EnvName ml -Force    # skip confirmation prompt
#>
param(
    [Parameter(Mandatory=$true)] [string]$WorkspaceHost,
    [Parameter(Mandatory=$true)] [string]$Token,
    [Parameter(Mandatory=$true)] [string]$EnvName,
    [switch]$Force
)

Set-StrictMode -Version Latest

function Log  { param($msg) Write-Host "[INFO]  $msg" -ForegroundColor Cyan }
function Ok   { param($msg) Write-Host "[OK]    $msg" -ForegroundColor Green }
function Warn { param($msg) Write-Host "[WARN]  $msg" -ForegroundColor Yellow }
function Fail { param($msg) Write-Host "[FAIL]  $msg" -ForegroundColor Red; exit 1 }

$hdrs = @{ Authorization = "Bearer $Token"; "Content-Type" = "application/json" }

function Invoke-DB {
    param([string]$Ver="2.0", [string]$Method="GET", [string]$Path, $Body=$null)
    $uri  = "$WorkspaceHost/api/$Ver/$Path"
    $json = if ($Body) { $Body | ConvertTo-Json -Depth 10 } else { $null }
    try {
        Invoke-RestMethod -Uri $uri -Headers $hdrs -Method $Method -Body $json -ContentType "application/json"
    } catch {
        $msg = $_.Exception.Message
        if ($_.Exception.Response) {
            try {
                $stream = $_.Exception.Response.GetResponseStream()
                $reader = [System.IO.StreamReader]::new($stream)
                $msg   += " — " + $reader.ReadToEnd()
            } catch {}
        }
        throw $msg
    }
}

function Run-SQL {
    param([string]$Sql, [string]$WarehouseId)
    $body = @{ statement = $Sql; warehouse_id = $WarehouseId; wait_timeout = "50s"; disposition = "INLINE"; format = "JSON_ARRAY" } | ConvertTo-Json -Depth 5
    $resp = Invoke-RestMethod -Uri "$WorkspaceHost/api/2.0/sql/statements" -Headers $hdrs -Method POST -Body $body -ContentType "application/json"
    $id   = $resp.statement_id
    while ($resp.status.state -in @("PENDING","RUNNING")) {
        Start-Sleep -Seconds 5
        $resp = Invoke-RestMethod -Uri "$WorkspaceHost/api/2.0/sql/statements/$id" -Headers $hdrs -Method GET
    }
    if ($resp.status.state -ne "SUCCEEDED") { Warn "SQL '$Sql' ended: $($resp.status.state) — $($resp.status.error.message)" }
    return $resp
}

# ── Guard ─────────────────────────────────────────────────────────────────────
$CATALOG  = "wildfire_$EnvName"
$WsPath   = "/Shared/wildfire_$EnvName"
$JobName  = "wildfire-$EnvName-pipeline"

if ($EnvName -eq "poc" -and -not $Force) {
    Fail "Refusing to destroy 'poc' (prod) without -Force. Are you sure? Use -Force to override."
}

Write-Host ""
Write-Host "⚠  About to DESTROY environment: $EnvName" -ForegroundColor Red
Write-Host "   Catalog  : $CATALOG  (DROP CASCADE — all Delta tables deleted)" -ForegroundColor Red
Write-Host "   Folder   : $WsPath" -ForegroundColor Red
Write-Host "   Job      : $JobName" -ForegroundColor Red
Write-Host ""

if (-not $Force) {
    $confirm = Read-Host "Type the environment name to confirm: "
    if ($confirm -ne $EnvName) { Fail "Confirmation mismatch — aborting." }
}

# ── Step 1: Cancel any active runs ────────────────────────────────────────────
Log "Cancelling active runs for '$JobName'..."
try {
    $jobs = (Invoke-DB -Ver "2.1" -Path "jobs/list").jobs
    $job  = $jobs | Where-Object { $_.settings.name -eq $JobName }
    if ($job) {
        $runResp = Invoke-DB -Ver "2.1" -Path "jobs/runs/list?job_id=$($job.job_id)&active_only=true"
        $runs = if ($runResp.PSObject.Properties["runs"]) { $runResp.runs } else { @() }
        foreach ($run in $runs) {
            Invoke-DB -Ver "2.1" -Method POST -Path "jobs/runs/cancel" -Body @{ run_id = $run.run_id } | Out-Null
            Log "  Cancelled run $($run.run_id)"
        }
    }
} catch { Warn "Could not list/cancel runs: $_" }

# ── Step 2: Delete the job ────────────────────────────────────────────────────
Log "Deleting job '$JobName'..."
try {
    $jobs = (Invoke-DB -Ver "2.1" -Path "jobs/list").jobs
    $job  = $jobs | Where-Object { $_.settings.name -eq $JobName }
    if ($job) {
        Invoke-DB -Ver "2.1" -Method POST -Path "jobs/delete" -Body @{ job_id = $job.job_id } | Out-Null
        Ok "  Job deleted (id=$($job.job_id))."
    } else { Warn "  Job '$JobName' not found — skipping." }
} catch { Warn "  Job delete failed: $_" }

# ── Step 3: Drop Unity Catalog (CASCADE) ──────────────────────────────────────
Log "Dropping catalog '$CATALOG' (CASCADE)..."
try {
    $wh = (Invoke-RestMethod -Uri "$WorkspaceHost/api/2.0/sql/warehouses" -Headers $hdrs -Method GET).warehouses
    $warehouseId = ($wh | Select-Object -First 1).id
    if (-not $warehouseId) { Warn "No SQL warehouse found — catalog must be dropped manually." }
    else {
        Run-SQL -Sql "DROP CATALOG IF EXISTS $CATALOG CASCADE" -WarehouseId $warehouseId | Out-Null
        Ok "  Catalog $CATALOG dropped."
    }
} catch { Warn "  Catalog drop failed: $_" }

# ── Step 4: Delete workspace folder ───────────────────────────────────────────
Log "Deleting workspace folder '$WsPath'..."
try {
    Invoke-DB -Method POST -Path "workspace/delete" -Body @{ path = $WsPath; recursive = $true } | Out-Null
    Ok "  Folder $WsPath deleted."
} catch { Warn "  Folder delete failed (may not exist): $_" }

# ── Done ──────────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "══════════════════════════════════════════════" -ForegroundColor Green
Write-Host "  Environment '$EnvName' destroyed." -ForegroundColor White
Write-Host "  Catalog $CATALOG — gone." -ForegroundColor White
Write-Host "  To recreate: .\deploy.ps1 ... -EnvName $EnvName" -ForegroundColor Gray
Write-Host "══════════════════════════════════════════════" -ForegroundColor Green
