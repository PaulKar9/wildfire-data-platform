<#
.SYNOPSIS
    Deploy the Wildfire Data Platform to a Databricks workspace environment.

.EXAMPLE
    # Deploy / refresh prod
    .\deploy.ps1 -WorkspaceHost "https://dbc-xxx.cloud.databricks.com" -Token "dapi..." -EnvName poc

    # Spin up a new ML environment (full isolation — own catalog + data)
    .\deploy.ps1 -WorkspaceHost "https://dbc-xxx.cloud.databricks.com" -Token "dapi..." -EnvName ml `
                 -GitHubUser "youruser" -GitHubRepo "wildfire-data-platform" -GitHubToken "ghp_..."

    # Fast redeploy — skip CSV upload (data already there)
    .\deploy.ps1 ... -EnvName ml -SkipDataUpload

    # Deploy and immediately run the pipeline
    .\deploy.ps1 ... -EnvName ml -RunPipeline
#>
param(
    [Parameter(Mandatory=$true)]  [string]$WorkspaceHost,
    [Parameter(Mandatory=$true)]  [string]$Token,
    [string]$EnvName        = "poc",
    [string]$Branch         = "main",
    [string]$GitHubUser     = "",
    [string]$GitHubRepo     = "wildfire-data-platform",
    [string]$GitHubToken    = "",
    [switch]$SkipDataUpload,
    [switch]$RunPipeline,
    [string]$LocalRoot      = "C:\Users\User\modern_data_platform"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ── Helpers ───────────────────────────────────────────────────────────────────
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
                $msg   += ": " + $reader.ReadToEnd()
            } catch {}
        }
        throw $msg
    }
}

function Upload-Notebook {
    param([string]$LocalFile, [string]$RemotePath)
    $bytes = [System.IO.File]::ReadAllBytes($LocalFile)
    if ($bytes.Length -ge 3 -and $bytes[0] -eq 0xEF -and $bytes[1] -eq 0xBB -and $bytes[2] -eq 0xBF) {
        $bytes = $bytes[3..($bytes.Length - 1)]
    }
    Invoke-DB -Method POST -Path "workspace/import" -Body @{
        path = $RemotePath; format = "SOURCE"; language = "PYTHON"
        content = [Convert]::ToBase64String($bytes); overwrite = $true
    } | Out-Null
}

function Upload-WorkspaceFile {
    param([string]$LocalFile, [string]$RemotePath)
    $bytes   = [System.IO.File]::ReadAllBytes($LocalFile)
    $b64     = [Convert]::ToBase64String($bytes)
    Invoke-DB -Method POST -Path "workspace/import" -Body @{
        path = $RemotePath; format = "AUTO"; overwrite = $true; content = $b64
    } | Out-Null
}

function Wait-ForRun {
    param([long]$RunId)
    do {
        $r     = Invoke-DB -Ver "2.1" -Path "jobs/runs/get?run_id=$RunId"
        $state = $r.state.life_cycle_state
        $tasks = $r.tasks | ForEach-Object {
            $rs = if ($_.state.PSObject.Properties["result_state"]) { $_.state.result_state } else { "" }
            "  $($_.task_key.PadRight(15)) $($_.state.life_cycle_state) $rs"
        }
        Write-Host "`r$state - $(Get-Date -Format 'HH:mm:ss')" -NoNewline
        if ($state -notin @("PENDING","RUNNING","WAITING_FOR_RETRY")) { break }
        Start-Sleep -Seconds 12
    } while ($true)
    Write-Host ""
    $tasks | ForEach-Object { Write-Host $_ }
    return $r.state.result_state
}

# ── Derived names ─────────────────────────────────────────────────────────────
$CATALOG       = "wildfire_$EnvName"
$WsPath        = "/Shared/wildfire_$EnvName"
$JobName       = "wildfire-$EnvName-pipeline"
$UseGitFolders = $GitHubUser -ne ""

Log "=== Wildfire Platform Deploy ==="
Log "Environment  : $EnvName"
Log "Catalog      : $CATALOG"
Log "Workspace    : $WsPath"
Log "Git Folders  : $UseGitFolders"
if ($UseGitFolders) { Log "Branch       : $Branch" }

# ── Step 0: Verify connectivity ───────────────────────────────────────────────
Log "Verifying workspace connectivity..."
try { Invoke-DB -Path "clusters/list" | Out-Null; Ok "Connected to $WorkspaceHost" }
catch { Fail "Cannot reach workspace: $_" }

# ── Step 1: Workspace folders ─────────────────────────────────────────────────
Log "Creating workspace folders..."
Invoke-DB -Method POST -Path "workspace/mkdirs" -Body @{ path = $WsPath } | Out-Null
Invoke-DB -Method POST -Path "workspace/mkdirs" -Body @{ path = "$WsPath/data" } | Out-Null
Ok "Workspace folders: $WsPath"

# ── Step 2: Notebooks — Git Folders or direct upload ─────────────────────────
if ($UseGitFolders) {
    Log "Configuring Git Folders integration..."

    # Store / update git credentials
    $existingCreds = Invoke-DB -Path "git-credentials"
    $credList   = if ($existingCreds.PSObject.Properties["credentials"]) { $existingCreds.credentials } else { @() }
    $existingGh = $credList | Where-Object { $_.git_provider -eq "gitHub" }
    if ($existingGh) {
        Log "  Updating existing GitHub credential (id=$($existingGh.credential_id))..."
        Invoke-DB -Method PATCH -Path "git-credentials/$($existingGh.credential_id)" -Body @{
            git_provider          = "gitHub"
            git_username          = $GitHubUser
            personal_access_token = $GitHubToken
        } | Out-Null
    } else {
        Log "  Adding GitHub credential..."
        Invoke-DB -Method POST -Path "git-credentials" -Body @{
            git_provider          = "gitHub"
            git_username          = $GitHubUser
            personal_access_token = $GitHubToken
        } | Out-Null
    }
    Ok "  GitHub credentials stored."

    # Resolve workspace user email for the Repos path
    $me        = Invoke-DB -Path "preview/scim/v2/Me"
    $wsEmail   = $me.emails | Where-Object { $_.primary -eq $true } | Select-Object -ExpandProperty value
    if (-not $wsEmail) { $wsEmail = $me.userName }
    Log "  Workspace user: $wsEmail"

    # Create or update the Git Folder (Repo)
    $repoUrl    = "https://github.com/$GitHubUser/$GitHubRepo"
    $repoPath   = "/Repos/$wsEmail/$GitHubRepo"
    $existingRepos = Invoke-DB -Path "repos?path_prefix=/Repos/$wsEmail"
    $repoList   = if ($existingRepos.PSObject.Properties["repos"]) { $existingRepos.repos } else { @() }
    $existing   = $repoList | Where-Object { $_.path -eq $repoPath }

    if ($existing) {
        Log "  Git Folder exists - checking out branch '$Branch'..."
        Invoke-DB -Method PATCH -Path "repos/$($existing.id)" -Body @{ branch = $Branch } | Out-Null
        $repoId = $existing.id
    } else {
        Log "  Creating Git Folder from $repoUrl (branch: $Branch)..."
        $repo   = Invoke-DB -Method POST -Path "repos" -Body @{
            url      = $repoUrl
            provider = "gitHub"
            path     = $repoPath
        }
        $repoId = $repo.id
        if ($Branch -ne "main") {
            Invoke-DB -Method PATCH -Path "repos/$repoId" -Body @{ branch = $Branch } | Out-Null
        }
    }
    Ok "  Git Folder ready: $repoPath (id=$repoId, branch=$Branch)"
    $NotebookBase = "$repoPath/notebooks"

} else {
    Log "Uploading notebooks (direct)..."
    Invoke-DB -Method POST -Path "workspace/mkdirs" -Body @{ path = "$WsPath/notebooks" } | Out-Null
    $nbs = @("00_setup","01_bronze_ingestion","02_silver_transform","03_gold_analytics","04_data_quality")
    foreach ($nb in $nbs) {
        $local  = Join-Path $LocalRoot "notebooks\$nb.py"
        Upload-Notebook -LocalFile $local -RemotePath "$WsPath/notebooks/$nb"
        Ok "  $nb"
    }
    $NotebookBase = "$WsPath/notebooks"
}

# ── Step 3: CSV data upload ───────────────────────────────────────────────────
if (-not $SkipDataUpload) {
    Log "Uploading CSV source files to $WsPath/data/ ..."
    $csvMap = @{
        "af-historic-wildfires-1961-1982-data (1).csv"  = "af-historic-wildfires-1961-1982-data.csv"
        "af-historic-wildfires-1983-1995-data (1).csv"  = "af-historic-wildfires-1983-1995-data.csv"
        "af-historic-wildfires-1996-2005-data (1).csv"  = "af-historic-wildfires-1996-2005-data.csv"
        "fp-historical-wildfire-data-2006-2025 (1).csv" = "fp-historical-wildfire-data-2006-2025.csv"
    }
    foreach ($src in $csvMap.Keys) {
        $local  = Join-Path $LocalRoot $src
        $remote = "$WsPath/data/$($csvMap[$src])"
        if (-not (Test-Path $local)) { Warn "  Not found locally: $src - skipping."; continue }
        try {
            Upload-WorkspaceFile -LocalFile $local -RemotePath $remote
            Ok "  $($csvMap[$src])"
        } catch { Warn "  Upload failed for $src : $_" }
    }
} else {
    Log "Skipping CSV upload (-SkipDataUpload)."
}

# ── Step 4: Create / update Workflow job ──────────────────────────────────────
Log "Creating job '$JobName'..."

$envParam = @{ env = $EnvName }

$tasks = @(
    @{ task_key = "setup";        notebook_task = @{ notebook_path = "$NotebookBase/00_setup";           base_parameters = $envParam }; depends_on = @() },
    @{ task_key = "bronze";       notebook_task = @{ notebook_path = "$NotebookBase/01_bronze_ingestion"; base_parameters = $envParam }; depends_on = @(@{ task_key = "setup" }) },
    @{ task_key = "silver";       notebook_task = @{ notebook_path = "$NotebookBase/02_silver_transform"; base_parameters = $envParam }; depends_on = @(@{ task_key = "bronze" }) },
    @{ task_key = "gold";         notebook_task = @{ notebook_path = "$NotebookBase/03_gold_analytics";   base_parameters = $envParam }; depends_on = @(@{ task_key = "silver" }) },
    @{ task_key = "data_quality"; notebook_task = @{ notebook_path = "$NotebookBase/04_data_quality";     base_parameters = $envParam }; depends_on = @(@{ task_key = "gold" }) },
    @{ task_key = "ml_features";  notebook_task = @{ notebook_path = "$NotebookBase/05_feature_engineering"; base_parameters = $envParam }; depends_on = @(@{ task_key = "data_quality" }) },
    @{ task_key = "regional_risk"; notebook_task = @{ notebook_path = "$NotebookBase/06_regional_risk_profile"; base_parameters = $envParam }; depends_on = @(@{ task_key = "ml_features" }) }
)

# Find and delete any existing job with the same name so we start fresh
$jobListResp  = Invoke-DB -Ver "2.1" -Path "jobs/list"
$existingJobs = if ($jobListResp.PSObject.Properties["jobs"]) { $jobListResp.jobs } else { @() }
$existing     = $existingJobs | Where-Object { $_.settings.name -eq $JobName }
if ($existing) {
    Log "  Removing old job id=$($existing.job_id)..."
    Invoke-DB -Ver "2.1" -Method POST -Path "jobs/delete" -Body @{ job_id = $existing.job_id } | Out-Null
}

$job = Invoke-DB -Ver "2.1" -Method POST -Path "jobs/create" -Body @{
    name  = $JobName
    tasks = $tasks
    tags  = @{ environment = $EnvName; project = "wildfire-platform" }
}
Ok "Job created: id=$($job.job_id)  name=$JobName"

# ── Step 5: Optionally run ────────────────────────────────────────────────────
if ($RunPipeline) {
    Log "Triggering pipeline run..."
    $run = Invoke-DB -Ver "2.1" -Method POST -Path "jobs/run-now" -Body @{ job_id = $job.job_id }
    Log "  Run id=$($run.run_id) - waiting..."
    $result = Wait-ForRun -RunId $run.run_id
    if ($result -eq "SUCCESS") { Ok "Pipeline completed successfully." }
    else { Fail "Pipeline run ended with result: $result" }
}

# ── Summary ───────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "══════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "  Environment  : $EnvName" -ForegroundColor White
Write-Host "  Catalog      : $CATALOG" -ForegroundColor White
Write-Host "  Job          : $JobName  (id=$($job.job_id))" -ForegroundColor White
Write-Host "  Notebooks    : $NotebookBase" -ForegroundColor White
Write-Host "  Data         : $WsPath/data" -ForegroundColor White
if ($RunPipeline) {
    Write-Host "  Tables       : $CATALOG.{bronze,silver,gold,meta}" -ForegroundColor White
}
Write-Host "══════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""
Write-Host "To run the pipeline:" -ForegroundColor Gray
Write-Host "  .\deploy.ps1 ... -EnvName $EnvName -RunPipeline" -ForegroundColor Gray
Write-Host "To tear down:" -ForegroundColor Gray
Write-Host "  .\destroy.ps1 -WorkspaceHost `"$WorkspaceHost`" -Token `"$Token`" -EnvName $EnvName" -ForegroundColor Gray
