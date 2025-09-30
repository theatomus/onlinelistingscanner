Param(
  [string]$BackupsRoot = ".\backups\itemcontents",
  [string]$ItemsDir    = ".\item_contents",
  [string]$TrainingDir = ".\training",
  [string]$LLMUrl      = "http://127.0.0.1:8080",
  [string]$LLMFallback = "http://127.0.0.1:8081"
)

$ErrorActionPreference = 'Stop'

function Find-BadDeviceType {
  $expect = @{
    'laptop'  = @('laptop','netbook')
    'desktop' = @('desktop','all-in-one','all in one')
    'tablet'  = @('tablet','ebook')
    'phone'   = @('cell phone','smartphone','phones')
    'server'  = @('server')
    'monitor' = @('monitor')
    'switch'  = @('switch')
    'router'  = @('router')
  }

  Get-ChildItem -Recurse -Path $BackupsRoot -Filter "python_parsed_*.txt" |
    ForEach-Object {
      $txt = Get-Content $_.FullName -Raw
      $leaf = ($txt | Select-String -Pattern '^[\[]leaf_category_key[\]]\s*Category:\s*(.+)$' -AllMatches).Matches.Value |
              ForEach-Object { ($_ -replace '^[\[]leaf_category_key[\]]\s*Category:\s*','').Trim() } | Select-Object -First 1
      $dtype = ($txt | Select-String -Pattern '^[\[]title_device_type_key[\]]\s*device_type:\s*(.+)$' -AllMatches).Matches.Value |
               ForEach-Object { ($_ -replace '^[\[]title_device_type_key[\]]\s*device_type:\s*','').Trim().ToLower() } | Select-Object -First 1
      if ($leaf -and $dtype) {
        $want = $expect[$dtype]
        if ($want -and -not ($want | Where-Object { $leaf.ToLower() -like "*$_*" })) {
          [pscustomobject]@{
            Item       = ([regex]::Match($_.Name,'python_parsed_(\d+)\.txt').Groups[1].Value)
            DeviceType = $dtype
            Leaf       = $leaf
            Path       = $_.FullName
          }
        }
      }
    } | Select-Object -First 1
}

function Ensure-TrainingSchema {
  if (Test-Path (Join-Path $TrainingDir 'schema.json')) { return }
  Write-Host "Building training dataset and schema (one-time)..."
  python .\tools\training\training_data_builder.py --items-dir $ItemsDir --backups-root $BackupsRoot --out (Join-Path $TrainingDir 'training_dataset.json') | Select-String "Wrote|ERROR"
  python .\tools\training\run_workflow.py --items-dir $ItemsDir --backups-root $BackupsRoot --out-dir $TrainingDir | Select-String "Found schema issues|No schema issues|ERROR"
}

function Get-HealthyLLMUrl {
  param([string]$Primary, [string]$Fallback)
  try { $r = Invoke-WebRequest -Uri $Primary -UseBasicParsing -TimeoutSec 1; if ($r.StatusCode -ge 200) { return $Primary } } catch {}
  try { $r = Invoke-WebRequest -Uri $Fallback -UseBasicParsing -TimeoutSec 1; if ($r.StatusCode -ge 200) { return $Fallback } } catch {}
  return $Primary
}

$hit = Find-BadDeviceType
if (-not $hit) {
  Write-Host "No mismatched device_type found in backups." -ForegroundColor Yellow
  return
}

"Item:       $($hit.Item)"
"DeviceType: $($hit.DeviceType)"
"Leaf:       $($hit.Leaf)"
"Path:       $($hit.Path)"

Ensure-TrainingSchema

$Schema = Join-Path $TrainingDir 'schema.json'
$MiniLM = Join-Path $TrainingDir 'mini_lm.json'
$UseUrl = Get-HealthyLLMUrl -Primary $LLMUrl -Fallback $LLMFallback

Write-Host "\nValidating with current system (schema + normalizers + mini-LM + optional LLM/web)..." -ForegroundColor Cyan
$cmd = @(
  'python', '.\tools\training\ai_validator.py', 'validate', $hit.Path,
  '--schema', $Schema,
  '--lm', $MiniLM,
  '--llm-url', $UseUrl,
  '--web-verify'
)

& $cmd | Select-String -Pattern 'Device type|Category mismatch|WEB|Unexpected key|Normalize|Suggest' -Context 0,1


