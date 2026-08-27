# 市場資料操作手冊（PowerShell）

本文件是給日後執行者與 automation 維護者的操作契約。重點是讓命令列參數、JSON capture、日期與 watchdog 行為保持一致；它不取代 `SKILL.md` 的研究輸出契約。

## 1. 固定工作目錄與路徑

每次使用相對路徑前，先以 `-LiteralPath` 切換到 skill 根目錄：

```powershell
Set-Location -LiteralPath 'C:\Users\a0953041880\.codex\skills\tw-sector-screener'
```

canonical database 固定是：

```powershell
$outputRoot = Join-Path $env:USERPROFILE 'tw-sector-screener-output'
$database = Join-Path $outputRoot 'cache\market\market_data.sqlite'
```

`daily_bars.sqlite` 與 `quarterly_fundamentals.sqlite` 只作遷移來源；日常同步不可把它們當成新的寫入目標。

## 2. 參數規則

### 主題與資料集是單一逗號分隔參數

正確：

```powershell
--themes 'AI,半導體'
--datasets 'daily_bars,index_bars,security_master,monthly_revenue,period_bars'
```

錯誤：

```powershell
--themes AI 半導體
```

錯誤寫法會讓 `半導體` 被 argparse 當成多餘 positional argument，產生 `unrecognized arguments: 半導體`。

### 日期一律使用 ISO 格式

```powershell
--as-of 2026-08-26
```

不可使用 `20260826`。正常交易日的 current-day gate 要求 verified tail 等於 `as_of`；抓不到當日資料時必須失敗，不能用前一天代替。

### 帶路徑的參數要明確引號

```powershell
--output-root "$outputRoot"
--database "$database"
```

不要把 `Set-Location` 的輸出拿來當 JSON 變數。以下是錯誤模式：

```powershell
$verification = Set-Location -LiteralPath 'C:\Users\a0953041880\.codex\skills\tw-sector-screener'; python ... | ConvertFrom-Json
```

正確做法是分開執行 location、命令、exit code 與 JSON parse：

```powershell
Set-Location -LiteralPath 'C:\Users\a0953041880\.codex\skills\tw-sector-screener'
$verificationText = & python scripts\verify_market_data.py `
  --database "$database" `
  --themes 'AI,半導體' `
  --universe-mode coverage `
  --as-of 2026-08-26 `
  --lookback 253 `
  --benchmark TAIEX 2>&1
$verificationExit = $LASTEXITCODE
$verification = ($verificationText -join [Environment]::NewLine) | ConvertFrom-Json
if ($verificationExit -ne 0) {
    throw "market data verification failed with exit code $verificationExit"
}
```

## 3. 正確的 sync → verify 順序

```powershell
Set-Location -LiteralPath 'C:\Users\a0953041880\.codex\skills\tw-sector-screener'
$outputRoot = Join-Path $env:USERPROFILE 'tw-sector-screener-output'
$database = Join-Path $outputRoot 'cache\market\market_data.sqlite'
$asOf = '2026-08-26'

$syncArgs = @(
  'scripts\sync_market_data.py',
  '--themes', 'AI,半導體',
  '--universe-mode', 'coverage',
  '--as-of', $asOf,
  '--lookback', '253',
  '--mode', 'incremental',
  '--datasets', 'daily_bars,index_bars,security_master,monthly_revenue,period_bars',
  '--output-root', $outputRoot,
  '--database', $database
)
$syncOutput = & python @syncArgs 2>&1
$syncExit = $LASTEXITCODE
$syncOutput | Out-Host
if ($syncExit -ne 0) { throw "sync failed with exit code $syncExit" }

$verifyArgs = @(
  'scripts\verify_market_data.py',
  '--database', $database,
  '--themes', 'AI,半導體',
  '--universe-mode', 'coverage',
  '--as-of', $asOf,
  '--lookback', '253',
  '--benchmark', 'TAIEX',
  '--output', (Join-Path $outputRoot "audit\$($asOf.Replace('-', ''))\market-data-verify-$($asOf.Replace('-', '')).json")
)
$verifyOutput = & python @verifyArgs 2>&1
$verifyExit = $LASTEXITCODE
$verifyOutput | Out-Host
if ($verifyExit -ne 0) { throw "verification failed with exit code $verifyExit" }
```

`sync_market_data.py` 會更新 canonical DB 與 market-sync manifest；`verify_market_data.py` 是 read-only gate，不初始化、不遷移、不修補 DB。只有 verification status 為 `complete` 才能進入主題報告。

## 4. Watchdog 操作

每個 theme 應使用獨立 process、獨立 stdout/stderr dated log，並記錄 PID、完整 command line、預期 artifact 與 start time。process 尚未結束時，210 秒只是 progress polling 間隔：

```powershell
while (-not $process.HasExited) {
    Get-Process -Id $process.Id | Select-Object Id,CPU,StartTime,HasExited
    Get-Item -LiteralPath $expectedArtifacts | Select-Object FullName,LastWriteTime,Length
    Start-Sleep -Seconds 210
}
$exitCode = $process.ExitCode
```

若已觀察到 `HasExited`、自然 return 或 exit event，立即驗證 exit code、log 與 artifacts，不必再等待 210 秒。不要用 `timeout_ms` 當 progress probe；不要因為安靜就 kill、force-kill 或 relaunch；若 wrapper timeout，先檢查 child PID、log 與 artifacts。

## 5. 常見故障判讀

| 現象 | 判讀與處理 |
|---|---|
| `unrecognized arguments: 半導體` | `--themes` 沒有使用單一 `'AI,半導體'` 參數。 |
| `Cannot call a method on a null-valued expression` | PowerShell 把 `Set-Location` 放進 JSON assignment；拆成獨立 statement。 |
| verification JSON 是空值或 parse error | 先保存 stdout/stderr、檢查 exit code，再對 `$verificationText` 做 `ConvertFrom-Json`。 |
| 最新日線不是 `as_of` | 這是 current-day gate 失敗；不可用舊 cache 靜默補值。 |
| DB 有 253 根但 sync 仍抓取 | 先檢查 current-day marker、`market_data_sync_state` 與 provider provenance；253 根本身不代表今日已驗證。 |
| 308 出現在 audit | 只有安全同源 HTTPS、allowlist 內、無循環且保留 method/body 的 redirect 才可視為 recovered；拒絕或 fallback 全失敗要維持 failed。 |

## 6. 執行前後 checklist

- [ ] `Set-Location -LiteralPath` 已獨立執行。
- [ ] `--themes`、`--datasets` 各自是單一逗號分隔參數。
- [ ] `--as-of` 是 `YYYY-MM-DD`，且符合當次資料日期。
- [ ] sync 使用 canonical `market_data.sqlite`，不是 legacy SQLite。
- [ ] sync manifest 與 verify JSON 均存在且可解析。
- [ ] verify status 是 `complete`，AI／半導體 coverage 與 TAIEX current-day gate 通過。
- [ ] 報告 process 有 PID、log、start time 與預期 artifact 記錄。
- [ ] process 未結束時只按 210 秒間隔 read-only polling。
- [ ] 報告四件 daily artifacts 都 fresh、可解析且 CSV 達 Top 30 gate。
