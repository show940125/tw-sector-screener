# 市場資料操作手冊（PowerShell）

本手冊是日後執行者與 automation 維護者的操作契約。它特別固定工作目錄、逗號參數、ISO 日期、JSON capture、SQLite 讀寫邊界與 watchdog 行為，避免再次因 PowerShell 參數/管線誤用造成假成功或長時間停滯。

## 1. 固定工作目錄與變數

每次使用相對路徑前，先獨立執行以下命令；不要把 `Set-Location` 放進會被當成 JSON 或 stdout 的 assignment：

```powershell
Set-Location -LiteralPath 'C:\Users\a0953041880\.codex\skills\tw-sector-screener'
$outputRoot = Join-Path $env:USERPROFILE 'tw-sector-screener-output'
$database = Join-Path $outputRoot 'cache\market\market_data.sqlite'
$day = '2026-08-27'
$dayKey = $day.Replace('-', '')
```

`market_data.sqlite` 是唯一日常寫入目標；`daily_bars.sqlite` 與 `quarterly_fundamentals.sqlite` 只作保留的 migration source。SQLite、raw payload 與完整執行輸出留在 output root，不上傳 repo。

## 2. PowerShell 參數規則

### 主題/資料集是單一逗號參數

正確：

```powershell
--themes 'AI,半導體'
--datasets 'daily_bars,index_bars,security_master,monthly_revenue,period_bars'
```

錯誤：

```powershell
--themes AI 半導體
```

後者會讓 `半導體` 成為多餘 positional argument，出現 `unrecognized arguments: 半導體`。

### 日期固定 ISO

```powershell
--as-of 2026-08-27
--from-date 2021-01-01
--to-date 2026-08-27
```

不可使用 `20260827`。`daily` profile 省略 `--from-date` 時，provider 回傳以 `to-date/as-of` 結尾的 lookback window；若指定 from-date，才視為明確歷史 range。

### 帶路徑參數要引號

```powershell
--output-root "$outputRoot"
--database "$database"
```

不要以這種方式 capture：

```powershell
$verification = Set-Location -LiteralPath 'C:\Users\a0953041880\.codex\skills\tw-sector-screener'; python ... | ConvertFrom-Json
```

正確做法是分開 location、命令、exit code 與 JSON parse：

```powershell
Set-Location -LiteralPath 'C:\Users\a0953041880\.codex\skills\tw-sector-screener'
$verificationText = & python scripts\verify_market_data.py `
  --database "$database" `
  --themes 'AI,半導體' `
  --universe-mode coverage `
  --as-of $day `
  --lookback 253 `
  --benchmark TAIEX 2>&1
$verificationExit = $LASTEXITCODE
$verificationText | Out-Host
if ($verificationExit -eq 0) {
    $verification = ($verificationText -join [Environment]::NewLine) | ConvertFrom-Json
} else {
    # 失敗輸出仍要先保存；只有確認是 JSON 才 ConvertFrom-Json。
    $verification = $null
}
```

PowerShell 的 `$PID` 是唯讀的目前 shell process id，watchdog 請使用
`$watchPid`、`$aiProcess` 等 task-specific 變數；`Start-Process -ArgumentList`
的第一個元素也必須是 Python script path（例如
`'scripts\tw_sector_screener.py'`），不能只放 `--theme`。

## 3. Sync profile

### daily：16:30 日報前的短路徑

daily 預設資料集是 `daily_bars,index_bars,security_master,monthly_revenue,period_bars`。它先查 canonical DB，完整 253 根且 current-day marker 正確時不發歷史月份 request；只補缺口與當日。正常市場日任何候選或 TAIEX 沒有 `to-date` 都失敗。

```powershell
python scripts\sync_market_data.py `
  --profile daily `
  --themes 'AI,半導體' `
  --universe-mode coverage `
  --as-of $day `
  --lookback 253 `
  --mode incremental `
  --datasets 'daily_bars,index_bars,security_master,monthly_revenue,period_bars' `
  --output-root "$outputRoot" `
  --database "$database"
```

### enrichment：受控的長期回補

歷史回補使用 `--profile enrichment`，明確指定 from/to；不要塞入日報 watchdog。尚未有 validated adapter 的資料集被選中時，命令會輸出 `status=failed`、`not_implemented_datasets` 並以非零 exit code 結束，不能被當成「已處理」。

```powershell
python scripts\sync_market_data.py `
  --profile enrichment `
  --themes 'AI,半導體' `
  --universe-mode coverage `
  --as-of $day `
  --from-date 2021-01-01 `
  --to-date $day `
  --datasets 'monthly_revenue' `
  --mode incremental `
  --output-root "$outputRoot" `
  --database "$database"
```

注意：`--profile enrichment` 不是「把所有 catalog dataset 靜默跑一遍」；每個 dataset 必須有 adapter、parse、validate、upsert、completeness report 與 PIT tests 後才能納入。

## 4. Sync → verify → report 順序

同步 manifest 位於 `audit\YYYYMMDD\market-sync-YYYYMMDD.json/.md`。接著執行唯讀 verification：

```powershell
python scripts\verify_market_data.py `
  --database "$database" `
  --themes 'AI,半導體' `
  --universe-mode coverage `
  --as-of $day `
  --lookback 253 `
  --benchmark TAIEX `
  --output (Join-Path $outputRoot "audit\$dayKey\market-data-verify-$dayKey.json")
```

`verify_market_data.py` 使用 SQLite `mode=ro`/`query_only`，不初始化、不 migration、不修補。它會檢查 schema v3、SQLite integrity、FK、研究表、每檔 coverage、253 bars、current-day marker 與 TAIEX。非零 exit 或 JSON status 非 `complete` 時，不得開始報告。

報告命令仍必須明確帶：

```text
--theme AI|半導體
--universe-mode coverage
--benchmark TAIEX
--as-of YYYY-MM-DD
--top-n 30
--lookback 253
--market-database <outputRoot>\cache\market\market_data.sqlite
--recommendation-mode deterministic
--output-format md,json,csv
--quality-update-mode auto
--quality-update-budget-sec 3
--quality-history-depth 8
--output-root <outputRoot>
```

星期一才加 `--run-backtest`；其他工作日不要加。報告 gate 仍要求 fresh MD/JSON/CSV/audit、CSV 至少 30 rows、`ranked_count == attempted_count == coverage_count`、current-day 與 unresolved 308 通過。

## 5. Dry-run 與資料庫邊界

```powershell
python scripts\sync_market_data.py `
  --profile daily `
  --themes 'AI,半導體' `
  --universe-mode coverage `
  --as-of $day `
  --lookback 253 `
  --dry-run `
  --output-root "$outputRoot" `
  --database "$database"
```

dry-run 只計算 coverage 與輸出 planned manifest，不建立 canonical DB、不初始化 schema、不查網路、不寫 source payload。需要 schema migration、upsert 或 period rebuild 時，才執行非 dry-run 的 sync；執行前要保存 DB row counts、integrity 與必要的 hash baseline。

## 6. Watchdog 契約

每個 theme 使用獨立 process，stdout/stderr redirect 到：

```text
%USERPROFILE%\tw-sector-screener-output\logs\YYYYMMDD\
```

啟動前要記錄 PID、完整 command line、預期 artifacts、database path 與 start time。PowerShell 範例：

```powershell
$logDir = Join-Path $outputRoot "logs\$dayKey"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$stdout = Join-Path $logDir 'ai-sync.stdout.log'
$stderr = Join-Path $logDir 'ai-sync.stderr.log'
$arguments = @(
  'scripts\sync_market_data.py', '--profile', 'daily', '--themes', 'AI,半導體',
  '--universe-mode', 'coverage', '--as-of', $day, '--lookback', '253',
  '--output-root', $outputRoot, '--database', $database
)
$process = Start-Process -FilePath 'python' -ArgumentList $arguments -WorkingDirectory 'C:\Users\a0953041880\.codex\skills\tw-sector-screener' -RedirectStandardOutput $stdout -RedirectStandardError $stderr -PassThru -WindowStyle Hidden
"pid=$($process.Id) start=$([DateTime]::Now.ToString('o')) command=python $($arguments -join ' ')" | Set-Content -LiteralPath (Join-Path $logDir 'ai-sync.process.txt')
```

若 process 尚未結束，progress polling 間隔不得短於 210 秒：

```powershell
while (-not $process.HasExited) {
    Get-Process -Id $process.Id | Select-Object Id,CPU,StartTime,HasExited
    Get-Item -LiteralPath $expectedArtifacts -ErrorAction SilentlyContinue | Select-Object FullName,LastWriteTime,Length
    Start-Sleep -Seconds 210
}
$exitCode = $process.ExitCode
```

自然 return、`HasExited` 或 exit event 一旦可觀察，立即檢查 exit code、logs 與 artifacts，不必再等 210 秒。不要以 `timeout_ms` 作 progress probe；不要因安靜、CPU 低或暫時無輸出而 kill、force-kill、relaunch 或 duplicate。wrapper timeout 時先檢查 child PID、logs、DB state 與 artifacts。

## 7. 常見故障判讀

| 現象 | 判讀 |
|---|---|
| `unrecognized arguments: 半導體` | `--themes` 沒有用單一逗號參數。 |
| `Cannot call a method on a null-valued expression` | location 與 JSON capture 混在同一 statement。 |
| sync 只回一筆日線、253 gate 失敗 | 不應把 daily 預設 `as_of` 當 `from_date`；省略 from-date 取得 lookback。 |
| DB 有 253 根但仍抓網路 | 檢查 `last_current_day_verified_date`；253 根不代表今日來源已驗證。 |
| 最新日線不是 as-of | current-day gate 失敗；不可用前一天或舊 cache 補值。 |
| 308 出現在 audit | 安全導向成功才算 recovered；拒絕/循環/超層數或 fallback 全失敗要保持 failed。 |
| enrichment 顯示 `not_implemented` | adapter 尚未交付；保留 failed manifest，不能改成 warning 後繼續。 |

## 8. 執行前後 checklist

- [ ] `Set-Location -LiteralPath` 已獨立執行。
- [ ] `--themes`、`--datasets` 各自是單一逗號分隔參數。
- [ ] 所有日期是 `YYYY-MM-DD`。
- [ ] daily 使用 canonical `market_data.sqlite`，省略 from-date 或明確 range；沒有用 as-of 冒充 from-date。
- [ ] dry-run 沒有建立/改動 DB；非 dry-run 前有 baseline。
- [ ] sync JSON/Markdown manifest 存在且可解析。
- [ ] read-only verify JSON status 是 `complete`。
- [ ] report process 有 PID、log、start time、command 與 expected artifacts。
- [ ] process 尚未結束時只按至少 210 秒 read-only polling。
- [ ] 每個 theme 的四件 daily artifacts fresh、非空/可解析、CSV 達 Top 30 gate。
