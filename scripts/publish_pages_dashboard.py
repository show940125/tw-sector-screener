from __future__ import annotations

import argparse
import html
import json
import shutil
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stage simulator dashboard outputs for GitHub Pages.")
    parser.add_argument("--run-dir", required=True, help="Path to simulations/<run_id>.")
    parser.add_argument("--site-dir", required=True, help="Output directory uploaded to Pages.")
    parser.add_argument("--date", default=None, help="Archive date in YYYYMMDD or YYYY-MM-DD. Defaults to summary end_date/as_of.")
    parser.add_argument("--base-url", default="", help="Optional absolute GitHub Pages base URL.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_dir = Path(args.run_dir).resolve()
    site_dir = Path(args.site_dir).resolve()
    summary_path = run_dir / "summary.json"
    dashboard_path = run_dir / "dashboard.html"
    equity_path = run_dir / "daily-equity.csv"
    for path in [summary_path, dashboard_path, equity_path]:
        if not path.exists():
            raise SystemExit(f"missing required simulator artifact: {path}")

    summary = _load_json(summary_path)
    date_tag = _resolve_date_tag(args.date, summary)
    latest_dir = site_dir / "latest"
    archive_dir = site_dir / "archive" / date_tag
    for target in [latest_dir, archive_dir]:
        target.mkdir(parents=True, exist_ok=True)
        shutil.copy2(dashboard_path, target / "dashboard.html")
        shutil.copy2(summary_path, target / "summary.json")
        shutil.copy2(equity_path, target / "daily-equity.csv")

    manifest = _manifest(summary, date_tag, args.base_url)
    site_dir.mkdir(parents=True, exist_ok=True)
    (site_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    (site_dir / "index.html").write_text(_index_html(summary, manifest), encoding="utf-8")
    return 0


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    return payload if isinstance(payload, dict) else {}


def _resolve_date_tag(value: str | None, summary: dict[str, Any]) -> str:
    if value and value.strip().lower() == "today":
        value = None
    raw = value or summary.get("end_date") or summary.get("as_of") or date.today().isoformat()
    raw_text = str(raw).strip()
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw_text[:10], fmt).strftime("%Y%m%d")
        except ValueError:
            continue
    return date.today().strftime("%Y%m%d")


def _url(base_url: str, path: str) -> str:
    if not base_url:
        return path
    return f"{base_url.rstrip('/')}/{path.lstrip('/')}"


def _manifest(summary: dict[str, Any], date_tag: str, base_url: str) -> dict[str, Any]:
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    return {
        "generated_at": generated_at,
        "date": date_tag,
        "run_id": summary.get("run_id"),
        "themes": summary.get("themes") or [],
        "latest": {
            "dashboard_url": _url(base_url, "latest/dashboard.html"),
            "summary_url": _url(base_url, "latest/summary.json"),
            "daily_equity_url": _url(base_url, "latest/daily-equity.csv"),
        },
        "archive": {
            "dashboard_url": _url(base_url, f"archive/{date_tag}/dashboard.html"),
            "summary_url": _url(base_url, f"archive/{date_tag}/summary.json"),
            "daily_equity_url": _url(base_url, f"archive/{date_tag}/daily-equity.csv"),
        },
    }


def _index_html(summary: dict[str, Any], manifest: dict[str, Any]) -> str:
    themes = ", ".join(str(item) for item in manifest.get("themes") or []) or "N/A"
    latest = manifest.get("latest") or {}
    archive = manifest.get("archive") or {}
    market = summary.get("market_status") or {}
    note = market.get("note") or ""
    rows = [
        ("Latest dashboard", latest.get("dashboard_url")),
        ("Latest summary.json", latest.get("summary_url")),
        ("Latest daily-equity.csv", latest.get("daily_equity_url")),
        ("Archived dashboard", archive.get("dashboard_url")),
        ("Archived summary.json", archive.get("summary_url")),
        ("Archived daily-equity.csv", archive.get("daily_equity_url")),
    ]
    link_items = "\n".join(
        f'<li><a href="{html.escape(str(url or "#"))}">{html.escape(label)}</a></li>'
        for label, url in rows
    )
    return f"""<!doctype html>
<html lang="zh-Hant">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>TW Sector Screener Daily Dashboard</title>
  <style>
    body {{ margin: 0; font-family: Segoe UI, Noto Sans TC, Arial, sans-serif; color: #172033; background: #f7f8fb; }}
    main {{ max-width: 920px; margin: 0 auto; padding: 40px 24px; }}
    h1 {{ margin: 0 0 8px; font-size: 30px; }}
    .muted {{ color: #667085; }}
    .panel {{ margin-top: 24px; padding: 20px; background: #fff; border: 1px solid #d9dee8; border-radius: 8px; }}
    li {{ margin: 10px 0; }}
    a {{ color: #155eef; }}
  </style>
</head>
<body>
  <main>
    <h1>TW Sector Screener Daily Dashboard</h1>
    <p class="muted">Latest run: {html.escape(str(manifest.get("date") or "N/A"))} ｜ run id: {html.escape(str(manifest.get("run_id") or "N/A"))} ｜ themes: {html.escape(themes)}</p>
    <div class="panel">
      <h2>Latest</h2>
      <ul>
        {link_items}
      </ul>
    </div>
    <div class="panel">
      <h2>Market Status</h2>
      <p>{html.escape(str(note or "No market status note."))}</p>
    </div>
    <p class="muted">Generated at {html.escape(str(manifest.get("generated_at") or ""))}</p>
  </main>
</body>
</html>
"""


if __name__ == "__main__":
    raise SystemExit(main())
