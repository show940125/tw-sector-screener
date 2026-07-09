from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import tempfile
import uuid
from pathlib import Path
from typing import Any

from publish_pages_dashboard import main as stage_pages_main


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish the latest local simulator dashboard to gh-pages.")
    parser.add_argument("--run-dir", required=True, help="Path to simulations/<run_id>.")
    parser.add_argument("--repo-root", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--date", default=None, help="Archive date in YYYYMMDD or YYYY-MM-DD. Defaults to summary end_date/as_of.")
    parser.add_argument("--base-url", default="https://show940125.github.io/tw-sector-screener")
    parser.add_argument("--remote", default="origin")
    parser.add_argument("--pages-branch", default="gh-pages")
    parser.add_argument(
        "--tmp-root",
        default=None,
        help="Writable temporary root for staging and gh-pages worktree. Defaults to USERPROFILE/tw-sector-screener-output/.pages-publish-tmp.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_dir = Path(args.run_dir).resolve()
    repo_root = Path(args.repo_root).resolve()
    summary = _load_json(run_dir / "summary.json")
    date_tag = _date_tag(args.date, summary)
    tmp_root = _resolve_tmp_root(args.tmp_root)
    _verify_tmp_root(tmp_root)
    stage_tmp_path: Path | None = None
    work_tmp_path: Path | None = None
    with tempfile.TemporaryDirectory(prefix="tw-sector-pages-stage-", dir=str(tmp_root), ignore_cleanup_errors=True) as stage_tmp, tempfile.TemporaryDirectory(
        prefix="tw-sector-gh-pages-", dir=str(tmp_root), ignore_cleanup_errors=True
    ) as work_tmp:
        stage_tmp_path = Path(stage_tmp).resolve()
        work_tmp_path = Path(work_tmp).resolve()
        stage_dir = Path(stage_tmp).resolve()
        work_dir = (Path(work_tmp) / "worktree").resolve()
        _stage(run_dir, stage_dir, date_tag, args.base_url)
        _run(["git", "fetch", args.remote, args.pages_branch], cwd=repo_root)
        _run(["git", "worktree", "add", str(work_dir), f"{args.remote}/{args.pages_branch}"], cwd=repo_root)
        try:
            _copy_public_files(stage_dir, work_dir, date_tag)
            _run(["git", "add", "index.html", "manifest.json", "latest", f"archive/{date_tag}"], cwd=work_dir)
            if _has_changes(work_dir):
                _run(["git", "commit", "-m", f"publish: update latest dashboard {date_tag}"], cwd=work_dir)
                _run(["git", "push", args.remote, f"HEAD:{args.pages_branch}"], cwd=work_dir)
                print(f"[publish-pages] updated {args.pages_branch} latest dashboard to {date_tag}")
            else:
                print(f"[publish-pages] no dashboard changes for {date_tag}")
        finally:
            _run(["git", "worktree", "remove", str(work_dir), "--force"], cwd=repo_root, check=False)
    _warn_if_residual_tmp([stage_tmp_path, work_tmp_path])
    return 0


def _resolve_tmp_root(value: str | None) -> Path:
    if value:
        return Path(value).expanduser().resolve()
    userprofile = Path.home()
    return (userprofile / "tw-sector-screener-output" / ".pages-publish-tmp").resolve()


def _verify_tmp_root(tmp_root: Path) -> None:
    try:
        tmp_root.mkdir(parents=True, exist_ok=True)
        probe = tmp_root / f".write-probe-{uuid.uuid4().hex}"
        probe.mkdir()
        marker = probe / "ok.txt"
        marker.write_text("ok", encoding="utf-8")
        marker.unlink()
        probe.rmdir()
    except OSError as exc:
        raise SystemExit(f"temporary publish root is not writable: {tmp_root} ({exc})") from exc


def _warn_if_residual_tmp(paths: list[Path | None]) -> None:
    residual = [path for path in paths if path and path.exists()]
    if residual:
        joined = "; ".join(str(path) for path in residual)
        print(f"[publish-pages] warning: temporary cleanup left residual path(s): {joined}")


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    return payload if isinstance(payload, dict) else {}


def _date_tag(value: str | None, summary: dict[str, Any]) -> str:
    raw = value or summary.get("end_date") or summary.get("as_of")
    if not raw:
        raise SystemExit("cannot resolve publish date from --date or summary.json")
    return str(raw)[:10].replace("-", "")


def _stage(run_dir: Path, stage_dir: Path, date_tag: str, base_url: str) -> None:
    import sys

    original_argv = sys.argv[:]
    try:
        sys.argv = [
            "publish_pages_dashboard.py",
            "--run-dir",
            str(run_dir),
            "--site-dir",
            str(stage_dir),
            "--date",
            date_tag,
            "--base-url",
            base_url,
        ]
        stage_pages_main()
    finally:
        sys.argv = original_argv


def _copy_public_files(stage_dir: Path, work_dir: Path, date_tag: str) -> None:
    for relative in ["index.html", "manifest.json"]:
        shutil.copy2(stage_dir / relative, work_dir / relative)
    for folder in ["latest", f"archive/{date_tag}"]:
        source = stage_dir / folder
        target = work_dir / folder
        target.mkdir(parents=True, exist_ok=True)
        for filename in ["dashboard.html", "summary.json", "daily-equity.csv"]:
            shutil.copy2(source / filename, target / filename)


def _has_changes(cwd: Path) -> bool:
    result = _run(["git", "status", "--porcelain"], cwd=cwd, capture=True)
    return bool(result.stdout.strip())


def _run(command: list[str], cwd: Path, check: bool = True, capture: bool = False) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        command,
        cwd=str(cwd),
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.STDOUT if capture else None,
    )
    if check and result.returncode != 0:
        rendered = " ".join(command)
        detail = f"\n{result.stdout.strip()}" if capture and result.stdout else ""
        raise SystemExit(f"publish command failed ({result.returncode}) in {cwd}: {rendered}{detail}")
    return result


if __name__ == "__main__":
    raise SystemExit(main())
