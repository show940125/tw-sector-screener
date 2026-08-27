from __future__ import annotations

"""Create an integrity-checked SQLite backup before market-data migration.

The command is intentionally local and read-only with respect to the source
database.  It uses SQLite's backup API so a WAL-backed database is copied at a
consistent point in time, then writes a manifest containing source/backup
SHA-256 and the preflight integrity evidence.
"""

import argparse
import hashlib
import json
import os
import sqlite3
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def _read_only_connect(path: Path) -> sqlite3.Connection:
    resolved = Path(path).resolve()
    if not resolved.exists():
        raise FileNotFoundError(resolved)
    uri = f"file:{quote(str(resolved), safe='/:')}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _payload_integrity(conn: sqlite3.Connection) -> dict[str, object]:
    missing: list[str] = []
    mismatches: list[str] = []
    orphans: list[str] = []
    tables = {
        str(row[0])
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    if "source_payloads" in tables:
        for row in conn.execute(
            "SELECT payload_id, storage_mode, storage_uri, payload_sha256, raw_payload_json "
            "FROM source_payloads"
        ).fetchall():
            payload_id = str(row["payload_id"])
            if str(row["storage_mode"] or "inline") == "external":
                uri = Path(str(row["storage_uri"] or ""))
                if not uri.exists():
                    missing.append(payload_id)
                    continue
                actual = _sha256(uri)
            else:
                actual = hashlib.sha256(
                    str(row["raw_payload_json"] or "").encode("utf-8")
                ).hexdigest()
            if actual != str(row["payload_sha256"] or ""):
                mismatches.append(payload_id)
    if {"market_data_source_links", "source_payloads"}.issubset(tables):
        orphans = [
            str(row["record_identity"])
            for row in conn.execute(
                """
                SELECT link.record_identity
                FROM market_data_source_links AS link
                LEFT JOIN source_payloads AS payload
                  ON payload.payload_id = link.payload_id
                WHERE payload.payload_id IS NULL
                """
            ).fetchall()
        ]
    return {
        "external_payload_missing": missing,
        "hash_mismatches": mismatches,
        "source_link_orphans": orphans,
        "status": "verified" if not missing and not mismatches and not orphans else "failed",
    }


def _preflight(path: Path) -> dict[str, object]:
    conn = _read_only_connect(path)
    try:
        integrity = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
        foreign_keys = [dict(row) for row in conn.execute("PRAGMA foreign_key_check").fetchall()]
        schema = {
            str(row["key"]): str(row["value"])
            for row in conn.execute("SELECT key, value FROM schema_meta").fetchall()
        }
        tables = {
            str(row[0])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        table_counts = {
            table: int(conn.execute(f"SELECT COUNT(*) FROM \"{table}\"").fetchone()[0])
            for table in sorted(tables)
            if table not in {"sqlite_sequence"}
        }
        payloads = _payload_integrity(conn)
    finally:
        conn.close()
    return {
        "integrity_check": integrity,
        "foreign_key_violations": foreign_keys,
        "schema_meta": schema,
        "table_counts": table_counts,
        "source_payload_integrity": payloads,
        "status": (
            "verified"
            if integrity == "ok" and not foreign_keys and payloads["status"] == "verified"
            else "failed"
        ),
    }


def backup_database(
    database_path: Path,
    *,
    output_dir: Path | None = None,
    label: str | None = None,
) -> dict[str, Path]:
    source = Path(database_path).resolve()
    preflight = _preflight(source)
    timestamp = datetime.now().astimezone()
    stamp = timestamp.strftime("%Y%m%dT%H%M%S%z")
    stem = label or "market-data"
    target_dir = Path(output_dir).resolve() if output_dir else source.parent / "backups"
    target_dir.mkdir(parents=True, exist_ok=True)
    backup_path = target_dir / f"{stem}-{stamp}.sqlite"
    manifest_path = target_dir / f"{stem}-{stamp}.json"
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{stem}-{stamp}-",
        suffix=".sqlite.tmp",
        dir=target_dir,
    )
    os.close(fd)
    temporary_path = Path(temporary_name)
    try:
        source_conn = _read_only_connect(source)
        backup_conn = sqlite3.connect(temporary_path)
        try:
            source_conn.backup(backup_conn)
            backup_conn.commit()
        finally:
            backup_conn.close()
            source_conn.close()
        os.replace(temporary_path, backup_path)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()

    backup_preflight = _preflight(backup_path)
    logical_parity = preflight.get("table_counts") == backup_preflight.get("table_counts")
    source_sha256 = _sha256(source)
    backup_sha256 = _sha256(backup_path)
    manifest = {
        "status": (
            "complete"
            if preflight["status"] == "verified"
            and backup_preflight["status"] == "verified"
            and logical_parity
            else "failed"
        ),
        "created_at": timestamp.isoformat(),
        "source_database": str(source),
        "source_size": source.stat().st_size,
        "source_sha256": source_sha256,
        "backup_database": str(backup_path),
        "backup_size": backup_path.stat().st_size,
        "backup_sha256": backup_sha256,
        "source_preflight": preflight,
        "backup_preflight": backup_preflight,
        "sha256_equal": source_sha256 == backup_sha256,
        "logical_parity": logical_parity,
    }
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    return {"database": backup_path, "manifest": manifest_path}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="備份並驗證 canonical market_data.sqlite")
    parser.add_argument("--database", required=True, help="canonical SQLite 路徑")
    parser.add_argument("--output-dir", default=None, help="備份與 manifest 輸出目錄")
    parser.add_argument("--label", default="market-data", help="備份檔名前綴")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        outputs = backup_database(
            Path(args.database),
            output_dir=Path(args.output_dir) if args.output_dir else None,
            label=args.label,
        )
        manifest = json.loads(outputs["manifest"].read_text(encoding="utf-8"))
        print(f"[backup-market-data] database: {outputs['database']}")
        print(f"[backup-market-data] manifest: {outputs['manifest']}")
        return 0 if manifest.get("status") == "complete" and manifest.get("logical_parity") else 1
    except Exception as exc:
        print(f"[backup-market-data] error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
