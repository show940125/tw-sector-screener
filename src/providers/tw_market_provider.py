from __future__ import annotations

import hashlib
import json
import re
import ssl
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlencode, urljoin, urlparse
from urllib.request import HTTPRedirectHandler, HTTPSHandler, Request, build_opener

from src.analysis.factors import safe_float
from src.providers.daily_bar_store import (
    VerifiedDailyBar,
    get_bars,
    import_verified_bars,
    is_current_day_verified,
    mark_current_day_verified,
)
from src.providers.market_data_store import (
    ensure_market_data_db,
    get_index_bars,
    record_fetch_attempt,
    record_source_payload,
    rebuild_period_bars,
    upsert_index_bars,
    upsert_monthly_revenue,
    upsert_security_master,
    upsert_valuation_snapshot,
)
from src.providers.quarterly_store import (
    claim_backfill_batch,
    create_backfill_run,
    enqueue_backfill_targets,
    finish_backfill_run,
    get_backfill_run,
    get_period_rows,
    get_latest_refresh_run,
    get_latest_periods,
    init_db,
    insert_fundamental_snapshot,
    mark_backfill_result,
    summarize_coverage,
    upsert_refresh_run,
)
from src.themes import core_themes, theme_rule


TWSE_BASICS_URL = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
TWSE_REVENUE_URL = "https://openapi.twse.com.tw/v1/opendata/t187ap05_L"
# The historical page currently exposes the rwd/zh route. Keep the former
# exchangeReport route as a source-level fallback for older edge locations.
TWSE_STOCK_DAY_PRIMARY_URL = "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY"
TWSE_STOCK_DAY_URL = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
TWSE_BWIBBU_URL = "https://www.twse.com.tw/exchangeReport/BWIBBU_d"
TWSE_FMTQIK_URL = "https://www.twse.com.tw/exchangeReport/FMTQIK"
TWSE_EPS_URL = "https://openapi.twse.com.tw/v1/opendata/t187ap14_L"

TPEX_BASICS_URL = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O"
TPEX_REVENUE_URL = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O"
TPEX_TRADING_STOCK_URL = "https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingStock"
# Daily all-market JSON is an intentionally bounded historical fallback for a
# month whose per-stock endpoint cannot be verified. Payloads are cached by
# date and shared across TPEx symbols during one run.
TPEX_DAILY_QUOTES_URL = "https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php"
TPEX_PE_QRY_DATE_URL = "https://www.tpex.org.tw/www/zh-tw/afterTrading/peQryDate"
TPEX_EPS_URL = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap14_O"

_REDIRECT_CODES = {301, 302, 303, 307, 308}
_REDIRECT_ALLOWED_HOSTS = {
    "www.twse.com.tw",
    "openapi.twse.com.tw",
    "www.tpex.org.tw",
}


class MarketDataFetchError(RuntimeError):
    """Raised when all bounded source attempts fail for a market-data request."""

    def __init__(self, message: str, attempts: list[dict[str, Any]] | None = None) -> None:
        super().__init__(message)
        self.attempts = attempts or []


@dataclass
class _MarketDataStats:
    request_count: int = 0
    cache_hit_count: int = 0
    network_success_count: int = 0
    fallback_success_count: int = 0
    redirect_count: int = 0
    redirect_308_count: int = 0
    redirect_308_recovered_count: int = 0
    redirect_failure_count: int = 0
    http_error_counts: dict[str, int] | None = None
    retry_count: int = 0
    endpoint_attempts: dict[str, int] | None = None
    endpoint_successes: dict[str, int] | None = None
    endpoint_fallback_successes: dict[str, int] | None = None
    db_hit_count: int = 0
    db_write_count: int = 0
    db_missing_count: int = 0
    incremental_fetch_count: int = 0
    current_day_verified_count: int = 0
    current_day_failure_count: int = 0

    def __post_init__(self) -> None:
        self.endpoint_attempts = self.endpoint_attempts or {}
        self.endpoint_successes = self.endpoint_successes or {}
        self.endpoint_fallback_successes = self.endpoint_fallback_successes or {}
        self.http_error_counts = self.http_error_counts or {}

    def as_dict(self) -> dict[str, Any]:
        return {
            "request_count": self.request_count,
            "cache_hit_count": self.cache_hit_count,
            "network_success_count": self.network_success_count,
            "fallback_success_count": self.fallback_success_count,
            "redirect_count": self.redirect_count,
            "redirect_308_count": self.redirect_308_count,
            "redirect_308_recovered_count": self.redirect_308_recovered_count,
            "redirect_308_unresolved_count": max(
                0, self.redirect_308_count - self.redirect_308_recovered_count
            ),
            "redirect_failure_count": self.redirect_failure_count,
            "http_error_counts": dict(self.http_error_counts or {}),
            "retry_count": self.retry_count,
            "endpoint_attempts": dict(self.endpoint_attempts or {}),
            "endpoint_successes": dict(self.endpoint_successes or {}),
            "endpoint_fallback_successes": dict(self.endpoint_fallback_successes or {}),
            "db_hit_count": self.db_hit_count,
            "db_write_count": self.db_write_count,
            "db_missing_count": self.db_missing_count,
            "incremental_fetch_count": self.incremental_fetch_count,
            "current_day_verified_count": self.current_day_verified_count,
            "current_day_failure_count": self.current_day_failure_count,
        }


class _NoAutomaticRedirectHandler(HTTPRedirectHandler):
    """Expose redirects to the provider so 308 handling is explicit and bounded."""

    @staticmethod
    def _raise_redirect(req: Request, fp: Any, code: int, msg: str, headers: Any) -> Any:
        raise HTTPError(req.full_url, code, msg, headers, fp)

    def http_error_301(self, req: Request, fp: Any, code: int, msg: str, headers: Any) -> Any:
        return self._raise_redirect(req, fp, code, msg, headers)

    def http_error_302(self, req: Request, fp: Any, code: int, msg: str, headers: Any) -> Any:
        return self._raise_redirect(req, fp, code, msg, headers)

    def http_error_303(self, req: Request, fp: Any, code: int, msg: str, headers: Any) -> Any:
        return self._raise_redirect(req, fp, code, msg, headers)

    def http_error_307(self, req: Request, fp: Any, code: int, msg: str, headers: Any) -> Any:
        return self._raise_redirect(req, fp, code, msg, headers)

    def http_error_308(self, req: Request, fp: Any, code: int, msg: str, headers: Any) -> Any:
        return self._raise_redirect(req, fp, code, msg, headers)

TWSE_INCOME_URLS = {
    "ci": "https://openapi.twse.com.tw/v1/opendata/t187ap06_L_ci",
    "mim": "https://openapi.twse.com.tw/v1/opendata/t187ap06_L_mim",
    "bd": "https://openapi.twse.com.tw/v1/opendata/t187ap06_L_bd",
    "fh": "https://openapi.twse.com.tw/v1/opendata/t187ap06_L_fh",
    "ins": "https://openapi.twse.com.tw/v1/opendata/t187ap06_L_ins",
}
TWSE_BALANCE_URLS = {
    "ci": "https://openapi.twse.com.tw/v1/opendata/t187ap07_L_ci",
    "mim": "https://openapi.twse.com.tw/v1/opendata/t187ap07_L_mim",
    "bd": "https://openapi.twse.com.tw/v1/opendata/t187ap07_L_bd",
    "fh": "https://openapi.twse.com.tw/v1/opendata/t187ap07_L_fh",
    "ins": "https://openapi.twse.com.tw/v1/opendata/t187ap07_L_ins",
}
TPEX_INCOME_URLS = {
    "ci": "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap06_O_ci",
    "mim": "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap06_O_mim",
    "bd": "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap06_O_bd",
    "fh": "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap06_O_fh",
    "ins": "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap06_O_ins",
}
TPEX_BALANCE_URLS = {
    "ci": "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap07_O_ci",
    "mim": "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap07_O_mim",
    "bd": "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap07_O_bd",
    "fh": "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap07_O_fh",
    "ins": "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap07_O_ins",
}


def _is_stock_symbol(symbol: str) -> bool:
    return len(symbol) == 4 and symbol.isdigit()


def _parse_roc_slash(value: str) -> date:
    parts = [re.sub(r"[^0-9]", "", x) for x in value.strip().split("/")]
    year, month, day = [int(x) for x in parts]
    return date(year + 1911, month, day)


def _try_parse_roc_slash(value: str) -> date | None:
    try:
        return _parse_roc_slash(value)
    except Exception:
        return None


def _is_weekday(day: date) -> bool:
    return day.weekday() < 5


def _shift_month(d: date, delta: int) -> date:
    month_index = d.year * 12 + (d.month - 1) + delta
    year = month_index // 12
    month = month_index % 12 + 1
    return date(year, month, 1)


def _previous_period(period: str) -> str | None:
    match = re.fullmatch(r"(\d{3,4})Q([1-4])", period)
    if not match:
        return None
    year = int(match.group(1))
    quarter = int(match.group(2))
    if quarter == 1:
        return f"{year - 1}Q4"
    return f"{year}Q{quarter - 1}"


class TwMarketProvider:
    def __init__(
        self,
        timeout: float = 10.0,
        cache_dir: Path | None = None,
        market_database_path: Path | None = None,
        sync_run_id: str | None = None,
    ) -> None:
        self.timeout = timeout
        self.cache_dir = cache_dir or (Path(__file__).resolve().parents[2] / ".cache" / "market")
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.legacy_daily_store_path = self.cache_dir / "daily_bars.sqlite"
        self.legacy_quarterly_store_path = self.cache_dir / "quarterly_fundamentals.sqlite"
        self.market_data_db_path = Path(market_database_path) if market_database_path else self.cache_dir / "market_data.sqlite"
        self.sync_run_id = sync_run_id
        ensure_market_data_db(
            self.market_data_db_path,
            daily_source=self.legacy_daily_store_path,
            quarterly_source=self.legacy_quarterly_store_path,
        )
        # Existing quarterly APIs use this attribute. Pointing it at the
        # canonical DB keeps their public behaviour while unifying storage.
        self.quarterly_store_path = self.market_data_db_path
        self._twse_valuation_cache: dict[str, dict[str, dict[str, float]]] = {}
        self._tpex_valuation_cache: dict[str, dict[str, dict[str, float]]] = {}
        self._ohlcv_cache: dict[tuple[str, str, str, int, bool], list[dict[str, Any]]] = {}
        self._reported_period_cache: dict[tuple[str, str], str] = {}
        self._tpex_daily_quotes_cache: dict[date, dict[str, dict[str, Any]]] = {}
        self._market_data_stats = _MarketDataStats()
        self._last_resolved_source_url: str | None = None
        self._last_redirect_chain: list[str] = []
        self._basics_payload_hash: str | None = None
        self._revenue_payload_hash: str | None = None
        # The TWSE/TPEx CDN can emit a self-308/428 when historical requests
        # are burst too quickly. Keep the transport bounded and deterministic.
        self._network_request_interval_seconds = 1.0
        self._last_network_request_at = 0.0
        self._http_opener = build_opener(_NoAutomaticRedirectHandler())
        self._insecure_http_opener = build_opener(
            _NoAutomaticRedirectHandler(),
            HTTPSHandler(context=ssl._create_unverified_context()),
        )

    def get_market_data_diagnostics(self) -> dict[str, Any]:
        """Return serialisable transport/source counters for the audit trail."""
        return self._market_data_stats.as_dict()

    def get_market_data_store_diagnostics(self) -> dict[str, Any]:
        """Return canonical SQLite integrity and coverage diagnostics."""
        from src.providers.market_data_store import database_integrity

        return database_integrity(self.market_data_db_path)

    @staticmethod
    def _endpoint_name(url: str) -> str:
        parsed = urlparse(url)
        return f"{parsed.netloc}{parsed.path}"

    def _record_endpoint_attempt(self, endpoint: str) -> None:
        self._market_data_stats.endpoint_attempts[endpoint] = self._market_data_stats.endpoint_attempts.get(endpoint, 0) + 1

    def _record_endpoint_success(self, endpoint: str, fallback: bool = False) -> None:
        self._market_data_stats.endpoint_successes[endpoint] = self._market_data_stats.endpoint_successes.get(endpoint, 0) + 1
        if fallback:
            self._market_data_stats.endpoint_fallback_successes[endpoint] = (
                self._market_data_stats.endpoint_fallback_successes.get(endpoint, 0) + 1
            )

    def _record_fetch_attempt(
        self,
        req: Request,
        *,
        dataset_key: str,
        started_at: str,
        finished_at: str,
        status: str,
        final_url: str | None = None,
        http_status: int | None = None,
        cache_status: str | None = None,
        fallback_level: int = 0,
        payload_sha256: str | None = None,
        error: str | None = None,
    ) -> None:
        """Persist transport provenance without making it a data gate."""

        try:
            record_fetch_attempt(
                self.market_data_db_path,
                run_id=self.sync_run_id,
                dataset_key=dataset_key,
                request_method=req.get_method(),
                request_url=req.full_url,
                final_url=final_url,
                redirect_chain=list(self._last_redirect_chain),
                http_status=http_status,
                cache_status=cache_status,
                fallback_level=fallback_level,
                started_at=started_at,
                finished_at=finished_at,
                payload_sha256=payload_sha256,
                status=status,
                error=error,
            )
        except Exception:
            # A provenance write cannot turn a validated official payload into
            # a failed report. The source payload/bar stores remain the gate.
            return

    def _persist_loaded_payload(
        self,
        req: Request,
        payload: Any,
        *,
        endpoint_label: str | None,
        source_url: str,
    ) -> None:
        body = req.data if isinstance(req.data, (bytes, bytearray)) else b""
        effective = self._request_date(req) or date.today()
        try:
            record_source_payload(
                self.market_data_db_path,
                dataset_key=endpoint_label or self._endpoint_name(source_url),
                request_method=req.get_method(),
                source_endpoint=self._endpoint_name(source_url),
                source_url=source_url,
                request_body_sha256=hashlib.sha256(body).hexdigest() if body else None,
                payload=payload,
                effective_date=effective.isoformat(),
                fetched_at=datetime.now().astimezone().isoformat(),
                cache_file=str(self._cache_path(req)),
                validation_status="unvalidated",
                raw_storage_root=self.cache_dir / "raw_payloads",
            )
        except Exception:
            # Source payload retention is best-effort; the validated bar/store
            # write remains the correctness boundary for a report.
            return

    def _open_request(self, req: Request, *, insecure: bool = False) -> Any:
        elapsed = time.monotonic() - self._last_network_request_at
        wait_seconds = self._network_request_interval_seconds - elapsed
        if wait_seconds > 0:
            time.sleep(wait_seconds)
        self._last_network_request_at = time.monotonic()
        opener = self._insecure_http_opener if insecure else self._http_opener
        return opener.open(req, timeout=self.timeout)

    @staticmethod
    def _redirect_request(req: Request, location: str, code: int, visited: set[str]) -> Request:
        target = urljoin(req.full_url, location)
        try:
            parsed = urlparse(target)
            hostname = (parsed.hostname or "").lower()
        except ValueError as exc:
            raise ValueError(f"無效 redirect Location：{location}") from exc
        if parsed.scheme.lower() != "https" or hostname not in _REDIRECT_ALLOWED_HOSTS:
            raise ValueError(f"拒絕不安全或非 allowlist redirect：{target}")
        if target in visited:
            raise ValueError(f"redirect loop：{target}")
        headers = {key: value for key, value in req.header_items()}
        if code in {301, 302, 303}:
            headers.pop("Content-Type", None)
            headers.pop("Content-Length", None)
            return Request(target, headers=headers, method="GET")
        return Request(target, data=req.data, headers=headers, method=req.get_method())

    def _read_json_request(self, req: Request, *, insecure: bool = False) -> tuple[Any, str]:
        current = req
        visited = {req.full_url}
        redirect_chain: list[str] = []
        self._last_redirect_chain = []
        for _ in range(4):
            try:
                with self._open_request(current, insecure=insecure) as resp:
                    payload = json.loads(resp.read().decode("utf-8-sig"))
                self._last_redirect_chain = list(redirect_chain)
                return payload, current.full_url
            except HTTPError as exc:
                if exc.code not in _REDIRECT_CODES:
                    raise
                location = exc.headers.get("Location") if exc.headers else None
                if not location:
                    self._market_data_stats.redirect_failure_count += 1
                    raise RuntimeError(f"HTTP {exc.code} 缺少 Location：{current.full_url}") from exc
                self._market_data_stats.redirect_count += 1
                if exc.code == 308:
                    self._market_data_stats.redirect_308_count += 1
                try:
                    next_request = self._redirect_request(current, location, exc.code, visited)
                except Exception:
                    self._market_data_stats.redirect_failure_count += 1
                    raise
                visited.add(next_request.full_url)
                redirect_chain.append(next_request.full_url)
                current = next_request
        self._market_data_stats.redirect_failure_count += 1
        self._last_redirect_chain = list(redirect_chain)
        raise RuntimeError(f"redirect 超過 3 層：{req.full_url}")

    def _load_json(
        self,
        req: Request,
        *,
        endpoint_label: str | None = None,
        use_cache: bool = True,
        fallback_level: int = 0,
    ) -> Any:
        last_exc: Exception | None = None
        cache_file = self._cache_path(req)
        if use_cache:
            cached = self._read_cache(cache_file, self._cache_ttl_seconds(req), req)
            if cached is not None:
                self._market_data_stats.cache_hit_count += 1
                self._last_resolved_source_url = req.full_url
                now = datetime.now().astimezone().isoformat()
                self._record_fetch_attempt(
                    req,
                    dataset_key=endpoint_label or self._endpoint_name(req.full_url),
                    started_at=now,
                    finished_at=now,
                    final_url=req.full_url,
                    cache_status="fresh",
                    fallback_level=fallback_level,
                    payload_sha256=self._payload_sha256(cached),
                    status="cache_hit",
                )
                return cached
        endpoint = endpoint_label or self._endpoint_name(req.full_url)
        self._record_endpoint_attempt(endpoint)
        self._market_data_stats.request_count += 1
        redirects_seen_for_request = 0
        request_started_at = datetime.now().astimezone().isoformat()
        for attempt in range(3):
            redirect_308_before = self._market_data_stats.redirect_308_count
            try:
                payload, resolved_source_url = self._read_json_request(req)
                self._last_resolved_source_url = resolved_source_url
                redirects_seen_for_request += self._market_data_stats.redirect_308_count - redirect_308_before
                self._write_cache(cache_file, payload)
                self._persist_loaded_payload(
                    req,
                    payload,
                    endpoint_label=endpoint_label,
                    source_url=resolved_source_url,
                )
                self._market_data_stats.network_success_count += 1
                self._record_endpoint_success(endpoint)
                self._market_data_stats.redirect_308_recovered_count += redirects_seen_for_request
                self._record_fetch_attempt(
                    req,
                    dataset_key=endpoint_label or endpoint,
                    started_at=request_started_at,
                    finished_at=datetime.now().astimezone().isoformat(),
                    final_url=resolved_source_url,
                    http_status=200,
                    cache_status="network",
                    fallback_level=fallback_level,
                    payload_sha256=self._payload_sha256(payload),
                    status="network_success",
                )
                return payload
            except Exception as exc:
                redirects_seen_for_request += self._market_data_stats.redirect_308_count - redirect_308_before
                last_exc = exc
                if isinstance(exc, HTTPError):
                    code = str(exc.code)
                    self._market_data_stats.http_error_counts[code] = (
                        self._market_data_stats.http_error_counts.get(code, 0) + 1
                    )
                reason = getattr(exc, "reason", None)
                if isinstance(exc, ssl.SSLCertVerificationError) or isinstance(reason, ssl.SSLCertVerificationError):
                    insecure_redirect_308_before = self._market_data_stats.redirect_308_count
                    payload, resolved_source_url = self._read_json_request(req, insecure=True)
                    self._last_resolved_source_url = resolved_source_url
                    redirects_seen_for_request += (
                        self._market_data_stats.redirect_308_count - insecure_redirect_308_before
                    )
                    self._write_cache(cache_file, payload)
                    self._persist_loaded_payload(
                        req,
                        payload,
                        endpoint_label=endpoint_label,
                        source_url=resolved_source_url,
                    )
                    self._market_data_stats.network_success_count += 1
                    self._record_endpoint_success(endpoint)
                    self._market_data_stats.redirect_308_recovered_count += redirects_seen_for_request
                    self._record_fetch_attempt(
                        req,
                        dataset_key=endpoint_label or endpoint,
                        started_at=request_started_at,
                        finished_at=datetime.now().astimezone().isoformat(),
                        final_url=resolved_source_url,
                        http_status=200,
                        cache_status="network_insecure_tls_fallback",
                        fallback_level=fallback_level,
                        payload_sha256=self._payload_sha256(payload),
                        status="network_success",
                    )
                    return payload
                if attempt < 2:
                    transient_http = isinstance(exc, HTTPError) and exc.code in {408, 425, 428, 429, 500, 502, 503, 504}
                    if transient_http or redirects_seen_for_request:
                        time.sleep(2.0 * (2**attempt))
                    else:
                        time.sleep(0.6 * (attempt + 1))
                    self._market_data_stats.retry_count += 1
                    continue
        if last_exc is not None:
            self._record_fetch_attempt(
                req,
                dataset_key=endpoint_label or endpoint,
                started_at=request_started_at,
                finished_at=datetime.now().astimezone().isoformat(),
                final_url=self._last_resolved_source_url,
                http_status=getattr(last_exc, "code", None),
                    cache_status="network",
                    fallback_level=fallback_level,
                status="failed",
                error=str(last_exc),
            )
            raise last_exc
        raise RuntimeError("無法讀取 JSON")

    def _load_json_candidates(
        self,
        requests: list[Request],
        *,
        endpoint_label: str,
        validator: Any | None = None,
        use_cache: bool = True,
    ) -> Any:
        attempts: list[dict[str, Any]] = []
        failed_candidate_redirect_308_count = 0
        for index, req in enumerate(requests):
            endpoint = self._endpoint_name(req.full_url)
            redirect_308_before = self._market_data_stats.redirect_308_count
            try:
                payload = self._load_json(
                    req,
                    endpoint_label=endpoint_label,
                    use_cache=use_cache,
                    fallback_level=index,
                )
                if validator is not None and not validator(payload):
                    raise ValueError("回應 JSON schema/status 不符合預期")
                if index > 0:
                    self._market_data_stats.fallback_success_count += 1
                    self._market_data_stats.endpoint_fallback_successes[endpoint] = (
                        self._market_data_stats.endpoint_fallback_successes.get(endpoint, 0) + 1
                    )
                    self._market_data_stats.redirect_308_recovered_count += failed_candidate_redirect_308_count
                return payload
            except Exception as exc:
                failed_candidate_redirect_308_count += (
                    self._market_data_stats.redirect_308_count - redirect_308_before
                )
                attempts.append(
                    {
                        "url": req.full_url,
                        "method": req.get_method(),
                        "error": str(exc),
                    }
                )
        details = "; ".join(f"{item['method']} {item['url']}: {item['error']}" for item in attempts)
        raise MarketDataFetchError(f"{endpoint_label} 所有來源失敗：{details}", attempts)

    def _cache_path(self, req: Request) -> Path:
        body = req.data.decode("utf-8", errors="ignore") if isinstance(req.data, (bytes, bytearray)) else ""
        digest = hashlib.sha256(f"{req.full_url}|{body}".encode("utf-8")).hexdigest()
        return self.cache_dir / f"{digest}.json"

    def _cache_ttl_seconds(self, req: Request) -> int:
        url = req.full_url.lower()
        if self._is_incremental_monthly_endpoint(req):
            return 20 * 60
        if any(marker in url for marker in ["date=", "stockno=", "code="]):
            return 365 * 24 * 3600
        return 12 * 3600

    def _read_cache(self, cache_file: Path, ttl_seconds: int, req: Request | None = None) -> Any:
        if not cache_file.exists():
            return None
        age_seconds = time.time() - cache_file.stat().st_mtime
        if age_seconds > ttl_seconds:
            return None
        try:
            payload = json.loads(cache_file.read_text(encoding="utf-8"))
        except Exception:
            return None
        if req is not None and self._cached_incremental_payload_is_stale(req, payload):
            return None
        return payload

    def _write_cache(self, cache_file: Path, payload: Any) -> None:
        try:
            cache_file.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        except Exception:
            return

    def _is_incremental_monthly_endpoint(self, req: Request) -> bool:
        url = req.full_url.lower()
        return any(marker in url for marker in ["stock_day", "fmtqik", "tradingstock"])

    def _cached_incremental_payload_is_stale(self, req: Request, payload: Any, today: date | None = None) -> bool:
        if not isinstance(payload, dict):
            return False
        if "stk_wn1430" in req.full_url.lower():
            requested_day = self._request_date(req)
            today = today or date.today()
            if requested_day != today or not _is_weekday(today):
                return False
            latest = self._latest_payload_trade_date(payload)
            return latest != today
        if not self._is_incremental_monthly_endpoint(req):
            return False
        requested_month = self._request_month(req)
        today = today or date.today()
        if requested_month != (today.year, today.month):
            return False
        latest = self._latest_payload_trade_date(payload)
        return latest is not None and latest < today and _is_weekday(today)

    def _request_date(self, req: Request) -> date | None:
        parsed = urlparse(req.full_url)
        query = parse_qs(parsed.query)
        if not query and isinstance(req.data, (bytes, bytearray)):
            query = parse_qs(req.data.decode("utf-8", errors="ignore"))
        raw = (query.get("date") or query.get("d") or [""])[0]
        digits = re.sub(r"[^0-9]", "", raw)
        try:
            if len(digits) == 7:
                return date(int(digits[:3]) + 1911, int(digits[3:5]), int(digits[5:7]))
            if len(digits) >= 8:
                return date(int(digits[:4]), int(digits[4:6]), int(digits[6:8]))
        except ValueError:
            return None
        return None

    def _request_month(self, req: Request) -> tuple[int, int] | None:
        parsed = urlparse(req.full_url)
        query = parse_qs(parsed.query)
        if not query and isinstance(req.data, (bytes, bytearray)):
            query = parse_qs(req.data.decode("utf-8", errors="ignore"))
        raw = (query.get("date") or [""])[0]
        digits = re.sub(r"[^0-9]", "", raw)
        if len(digits) == 7:
            return int(digits[:3]) + 1911, int(digits[3:5])
        if len(digits) < 6:
            return None
        year = int(digits[:4])
        month = int(digits[4:6])
        return year, month

    def _latest_payload_trade_date(self, payload: dict[str, Any]) -> date | None:
        candidates: list[date] = []
        for raw_date in [payload.get("date")]:
            parsed = _try_parse_roc_slash(str(raw_date)) if raw_date else None
            if parsed:
                candidates.append(parsed)
        rows = payload.get("data") if isinstance(payload.get("data"), list) else []
        for row in rows:
            if isinstance(row, list) and row:
                parsed = _try_parse_roc_slash(str(row[0]))
                if parsed:
                    candidates.append(parsed)
        tables = payload.get("tables")
        if isinstance(tables, list):
            for table in tables:
                if isinstance(table, dict):
                    parsed = _try_parse_roc_slash(str(table.get("date") or ""))
                    if parsed:
                        candidates.append(parsed)
                table_rows = table.get("data") if isinstance(table, dict) else []
                for row in table_rows or []:
                    if isinstance(row, list) and row:
                        parsed = _try_parse_roc_slash(str(row[0]))
                        if parsed:
                            candidates.append(parsed)
        return max(candidates) if candidates else None

    def _symbol_market_from_theme_rules(self, symbol: str) -> str:
        return "TPEx" if symbol.startswith("6") or symbol.startswith("8") else "TWSE"

    def _legacy_quarterly_snapshot_dir(self) -> Path:
        path = self.cache_dir / "quarterly"
        path.mkdir(parents=True, exist_ok=True)
        return path

    @staticmethod
    def _build_get_request(url: str, params: dict[str, Any] | None = None) -> Request:
        query = urlencode(params) if params else ""
        full_url = f"{url}?{query}" if query else url
        return Request(
            full_url,
            headers={
                "User-Agent": "Mozilla/5.0",
                # The TWSE CDN has returned a self-308 to the JSON-specific
                # Accept header on historical paths. The payload is still
                # validated as JSON below, while */* avoids that edge bug.
                "Accept": "*/*",
                # HiNetCDN has intermittently served a cached self-308 for
                # historical endpoints. Force a fresh edge lookup while the
                # explicit redirect layer remains responsible for redirects.
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Connection": "close",
            },
        )

    @staticmethod
    def _build_post_request(url: str, data: dict[str, Any]) -> Request:
        return Request(
            url,
            data=urlencode(data).encode("utf-8"),
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "*/*",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Connection": "close",
            },
        )

    def _get_json(self, url: str, params: dict[str, Any] | None = None) -> Any:
        req = self._build_get_request(url, params)
        return self._load_json(req)

    def _safe_get_json(self, url: str, default: Any) -> Any:
        try:
            return self._get_json(url)
        except Exception:
            return default

    def _post_json(self, url: str, data: dict[str, Any]) -> Any:
        req = self._build_post_request(url, data)
        return self._load_json(req)

    def load_theme_universe(
        self,
        theme: str,
        min_monthly_revenue: float = 0.0,
        theme_mode: str = "strict",
        universe_mode: str | None = None,
    ) -> list[dict[str, Any]]:
        rule = theme_rule(theme, theme_mode=theme_mode, universe_mode=universe_mode)
        output: list[dict[str, Any]] = []
        for row in self.load_all_universe(min_monthly_revenue=min_monthly_revenue):
            if self._theme_match(row["symbol"], row["name"], row.get("industry") or "", rule):
                output.append({**row, **self._theme_metadata(row["symbol"], rule)})

        output.sort(key=lambda x: x.get("monthly_revenue", 0.0), reverse=True)
        return output

    def load_all_universe(self, min_monthly_revenue: float = 0.0) -> list[dict[str, Any]]:
        basics = self._load_basics()
        revenue_map = self._load_latest_revenue_map()
        output: list[dict[str, Any]] = []
        for symbol, item in basics.items():
            rev = revenue_map.get(symbol, {})
            industry = str(rev.get("industry") or item.get("industry") or "").strip()
            monthly_revenue = float(rev.get("monthly_revenue") or 0.0)
            if monthly_revenue < min_monthly_revenue:
                continue
            output.append(
                {
                    "symbol": symbol,
                    "name": item.get("name", ""),
                    "market": item.get("market", "TWSE"),
                    "industry": industry or "未分類",
                    "monthly_revenue": monthly_revenue,
                    "revenue_yoy": safe_float(rev.get("revenue_yoy")),
                    "revenue_mom": safe_float(rev.get("revenue_mom")),
                }
            )
        output.sort(key=lambda x: x.get("monthly_revenue", 0.0), reverse=True)
        return output

    def load_industry_universes(
        self,
        min_monthly_revenue: float = 0.0,
        min_count: int = 1,
    ) -> dict[str, list[dict[str, Any]]]:
        buckets: dict[str, list[dict[str, Any]]] = {}
        for row in self.load_all_universe(min_monthly_revenue=min_monthly_revenue):
            industry = str(row.get("industry") or "未分類").strip() or "未分類"
            buckets.setdefault(industry, []).append(row)
        filtered = {k: v for k, v in buckets.items() if len(v) >= min_count}
        return dict(sorted(filtered.items(), key=lambda item: (-len(item[1]), item[0])))

    def get_taiex_series(self, as_of: date, lookback: int = 252) -> list[dict[str, Any]]:
        """TAIEX (發行量加權股價指數) close series up to as_of.

        Data source: TWSE exchangeReport/FMTQIK (monthly market trading info).
        """
        cached = get_index_bars(
            self.market_data_db_path,
            index_code="TAIEX",
            as_of=as_of,
            limit=max(lookback, 1),
        )
        expected = as_of if _is_weekday(as_of) else (cached[-1]["trade_date"] if cached else None)
        if len(cached) >= lookback and (expected is None or str(cached[-1]["trade_date"]) == str(expected)):
            self._market_data_stats.db_hit_count += 1
            if _is_weekday(as_of):
                self._market_data_stats.current_day_verified_count += 1
            return [
                {
                    "date": date.fromisoformat(str(item["trade_date"])),
                    "close": float(item["close"]),
                    "change_points": item.get("change_points"),
                }
                for item in cached[-lookback:]
            ]

        collected: dict[date, dict[str, Any]] = {}
        cursor = date(as_of.year, as_of.month, 1)
        months_checked = 0

        while months_checked < 36 and len(collected) < (lookback + 10):
            fmt_request = self._build_get_request(
                TWSE_FMTQIK_URL,
                {"response": "json", "date": cursor.strftime("%Y%m%d")},
            )
            payload = self._load_json(fmt_request, endpoint_label="twse.fmtqik")
            if isinstance(payload, dict) and payload.get("stat") == "OK":
                self._record_market_payload(
                    dataset_key="twse.fmtqik",
                    request=fmt_request,
                    payload=payload,
                    effective_date=cursor,
                )
                fields = payload.get("fields") or []
                rows = payload.get("data") or []
                idx = {str(name).strip(): i for i, name in enumerate(fields)}
                date_idx = idx.get("日期", 0)
                close_idx = idx.get("發行量加權股價指數")
                chg_idx = idx.get("漲跌點數")
                for row in rows:
                    if not isinstance(row, list) or date_idx >= len(row):
                        continue
                    d = _try_parse_roc_slash(str(row[date_idx]))
                    if d is None:
                        continue
                    if d > as_of:
                        continue
                    close = safe_float(row[close_idx] if close_idx is not None and close_idx < len(row) else None)
                    chg_pts = safe_float(row[chg_idx] if chg_idx is not None and chg_idx < len(row) else None)
                    if close is None:
                        continue
                    collected[d] = {"date": d, "close": float(close), "change_points": chg_pts}

            cursor = _shift_month(cursor, -1)
            months_checked += 1

        series = sorted(collected.values(), key=lambda x: x["date"])
        if not series:
            raise RuntimeError("無法取得加權指數資料（TWSE FMTQIK）")
        source_url = self._last_resolved_source_url or TWSE_FMTQIK_URL
        payload_hash = hashlib.sha256(
            json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()
        upsert_index_bars(
            self.market_data_db_path,
            [
                {
                    "index_code": "TAIEX",
                    "trade_date": item["date"].isoformat(),
                    "close": item["close"],
                    "change_points": item.get("change_points"),
                    "source_endpoint": "twse.fmtqik",
                    "source_url": source_url,
                    "source_payload_sha256": payload_hash,
                    "fetched_at": datetime.now().astimezone().isoformat(),
                    "data_status": "verified",
                }
                for item in series
            ],
        )
        self._market_data_stats.db_write_count += len(series)
        stored = get_index_bars(
            self.market_data_db_path,
            index_code="TAIEX",
            as_of=as_of,
            limit=max(lookback, 1),
        )
        if len(stored) < lookback:
            self._market_data_stats.current_day_failure_count += 1 if _is_weekday(as_of) else 0
            raise MarketDataFetchError(
                f"TAIEX 歷史資料不足：{len(stored)}/{lookback}"
            )
        series = [
            {
                "date": date.fromisoformat(str(item["trade_date"])),
                "close": float(item["close"]),
                "change_points": item.get("change_points"),
            }
            for item in stored
        ]
        if _is_weekday(as_of) and series[-1]["date"] != as_of:
            self._market_data_stats.current_day_failure_count += 1
            raise MarketDataFetchError(
                f"TAIEX 當日資料缺口：expected={as_of.isoformat()} actual={series[-1]['date'].isoformat()}"
            )
        if _is_weekday(as_of):
            self._market_data_stats.current_day_verified_count += 1
        return series[-lookback:]

    def _theme_match(self, symbol: str, name: str, industry: str, rule: dict[str, Any]) -> bool:
        if symbol in set(rule.get("symbols") or []):
            return True
        if rule.get("universe_mode") != "broad":
            return False
        text = f"{name} {industry}".lower()
        for kw in rule.get("name_keywords") or []:
            if str(kw).lower() in text:
                return True
        for kw in rule.get("industry_keywords") or []:
            if str(kw).lower() in industry.lower():
                return True
        return False

    def _theme_metadata(self, symbol: str, rule: dict[str, Any]) -> dict[str, Any]:
        bucket_map = rule.get("bucket_map") or {}
        buckets = list(bucket_map.get(symbol) or [])
        if not buckets:
            buckets = [str(rule.get("name") or "theme").lower().replace(" ", "_")]
        universe_mode = str(rule.get("universe_mode") or "coverage")
        core_symbols = set(rule.get("core_symbols") or rule.get("strict_symbols") or [])
        coverage_symbols = set(rule.get("coverage_symbols") or [])
        if symbol in core_symbols:
            source = "core"
        elif symbol in coverage_symbols:
            source = "coverage"
        else:
            source = "broad_keyword"
        return {
            "universe_mode": universe_mode,
            "universe_source": source,
            "theme_buckets": buckets,
            "primary_bucket": buckets[0] if buckets else None,
            "coverage_reason": f"{source}:{','.join(buckets)}",
            "core_watchlist_member": symbol in core_symbols,
        }

    def _symbol_field(self, row: dict[str, Any]) -> str:
        return str(
            row.get("公司代號")
            or row.get("SecuritiesCompanyCode")
            or row.get("公司代號 ")
            or row.get("股票代號")
            or ""
        ).strip()

    def _period_from_row(self, row: dict[str, Any], as_of: date) -> str:
        year_value = str(
            row.get("年度")
            or row.get("年")
            or row.get("Year")
            or row.get("資料年度")
            or ""
        ).strip()
        quarter_value = str(
            row.get("季別")
            or row.get("季")
            or row.get("Quarter")
            or row.get("資料季別")
            or ""
        ).strip()
        year_digits = re.sub(r"[^0-9]", "", year_value)
        quarter_digits = re.sub(r"[^0-9]", "", quarter_value)
        if year_digits and quarter_digits:
            return f"{year_digits}Q{quarter_digits}"
        approx_year = as_of.year - 1911
        approx_quarter = ((max(as_of.month - 1, 0)) // 3) + 1
        return f"{approx_year}Q{approx_quarter}"

    def _quarterly_source_urls(self, market: str) -> tuple[str, dict[str, str], dict[str, str], str]:
        if market == "TPEx":
            return TPEX_EPS_URL, TPEX_INCOME_URLS, TPEX_BALANCE_URLS, "tpex_openapi"
        return TWSE_EPS_URL, TWSE_INCOME_URLS, TWSE_BALANCE_URLS, "twse_openapi"

    def _find_row(self, rows: list[dict[str, Any]], symbol: str) -> dict[str, Any] | None:
        for row in rows:
            if self._symbol_field(row) == symbol:
                return row
        return None

    def _approx_period(self, as_of: date) -> str:
        approx_year = as_of.year - 1911
        approx_quarter = ((max(as_of.month - 1, 0)) // 3) + 1
        return f"{approx_year}Q{approx_quarter}"

    def _recent_periods(self, as_of: date, count: int) -> list[str]:
        roc_year = as_of.year - 1911
        quarter = ((as_of.month - 1) // 3) + 1
        periods: list[str] = []
        for _ in range(max(count, 0)):
            periods.append(f"{roc_year}Q{quarter}")
            quarter -= 1
            if quarter == 0:
                quarter = 4
                roc_year -= 1
        return periods

    def _period_sequence_from(self, anchor_period: str, count: int) -> list[str]:
        year_part, quarter_part = anchor_period.split("Q", 1)
        roc_year = int(year_part)
        quarter = int(quarter_part)
        periods: list[str] = []
        for _ in range(max(count, 0)):
            periods.append(f"{roc_year}Q{quarter}")
            quarter -= 1
            if quarter == 0:
                quarter = 4
                roc_year -= 1
        return periods

    def _latest_reported_period(self, market: str, as_of: date) -> str:
        cache_key = (market, as_of.isoformat())
        if cache_key in self._reported_period_cache:
            return self._reported_period_cache[cache_key]
        eps_url, _, _, _ = self._quarterly_source_urls(market)
        rows = self._safe_get_json(eps_url, []) or []
        best_period = ""
        if isinstance(rows, list):
            for row in rows[:200]:
                period = self._period_from_row(row, as_of)
                if period > best_period:
                    best_period = period
        if not best_period:
            best_period = self._approx_period(as_of)
        self._reported_period_cache[cache_key] = best_period
        return best_period

    def _legacy_quarterly_snapshot(self, symbol: str, market: str, period: str) -> dict[str, Any] | None:
        snapshot_dir = self._legacy_quarterly_snapshot_dir()
        for path in snapshot_dir.glob(f"{market.lower()}_*-{period}.json"):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            if self._find_row(payload.get("income") or [], symbol) and self._find_row(payload.get("balance") or [], symbol) and self._find_row(payload.get("eps") or [], symbol):
                payload = dict(payload)
                payload["source"] = str(payload.get("source") or "legacy_snapshot")
                return payload
        return None

    def _load_snapshot_for_period(self, symbol: str, market: str, period: str, as_of: date) -> dict[str, Any] | None:
        legacy = self._legacy_quarterly_snapshot(symbol, market, period)
        if legacy:
            return legacy
        current = self._load_current_quarter_snapshot(symbol, market, as_of)
        if current and str(current.get("period") or "") == period:
            return current
        return None

    def _load_current_quarter_snapshot(self, symbol: str, market: str, as_of: date) -> dict[str, Any] | None:
        eps_url, income_urls, balance_urls, source_label = self._quarterly_source_urls(market)
        eps_rows = self._get_json(eps_url) or []
        if not isinstance(eps_rows, list):
            eps_rows = []
        eps_row = self._find_row(eps_rows, symbol)
        if not eps_row:
            return None
        period = self._period_from_row(eps_row, as_of)
        for dataset_key, income_url in income_urls.items():
            income_rows = self._get_json(income_url) or []
            if not isinstance(income_rows, list):
                continue
            income_row = self._find_row(income_rows, symbol)
            if not income_row:
                continue
            balance_url = balance_urls.get(dataset_key)
            if not balance_url:
                continue
            balance_rows = self._get_json(balance_url) or []
            if not isinstance(balance_rows, list):
                continue
            balance_row = self._find_row(balance_rows, symbol)
            if not balance_row:
                continue
            snapshot = {
                "period": period,
                "dataset_key": f"{market.lower()}_{dataset_key}",
                "income": [income_row],
                "balance": [balance_row],
                "eps": [eps_row],
                "source": source_label,
            }
            return snapshot
        return None

    def _extract_quarterly_metrics(self, symbol: str, snapshot: dict[str, Any] | None) -> dict[str, float | None]:
        if not isinstance(snapshot, dict):
            return {"gross_margin": None, "eps": None, "roe": None}
        income_row = self._find_row(snapshot.get("income") or [], symbol)
        balance_row = self._find_row(snapshot.get("balance") or [], symbol)
        eps_row = self._find_row(snapshot.get("eps") or [], symbol)
        revenue = safe_float((income_row or {}).get("營業收入"))
        gross_profit = safe_float((income_row or {}).get("營業毛利（毛損）淨額") or (income_row or {}).get("營業毛利毛損淨額"))
        equity = safe_float(
            (balance_row or {}).get("歸屬於母公司業主之權益合計")
            or (balance_row or {}).get("權益總計")
            or (balance_row or {}).get("權益總額")
        )
        net_income = safe_float(
            (eps_row or {}).get("稅後淨利")
            or (eps_row or {}).get("本期淨利（淨損）")
            or (income_row or {}).get("本期淨利（淨損）")
        )
        eps = safe_float(
            (eps_row or {}).get("基本每股盈餘(元)")
            or (eps_row or {}).get("基本每股盈餘（元）")
            or (eps_row or {}).get("基本每股盈餘")
            or (eps_row or {}).get("每股盈餘")
        )
        gross_margin = ((gross_profit / revenue) * 100.0) if revenue and gross_profit is not None else None
        roe = ((net_income / equity) * 400.0) if equity and net_income is not None else None
        return {"gross_margin": gross_margin, "eps": eps, "roe": roe}

    def _is_complete_quarterly_row(self, row: dict[str, Any] | None) -> bool:
        if not row:
            return False
        return all(isinstance(row.get(key), (int, float)) for key in ["gross_margin", "eps", "roe"])

    def _build_quarterly_store_record(
        self,
        symbol: str,
        market: str,
        snapshot: dict[str, Any] | None,
        as_of: date,
        fetched_at: str,
        fetch_status: str,
        missing_reason: str | None,
    ) -> dict[str, Any]:
        period = str((snapshot or {}).get("period") or self._approx_period(as_of))
        dataset_key = str((snapshot or {}).get("dataset_key") or f"{market.lower()}_unknown")
        source = str((snapshot or {}).get("source") or f"{market.lower()}_openapi")
        metrics = self._extract_quarterly_metrics(symbol, snapshot)
        income_row = self._find_row((snapshot or {}).get("income") or [], symbol) or {}
        balance_row = self._find_row((snapshot or {}).get("balance") or [], symbol) or {}
        eps_row = self._find_row((snapshot or {}).get("eps") or [], symbol) or {}
        revenue = safe_float(income_row.get("營業收入"))
        gross_profit = safe_float(income_row.get("營業毛利（毛損）淨額") or income_row.get("營業毛利毛損淨額"))
        equity = safe_float(
            balance_row.get("歸屬於母公司業主之權益合計")
            or balance_row.get("權益總計")
            or balance_row.get("權益總額")
        )
        net_income = safe_float(
            eps_row.get("稅後淨利")
            or eps_row.get("本期淨利（淨損）")
            or income_row.get("本期淨利（淨損）")
        )
        return {
            "symbol": symbol,
            "market": market,
            "period": period,
            "dataset_key": dataset_key,
            "source": source,
            "fetched_at": fetched_at,
            "as_of_date": as_of.isoformat(),
            "gross_margin": round(metrics["gross_margin"], 4) if metrics["gross_margin"] is not None else None,
            "eps": round(metrics["eps"], 4) if metrics["eps"] is not None else None,
            "roe": round(metrics["roe"], 4) if metrics["roe"] is not None else None,
            "revenue": revenue,
            "gross_profit": gross_profit,
            "net_income": net_income,
            "equity": equity,
            "fetch_status": fetch_status,
            "missing_reason": missing_reason,
            "raw_payload_json": json.dumps(snapshot or {}, ensure_ascii=False),
        }

    def _ensure_quarterly_history(self, symbol: str, market: str, as_of: date) -> None:
        target_period = self._latest_reported_period(market, as_of)
        target_periods = self._period_sequence_from(target_period, 2)
        existing = get_period_rows(
            self.quarterly_store_path,
            symbol=symbol,
            market=market,
            periods=target_periods,
            as_of_date=as_of.isoformat(),
        )
        if len(existing) >= 2 and all(self._is_complete_quarterly_row(row) for row in existing[:2]):
            return

        fetched_at = datetime.now().replace(microsecond=0).isoformat()
        current = existing[0] if existing and str(existing[0].get("period") or "") == target_period else None
        if self._is_complete_quarterly_row(current):
            previous_period = target_periods[1] if len(target_periods) > 1 else ""
            if previous_period:
                self._backfill_single_period(
                    symbol=symbol,
                    market=market,
                    period=previous_period,
                    as_of=as_of,
                    attempted_at=fetched_at,
                )
            return
        try:
            current_snapshot = self._load_current_quarter_snapshot(symbol, market, as_of)
        except Exception:
            insert_fundamental_snapshot(
                self.quarterly_store_path,
                {
                    "symbol": symbol,
                    "market": market,
                    "period": target_period,
                    "dataset_key": f"{market.lower()}_unknown",
                    "source": f"{market.lower()}_openapi",
                    "fetched_at": fetched_at,
                    "as_of_date": as_of.isoformat(),
                    "gross_margin": None,
                    "eps": None,
                    "roe": None,
                    "revenue": None,
                    "gross_profit": None,
                    "net_income": None,
                    "equity": None,
                    "fetch_status": "fetch_failed",
                    "missing_reason": "fetch_failed",
                    "raw_payload_json": "{}",
                },
            )
            return

        if not current_snapshot:
            insert_fundamental_snapshot(
                self.quarterly_store_path,
                {
                    "symbol": symbol,
                    "market": market,
                    "period": target_period,
                    "dataset_key": f"{market.lower()}_unknown",
                    "source": f"{market.lower()}_openapi",
                    "fetched_at": fetched_at,
                    "as_of_date": as_of.isoformat(),
                    "gross_margin": None,
                    "eps": None,
                    "roe": None,
                    "revenue": None,
                    "gross_profit": None,
                    "net_income": None,
                    "equity": None,
                    "fetch_status": "unavailable",
                    "missing_reason": "unavailable",
                    "raw_payload_json": "{}",
                },
            )
            return

        current_record = self._build_quarterly_store_record(
            symbol=symbol,
            market=market,
            snapshot=current_snapshot,
            as_of=as_of,
            fetched_at=fetched_at,
            fetch_status="ok",
            missing_reason=None,
        )
        if any(current_record.get(key) is None for key in ["gross_margin", "eps", "roe"]):
            current_record["fetch_status"] = "partial"
            current_record["missing_reason"] = "partial_metrics"
        insert_fundamental_snapshot(self.quarterly_store_path, current_record)
        previous_period = target_periods[1] if len(target_periods) > 1 else ""
        if previous_period:
            self._backfill_single_period(
                symbol=symbol,
                market=market,
                period=previous_period,
                as_of=as_of,
                attempted_at=fetched_at,
            )

    def _backfill_single_period(
        self,
        symbol: str,
        market: str,
        period: str,
        as_of: date,
        attempted_at: str,
    ) -> dict[str, Any]:
        existing = get_latest_periods(
            self.quarterly_store_path,
            symbol=symbol,
            market=market,
            periods=8,
            as_of_date=as_of.isoformat(),
        )
        if any(str(item.get("period") or "") == period and str(item.get("fetch_status") or "") in {"ok", "partial"} for item in existing):
            return {"status": "done", "reason": "already_available"}

        try:
            snapshot = self._load_snapshot_for_period(symbol, market, period, as_of)
        except Exception as exc:
            insert_fundamental_snapshot(
                self.quarterly_store_path,
                {
                    "symbol": symbol,
                    "market": market,
                    "period": period,
                    "dataset_key": f"{market.lower()}_unknown",
                    "source": "backfill",
                    "fetched_at": attempted_at,
                    "as_of_date": as_of.isoformat(),
                    "gross_margin": None,
                    "eps": None,
                    "roe": None,
                    "revenue": None,
                    "gross_profit": None,
                    "net_income": None,
                    "equity": None,
                    "fetch_status": "fetch_failed",
                    "missing_reason": "fetch_failed",
                    "raw_payload_json": "{}",
                },
            )
            return {"status": "failed", "reason": str(exc)}

        if snapshot is None:
            insert_fundamental_snapshot(
                self.quarterly_store_path,
                {
                    "symbol": symbol,
                    "market": market,
                    "period": period,
                    "dataset_key": f"{market.lower()}_unknown",
                    "source": "backfill",
                    "fetched_at": attempted_at,
                    "as_of_date": as_of.isoformat(),
                    "gross_margin": None,
                    "eps": None,
                    "roe": None,
                    "revenue": None,
                    "gross_profit": None,
                    "net_income": None,
                    "equity": None,
                    "fetch_status": "unavailable",
                    "missing_reason": "unavailable",
                    "raw_payload_json": "{}",
                },
            )
            return {"status": "unavailable", "reason": "unavailable"}

        record = self._build_quarterly_store_record(
            symbol=symbol,
            market=market,
            snapshot=snapshot,
            as_of=as_of,
            fetched_at=attempted_at,
            fetch_status="ok",
            missing_reason=None,
        )
        if any(record.get(key) is None for key in ["gross_margin", "eps", "roe"]):
            record["fetch_status"] = "partial"
            record["missing_reason"] = "partial_metrics"
        insert_fundamental_snapshot(self.quarterly_store_path, record)
        return {"status": "done", "reason": record["fetch_status"]}

    def get_quarterly_fundamentals(self, symbol: str, market: str, as_of: date) -> dict[str, float | None]:
        flags: list[str] = []
        self._ensure_quarterly_history(symbol, market, as_of)
        anchor_period = self._latest_reported_period(market, as_of)
        target_periods = self._period_sequence_from(anchor_period, 2)
        periods = get_period_rows(
            self.quarterly_store_path,
            symbol=symbol,
            market=market,
            periods=target_periods,
            as_of_date=as_of.isoformat(),
        )
        current_row = periods[0] if periods else None
        previous_row = periods[1] if len(periods) > 1 else None
        fetch_status = str((current_row or {}).get("fetch_status") or "unavailable")
        missing_reason = (current_row or {}).get("missing_reason")
        periods_used = [str(row.get("period") or "") for row in periods if row.get("period")]
        sources = [str(row.get("source") or "") for row in periods if row.get("source")]
        data_source = "sqlite:" + ",".join(sorted(dict.fromkeys(sources))) if sources else "sqlite"

        current_metrics = {
            "gross_margin": (current_row or {}).get("gross_margin"),
            "eps": (current_row or {}).get("eps"),
            "roe": (current_row or {}).get("roe"),
        }
        previous_metrics = {
            "gross_margin": (previous_row or {}).get("gross_margin"),
            "eps": (previous_row or {}).get("eps"),
            "roe": (previous_row or {}).get("roe"),
        }
        if fetch_status == "fetch_failed":
            flags.append("quality:fetch_failed")
        elif fetch_status == "unavailable":
            flags.append("quality:unavailable")
        elif fetch_status == "partial":
            flags.append("quality:partial_current_metrics")

        if not previous_row:
            missing_reason = missing_reason or "previous_period_unavailable"
            flags.append("quality:previous_period_unavailable")
        elif any(previous_metrics[key] is None for key in ["gross_margin", "eps", "roe"]):
            missing_reason = missing_reason or "previous_period_unavailable"
            flags.append("quality:previous_period_unavailable")

        return {
            "gross_margin_latest": round(current_metrics["gross_margin"], 2) if current_metrics["gross_margin"] is not None else None,
            "gross_margin_prev": round(previous_metrics["gross_margin"], 2) if previous_metrics["gross_margin"] is not None else None,
            "eps_latest": round(current_metrics["eps"], 2) if current_metrics["eps"] is not None else None,
            "eps_prev": round(previous_metrics["eps"], 2) if previous_metrics["eps"] is not None else None,
            "roe_latest": round(current_metrics["roe"], 2) if current_metrics["roe"] is not None else None,
            "roe_prev": round(previous_metrics["roe"], 2) if previous_metrics["roe"] is not None else None,
            "quality_fetch_status": fetch_status,
            "quality_missing_reason": missing_reason,
            "quality_data_source": data_source,
            "quality_periods_used": [x for x in periods_used if x],
            "data_quality_flags": flags,
        }

    def summarize_quality_coverage(
        self,
        rows: list[dict[str, Any]],
        top_n: int = 3,
        history_depth: int = 8,
        as_of: date | None = None,
    ) -> dict[str, Any]:
        symbols = [
            (
                str(row.get("symbol") or "").strip(),
                str(row.get("market") or self._symbol_market_from_theme_rules(str(row.get("symbol") or ""))).strip(),
            )
            for row in rows
            if str(row.get("symbol") or "").strip()
        ]
        anchor_day = as_of or date.today()
        anchor_period = self._latest_reported_period("TWSE", anchor_day)
        return summarize_coverage(
            self.quarterly_store_path,
            symbols,
            periods_required=2,
            as_of_date=anchor_day.isoformat(),
            top_n=top_n,
            history_depth=history_depth,
            anchor_period=anchor_period,
        )

    def run_quality_update_check(
        self,
        theme: str,
        universe: list[dict[str, Any]],
        as_of: date,
        mode: str = "auto",
        budget_sec: float = 3.0,
        history_depth: int = 8,
        top_n: int = 3,
        theme_mode: str = "strict",
    ) -> dict[str, Any]:
        theme_symbols = [(str(row["symbol"]), str(row["market"])) for row in universe]
        coverage = summarize_coverage(
            self.quarterly_store_path,
            theme_symbols,
            periods_required=2,
            as_of_date=as_of.isoformat(),
            top_n=top_n,
            history_depth=history_depth,
            anchor_period=self._latest_reported_period("TWSE", as_of),
        )
        latest_refresh = get_latest_refresh_run(self.quarterly_store_path, theme=theme, theme_mode=theme_mode)
        refresh_run_id = (latest_refresh or {}).get("run_id")
        stale_days = 1 if as_of.month in {3, 5, 8, 11} else 7
        refresh_stale = True
        if latest_refresh and latest_refresh.get("as_of_date"):
            last_refresh_day = date.fromisoformat(str(latest_refresh["as_of_date"])[:10])
            refresh_stale = (as_of - last_refresh_day).days >= stale_days

        top_gap_symbols = [item.get("symbol") for item in coverage.get("top_candidate_gaps") or [] if item.get("symbol")]
        needs_sync = refresh_stale or bool(top_gap_symbols)
        refreshed_symbols: list[str] = []
        decision = "no-op"
        if mode == "skip":
            decision = "skipped"
        elif mode == "force" or (mode == "auto" and needs_sync):
            decision = "forced-sync-repair" if mode == "force" else "sync-repair"
            deadline = time.monotonic() + max(budget_sec, 0.1)
            for row in universe:
                if time.monotonic() > deadline:
                    break
                payload = self.get_quarterly_fundamentals(str(row["symbol"]), str(row["market"]), as_of)
                if payload.get("quality_periods_used"):
                    refreshed_symbols.append(str(row["symbol"]))
            coverage = summarize_coverage(
                self.quarterly_store_path,
                theme_symbols,
                periods_required=2,
                as_of_date=as_of.isoformat(),
                top_n=top_n,
                history_depth=history_depth,
                anchor_period=self._latest_reported_period("TWSE", as_of),
            )

        backfill_enqueued = False
        backfill_run_id: str | None = None
        if coverage.get("history_complete_pct", 0.0) < 100.0 and theme_symbols:
            anchor_period = self._latest_reported_period("TWSE", as_of)
            target_periods = self._period_sequence_from(anchor_period, history_depth)
            queued_count = enqueue_backfill_targets(
                self.quarterly_store_path,
                symbols=theme_symbols,
                periods=target_periods,
                priority=10 if mode == "force" else 50,
                source_hint="auto-check",
            )
            if queued_count > 0:
                backfill_enqueued = True
                backfill_run_id = create_backfill_run(
                    self.quarterly_store_path,
                    trigger_type="auto-check",
                    as_of_date=as_of.isoformat(),
                    scope_json=json.dumps({"theme": theme, "theme_mode": theme_mode}, ensure_ascii=False),
                    target_periods_json=json.dumps(target_periods, ensure_ascii=False),
                    queued_count=queued_count,
                    started_at=datetime.now().replace(microsecond=0).isoformat(),
                )
                finish_backfill_run(
                    self.quarterly_store_path,
                    run_id=backfill_run_id,
                    completed_count=0,
                    unavailable_count=0,
                    failed_count=0,
                    finished_at=datetime.now().replace(microsecond=0).isoformat(),
                    status="queued",
                )

        return {
            "mode": mode,
            "decision": decision,
            "refresh_run_id": refresh_run_id,
            "repair_refreshed_symbols": refreshed_symbols,
            "history_depth_target": history_depth,
            "history_complete_pct": coverage.get("history_complete_pct", 0.0),
            "backfill_enqueued": backfill_enqueued,
            "backfill_run_id": backfill_run_id,
        }

    def backfill_quarterly_history(
        self,
        as_of: date,
        themes: list[str],
        theme_mode: str = "strict",
        periods: int = 8,
        only_missing: bool = True,
        limit_symbols: int | None = None,
        batch_size: int = 20,
        force_retry_days: int = 30,
        trigger_type: str = "manual",
    ) -> dict[str, Any]:
        if trigger_type in {"manual", "force", "quality-repair"}:
            force_retry_days = 0
        theme_payloads: list[dict[str, Any]] = []
        all_symbols: dict[str, dict[str, Any]] = {}
        for theme in themes:
            rows = self.load_theme_universe(theme, theme_mode=theme_mode)
            if limit_symbols is not None:
                rows = rows[:limit_symbols]
            theme_payloads.append({"theme": theme, "symbol_count": len(rows), "symbols": [row["symbol"] for row in rows]})
            for row in rows:
                all_symbols[row["symbol"]] = row

        symbol_pairs = [(str(row["symbol"]), str(row["market"])) for row in all_symbols.values()]
        anchor_period = self._latest_reported_period("TWSE", as_of)
        target_periods = self._period_sequence_from(anchor_period, periods)
        queued_count = enqueue_backfill_targets(
            self.quarterly_store_path,
            symbols=symbol_pairs,
            periods=target_periods,
            priority=20,
            source_hint=trigger_type,
        )
        started_at = datetime.now().replace(microsecond=0).isoformat()
        run_id = create_backfill_run(
            self.quarterly_store_path,
            trigger_type=trigger_type,
            as_of_date=as_of.isoformat(),
            scope_json=json.dumps({"themes": themes, "theme_mode": theme_mode}, ensure_ascii=False),
            target_periods_json=json.dumps(target_periods, ensure_ascii=False),
            queued_count=queued_count,
            started_at=started_at,
        )

        completed_count = 0
        unavailable_count = 0
        failed_count = 0
        warnings: list[str] = []
        now_iso = started_at
        while True:
            batch = claim_backfill_batch(self.quarterly_store_path, limit=batch_size, now_iso=now_iso)
            if not batch:
                break
            progressed = False
            for item in batch:
                last_attempt_at = str(item.get("last_attempt_at") or "")
                if last_attempt_at:
                    try:
                        delta = as_of - date.fromisoformat(last_attempt_at[:10])
                        if delta.days < force_retry_days and str(item.get("status") or "") in {"failed", "unavailable"}:
                            continue
                    except Exception:
                        pass
                result = self._backfill_single_period(
                    symbol=str(item["symbol"]),
                    market=str(item["market"]),
                    period=str(item["period"]),
                    as_of=as_of,
                    attempted_at=datetime.now().replace(microsecond=0).isoformat(),
                )
                mark_backfill_result(
                    self.quarterly_store_path,
                    symbol=str(item["symbol"]),
                    market=str(item["market"]),
                    period=str(item["period"]),
                    status=str(result["status"]),
                    error=None if result["status"] == "done" else str(result.get("reason") or ""),
                    attempted_at=datetime.now().replace(microsecond=0).isoformat(),
                )
                progressed = True
                if result["status"] == "done":
                    completed_count += 1
                elif result["status"] == "unavailable":
                    unavailable_count += 1
                else:
                    failed_count += 1
                    warnings.append(f"{item['symbol']} {item['period']} backfill failed: {result.get('reason')}")
            if not progressed:
                break

        finish_backfill_run(
            self.quarterly_store_path,
            run_id=run_id,
            completed_count=completed_count,
            unavailable_count=unavailable_count,
            failed_count=failed_count,
            finished_at=datetime.now().replace(microsecond=0).isoformat(),
            status="completed",
        )
        summary = summarize_coverage(
            self.quarterly_store_path,
            symbol_pairs,
            periods_required=2,
            as_of_date=as_of.isoformat(),
            history_depth=periods,
            anchor_period=anchor_period,
        )
        unresolved_symbols = [item.get("symbol") for item in (summary.get("top_candidate_gaps") or []) if item.get("symbol")]
        return {
            "as_of": as_of.isoformat(),
            "theme_mode": theme_mode,
            "themes": theme_payloads,
            "periods": periods,
            "target_periods": target_periods,
            "target_symbol_count": len(symbol_pairs),
            "queued_count": queued_count,
            "completed_count": completed_count,
            "unavailable_count": unavailable_count,
            "failed_count": failed_count,
            "quarterly_store_path": str(self.quarterly_store_path),
            "backfill_run_id": run_id,
            "quality_coverage_summary": summary,
            "unresolved_symbols": unresolved_symbols,
            "warnings": warnings,
        }

    def refresh_quarterly_snapshots(
        self,
        as_of: date,
        themes: list[str] | None = None,
        theme_mode: str = "strict",
        universe_mode: str | None = None,
        min_monthly_revenue: float = 0.0,
    ) -> dict[str, Any]:
        selected_themes = themes or core_themes()
        theme_payloads: list[dict[str, Any]] = []
        all_symbols: dict[str, dict[str, Any]] = {}
        for theme in selected_themes:
            rows = self.load_theme_universe(
                theme,
                min_monthly_revenue=min_monthly_revenue,
                theme_mode=theme_mode,
                universe_mode=universe_mode,
            )
            symbols = [row["symbol"] for row in rows]
            theme_payloads.append({"theme": theme, "symbol_count": len(symbols), "symbols": symbols})
            for row in rows:
                all_symbols[row["symbol"]] = row

        refreshed_rows: list[dict[str, Any]] = []
        warnings: list[str] = []
        anchor_period = self._latest_reported_period("TWSE", as_of)
        target_periods = self._period_sequence_from(anchor_period, 2)
        for symbol, row in sorted(all_symbols.items()):
            market = str(row.get("market") or self._symbol_market_from_theme_rules(symbol))
            prior_rows = get_period_rows(
                self.quarterly_store_path,
                symbol=symbol,
                market=market,
                periods=target_periods[:1],
                as_of_date=as_of.isoformat(),
            )
            prior_current = prior_rows[0] if prior_rows else {}
            try:
                payload = self.get_quarterly_fundamentals(symbol, market, as_of)
            except Exception as exc:
                warnings.append(f"{symbol} refresh failed: {exc}")
                payload = {
                    "quality_fetch_status": "fetch_failed",
                    "quality_missing_reason": "refresh_failed",
                    "data_quality_flags": ["quality:refresh_failed"],
                }
            refreshed_rows.append(
                {
                    "symbol": symbol,
                    "market": market,
                    "prior_quality_fetch_status": prior_current.get("fetch_status"),
                    "prior_quality_missing_reason": prior_current.get("missing_reason"),
                    **payload,
                }
            )

        summary = summarize_coverage(
            self.quarterly_store_path,
            [(row["symbol"], row["market"]) for row in refreshed_rows],
            periods_required=2,
            as_of_date=as_of.isoformat(),
            history_depth=8,
            anchor_period=anchor_period,
        )
        run_id = f"refresh-{as_of.strftime('%Y%m%d')}-{theme_mode}"
        upsert_refresh_run(
            self.quarterly_store_path,
            {
                "run_id": run_id,
                "as_of_date": as_of.isoformat(),
                "theme_mode": theme_mode,
                "themes_json": json.dumps(selected_themes, ensure_ascii=False),
                "symbol_count": len(refreshed_rows),
                "current_complete_pct": summary["current_complete_pct"],
                "previous_complete_pct": summary["previous_complete_pct"],
                "warnings_json": json.dumps(warnings, ensure_ascii=False),
                "created_at": datetime.now().replace(microsecond=0).isoformat(),
            },
        )
        return {
            "as_of": as_of.isoformat(),
            "theme_mode": theme_mode,
            "universe_mode": universe_mode,
            "themes": theme_payloads,
            "symbol_count": len(refreshed_rows),
            "quarterly_store_path": str(self.quarterly_store_path),
            "refresh_run_id": run_id,
            "quality_coverage_summary": summary,
            "rows": refreshed_rows,
            "warnings": warnings,
        }

    def _load_basics(self) -> dict[str, dict[str, Any]]:
        rows_twse = self._safe_get_json(TWSE_BASICS_URL, []) or []
        rows_tpex = self._safe_get_json(TPEX_BASICS_URL, []) or []
        self._basics_payload_hash = self._payload_sha256({"twse": rows_twse, "tpex": rows_tpex})
        merged: dict[str, dict[str, Any]] = {}
        for row in rows_twse:
            symbol = str(row.get("公司代號", "")).strip()
            if not _is_stock_symbol(symbol):
                continue
            merged[symbol] = {
                "symbol": symbol,
                "name": str(row.get("公司簡稱") or row.get("公司名稱") or "").strip(),
                "industry": str(row.get("產業別") or "").strip(),
                "market": "TWSE",
            }
        for row in rows_tpex:
            symbol = str(row.get("SecuritiesCompanyCode", "")).strip()
            if not _is_stock_symbol(symbol):
                continue
            merged[symbol] = {
                "symbol": symbol,
                "name": str(row.get("CompanyAbbreviation") or row.get("CompanyName") or "").strip(),
                "industry": str(row.get("SecuritiesIndustryCode") or "").strip(),
                "market": "TPEx",
            }
        return merged

    def _load_latest_revenue_map(self) -> dict[str, dict[str, Any]]:
        rows_twse = self._safe_get_json(TWSE_REVENUE_URL, []) or []
        rows_tpex = self._safe_get_json(TPEX_REVENUE_URL, []) or []
        self._revenue_payload_hash = self._payload_sha256({"twse": rows_twse, "tpex": rows_tpex})
        mapped: dict[str, dict[str, Any]] = {}
        for row in rows_twse:
            symbol = str(row.get("公司代號", "")).strip()
            if not _is_stock_symbol(symbol):
                continue
            mapped[symbol] = {
                "industry": str(row.get("產業別") or "").strip(),
                "monthly_revenue": safe_float(row.get("營業收入-當月營收")) or 0.0,
                "revenue_mom": row.get("營業收入-上月比較增減(%)"),
                "revenue_yoy": row.get("營業收入-去年同月增減(%)"),
                "revenue_month": str(row.get("資料年月") or row.get("年月") or "").strip(),
            }
        for row in rows_tpex:
            symbol = str(row.get("公司代號", "")).strip()
            if not _is_stock_symbol(symbol):
                continue
            mapped[symbol] = {
                "industry": str(row.get("產業別") or "").strip(),
                "monthly_revenue": safe_float(row.get("營業收入-當月營收")) or 0.0,
                "revenue_mom": row.get("營業收入-上月比較增減(%)"),
                "revenue_yoy": row.get("營業收入-去年同月增減(%)"),
                "revenue_month": str(row.get("資料年月") or row.get("年月") or "").strip(),
            }
        return mapped

    @staticmethod
    def _valid_twse_stock_day_payload(payload: Any) -> bool:
        return isinstance(payload, dict) and payload.get("stat") == "OK" and isinstance(payload.get("data"), list)

    @staticmethod
    def _valid_tpex_stock_payload(payload: Any) -> bool:
        if not isinstance(payload, dict) or str(payload.get("stat") or "").lower() != "ok":
            return False
        tables = payload.get("tables")
        return isinstance(tables, list) and bool(tables) and isinstance(tables[0], dict) and isinstance(tables[0].get("data"), list)

    @staticmethod
    def _valid_tpex_bulk_payload(payload: Any) -> bool:
        if not isinstance(payload, dict):
            return False
        tables = payload.get("tables")
        return isinstance(tables, list) and bool(tables) and isinstance(tables[0], dict) and isinstance(tables[0].get("data"), list)

    @staticmethod
    def _candle_from_values(
        trade_date: date,
        open_value: Any,
        high_value: Any,
        low_value: Any,
        close_value: Any,
        volume_value: Any,
    ) -> dict[str, Any] | None:
        open_price = safe_float(open_value)
        high_price = safe_float(high_value)
        low_price = safe_float(low_value)
        close_price = safe_float(close_value)
        volume = safe_float(volume_value)
        if None in {open_price, high_price, low_price, close_price, volume}:
            return None
        if min(open_price, high_price, low_price, close_price) <= 0 or volume < 0:
            return None
        if high_price < max(open_price, close_price, low_price) or low_price > min(open_price, close_price, high_price):
            return None
        return {
            "date": trade_date,
            "open": float(open_price),
            "high": float(high_price),
            "low": float(low_price),
            "close": float(close_price),
            "volume": float(volume),
        }

    @staticmethod
    def _month_weekdays(month_start: date, as_of: date) -> list[date]:
        next_month = _shift_month(month_start, 1)
        current = month_start
        days: list[date] = []
        while current < next_month and current <= as_of:
            if _is_weekday(current):
                days.append(current)
            current += timedelta(days=1)
        return days

    def _get_tpex_daily_quotes_for_day(
        self,
        day: date,
        *,
        force_network: bool = False,
    ) -> dict[str, dict[str, Any]]:
        if day in self._tpex_daily_quotes_cache:
            return self._tpex_daily_quotes_cache[day]
        request = self._build_get_request(
            TPEX_DAILY_QUOTES_URL,
            {
                "l": "zh-tw",
                "d": f"{day.year - 1911:03d}/{day:%m/%d}",
                "se": "EW",
                "o": "json",
            },
        )
        payload = self._load_json_candidates(
            [request],
            endpoint_label="tpex.daily_quotes",
            validator=self._valid_tpex_bulk_payload,
            use_cache=not force_network,
        )
        tables = payload.get("tables") or []
        table = tables[0] if tables and isinstance(tables[0], dict) else {}
        fields = table.get("fields") or []
        rows = table.get("data") or []
        table_date = _try_parse_roc_slash(str(table.get("date") or "")) or day
        field_indexes = {
            "symbol": 0,
            "close": 2,
            "open": 4,
            "high": 5,
            "low": 6,
            "volume": 7,
        }
        if isinstance(fields, list):
            normalised = {re.sub(r"\s+", "", str(name)).replace("<br>", ""): index for index, name in enumerate(fields)}
            for key, names in {
                "symbol": ["代號"],
                "close": ["收盤"],
                "open": ["開盤"],
                "high": ["最高"],
                "low": ["最低"],
                "volume": ["成交股數"],
            }.items():
                for name in names:
                    if name in normalised:
                        field_indexes[key] = normalised[name]
                        break
        mapping: dict[str, dict[str, Any]] = {}
        for row in rows:
            if not isinstance(row, list) or len(row) <= max(field_indexes.values()):
                continue
            symbol = str(row[field_indexes["symbol"]]).strip()
            if not _is_stock_symbol(symbol):
                continue
            candle = self._candle_from_values(
                table_date,
                row[field_indexes["open"]],
                row[field_indexes["high"]],
                row[field_indexes["low"]],
                row[field_indexes["close"]],
                row[field_indexes["volume"]],
            )
            if candle is not None:
                mapping[symbol] = candle
        self._tpex_daily_quotes_cache[day] = mapping
        return mapping

    def _get_tpex_bulk_month(
        self,
        symbol: str,
        month_start: date,
        as_of: date,
        *,
        force_network: bool = False,
    ) -> list[dict[str, Any]]:
        collected: list[dict[str, Any]] = []
        for day in self._month_weekdays(month_start, as_of):
            try:
                candle = self._get_tpex_daily_quotes_for_day(
                    day,
                    force_network=force_network,
                ).get(symbol)
            except Exception:
                continue
            if candle is not None and candle["date"] <= as_of:
                collected.append(candle)
        return collected

    @staticmethod
    def _payload_sha256(payload: Any) -> str:
        return hashlib.sha256(
            json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()

    def _verified_bar_from_candle(
        self,
        *,
        market: str,
        symbol: str,
        candle: dict[str, Any],
        source_endpoint: str,
        source_url: str,
        source_cache_file: str,
        source_payload_sha256: str,
        source_priority: int = 100,
    ) -> VerifiedDailyBar:
        return VerifiedDailyBar(
            market=market,
            symbol=symbol,
            trade_date=candle["date"],
            open=float(candle["open"]),
            high=float(candle["high"]),
            low=float(candle["low"]),
            close=float(candle["close"]),
            volume=float(candle["volume"]),
            source_endpoint=source_endpoint,
            source_url=source_url,
            source_cache_file=source_cache_file,
            source_payload_sha256=source_payload_sha256,
            source_fetched_at=datetime.now().astimezone().isoformat(),
            source_priority=source_priority,
        )

    def _record_market_payload(
        self,
        *,
        dataset_key: str,
        request: Request,
        payload: Any,
        effective_date: date,
        validation_status: str = "verified",
    ) -> str:
        source_url = self._last_resolved_source_url or request.full_url
        body = request.data if isinstance(request.data, (bytes, bytearray)) else b""
        payload_hash = self._payload_sha256(payload)
        try:
            record_source_payload(
                self.market_data_db_path,
                dataset_key=dataset_key,
                request_method=request.get_method(),
                source_endpoint=self._endpoint_name(source_url),
                source_url=source_url,
                request_body_sha256=hashlib.sha256(body).hexdigest() if body else None,
                payload=payload,
                effective_date=effective_date.isoformat(),
                fetched_at=datetime.now().astimezone().isoformat(),
                cache_file=str(self._cache_path(request)),
                validation_status=validation_status,
                raw_storage_root=self.cache_dir / "raw_payloads",
            )
            return payload_hash
        except Exception:
            # Raw-payload persistence is audit enrichment. It must not turn a
            # valid official bar into a failed ranking because SQLite is busy.
            return payload_hash

    def _fetch_twse_month_bars(
        self,
        symbol: str,
        month_start: date,
        as_of: date,
        *,
        force_network: bool = False,
    ) -> list[VerifiedDailyBar]:
        params = {"response": "json", "date": month_start.strftime("%Y%m01"), "stockNo": symbol}
        requests = [
            self._build_get_request(TWSE_STOCK_DAY_PRIMARY_URL, params),
            self._build_get_request(TWSE_STOCK_DAY_URL, params),
        ]
        payload = self._load_json_candidates(
            requests,
            endpoint_label="twse.stock_day",
            validator=self._valid_twse_stock_day_payload,
            use_cache=not force_network,
        )
        source_url = self._last_resolved_source_url or requests[0].full_url
        payload_hash = self._record_market_payload(
            dataset_key="twse.stock_day",
            request=requests[0],
            payload=payload,
            effective_date=month_start,
        )
        bars: list[VerifiedDailyBar] = []
        for row in payload.get("data") or []:
            if not isinstance(row, list) or len(row) < 7:
                continue
            trade_date = _try_parse_roc_slash(str(row[0]))
            if trade_date is None or trade_date > as_of:
                continue
            if (trade_date.year, trade_date.month) != (month_start.year, month_start.month):
                continue
            candle = self._candle_from_values(trade_date, row[3], row[4], row[5], row[6], row[1])
            if candle is None:
                continue
            bars.append(
                self._verified_bar_from_candle(
                    market="TWSE",
                    symbol=symbol,
                    candle=candle,
                    source_endpoint="twse.stock_day",
                    source_url=source_url,
                    source_cache_file=str(self._cache_path(requests[0])),
                    source_payload_sha256=payload_hash,
                    source_priority=10 if source_url.startswith(TWSE_STOCK_DAY_PRIMARY_URL) else 20,
                )
            )
        return bars

    def _fetch_tpex_month_bars(
        self,
        symbol: str,
        month_start: date,
        as_of: date,
        *,
        force_network: bool = False,
    ) -> list[VerifiedDailyBar]:
        params = {"code": symbol, "date": month_start.strftime("%Y/%m/01"), "response": "json"}
        requests = [
            self._build_get_request(TPEX_TRADING_STOCK_URL, params),
            self._build_post_request(TPEX_TRADING_STOCK_URL, params),
        ]
        try:
            payload = self._load_json_candidates(
                requests,
                endpoint_label="tpex.trading_stock",
                validator=self._valid_tpex_stock_payload,
                use_cache=not force_network,
            )
            source_url = self._last_resolved_source_url or requests[0].full_url
            payload_hash = self._record_market_payload(
                dataset_key="tpex.trading_stock",
                request=requests[0],
                payload=payload,
                effective_date=month_start,
            )
            tables = payload.get("tables") or []
            table = tables[0] if tables and isinstance(tables[0], dict) else {}
            bars: list[VerifiedDailyBar] = []
            for row in table.get("data") or []:
                if not isinstance(row, list) or len(row) < 7:
                    continue
                trade_date = _try_parse_roc_slash(str(row[0]))
                if trade_date is None or trade_date > as_of:
                    continue
                if (trade_date.year, trade_date.month) != (month_start.year, month_start.month):
                    continue
                candle = self._candle_from_values(trade_date, row[3], row[4], row[5], row[6], row[1])
                if candle is None:
                    continue
                bars.append(
                    self._verified_bar_from_candle(
                        market="TPEx",
                        symbol=symbol,
                        candle=candle,
                        source_endpoint="tpex.trading_stock",
                        source_url=source_url,
                        source_cache_file=str(self._cache_path(requests[0])),
                        source_payload_sha256=payload_hash,
                        source_priority=10 if requests[0].get_method() == "GET" else 20,
                    )
                )
            return bars
        except Exception:
            # The bulk endpoint is a bounded fallback. It is shared in memory
            # by all TPEx symbols, so one successful day request fills many
            # missing individual histories without re-querying the stock API.
            bulk_bars = self._get_tpex_bulk_month(
                symbol,
                month_start,
                as_of,
                force_network=force_network,
            )
            bars: list[VerifiedDailyBar] = []
            for candle in bulk_bars:
                day_request = self._build_get_request(
                    TPEX_DAILY_QUOTES_URL,
                    {
                        "l": "zh-tw",
                        "d": f"{candle['date'].year - 1911:03d}/{candle['date']:%m/%d}",
                        "se": "EW",
                        "o": "json",
                    },
                )
                source_url = self._last_resolved_source_url or day_request.full_url
                bars.append(
                    self._verified_bar_from_candle(
                        market="TPEx",
                        symbol=symbol,
                        candle=candle,
                        source_endpoint="tpex.daily_quotes",
                        source_url=source_url,
                        source_cache_file=str(self._cache_path(day_request)),
                        source_payload_sha256=self._payload_sha256({"symbol": symbol, **candle}),
                        source_priority=30,
                    )
                )
            return bars

    def get_ohlcv(self, symbol: str, market: str, as_of: date, lookback: int = 252) -> list[dict[str, Any]]:
        cache_key = (symbol, market, as_of.isoformat(), lookback, _is_weekday(as_of))
        if cache_key in self._ohlcv_cache:
            return self._ohlcv_cache[cache_key]
        cached = get_bars(
            self.market_data_db_path,
            market=market,
            symbol=symbol,
            as_of=as_of,
            limit=max(lookback, 1),
        )
        expected = as_of if _is_weekday(as_of) else (cached[-1]["date"] if cached else None)
        current_day_ready = not _is_weekday(as_of) or is_current_day_verified(
            self.market_data_db_path,
            market=market,
            symbol=symbol,
            trade_date=as_of,
        )
        if len(cached) >= lookback and (expected is None or cached[-1]["date"] == expected) and current_day_ready:
            self._market_data_stats.db_hit_count += 1
            if _is_weekday(as_of):
                self._market_data_stats.current_day_verified_count += 1
            candles = cached[-lookback:]
            self._ohlcv_cache[cache_key] = candles
            return candles

        self._market_data_stats.db_missing_count += 1
        self._market_data_stats.incremental_fetch_count += 1
        known_dates = {item["date"] for item in cached}
        fetched: list[VerifiedDailyBar] = []
        errors: list[str] = []
        network_current_day_seen = False
        need_historical = len(cached) < lookback
        anchor = date(as_of.year, as_of.month, 1)
        max_months = max(6, (lookback // 18) + 6)
        for index in range(max_months):
            month_start = _shift_month(anchor, -index)
            if index > 0 and not need_historical:
                break
            try:
                month_bars = (
                    self._fetch_tpex_month_bars(
                        symbol,
                        month_start,
                        as_of,
                        force_network=month_start == anchor,
                    )
                    if market == "TPEx"
                    else self._fetch_twse_month_bars(
                        symbol,
                        month_start,
                        as_of,
                        force_network=month_start == anchor,
                    )
                )
            except Exception as exc:
                errors.append(f"{month_start:%Y-%m}: {exc}")
                continue
            for bar in month_bars:
                if _is_weekday(as_of) and bar.trade_date == as_of:
                    network_current_day_seen = True
                if bar.trade_date not in known_dates:
                    fetched.append(bar)
                    known_dates.add(bar.trade_date)
            if len(known_dates) >= lookback:
                break

        if fetched:
            storage_stats = import_verified_bars(
                self.market_data_db_path,
                fetched,
                imported_at=datetime.now().astimezone().isoformat(),
            )
            self._market_data_stats.db_write_count += storage_stats.inserted_rows + storage_stats.updated_rows
            rebuild_period_bars(self.market_data_db_path, market=market, symbol=symbol)

        candles = get_bars(
            self.market_data_db_path,
            market=market,
            symbol=symbol,
            as_of=as_of,
            limit=max(lookback, 1),
        )
        if len(candles) < lookback:
            detail = f"；最近錯誤：{' | '.join(errors[-3:])}" if errors else ""
            raise MarketDataFetchError(
                f"{market} {symbol} 日線不足：{len(candles)}/{lookback}{detail}",
                [{"symbol": symbol, "market": market, "error": error} for error in errors],
            )
        if _is_weekday(as_of) and candles[-1]["date"] != as_of:
            self._market_data_stats.current_day_failure_count += 1
            actual = candles[-1]["date"].isoformat() if candles else None
            detail = f"；最近錯誤：{' | '.join(errors[-3:])}" if errors else ""
            raise MarketDataFetchError(
                f"{market} {symbol} 當日資料缺口：expected={as_of.isoformat()} actual={actual}{detail}",
                [{"symbol": symbol, "market": market, "error": error} for error in errors],
            )
        if _is_weekday(as_of):
            if network_current_day_seen:
                mark_current_day_verified(
                    self.market_data_db_path,
                    market=market,
                    symbol=symbol,
                    trade_date=as_of,
                )
            elif not is_current_day_verified(
                self.market_data_db_path,
                market=market,
                symbol=symbol,
                trade_date=as_of,
            ):
                self._market_data_stats.current_day_failure_count += 1
                raise MarketDataFetchError(
                    f"{market} {symbol} 當日資料未經目前來源驗證：expected={as_of.isoformat()}",
                )
            self._market_data_stats.current_day_verified_count += 1
        candles = candles[-lookback:]
        self._ohlcv_cache[cache_key] = candles
        return candles

    def _get_twse_ohlcv(self, symbol: str, as_of: date, lookback: int) -> list[dict[str, Any]]:
        collected: list[dict[str, Any]] = []
        errors: list[str] = []
        anchor = date(as_of.year, as_of.month, 1)
        max_months = max(6, (lookback // 18) + 6)
        for i in range(max_months):
            d = _shift_month(anchor, -i)
            params = {"response": "json", "date": d.strftime("%Y%m01"), "stockNo": symbol}
            try:
                payload = self._load_json_candidates(
                    [
                        self._build_get_request(TWSE_STOCK_DAY_PRIMARY_URL, params),
                        self._build_get_request(TWSE_STOCK_DAY_URL, params),
                    ],
                    endpoint_label="twse.stock_day",
                    validator=self._valid_twse_stock_day_payload,
                )
            except Exception as exc:
                errors.append(f"{d:%Y-%m}: {exc}")
                continue
            for row in payload.get("data") or []:
                if not isinstance(row, list) or len(row) < 7:
                    continue
                trade_date = _try_parse_roc_slash(str(row[0]))
                if trade_date is None or trade_date > as_of or (trade_date.year, trade_date.month) != (d.year, d.month):
                    continue
                candle = self._candle_from_values(trade_date, row[3], row[4], row[5], row[6], row[1])
                if candle is not None:
                    collected.append(candle)
            if len(collected) >= lookback:
                break
        if not collected:
            detail = f"；最近錯誤：{' | '.join(errors[-3:])}" if errors else ""
            raise MarketDataFetchError(f"TWSE 無法取得 {symbol} 日線{detail}")
        dedup = {c["date"]: c for c in collected}
        return sorted(dedup.values(), key=lambda x: x["date"])[-lookback:]

    def _get_tpex_ohlcv(self, symbol: str, as_of: date, lookback: int) -> list[dict[str, Any]]:
        collected: list[dict[str, Any]] = []
        errors: list[str] = []
        anchor = date(as_of.year, as_of.month, 1)
        max_months = max(6, (lookback // 18) + 6)
        for i in range(max_months):
            d = _shift_month(anchor, -i)
            params = {"code": symbol, "date": d.strftime("%Y/%m/01"), "response": "json"}
            try:
                payload = self._load_json_candidates(
                    [
                        self._build_get_request(TPEX_TRADING_STOCK_URL, params),
                        self._build_post_request(TPEX_TRADING_STOCK_URL, params),
                    ],
                    endpoint_label="tpex.trading_stock",
                    validator=self._valid_tpex_stock_payload,
                )
            except Exception as exc:
                errors.append(f"{d:%Y-%m}: {exc}")
                collected.extend(self._get_tpex_bulk_month(symbol, d, as_of))
                if len(collected) >= lookback:
                    break
                continue
            tables = payload.get("tables") or []
            table = tables[0] if tables and isinstance(tables[0], dict) else {}
            for row in table.get("data") or []:
                if not isinstance(row, list) or len(row) < 7:
                    continue
                trade_date = _try_parse_roc_slash(str(row[0]))
                if trade_date is None or trade_date > as_of or (trade_date.year, trade_date.month) != (d.year, d.month):
                    continue
                candle = self._candle_from_values(trade_date, row[3], row[4], row[5], row[6], row[1])
                if candle is not None:
                    collected.append(candle)
            if len(collected) >= lookback:
                break
        if not collected:
            detail = f"；最近錯誤：{' | '.join(errors[-3:])}" if errors else ""
            raise MarketDataFetchError(f"TPEx 無法取得 {symbol} 日線{detail}")
        dedup = {c["date"]: c for c in collected}
        return sorted(dedup.values(), key=lambda x: x["date"])[-lookback:]

    def get_latest_valuation(self, symbol: str, market: str, as_of: date, max_backtrack_days: int = 20) -> dict[str, float] | None:
        if market == "TPEx":
            result = self._get_tpex_latest_valuation(symbol, as_of, max_backtrack_days)
            source_endpoint = "tpex.peQryDate"
            source_url = TPEX_PE_QRY_DATE_URL
        else:
            result = self._get_twse_latest_valuation(symbol, as_of, max_backtrack_days)
            source_endpoint = "twse.BWIBBU_d"
            source_url = TWSE_BWIBBU_URL
        if result is not None:
            try:
                upsert_valuation_snapshot(
                    self.market_data_db_path,
                    market=market,
                    symbol=symbol,
                    trade_date=as_of,
                    pe=result.get("pe"),
                    pb=result.get("pb"),
                    dividend_yield=result.get("dividend_yield"),
                    source_endpoint=source_endpoint,
                    source_url=source_url,
                    fetched_at=datetime.now().astimezone().isoformat(),
                )
            except Exception:
                pass
        return result

    def _get_twse_valuation_table(self, d: date) -> dict[str, dict[str, float]]:
        key = d.isoformat()
        if key in self._twse_valuation_cache:
            return self._twse_valuation_cache[key]
        payload = self._get_json(
            TWSE_BWIBBU_URL,
            {"response": "json", "date": d.strftime("%Y%m%d"), "selectType": "ALL"},
        )
        result: dict[str, dict[str, float]] = {}
        if isinstance(payload, dict) and payload.get("stat") == "OK":
            fields = payload.get("fields") or []
            rows = payload.get("data") or []
            idx = {str(name).strip(): i for i, name in enumerate(fields)}
            code_idx = idx.get("證券代號", 0)
            pe_idx = idx.get("本益比")
            pb_idx = idx.get("股價淨值比")
            dy_idx = idx.get("殖利率(%)")
            for row in rows:
                if not isinstance(row, list) or code_idx >= len(row):
                    continue
                symbol = str(row[code_idx]).strip()
                pe = safe_float(row[pe_idx] if pe_idx is not None and pe_idx < len(row) else None)
                pb = safe_float(row[pb_idx] if pb_idx is not None and pb_idx < len(row) else None)
                dy = safe_float(row[dy_idx] if dy_idx is not None and dy_idx < len(row) else None)
                result[symbol] = {
                    "pe": pe if pe and pe > 0 else 0.0,
                    "pb": pb if pb and pb > 0 else 0.0,
                    "dividend_yield": dy if dy and dy >= 0 else 0.0,
                }
        self._twse_valuation_cache[key] = result
        return result

    def _get_tpex_valuation_table(self, d: date) -> dict[str, dict[str, float]]:
        key = d.isoformat()
        if key in self._tpex_valuation_cache:
            return self._tpex_valuation_cache[key]
        payload = self._post_json(TPEX_PE_QRY_DATE_URL, {"date": d.strftime("%Y/%m/%d"), "response": "json"})
        result: dict[str, dict[str, float]] = {}
        if isinstance(payload, dict) and payload.get("stat") == "ok":
            tables = payload.get("tables")
            table0 = tables[0] if isinstance(tables, list) and tables and isinstance(tables[0], dict) else {}
            fields = table0.get("fields") or []
            rows = table0.get("data") or []
            idx = {str(name).strip(): i for i, name in enumerate(fields)}
            code_idx = idx.get("股票代號", 0)
            pe_idx = idx.get("本益比")
            pb_idx = idx.get("股價淨值比")
            dy_idx = idx.get("殖利率(%)")
            for row in rows:
                if not isinstance(row, list) or code_idx >= len(row):
                    continue
                symbol = str(row[code_idx]).strip()
                pe = safe_float(row[pe_idx] if pe_idx is not None and pe_idx < len(row) else None)
                pb = safe_float(row[pb_idx] if pb_idx is not None and pb_idx < len(row) else None)
                dy = safe_float(row[dy_idx] if dy_idx is not None and dy_idx < len(row) else None)
                result[symbol] = {
                    "pe": pe if pe and pe > 0 else 0.0,
                    "pb": pb if pb and pb > 0 else 0.0,
                    "dividend_yield": dy if dy and dy >= 0 else 0.0,
                }
        self._tpex_valuation_cache[key] = result
        return result

    def _get_twse_latest_valuation(self, symbol: str, as_of: date, max_backtrack_days: int) -> dict[str, float] | None:
        for i in range(max_backtrack_days + 1):
            d = as_of - timedelta(days=i)
            table = self._get_twse_valuation_table(d)
            if symbol in table:
                return table[symbol]
        return None

    def _get_tpex_latest_valuation(self, symbol: str, as_of: date, max_backtrack_days: int) -> dict[str, float] | None:
        for i in range(max_backtrack_days + 1):
            d = as_of - timedelta(days=i)
            table = self._get_tpex_valuation_table(d)
            if symbol in table:
                return table[symbol]
        return None
