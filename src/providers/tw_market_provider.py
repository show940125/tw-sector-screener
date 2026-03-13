from __future__ import annotations

import hashlib
import json
import re
import ssl
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from src.analysis.factors import safe_float
from src.providers.quarterly_store import (
    get_latest_periods,
    init_db,
    insert_fundamental_snapshot,
    summarize_coverage,
    upsert_refresh_run,
)
from src.themes import core_themes, theme_rule


TWSE_BASICS_URL = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
TWSE_REVENUE_URL = "https://openapi.twse.com.tw/v1/opendata/t187ap05_L"
TWSE_STOCK_DAY_URL = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
TWSE_BWIBBU_URL = "https://www.twse.com.tw/exchangeReport/BWIBBU_d"
TWSE_FMTQIK_URL = "https://www.twse.com.tw/exchangeReport/FMTQIK"
TWSE_EPS_URL = "https://openapi.twse.com.tw/v1/opendata/t187ap14_L"

TPEX_BASICS_URL = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O"
TPEX_REVENUE_URL = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O"
TPEX_TRADING_STOCK_URL = "https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingStock"
TPEX_PE_QRY_DATE_URL = "https://www.tpex.org.tw/www/zh-tw/afterTrading/peQryDate"
TPEX_EPS_URL = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap14_O"

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
    def __init__(self, timeout: float = 10.0, cache_dir: Path | None = None) -> None:
        self.timeout = timeout
        self.cache_dir = cache_dir or (Path(__file__).resolve().parents[2] / ".cache" / "market")
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.quarterly_store_path = self.cache_dir / "quarterly_fundamentals.sqlite"
        init_db(self.quarterly_store_path)
        self._twse_valuation_cache: dict[str, dict[str, dict[str, float]]] = {}
        self._tpex_valuation_cache: dict[str, dict[str, dict[str, float]]] = {}
        self._ohlcv_cache: dict[tuple[str, str, str, int], list[dict[str, Any]]] = {}

    def _load_json(self, req: Request) -> Any:
        last_exc: Exception | None = None
        cache_file = self._cache_path(req)
        cached = self._read_cache(cache_file, self._cache_ttl_seconds(req))
        if cached is not None:
            return cached
        for attempt in range(3):
            try:
                with urlopen(req, timeout=self.timeout) as resp:
                    payload = json.loads(resp.read().decode("utf-8-sig"))
                    self._write_cache(cache_file, payload)
                    return payload
            except Exception as exc:
                last_exc = exc
                reason = getattr(exc, "reason", None)
                if isinstance(exc, ssl.SSLCertVerificationError) or isinstance(reason, ssl.SSLCertVerificationError):
                    insecure_ctx = ssl._create_unverified_context()
                    with urlopen(req, timeout=self.timeout, context=insecure_ctx) as resp:
                        payload = json.loads(resp.read().decode("utf-8-sig"))
                        self._write_cache(cache_file, payload)
                        return payload
                if attempt < 2:
                    time.sleep(0.6 * (attempt + 1))
                    continue
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("無法讀取 JSON")

    def _cache_path(self, req: Request) -> Path:
        body = req.data.decode("utf-8", errors="ignore") if isinstance(req.data, (bytes, bytearray)) else ""
        digest = hashlib.sha256(f"{req.full_url}|{body}".encode("utf-8")).hexdigest()
        return self.cache_dir / f"{digest}.json"

    def _cache_ttl_seconds(self, req: Request) -> int:
        url = req.full_url.lower()
        if any(marker in url for marker in ["date=", "stockno=", "code="]):
            return 365 * 24 * 3600
        return 12 * 3600

    def _read_cache(self, cache_file: Path, ttl_seconds: int) -> Any:
        if not cache_file.exists():
            return None
        age_seconds = time.time() - cache_file.stat().st_mtime
        if age_seconds > ttl_seconds:
            return None
        try:
            return json.loads(cache_file.read_text(encoding="utf-8"))
        except Exception:
            return None

    def _write_cache(self, cache_file: Path, payload: Any) -> None:
        try:
            cache_file.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        except Exception:
            return

    def _symbol_market_from_theme_rules(self, symbol: str) -> str:
        return "TPEx" if symbol.startswith("6") or symbol.startswith("8") else "TWSE"

    def _get_json(self, url: str, params: dict[str, Any] | None = None) -> Any:
        query = urlencode(params) if params else ""
        full_url = f"{url}?{query}" if query else url
        req = Request(full_url, headers={"User-Agent": "Mozilla/5.0"})
        return self._load_json(req)

    def _safe_get_json(self, url: str, default: Any) -> Any:
        try:
            return self._get_json(url)
        except Exception:
            return default

    def _post_json(self, url: str, data: dict[str, Any]) -> Any:
        req = Request(
            url,
            data=urlencode(data).encode("utf-8"),
            headers={
                "User-Agent": "Mozilla/5.0",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            },
        )
        return self._load_json(req)

    def load_theme_universe(
        self,
        theme: str,
        min_monthly_revenue: float = 0.0,
        theme_mode: str = "strict",
    ) -> list[dict[str, Any]]:
        rule = theme_rule(theme, theme_mode=theme_mode)
        output: list[dict[str, Any]] = []
        for row in self.load_all_universe(min_monthly_revenue=min_monthly_revenue):
            if self._theme_match(row["symbol"], row["name"], row.get("industry") or "", rule):
                output.append(row)

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
        collected: dict[date, dict[str, Any]] = {}
        cursor = date(as_of.year, as_of.month, 1)
        months_checked = 0

        while months_checked < 36 and len(collected) < (lookback + 10):
            payload = self._get_json(TWSE_FMTQIK_URL, {"response": "json", "date": cursor.strftime("%Y%m%d")})
            if isinstance(payload, dict) and payload.get("stat") == "OK":
                fields = payload.get("fields") or []
                rows = payload.get("data") or []
                idx = {str(name).strip(): i for i, name in enumerate(fields)}
                date_idx = idx.get("日期", 0)
                close_idx = idx.get("發行量加權股價指數")
                chg_idx = idx.get("漲跌點數")
                for row in rows:
                    if not isinstance(row, list) or date_idx >= len(row):
                        continue
                    d = _parse_roc_slash(str(row[date_idx]))
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
        return series[-lookback:]

    def _theme_match(self, symbol: str, name: str, industry: str, rule: dict[str, Any]) -> bool:
        if symbol in set(rule.get("symbols") or []):
            return True
        if rule.get("theme_mode") != "broad":
            return False
        text = f"{name} {industry}".lower()
        for kw in rule.get("name_keywords") or []:
            if str(kw).lower() in text:
                return True
        for kw in rule.get("industry_keywords") or []:
            if str(kw).lower() in industry.lower():
                return True
        return False

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
            or (eps_row or {}).get("每股盈餘")
        )
        gross_margin = ((gross_profit / revenue) * 100.0) if revenue and gross_profit is not None else None
        roe = ((net_income / equity) * 400.0) if equity and net_income is not None else None
        return {"gross_margin": gross_margin, "eps": eps, "roe": roe}

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
        existing = get_latest_periods(
            self.quarterly_store_path,
            symbol=symbol,
            market=market,
            periods=2,
            as_of_date=as_of.isoformat(),
        )
        if len(existing) >= 2:
            return

        fetched_at = datetime.now().replace(microsecond=0).isoformat()
        try:
            current_snapshot = self._load_current_quarter_snapshot(symbol, market, as_of)
        except Exception:
            insert_fundamental_snapshot(
                self.quarterly_store_path,
                {
                    "symbol": symbol,
                    "market": market,
                    "period": self._approx_period(as_of),
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
                    "period": self._approx_period(as_of),
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

    def get_quarterly_fundamentals(self, symbol: str, market: str, as_of: date) -> dict[str, float | None]:
        flags: list[str] = []
        self._ensure_quarterly_history(symbol, market, as_of)
        periods = get_latest_periods(
            self.quarterly_store_path,
            symbol=symbol,
            market=market,
            periods=2,
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

    def summarize_quality_coverage(self, rows: list[dict[str, Any]], top_n: int = 3) -> dict[str, Any]:
        symbols = [
            (
                str(row.get("symbol") or "").strip(),
                str(row.get("market") or self._symbol_market_from_theme_rules(str(row.get("symbol") or ""))).strip(),
            )
            for row in rows
            if str(row.get("symbol") or "").strip()
        ]
        return summarize_coverage(self.quarterly_store_path, symbols, periods_required=2, top_n=top_n)

    def refresh_quarterly_snapshots(
        self,
        as_of: date,
        themes: list[str] | None = None,
        theme_mode: str = "strict",
        min_monthly_revenue: float = 0.0,
    ) -> dict[str, Any]:
        selected_themes = themes or core_themes()
        theme_payloads: list[dict[str, Any]] = []
        all_symbols: dict[str, dict[str, Any]] = {}
        for theme in selected_themes:
            rows = self.load_theme_universe(theme, min_monthly_revenue=min_monthly_revenue, theme_mode=theme_mode)
            symbols = [row["symbol"] for row in rows]
            theme_payloads.append({"theme": theme, "symbol_count": len(symbols), "symbols": symbols})
            for row in rows:
                all_symbols[row["symbol"]] = row

        refreshed_rows: list[dict[str, Any]] = []
        warnings: list[str] = []
        for symbol, row in sorted(all_symbols.items()):
            market = str(row.get("market") or self._symbol_market_from_theme_rules(symbol))
            try:
                payload = self.get_quarterly_fundamentals(symbol, market, as_of)
            except Exception as exc:
                warnings.append(f"{symbol} refresh failed: {exc}")
                payload = {
                    "quality_fetch_status": "fetch_failed",
                    "quality_missing_reason": "refresh_failed",
                    "data_quality_flags": ["quality:refresh_failed"],
                }
            refreshed_rows.append({"symbol": symbol, "market": market, **payload})

        summary = summarize_coverage(
            self.quarterly_store_path,
            [(row["symbol"], row["market"]) for row in refreshed_rows],
            periods_required=2,
            as_of_date=as_of.isoformat(),
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
            }
        return mapped

    def get_ohlcv(self, symbol: str, market: str, as_of: date, lookback: int = 252) -> list[dict[str, Any]]:
        cache_key = (symbol, market, as_of.isoformat(), lookback)
        if cache_key in self._ohlcv_cache:
            return self._ohlcv_cache[cache_key]
        if market == "TPEx":
            candles = self._get_tpex_ohlcv(symbol, as_of, lookback)
        else:
            candles = self._get_twse_ohlcv(symbol, as_of, lookback)
        self._ohlcv_cache[cache_key] = candles
        return candles

    def _get_twse_ohlcv(self, symbol: str, as_of: date, lookback: int) -> list[dict[str, Any]]:
        collected: list[dict[str, Any]] = []
        anchor = date(as_of.year, as_of.month, 1)
        max_months = max(6, (lookback // 18) + 6)
        for i in range(max_months):
            d = _shift_month(anchor, -i)
            payload = self._get_json(
                TWSE_STOCK_DAY_URL,
                {"response": "json", "date": d.strftime("%Y%m01"), "stockNo": symbol},
            )
            if not isinstance(payload, dict) or payload.get("stat") != "OK":
                continue
            for row in payload.get("data") or []:
                if len(row) < 7:
                    continue
                trade_date = _parse_roc_slash(str(row[0]))
                if trade_date > as_of:
                    continue
                o = safe_float(row[3])
                h = safe_float(row[4])
                l = safe_float(row[5])
                c = safe_float(row[6])
                v = safe_float(row[1])
                if None in {o, h, l, c, v}:
                    continue
                collected.append(
                    {
                        "date": trade_date,
                        "open": float(o),
                        "high": float(h),
                        "low": float(l),
                        "close": float(c),
                        "volume": float(v),
                    }
                )
            if len(collected) >= lookback:
                break
        if not collected:
            raise ValueError(f"TWSE 無法取得 {symbol} 日線")
        dedup = {c["date"]: c for c in collected}
        return sorted(dedup.values(), key=lambda x: x["date"])[-lookback:]

    def _get_tpex_ohlcv(self, symbol: str, as_of: date, lookback: int) -> list[dict[str, Any]]:
        collected: list[dict[str, Any]] = []
        anchor = date(as_of.year, as_of.month, 1)
        max_months = max(6, (lookback // 18) + 6)
        for i in range(max_months):
            d = _shift_month(anchor, -i)
            payload = self._post_json(
                TPEX_TRADING_STOCK_URL,
                {"code": symbol, "date": d.strftime("%Y/%m/01"), "response": "json"},
            )
            if not isinstance(payload, dict) or payload.get("stat") != "ok":
                continue
            tables = payload.get("tables")
            rows = tables[0].get("data") if isinstance(tables, list) and tables and isinstance(tables[0], dict) else []
            for row in rows or []:
                if not isinstance(row, list) or len(row) < 7:
                    continue
                trade_date = _parse_roc_slash(str(row[0]))
                if trade_date > as_of:
                    continue
                o = safe_float(row[3])
                h = safe_float(row[4])
                l = safe_float(row[5])
                c = safe_float(row[6])
                v = safe_float(row[1])
                if None in {o, h, l, c, v}:
                    continue
                collected.append(
                    {
                        "date": trade_date,
                        "open": float(o),
                        "high": float(h),
                        "low": float(l),
                        "close": float(c),
                        "volume": float(v),
                    }
                )
            if len(collected) >= lookback:
                break
        if not collected:
            raise ValueError(f"TPEx 無法取得 {symbol} 日線")
        dedup = {c["date"]: c for c in collected}
        return sorted(dedup.values(), key=lambda x: x["date"])[-lookback:]

    def get_latest_valuation(self, symbol: str, market: str, as_of: date, max_backtrack_days: int = 20) -> dict[str, float] | None:
        if market == "TPEx":
            return self._get_tpex_latest_valuation(symbol, as_of, max_backtrack_days)
        return self._get_twse_latest_valuation(symbol, as_of, max_backtrack_days)

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
