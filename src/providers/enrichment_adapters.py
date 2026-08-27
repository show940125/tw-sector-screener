from __future__ import annotations

"""Small, provenance-preserving adapters for historical research datasets.

The adapters deliberately keep parsing independent from transport.  Production
sync supplies a bounded fetcher through ``AdapterContext.options`` while tests
can provide a fixture payload without opening a network connection.
"""

import hashlib
import json
import re
from datetime import date
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

from src.analysis.factors import safe_float
from src.providers.market_data_adapters import (
    AdapterContext,
    FetchRequest,
    FetchResult,
    ValidationResult,
)
from src.providers.market_data_store import (
    get_financial_facts_history,
    get_monthly_revenue_history,
    get_valuation_history,
    upsert_corporate_action,
    upsert_financial_fact,
    upsert_market_session,
    upsert_monthly_revenue,
    upsert_valuation_snapshot,
    update_source_payload_validation,
)


def _payload_hash(payload: Any) -> str:
    if isinstance(payload, (bytes, bytearray)):
        raw = bytes(payload)
    elif isinstance(payload, str):
        raw = payload.encode("utf-8")
    else:
        raw = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _clean_key(value: Any) -> str:
    return re.sub(r"\s+", "", str(value or "")).replace("<br>", "")


def _first_value(row: dict[str, Any], names: tuple[str, ...]) -> Any:
    normalised = {_clean_key(key): value for key, value in row.items()}
    for name in names:
        if _clean_key(name) in normalised:
            return normalised[_clean_key(name)]
    return None


def _rows_from_payload(payload: Any) -> list[dict[str, Any]]:
    """Convert common TWSE/TPEx list-fields-data envelopes to dictionaries."""

    if isinstance(payload, list):
        return [dict(row) for row in payload if isinstance(row, dict)]
    if not isinstance(payload, dict):
        return []

    candidates: list[tuple[list[Any], list[Any]]] = []
    data = payload.get("data")
    fields = payload.get("fields")
    if isinstance(data, list):
        candidates.append((data, fields if isinstance(fields, list) else []))
    tables = payload.get("tables")
    if isinstance(tables, list):
        for table in tables:
            if not isinstance(table, dict) or not isinstance(table.get("data"), list):
                continue
            table_fields = table.get("fields")
            candidates.append((table["data"], table_fields if isinstance(table_fields, list) else []))

    output: list[dict[str, Any]] = []
    for raw_rows, raw_fields in candidates:
        for row in raw_rows:
            if isinstance(row, dict):
                output.append(dict(row))
            elif isinstance(row, list) and raw_fields:
                output.append(
                    {
                        str(raw_fields[index]): value
                        for index, value in enumerate(row)
                        if index < len(raw_fields)
                    }
                )
    return output


def normalize_revenue_month(value: Any) -> str | None:
    """Normalize ROC/Gregorian month forms to an ISO ``YYYY-MM`` period."""

    raw = str(value or "").strip()
    digits = re.sub(r"[^0-9]", "", raw)
    try:
        first_component = re.split(r"[-/]", raw, maxsplit=1)[0]
        if (len(first_component) == 3 or len(raw) == 5) and len(digits) == 5:
            year, month = int(digits[:3]) + 1911, int(digits[3:])
        elif len(digits) >= 6:
            year, month = int(digits[:4]), int(digits[4:6])
        else:
            return None
        date(year, month, 1)
    except ValueError:
        return None
    return f"{year:04d}-{month:02d}"


def _normalize_date(value: Any) -> date | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    digits = re.sub(r"[^0-9]", "", raw)
    try:
        first_component = re.split(r"[-/]", raw, maxsplit=1)[0]
        if len(digits) == 7 and len(first_component) == 7:
            return date(int(digits[:3]) + 1911, int(digits[3:5]), int(digits[5:7]))
        if len(first_component) == 3 and len(digits) == 7:
            return date(int(digits[:3]) + 1911, int(digits[3:5]), int(digits[5:7]))
        if len(digits) >= 8:
            return date(int(digits[:4]), int(digits[4:6]), int(digits[6:8]))
        return date.fromisoformat(raw[:10])
    except ValueError:
        return None


def _normalize_fiscal_period(value: Any) -> str | None:
    raw = re.sub(r"\s+", "", str(value or "").upper())
    match = re.fullmatch(r"(\d{3,4})Q([1-4])", raw)
    if match:
        return f"{match.group(1)}Q{match.group(2)}"
    year_match = re.fullmatch(r"(\d{3,4})", raw)
    if year_match:
        return year_match.group(1)
    return raw or None


def parse_financial_facts_payload(
    payload: Any,
    *,
    market: str,
    source_endpoint: str,
    source_url: str,
    source_payload_sha256: str | None = None,
    default_fiscal_period: str | None = None,
    default_effective_date: date | str | None = None,
    default_available_date: date | str | None = None,
    published_at: str | None = None,
) -> list[dict[str, Any]]:
    """Normalize JSON facts while preserving revision/PIT metadata."""

    payload_hash = source_payload_sha256 or _payload_hash(payload)
    rows: list[dict[str, Any]] = []
    for raw in _rows_from_payload(payload):
        symbol = str(
            _first_value(raw, ("公司代號", "股票代號", "SecuritiesCompanyCode", "symbol")) or ""
        ).strip()
        fact_code = str(
            _first_value(raw, ("fact_code", "科目代號", "科目名稱", "fact", "name")) or ""
        ).strip()
        fiscal_period = _normalize_fiscal_period(
            _first_value(raw, ("fiscal_period", "財報年季", "資料年季", "年度季別", "年度"))
            or default_fiscal_period
        )
        effective_date = _normalize_date(
            _first_value(raw, ("effective_date", "財報截止日", "期間截止日", "報表日期"))
            or default_effective_date
        )
        available_date = _normalize_date(
            _first_value(raw, ("available_date", "可取得日", "公告日", "發布日期"))
            or default_available_date
        )
        if not symbol or not fact_code or fiscal_period is None:
            continue
        dimension = _first_value(raw, ("dimension_json", "dimensions", "維度"))
        if isinstance(dimension, dict):
            dimension_json: str | dict[str, Any] = dimension
        elif dimension:
            dimension_json = str(dimension)
        else:
            dimension_json = {}
        raw_value = _first_value(raw, ("value", "數值", "金額", "amount"))
        raw_revision_sequence = _first_value(raw, ("revision_sequence", "修訂序號"))
        try:
            revision_sequence = int(raw_revision_sequence or 1)
        except (TypeError, ValueError):
            revision_sequence = 0
        rows.append(
            {
                "market": market,
                "symbol": symbol,
                "fact_code": fact_code,
                "fiscal_period": fiscal_period,
                "value": safe_float(raw_value),
                "unit": str(_first_value(raw, ("unit", "單位")) or "unknown").strip(),
                "consolidation": str(
                    _first_value(raw, ("consolidation", "合併/個別", "合併個別"))
                    or "consolidated"
                ).strip(),
                "dimension_json": dimension_json,
                "effective_date": effective_date,
                "available_date": available_date,
                "published_at": _first_value(raw, ("published_at", "公告時間", "發布時間"))
                or published_at,
                "revision_id": str(
                    _first_value(raw, ("revision_id", "修訂編號")) or payload_hash
                ),
                "revision_sequence": revision_sequence,
                "source_endpoint": source_endpoint,
                "source_url": source_url,
                "source_payload_sha256": payload_hash,
                "raw_payload_json": raw,
            }
        )
    return rows


def parse_corporate_actions_payload(
    payload: Any,
    *,
    market: str,
    source_endpoint: str,
    source_url: str,
    source_payload_sha256: str | None = None,
    available_date: date | str | None = None,
) -> list[dict[str, Any]]:
    """Normalize dividend/split/ex-rights event rows without guessing dates."""

    payload_hash = source_payload_sha256 or _payload_hash(payload)
    rows: list[dict[str, Any]] = []
    for raw in _rows_from_payload(payload):
        symbol = str(
            _first_value(
                raw,
                (
                    "公司代號",
                    "股票代號",
                    "證券代號",
                    "SecuritiesCompanyCode",
                    "Code",
                    "symbol",
                ),
            )
            or ""
        ).strip()
        action_date = _normalize_date(
            _first_value(
                raw,
                ("action_date", "事件日期", "資料日期", "生效日", "除權息日期", "Date"),
            )
        )
        action_type = str(
            _first_value(
                raw,
                (
                    "action_type",
                    "事件類型",
                    "股利種類",
                    "異動類別",
                    "Exdividend",
                    "ExRightsDiviend",
                ),
            )
            or ""
        ).strip()
        if not symbol or action_date is None or not action_type:
            continue
        rows.append(
            {
                "market": market,
                "symbol": symbol,
                "action_date": action_date,
                "action_type": action_type,
                "ex_date": _normalize_date(_first_value(raw, ("ex_date", "除權息日", "Date"))),
                "record_date": _normalize_date(_first_value(raw, ("record_date", "停止過戶日"))),
                "payment_date": _normalize_date(_first_value(raw, ("payment_date", "發放日"))),
                "ratio": safe_float(
                    _first_value(
                        raw,
                        (
                            "ratio",
                            "配股率",
                            "股票股利",
                            "StockDividendRatio",
                            "StockDividend",
                        ),
                    )
                ),
                "cash_amount": safe_float(
                    _first_value(
                        raw,
                        ("cash_amount", "現金股利", "股利", "CashDividend", "CashDivdend"),
                    )
                ),
                "published_at": _first_value(raw, ("published_at", "公告時間", "公告日期")),
                "available_date": available_date,
                "source_endpoint": source_endpoint,
                "source_url": source_url,
                "source_payload_sha256": payload_hash,
                "raw_payload_json": raw,
            }
        )
    return rows


def parse_market_sessions_payload(
    payload: Any,
    *,
    market: str,
    source_endpoint: str,
    source_url: str,
    source_payload_sha256: str | None = None,
    available_date: date | str | None = None,
) -> list[dict[str, Any]]:
    """Normalize an exchange holiday/session calendar."""

    payload_hash = source_payload_sha256 or _payload_hash(payload)
    rows: list[dict[str, Any]] = []
    for raw in _rows_from_payload(payload):
        trade_date = _normalize_date(
            _first_value(raw, ("trade_date", "日期", "西元日期", "交易日", "Date"))
        )
        if trade_date is None:
            continue
        raw_open = _first_value(raw, ("is_open", "是否開市", "是否交易", "開市"))
        is_open = str(raw_open or "").strip().lower() in {"1", "y", "yes", "是", "開市", "true"}
        if raw_open is None:
            # The official holiday schedule uses descriptions instead of a
            # boolean.  Only an explicit trading/settlement description is
            # treated as open; ordinary missing fields remain closed/unknown.
            description = str(_first_value(raw, ("Description", "說明", "description")) or "")
            is_open = "交易" in description and "無交易" not in description and "休市" not in description
        rows.append(
            {
                "market": market,
                "trade_date": trade_date,
                "is_open": is_open,
                "session_type": str(_first_value(raw, ("session_type", "交易時段")) or "regular"),
                "published_at": _first_value(raw, ("published_at", "公告日期")),
                "available_date": available_date,
                "source_endpoint": source_endpoint,
                "source_url": source_url,
                "source_payload_sha256": payload_hash,
            }
        )
    return rows


def parse_monthly_revenue_payload(
    payload: Any,
    *,
    market: str,
    available_date: date | str | None,
    source_endpoint: str,
    source_url: str,
    source_payload_sha256: str | None = None,
    published_at: str | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    payload_hash = source_payload_sha256 or _payload_hash(payload)
    for raw in _rows_from_payload(payload):
        symbol = str(
            _first_value(raw, ("公司代號", "股票代號", "SecuritiesCompanyCode")) or ""
        ).strip()
        period = normalize_revenue_month(
            _first_value(raw, ("資料年月", "年月", "營業年月", "revenue_month"))
        )
        if not symbol or period is None:
            continue
        rows.append(
            {
                "market": market,
                "symbol": symbol,
                "revenue_month": period,
                "monthly_revenue": safe_float(
                    _first_value(raw, ("營業收入-當月營收", "當月營收", "monthly_revenue"))
                ),
                "revenue_mom": safe_float(
                    _first_value(raw, ("營業收入-上月比較增減(%)", "上月比較增減(%)", "revenue_mom"))
                ),
                "revenue_yoy": safe_float(
                    _first_value(raw, ("營業收入-去年同月增減(%)", "去年同月增減(%)", "revenue_yoy"))
                ),
                "available_date": available_date,
                "published_at": published_at
                or _first_value(raw, ("公布日期", "發布日期", "公告日期")),
                "source_endpoint": source_endpoint,
                "source_url": source_url,
                "source_payload_sha256": payload_hash,
            }
        )
    return rows


def _mops_result_value(data: Any, label: str) -> Any:
    """Return the first value for a labelled row in the MOPS SPA result."""

    if not isinstance(data, list):
        return None
    for row in data:
        if isinstance(row, (list, tuple)) and len(row) >= 2:
            if _clean_key(row[0]) == _clean_key(label):
                return row[1]
        elif isinstance(row, dict):
            key = _first_value(row, ("項目", "label", "name", "名稱"))
            if _clean_key(key) == _clean_key(label):
                return _first_value(row, ("數值", "value", "值", "data"))
    return None


def parse_mops_company_revenue_payload(
    payload: Any,
    *,
    market: str,
    symbol: str,
    available_date: date | str | None,
    source_endpoint: str,
    source_url: str,
    source_payload_sha256: str | None = None,
    published_at: str | None = None,
) -> list[dict[str, Any]]:
    """Parse the current official MOPS SPA single-company response.

    The new endpoint returns one company/month as JSON.  It has a YoY
    percentage but no prior-month percentage, so ``revenue_mom`` remains
    null rather than being inferred from an unrelated value.
    """

    if not isinstance(payload, dict) or str(payload.get("code") or "") not in {"200", "200.0"}:
        raise ValueError("MOPS company revenue response is not a successful JSON envelope")
    result = payload.get("result")
    if not isinstance(result, dict):
        raise ValueError("MOPS company revenue response has no result object")
    result_symbol = str(result.get("companyId") or symbol or "").strip()
    if result_symbol != str(symbol).strip():
        raise ValueError(f"MOPS company code mismatch: expected {symbol}, got {result_symbol}")
    period = normalize_revenue_month(result.get("yymm"))
    data = result.get("data")
    monthly_revenue = safe_float(_mops_result_value(data, "本月"))
    revenue_yoy = safe_float(_mops_result_value(data, "增減百分比"))
    if period is None or monthly_revenue is None:
        raise ValueError("MOPS company revenue response is missing period or monthly revenue")
    return [
        {
            "market": market,
            "symbol": result_symbol,
            "revenue_month": period,
            "monthly_revenue": monthly_revenue,
            "revenue_mom": None,
            "revenue_yoy": revenue_yoy,
            "available_date": available_date,
            "published_at": published_at,
            "source_endpoint": source_endpoint,
            "source_url": source_url,
            "source_payload_sha256": source_payload_sha256 or _payload_hash(payload),
        }
    ]


def _chart_series_value(chart: dict[str, Any], name: str) -> list[Any] | None:
    series = chart.get("series") if isinstance(chart, dict) else None
    if not isinstance(series, list):
        return None
    for item in series:
        if isinstance(item, dict) and str(item.get("name") or "").strip() == name:
            data = item.get("data")
            return list(data) if isinstance(data, list) else None
    return None


def parse_twse_company_financial_payload(
    payload: Any,
    *,
    market: str,
    symbol: str,
    available_date: date | str | None,
    source_endpoint: str,
    source_url: str,
    source_payload_sha256: str | None = None,
    published_at: str | None = None,
) -> list[dict[str, Any]]:
    """Parse TWSE IIH's official company financial chart response.

    This endpoint provides the latest 13 monthly observations.  Month-over-
    month and year-over-year percentages are recomputed only when the source
    chart contains the required preceding observation.
    """

    if not isinstance(payload, dict):
        raise ValueError("TWSE company financial response is not JSON")
    info = payload.get("info")
    if not isinstance(info, dict) or str(info.get("status") or "").lower() != "success":
        raise ValueError("TWSE company financial response is not successful")
    info_data = info.get("data") if isinstance(info.get("data"), dict) else {}
    response_symbol = str(info_data.get("code") or symbol).strip()
    if response_symbol != str(symbol).strip():
        raise ValueError(f"TWSE company code mismatch: expected {symbol}, got {response_symbol}")
    chart = payload.get("chart")
    revenue_chart = chart.get("revenue") if isinstance(chart, dict) else None
    if not isinstance(revenue_chart, dict):
        raise ValueError("TWSE company financial response has no revenue chart")
    categories = revenue_chart.get("categories")
    values = _chart_series_value(revenue_chart, "月營收")
    if not isinstance(categories, list) or not isinstance(values, list) or not categories:
        raise ValueError("TWSE company financial revenue chart is empty")
    if len(categories) != len(values):
        raise ValueError("TWSE company financial revenue chart length mismatch")
    pe_chart = chart.get("pe") if isinstance(chart, dict) else None
    pb_chart = chart.get("pb") if isinstance(chart, dict) else None
    pe_values = _chart_series_value(pe_chart, "本益比") or []
    pb_values = _chart_series_value(pb_chart, "股價淨值比") or []
    parsed: list[tuple[str, float | None]] = []
    for category, value in zip(categories, values):
        period = normalize_revenue_month(category)
        if period is None:
            continue
        parsed.append((period, safe_float(value)))
    if not parsed or any(value is None for _period, value in parsed):
        raise ValueError("TWSE company financial revenue chart contains invalid values")
    rows: list[dict[str, Any]] = []
    for index, (period, value) in enumerate(parsed):
        assert value is not None
        previous = parsed[index - 1][1] if index >= 1 else None
        prior_year = parsed[index - 12][1] if index >= 12 else None
        mom = ((value / previous) - 1.0) * 100.0 if previous not in {None, 0} else None
        yoy = ((value / prior_year) - 1.0) * 100.0 if prior_year not in {None, 0} else None
        rows.append(
            {
                "market": market,
                "symbol": response_symbol,
                "revenue_month": period,
                "monthly_revenue": value,
                "revenue_mom": mom,
                "revenue_yoy": yoy,
                "pe": safe_float(pe_values[index]) if index < len(pe_values) else None,
                "pb": safe_float(pb_values[index]) if index < len(pb_values) else None,
                "available_date": available_date,
                "published_at": published_at,
                "source_endpoint": source_endpoint,
                "source_url": source_url,
                "source_payload_sha256": source_payload_sha256 or _payload_hash(payload),
            }
        )
    return rows


class _HTMLTableParser(HTMLParser):
    """Collect table cells without making pandas a runtime dependency."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rows: list[list[str]] = []
        self._row: list[str] | None = None
        self._cell: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        if tag == "tr":
            self._row = []
        elif tag in {"th", "td"} and self._row is not None:
            self._cell = []

    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            self._cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in {"th", "td"} and self._row is not None and self._cell is not None:
            self._row.append(re.sub(r"\s+", " ", "".join(self._cell)).strip())
            self._cell = None
        elif tag == "tr" and self._row is not None:
            if any(cell for cell in self._row):
                self.rows.append(self._row)
            self._row = None


def parse_mops_revenue_html(
    payload: str | bytes,
    *,
    market: str,
    revenue_month: str,
    available_date: date | str | None,
    source_endpoint: str,
    source_url: str,
    source_payload_sha256: str | None = None,
    published_at: str | None = None,
) -> list[dict[str, Any]]:
    """Parse the tabular MOPS monthly-revenue response.

    MOPS has returned HTML for this historical query even when its current
    OpenAPI feed is JSON.  The parser intentionally accepts only a table with
    a company-code and monthly-revenue column; unknown layouts produce an
    empty result and are therefore handled as a failed partition by sync.
    """

    text = payload.decode("utf-8-sig", errors="replace") if isinstance(payload, bytes) else payload
    parser = _HTMLTableParser()
    parser.feed(str(text))
    period = normalize_revenue_month(revenue_month)
    if period is None:
        raise ValueError(f"invalid revenue month: {revenue_month}")
    raw_rows: list[dict[str, Any]] = []
    code_names = {"公司代號", "股票代號", "證券代號", "securitiescompanycode"}
    revenue_names = {"營業收入-當月營收", "當月營收", "monthly_revenue"}
    for table_row_index, cells in enumerate(parser.rows):
        cleaned = [_clean_key(cell) for cell in cells]
        if not any(name in cleaned for name in code_names):
            continue
        if not any(name in cleaned for name in revenue_names):
            continue
        header = cleaned
        for values in parser.rows[table_row_index + 1 :]:
            if len(values) < 2:
                continue
            mapped = {
                header[index]: value
                for index, value in enumerate(values)
                if index < len(header) and header[index]
            }
            symbol = _first_value(mapped, tuple(code_names))
            if symbol is None:
                continue
            raw_rows.append(
                {
                    "公司代號": symbol,
                    "資料年月": period,
                    "營業收入-當月營收": _first_value(mapped, tuple(revenue_names)),
                    "營業收入-上月比較增減(%)": _first_value(
                        mapped, ("營業收入-上月比較增減(%)", "上月比較增減(%)", "revenue_mom")
                    ),
                    "營業收入-去年同月增減(%)": _first_value(
                        mapped, ("營業收入-去年同月增減(%)", "去年同月增減(%)", "revenue_yoy")
                    ),
                }
            )
        break
    return parse_monthly_revenue_payload(
        raw_rows,
        market=market,
        available_date=available_date,
        published_at=published_at,
        source_endpoint=source_endpoint,
        source_url=source_url,
        source_payload_sha256=source_payload_sha256 or _payload_hash(text),
    )


def parse_valuation_payload(
    payload: Any,
    *,
    market: str,
    trade_date: date,
    source_endpoint: str,
    source_url: str,
    source_payload_sha256: str | None = None,
    published_at: str | None = None,
) -> list[dict[str, Any]]:
    payload_hash = source_payload_sha256 or _payload_hash(payload)
    rows: list[dict[str, Any]] = []
    for raw in _rows_from_payload(payload):
        symbol = str(
            _first_value(raw, ("證券代號", "股票代號", "公司代號", "SecuritiesCompanyCode")) or ""
        ).strip()
        if not symbol:
            continue
        rows.append(
            {
                "market": market,
                "symbol": symbol,
                "trade_date": trade_date,
                "pe": safe_float(_first_value(raw, ("本益比", "PE", "pe"))),
                "pb": safe_float(_first_value(raw, ("股價淨值比", "PB", "pb"))),
                "dividend_yield": safe_float(
                    _first_value(raw, ("殖利率(%)", "殖利率％", "殖利率", "dividend_yield"))
                ),
                "published_at": published_at,
                "source_endpoint": source_endpoint,
                "source_url": source_url,
                "source_payload_sha256": payload_hash,
            }
        )
    return rows


def _validate_fact_rows(rows: list[dict[str, Any]]) -> ValidationResult:
    errors: list[str] = []
    seen: set[str] = set()
    for index, row in enumerate(rows):
        required = ("market", "symbol", "fact_code", "fiscal_period", "unit", "consolidation")
        missing = [key for key in required if row.get(key) in {None, ""}]
        if missing:
            errors.append(f"row_{index}:missing:{','.join(missing)}")
        symbol = str(row.get("symbol") or "")
        if len(symbol) != 4 or not symbol.isdigit():
            errors.append(f"row_{index}:invalid_symbol:{symbol}")
        if row.get("effective_date") is None or row.get("available_date") is None:
            errors.append(f"row_{index}:missing_pit_date")
        if int(row.get("revision_sequence") or 0) < 1:
            errors.append(f"row_{index}:invalid_revision_sequence")
        identity = "|".join(
            str(row.get(key) or "")
            for key in ("market", "symbol", "fact_code", "fiscal_period", "revision_id")
        )
        if identity in seen:
            errors.append(f"row_{index}:duplicate_identity:{identity}")
        seen.add(identity)
    if not rows:
        errors.append("no_valid_rows")
    effective_dates = [row["effective_date"] for row in rows if isinstance(row.get("effective_date"), date)]
    return ValidationResult(
        status="verified" if not errors else "failed",
        errors=tuple(errors),
        row_count=len(rows),
        first_effective_date=min(effective_dates) if effective_dates else None,
        last_effective_date=max(effective_dates) if effective_dates else None,
    )


def _validate_rows(rows: list[dict[str, Any]], required: tuple[str, ...]) -> ValidationResult:
    errors: list[str] = []
    seen: set[str] = set()
    for index, row in enumerate(rows):
        missing = [key for key in required if row.get(key) in {None, ""}]
        if missing:
            errors.append(f"row_{index}:missing:{','.join(missing)}")
        symbol = str(row.get("symbol") or "").strip()
        if symbol and (len(symbol) != 4 or not symbol.isdigit()):
            errors.append(f"row_{index}:invalid_symbol:{symbol}")
        identity = "|".join(str(row.get(key) or "") for key in required)
        if identity in seen:
            errors.append(f"row_{index}:duplicate_identity:{identity}")
        seen.add(identity)
    if not rows:
        errors.append("no_valid_rows")
    return ValidationResult(
        status="verified" if not errors else "failed",
        errors=tuple(errors),
        row_count=len(rows),
    )


def _finalize_payload(context: AdapterContext, validation: ValidationResult) -> None:
    payload_id = context.options.get("source_payload_id")
    if not payload_id:
        return
    update_source_payload_validation(
        Path(context.database_path),
        payload_id=str(payload_id),
        validation_status=validation.status,
        validation_error="; ".join(validation.errors) if validation.errors else None,
    )


class _FetcherAdapter:
    dataset_key: str

    def fetch_range(self, request: FetchRequest, context: AdapterContext) -> FetchResult:
        fetcher = context.options.get("fetcher")
        if not callable(fetcher):
            raise ValueError(f"{self.dataset_key} requires options['fetcher']")
        result = fetcher(request)
        if isinstance(result, FetchResult):
            return result
        return FetchResult(
            status="fetched",
            payload=result,
            request=request,
            final_url=request.url,
            payload_sha256=_payload_hash(result),
        )


class HistoricalMonthlyRevenueAdapter(_FetcherAdapter):
    dataset_key = "monthly_revenue"

    def identity_key(self, row: dict[str, Any]) -> str:
        return f"{row['market']}|{row['symbol']}|{row['revenue_month']}"

    def partition_key(self, request: FetchRequest) -> str:
        if request.requested_from:
            return request.requested_from.strftime("%Y-%m")
        return request.requested_to.strftime("%Y-%m") if request.requested_to else "unknown"

    def parse(self, result: FetchResult, context: AdapterContext) -> list[dict[str, Any]]:
        source_endpoint = str(context.options.get("source_endpoint") or self.dataset_key)
        source_url = str(result.final_url or context.options.get("source_url") or result.request.url)
        market = str(result.request.market or context.options.get("market") or "")
        symbol = str(result.request.symbol or context.options.get("symbol") or "").strip()
        if isinstance(result.payload, dict) and result.payload.get("result") is not None:
            if not symbol:
                raise ValueError("MOPS company revenue JSON requires a symbol")
            return parse_mops_company_revenue_payload(
                result.payload,
                market=market,
                symbol=symbol,
                available_date=context.options.get("available_date"),
                published_at=context.options.get("published_at"),
                source_endpoint=source_endpoint,
                source_url=source_url,
                source_payload_sha256=result.payload_sha256,
            )
        if isinstance(result.payload, dict) and result.payload.get("chart") is not None:
            if not symbol:
                raise ValueError("TWSE company financial JSON requires a symbol")
            return parse_twse_company_financial_payload(
                result.payload,
                market=market,
                symbol=symbol,
                available_date=context.options.get("available_date"),
                published_at=context.options.get("published_at"),
                source_endpoint=source_endpoint,
                source_url=source_url,
                source_payload_sha256=result.payload_sha256,
            )
        if isinstance(result.payload, (str, bytes)):
            month = result.request.requested_from or result.request.requested_to
            if month is None:
                raise ValueError("monthly_revenue HTML payload requires a requested month")
            return parse_mops_revenue_html(
                result.payload,
                market=market,
                revenue_month=month.strftime("%Y-%m"),
                available_date=context.options.get("available_date"),
                published_at=context.options.get("published_at"),
                source_endpoint=source_endpoint,
                source_url=source_url,
                source_payload_sha256=result.payload_sha256,
            )
        return parse_monthly_revenue_payload(
            result.payload,
            market=market,
            available_date=context.options.get("available_date"),
            published_at=context.options.get("published_at"),
            source_endpoint=source_endpoint,
            source_url=source_url,
            source_payload_sha256=result.payload_sha256,
        )

    def validate(self, rows: list[dict[str, Any]], context: AdapterContext) -> ValidationResult:
        return _validate_rows(rows, ("market", "symbol", "revenue_month"))

    def upsert(self, rows: list[dict[str, Any]], context: AdapterContext) -> int:
        validation = self.validate(rows, context)
        if validation.status != "verified":
            _finalize_payload(context, validation)
            raise ValueError("monthly_revenue validation failed: " + "; ".join(validation.errors))
        _finalize_payload(context, validation)
        inserted = 0
        for row in rows:
            upsert_monthly_revenue(
                Path(context.database_path),
                market=row["market"],
                symbol=row["symbol"],
                revenue_month=row["revenue_month"],
                monthly_revenue=row.get("monthly_revenue"),
                revenue_mom=row.get("revenue_mom"),
                revenue_yoy=row.get("revenue_yoy"),
                source_endpoint=row["source_endpoint"],
                source_url=row["source_url"],
                source_payload_sha256=row.get("source_payload_sha256"),
                source_payload_id=context.options.get("source_payload_id"),
                fetched_at=context.options.get("fetched_at"),
                published_at=row.get("published_at"),
                available_date=row.get("available_date"),
                availability_precision=str(
                    context.options.get("availability_precision") or "unknown"
                ),
                validation_status=str(
                    context.options.get("validation_status") or "verified"
                ),
                data_gap_reason=context.options.get("data_gap_reason"),
            )
            inserted += 1
        return inserted

    def completeness_report(self, context: AdapterContext) -> dict[str, Any]:
        rows = get_monthly_revenue_history(Path(context.database_path))
        return {"dataset_key": self.dataset_key, "row_count": len(rows)}


class HistoricalValuationAdapter(_FetcherAdapter):
    dataset_key = "valuation_snapshots"

    def identity_key(self, row: dict[str, Any]) -> str:
        return f"{row['market']}|{row['symbol']}|{row['trade_date']}"

    def partition_key(self, request: FetchRequest) -> str:
        if request.requested_from:
            return request.requested_from.strftime("%Y-%m")
        return request.requested_to.strftime("%Y-%m") if request.requested_to else "unknown"

    def parse(self, result: FetchResult, context: AdapterContext) -> list[dict[str, Any]]:
        trade_date = context.options.get("trade_date") or result.request.requested_to
        if not isinstance(trade_date, date):
            raise ValueError("valuation_snapshots requires a trade_date")
        return parse_valuation_payload(
            result.payload,
            market=str(result.request.market or context.options.get("market") or ""),
            trade_date=trade_date,
            published_at=context.options.get("published_at"),
            source_endpoint=str(context.options.get("source_endpoint") or self.dataset_key),
            source_url=str(result.final_url or context.options.get("source_url") or result.request.url),
            source_payload_sha256=result.payload_sha256,
        )

    def validate(self, rows: list[dict[str, Any]], context: AdapterContext) -> ValidationResult:
        return _validate_rows(rows, ("market", "symbol", "trade_date"))

    def upsert(self, rows: list[dict[str, Any]], context: AdapterContext) -> int:
        validation = self.validate(rows, context)
        if validation.status != "verified":
            _finalize_payload(context, validation)
            raise ValueError("valuation_snapshots validation failed: " + "; ".join(validation.errors))
        _finalize_payload(context, validation)
        for row in rows:
            upsert_valuation_snapshot(
                Path(context.database_path),
                market=row["market"],
                symbol=row["symbol"],
                trade_date=row["trade_date"],
                pe=row.get("pe"),
                pb=row.get("pb"),
                dividend_yield=row.get("dividend_yield"),
                source_endpoint=row["source_endpoint"],
                source_url=row["source_url"],
                source_payload_sha256=row.get("source_payload_sha256"),
                source_payload_id=context.options.get("source_payload_id"),
                fetched_at=context.options.get("fetched_at"),
                published_at=row.get("published_at"),
                available_date=row["trade_date"],
                availability_precision=str(
                    context.options.get("availability_precision") or "source_observation_date"
                ),
                validation_status=str(
                    context.options.get("validation_status") or "verified"
                ),
                data_gap_reason=context.options.get("data_gap_reason"),
            )
        return len(rows)

    def completeness_report(self, context: AdapterContext) -> dict[str, Any]:
        rows = get_valuation_history(Path(context.database_path))
        return {"dataset_key": self.dataset_key, "row_count": len(rows)}


class HistoricalFinancialFactsAdapter(_FetcherAdapter):
    dataset_key = "financial_facts"

    def identity_key(self, row: dict[str, Any]) -> str:
        return "|".join(
            str(row.get(key) or "")
            for key in ("market", "symbol", "fact_code", "fiscal_period", "revision_id")
        )

    def partition_key(self, request: FetchRequest) -> str:
        if request.requested_from:
            return request.requested_from.strftime("%Y-%m")
        if request.requested_to:
            return request.requested_to.strftime("%Y-%m")
        return "unknown"

    def parse(self, result: FetchResult, context: AdapterContext) -> list[dict[str, Any]]:
        return parse_financial_facts_payload(
            result.payload,
            market=str(result.request.market or context.options.get("market") or ""),
            source_endpoint=str(context.options.get("source_endpoint") or self.dataset_key),
            source_url=str(result.final_url or context.options.get("source_url") or result.request.url),
            source_payload_sha256=result.payload_sha256,
            default_fiscal_period=context.options.get("fiscal_period"),
            default_effective_date=context.options.get("effective_date"),
            default_available_date=context.options.get("available_date"),
            published_at=context.options.get("published_at"),
        )

    def validate(self, rows: list[dict[str, Any]], context: AdapterContext) -> ValidationResult:
        return _validate_fact_rows(rows)

    def upsert(self, rows: list[dict[str, Any]], context: AdapterContext) -> int:
        validation = self.validate(rows, context)
        if validation.status != "verified":
            _finalize_payload(context, validation)
            raise ValueError("financial_facts validation failed: " + "; ".join(validation.errors))
        _finalize_payload(context, validation)
        for row in rows:
            upsert_financial_fact(
                Path(context.database_path),
                market=row["market"],
                symbol=row["symbol"],
                fact_code=row["fact_code"],
                fiscal_period=row["fiscal_period"],
                value=row.get("value"),
                unit=row["unit"],
                consolidation=row["consolidation"],
                dimension_json=row.get("dimension_json"),
                effective_date=row["effective_date"],
                available_date=row["available_date"],
                published_at=row.get("published_at"),
                revision_id=row.get("revision_id"),
                revision_sequence=int(row.get("revision_sequence") or 1),
                source_payload_id=context.options.get("source_payload_id"),
                source_payload_sha256=row.get("source_payload_sha256"),
                source_endpoint=row["source_endpoint"],
                source_url=row["source_url"],
                fetched_at=context.options.get("fetched_at"),
                validation_status=str(context.options.get("validation_status") or "verified"),
                data_gap_reason=context.options.get("data_gap_reason"),
                availability_precision=str(
                    context.options.get("availability_precision") or "unknown"
                ),
                raw_payload_json=row.get("raw_payload_json"),
            )
        return len(rows)

    def completeness_report(self, context: AdapterContext) -> dict[str, Any]:
        rows = get_financial_facts_history(Path(context.database_path))
        return {"dataset_key": self.dataset_key, "row_count": len(rows)}


def _validate_corporate_action_rows(rows: list[dict[str, Any]]) -> ValidationResult:
    errors: list[str] = []
    seen: set[str] = set()
    for index, row in enumerate(rows):
        symbol = str(row.get("symbol") or "")
        if len(symbol) != 4 or not symbol.isdigit():
            errors.append(f"row_{index}:invalid_symbol:{symbol}")
        for key in ("market", "action_date", "action_type"):
            if row.get(key) in {None, ""}:
                errors.append(f"row_{index}:missing:{key}")
        identity = "|".join(
            str(row.get(key) or "") for key in ("market", "symbol", "action_date", "action_type")
        )
        if identity in seen:
            errors.append(f"row_{index}:duplicate_identity:{identity}")
        seen.add(identity)
    if not rows:
        errors.append("no_valid_rows")
    return ValidationResult(status="verified" if not errors else "failed", errors=tuple(errors), row_count=len(rows))


class CorporateActionsAdapter(_FetcherAdapter):
    dataset_key = "corporate_actions"

    def identity_key(self, row: dict[str, Any]) -> str:
        return "|".join(
            str(row.get(key) or "") for key in ("market", "symbol", "action_date", "action_type")
        )

    def partition_key(self, request: FetchRequest) -> str:
        if request.requested_from:
            return request.requested_from.strftime("%Y-%m")
        return request.requested_to.strftime("%Y-%m") if request.requested_to else "unknown"

    def parse(self, result: FetchResult, context: AdapterContext) -> list[dict[str, Any]]:
        return parse_corporate_actions_payload(
            result.payload,
            market=str(result.request.market or context.options.get("market") or ""),
            source_endpoint=str(context.options.get("source_endpoint") or self.dataset_key),
            source_url=str(result.final_url or context.options.get("source_url") or result.request.url),
            source_payload_sha256=result.payload_sha256,
            available_date=context.options.get("available_date"),
        )

    def validate(self, rows: list[dict[str, Any]], context: AdapterContext) -> ValidationResult:
        return _validate_corporate_action_rows(rows)

    def upsert(self, rows: list[dict[str, Any]], context: AdapterContext) -> int:
        validation = self.validate(rows, context)
        if validation.status != "verified":
            _finalize_payload(context, validation)
            raise ValueError("corporate_actions validation failed: " + "; ".join(validation.errors))
        _finalize_payload(context, validation)
        for row in rows:
            upsert_corporate_action(
                Path(context.database_path),
                market=row["market"],
                symbol=row["symbol"],
                action_date=row["action_date"],
                action_type=row["action_type"],
                ex_date=row.get("ex_date"),
                record_date=row.get("record_date"),
                payment_date=row.get("payment_date"),
                ratio=row.get("ratio"),
                cash_amount=row.get("cash_amount"),
                source_endpoint=row["source_endpoint"],
                source_url=row["source_url"],
                source_payload_sha256=row.get("source_payload_sha256"),
                source_payload_id=context.options.get("source_payload_id"),
                fetched_at=context.options.get("fetched_at"),
                published_at=row.get("published_at"),
                available_date=row.get("available_date"),
                validation_status=str(context.options.get("validation_status") or "verified"),
                availability_precision=str(context.options.get("availability_precision") or "unknown"),
                data_gap_reason=context.options.get("data_gap_reason"),
                raw_payload_json=row.get("raw_payload_json"),
            )
        return len(rows)

    def completeness_report(self, context: AdapterContext) -> dict[str, Any]:
        return {"dataset_key": self.dataset_key, "status": "query_via_database_integrity"}


def _validate_session_rows(rows: list[dict[str, Any]]) -> ValidationResult:
    errors: list[str] = []
    seen: set[tuple[str, str]] = set()
    for index, row in enumerate(rows):
        market = str(row.get("market") or "")
        trade_date = row.get("trade_date")
        if not market or not isinstance(trade_date, date):
            errors.append(f"row_{index}:missing_market_or_date")
        identity = (market, str(trade_date or ""))
        if identity in seen:
            errors.append(f"row_{index}:duplicate_identity:{market}|{trade_date}")
        seen.add(identity)
    if not rows:
        errors.append("no_valid_rows")
    return ValidationResult(status="verified" if not errors else "failed", errors=tuple(errors), row_count=len(rows))


class MarketSessionsAdapter(_FetcherAdapter):
    dataset_key = "market_sessions"

    def identity_key(self, row: dict[str, Any]) -> str:
        return f"{row['market']}|{row['trade_date']}"

    def partition_key(self, request: FetchRequest) -> str:
        if request.requested_from:
            return request.requested_from.strftime("%Y")
        return request.requested_to.strftime("%Y") if request.requested_to else "unknown"

    def parse(self, result: FetchResult, context: AdapterContext) -> list[dict[str, Any]]:
        return parse_market_sessions_payload(
            result.payload,
            market=str(result.request.market or context.options.get("market") or ""),
            source_endpoint=str(context.options.get("source_endpoint") or self.dataset_key),
            source_url=str(result.final_url or context.options.get("source_url") or result.request.url),
            source_payload_sha256=result.payload_sha256,
            available_date=context.options.get("available_date"),
        )

    def validate(self, rows: list[dict[str, Any]], context: AdapterContext) -> ValidationResult:
        return _validate_session_rows(rows)

    def upsert(self, rows: list[dict[str, Any]], context: AdapterContext) -> int:
        validation = self.validate(rows, context)
        if validation.status != "verified":
            _finalize_payload(context, validation)
            raise ValueError("market_sessions validation failed: " + "; ".join(validation.errors))
        _finalize_payload(context, validation)
        for row in rows:
            upsert_market_session(
                Path(context.database_path),
                market=row["market"],
                trade_date=row["trade_date"],
                is_open=bool(row["is_open"]),
                session_type=row.get("session_type") or "regular",
                source_endpoint=row["source_endpoint"],
                source_url=row["source_url"],
                source_payload_id=context.options.get("source_payload_id"),
                source_payload_sha256=row.get("source_payload_sha256"),
                fetched_at=context.options.get("fetched_at"),
                published_at=row.get("published_at"),
                available_date=row.get("available_date"),
                validation_status=str(context.options.get("validation_status") or "verified"),
                availability_precision=str(
                    context.options.get("availability_precision") or "unknown"
                ),
                data_gap_reason=context.options.get("data_gap_reason"),
            )
        return len(rows)

    def completeness_report(self, context: AdapterContext) -> dict[str, Any]:
        return {"dataset_key": self.dataset_key, "status": "query_via_database_integrity"}


__all__ = [
    "HistoricalMonthlyRevenueAdapter",
    "HistoricalFinancialFactsAdapter",
    "HistoricalValuationAdapter",
    "CorporateActionsAdapter",
    "MarketSessionsAdapter",
    "normalize_revenue_month",
    "parse_monthly_revenue_payload",
    "parse_mops_company_revenue_payload",
    "parse_twse_company_financial_payload",
    "parse_mops_revenue_html",
    "parse_financial_facts_payload",
    "parse_corporate_actions_payload",
    "parse_market_sessions_payload",
    "parse_valuation_payload",
]
