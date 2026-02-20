from __future__ import annotations

import json
import ssl
import time
from datetime import date, timedelta
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from src.analysis.factors import safe_float
from src.themes import theme_rule


TWSE_BASICS_URL = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
TWSE_REVENUE_URL = "https://openapi.twse.com.tw/v1/opendata/t187ap05_L"
TWSE_STOCK_DAY_URL = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
TWSE_BWIBBU_URL = "https://www.twse.com.tw/exchangeReport/BWIBBU_d"

TPEX_BASICS_URL = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O"
TPEX_REVENUE_URL = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O"
TPEX_TRADING_STOCK_URL = "https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingStock"
TPEX_PE_QRY_DATE_URL = "https://www.tpex.org.tw/www/zh-tw/afterTrading/peQryDate"


def _is_stock_symbol(symbol: str) -> bool:
    return len(symbol) == 4 and symbol.isdigit()


def _parse_roc_slash(value: str) -> date:
    year, month, day = [int(x) for x in value.strip().split("/")]
    return date(year + 1911, month, day)


def _shift_month(d: date, delta: int) -> date:
    month_index = d.year * 12 + (d.month - 1) + delta
    year = month_index // 12
    month = month_index % 12 + 1
    return date(year, month, 1)


class TwMarketProvider:
    def __init__(self, timeout: float = 10.0) -> None:
        self.timeout = timeout
        self._twse_valuation_cache: dict[str, dict[str, dict[str, float]]] = {}
        self._tpex_valuation_cache: dict[str, dict[str, dict[str, float]]] = {}

    def _load_json(self, req: Request) -> Any:
        last_exc: Exception | None = None
        for attempt in range(3):
            try:
                with urlopen(req, timeout=self.timeout) as resp:
                    return json.loads(resp.read().decode("utf-8-sig"))
            except Exception as exc:
                last_exc = exc
                reason = getattr(exc, "reason", None)
                if isinstance(exc, ssl.SSLCertVerificationError) or isinstance(reason, ssl.SSLCertVerificationError):
                    insecure_ctx = ssl._create_unverified_context()
                    with urlopen(req, timeout=self.timeout, context=insecure_ctx) as resp:
                        return json.loads(resp.read().decode("utf-8-sig"))
                if attempt < 2:
                    time.sleep(0.6 * (attempt + 1))
                    continue
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("無法讀取 JSON")

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

    def load_theme_universe(self, theme: str, min_monthly_revenue: float = 0.0) -> list[dict[str, Any]]:
        rule = theme_rule(theme)
        basics = self._load_basics()
        revenue_map = self._load_latest_revenue_map()
        output: list[dict[str, Any]] = []

        for symbol, item in basics.items():
            rev = revenue_map.get(symbol, {})
            name = item.get("name", "")
            industry = str(rev.get("industry") or item.get("industry") or "")
            if not self._theme_match(symbol, name, industry, rule):
                continue
            monthly_revenue = float(rev.get("monthly_revenue") or 0.0)
            if monthly_revenue < min_monthly_revenue:
                continue
            output.append(
                {
                    "symbol": symbol,
                    "name": name,
                    "market": item.get("market", "TWSE"),
                    "industry": industry,
                    "monthly_revenue": monthly_revenue,
                    "revenue_yoy": safe_float(rev.get("revenue_yoy")),
                    "revenue_mom": safe_float(rev.get("revenue_mom")),
                }
            )

        output.sort(key=lambda x: x.get("monthly_revenue", 0.0), reverse=True)
        return output

    def _theme_match(self, symbol: str, name: str, industry: str, rule: dict[str, Any]) -> bool:
        if symbol in set(rule.get("seed_symbols") or []):
            return True
        text = f"{name} {industry}".lower()
        for kw in rule.get("name_keywords") or []:
            if str(kw).lower() in text:
                return True
        for kw in rule.get("industry_keywords") or []:
            if str(kw).lower() in industry.lower():
                return True
        return False

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
        if market == "TPEx":
            return self._get_tpex_ohlcv(symbol, as_of, lookback)
        return self._get_twse_ohlcv(symbol, as_of, lookback)

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
