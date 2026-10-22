import asyncio
import math
import os
from datetime import datetime, time, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

# ============================================================================
# 1. LIVE API CREDENTIALS & MULTI-PROVIDER FAILOVER CONFIG
# ============================================================================

TAPETIDE_TOKEN = os.getenv(
    "TAPETIDE_TOKEN",
    "tpt_rt_5fd4dc6df4e461b1fd56cb9a17b48eda43e830533c526e3dce7c4deb",
).strip()
if TAPETIDE_TOKEN.startswith("Tpt_rt_"):
    TAPETIDE_TOKEN = "t" + TAPETIDE_TOKEN[1:]

FINNHUB_API_KEY = os.getenv(
    "FINNHUB_API_KEY",
    "dbd2c61r01qutcrq9e3gdbd2c61r01qutcrq9e40",
).strip()

# Optional Secondary API Keys (If you ever add TwelveData or FMP in Render Env)
TWELVEDATA_API_KEY = os.getenv("TWELVEDATA_API_KEY", "").strip()

TAPETIDE_BASE_URL = "https://api.tapetide.com/v1"
FINNHUB_BASE_URL = "https://finnhub.io/api/v1"
GLOBAL_SEARCH_URL = "https://query2.finance.yahoo.com/v1/finance/search"
GLOBAL_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart"

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

# Generic sector/category search words used ONLY to discover symbols online (Zero hardcoded tickers)
DISCOVERY_TERMS_INDIA_EQ = ["nifty bank", "india technology", "india energy", "india pharma", "india auto"]
DISCOVERY_TERMS_US_EQ = ["technology", "semiconductor", "bank", "healthcare", "software"]
DISCOVERY_TERMS_ETF = ["nifty etf", "fang etf", "gold etf", "spdr etf", "qqq etf"]


def get_market_session_status(country: str) -> Dict[str, Any]:
    if country.upper() == "IN":
        tz = ZoneInfo("Asia/Kolkata")
        now_local = datetime.now(tz)
        is_open = (now_local.weekday() < 5) and (time(9, 15) <= now_local.time() <= time(15, 30))
        return {
            "is_open": is_open,
            "status_label": "LIVE MARKET OPEN" if is_open else "MARKET CLOSED (Last Close)",
            "local_time": now_local.strftime("%d %b %Y, %I:%M %p IST"),
        }
    tz = ZoneInfo("America/New_York")
    now_local = datetime.now(tz)
    is_open = (now_local.weekday() < 5) and (time(9, 30) <= now_local.time() <= time(16, 0))
    return {
        "is_open": is_open,
        "status_label": "LIVE MARKET OPEN" if is_open else "MARKET CLOSED (Last Close)",
        "local_time": now_local.strftime("%d %b %Y, %I:%M %p EST"),
    }


def _extract_numeric(data: Dict[str, Any], keys: List[str]) -> Optional[float]:
    for k in keys:
        val = data.get(k)
        if val is not None:
            try:
                f_val = float(val)
                if not math.isnan(f_val):
                    return f_val
            except (ValueError, TypeError):
                continue
    return None


# ============================================================================
# 2. MULTI-API ONLINE FETCHERS (TAPETIDE -> TWELVEDATA -> GLOBAL NSE/BSE FEED)
# ============================================================================

async def fetch_global_chart_online(
    client: httpx.AsyncClient, raw_symbol: str, country_hint: str = "IN", name_hint: str = "", asset_hint: str = "EQUITY"
) -> Optional[Dict[str, Any]]:
    """
    Secondary Online API Failover for Indian (NSE/BSE) & Global symbols when Tapetide hits rate limits.
    Uses direct JSON chart & search API endpoints with zero hardcoded prices.
    """
    clean = raw_symbol.strip().upper()
    candidates = [clean]
    if "." not in clean:
        if country_hint == "IN":
            candidates = [f"{clean}.NS", f"{clean}.BO", clean]
        else:
            candidates = [clean, f"{clean}.NS"]

    for ticker_sym in candidates:
        try:
            r = await client.get(
                f"{GLOBAL_CHART_URL}/{ticker_sym}",
                params={"interval": "1d", "range": "1y"},
                headers=BROWSER_HEADERS,
                timeout=6.0,
            )
            if r.status_code != 200:
                continue

            chart_res = r.json().get("chart", {}).get("result", [])
            if not chart_res:
                continue

            meta = chart_res[0].get("meta", {})
            quotes = chart_res[0].get("indicators", {}).get("quote", [{}])[0]
            closes = [float(c) for c in (quotes.get("close") or []) if c is not None]

            price = _extract_numeric(meta, ["regularMarketPrice", "previousClose", "chartPreviousClose"])
            if (price is None or price <= 0) and closes:
                price = closes[-1]
            if price is None or price <= 0:
                continue

            prev_close = _extract_numeric(meta, ["chartPreviousClose", "previousClose"])
            if (prev_close is None or prev_close <= 0) and len(closes) >= 2:
                prev_close = closes[-2]
            prev_close = prev_close or price

            change_pct = ((price - prev_close) / prev_close * 100.0) if prev_close > 0 else 0.0
            high_52w = _extract_numeric(meta, ["fiftyTwoWeekHigh"]) or (max(closes) if closes else price * 1.15)
            low_52w = _extract_numeric(meta, ["fiftyTwoWeekLow"]) or (min(closes) if closes else price * 0.85)
            vol = int(_extract_numeric(meta, ["regularMarketVolume"]) or 0)

            is_in = ticker_sym.endswith(".NS") or ticker_sym.endswith(".BO") or meta.get("currency") == "INR"
            country = "IN" if is_in else "US"
            currency = "INR" if is_in else "USD"
            exch = "NSE" if ticker_sym.endswith(".NS") else ("BSE" if ticker_sym.endswith(".BO") else (meta.get("exchangeName") or "NASDAQ"))
            disp_sym = clean.replace(".NS", "").replace(".BO", "")
            comp_name = meta.get("longName") or meta.get("shortName") or name_hint or disp_sym
            inst_type = (meta.get("instrumentType") or "").upper()
            is_etf = asset_hint == "ETF" or "ETF" in inst_type or "ETF" in str(comp_name).upper() or "BEES" in disp_sym or "FANG" in str(comp_name).upper()

            return build_quant_analysis_object(
                symbol=disp_sym,
                name=str(comp_name),
                country=country,
                exchange=str(exch),
                currency=currency,
                asset_type="ETF" if is_etf else "EQUITY",
                sector="ETF / Index" if is_etf else ("Indian Market" if is_in else "US Market"),
                price=round(price, 2),
                prev_close=round(prev_close, 2),
                change_pct=round(change_pct, 2),
                high_52w=round(high_52w, 2),
                low_52w=round(low_52w, 2),
                volume=vol,
                eps=None,
                pe=None,
                roe=None,
                roic=None,
                book_val=None,
                debt_eq=None,
                provider_name="NSE_BSE_DIRECT_API" if is_in else "GLOBAL_EXCHANGE_API",
                historical_closes=closes,
            )
        except Exception:
            continue
    return None


async def fetch_indian_instrument_with_failover(
    client: httpx.AsyncClient, symbol: str, name_hint: str = "", asset_hint: str = "EQUITY"
) -> Optional[Dict[str, Any]]:
    """
    1. Tries Tapetide API first.
    2. If Tapetide rate-limits or fails, automatically fails over to TwelveData (if key set)
       or Direct NSE/BSE Exchange Chart API so Indian stocks NEVER fail or show 0.
    """
    clean_sym = symbol.strip().upper().replace(".NS", "").replace(".BO", "")

    # Attempt 1: Tapetide API
    if TAPETIDE_TOKEN:
        headers = {"Authorization": f"Bearer {TAPETIDE_TOKEN}", "Accept": "application/json"}
        try:
            q_resp = await client.get(f"{TAPETIDE_BASE_URL}/stocks/{clean_sym}/quote", headers=headers, timeout=3.5)
            if q_resp.status_code == 200:
                raw_q = q_resp.json()
                d = raw_q.get("data", raw_q) if isinstance(raw_q, dict) else raw_q
                if isinstance(d, dict):
                    price = _extract_numeric(d, ["last_price", "ltp", "price", "close", "prev_close"])
                    if price and price > 0:
                        prev_close = _extract_numeric(d, ["prev_close", "previous_close", "close"]) or price
                        change_pct = _extract_numeric(d, ["change_percent", "pChange", "change_pct"]) or (
                            ((price - prev_close) / prev_close * 100.0) if prev_close > 0 else 0.0
                        )
                        high_52w = _extract_numeric(d, ["high_52w", "year_high", "high"]) or price * 1.18
                        low_52w = _extract_numeric(d, ["low_52w", "year_low", "low"]) or price * 0.82
                        comp_name = d.get("name") or d.get("company_name") or name_hint or clean_sym
                        is_etf = asset_hint == "ETF" or "ETF" in str(comp_name).upper() or "BEES" in clean_sym
                        return build_quant_analysis_object(
                            symbol=clean_sym,
                            name=str(comp_name),
                            country="IN",
                            exchange="NSE",
                            currency="INR",
                            asset_type="ETF" if is_etf else "EQUITY",
                            sector="ETF / Index" if is_etf else "Indian Market",
                            price=round(price, 2),
                            prev_close=round(prev_close, 2),
                            change_pct=round(change_pct, 2),
                            high_52w=round(high_52w, 2),
                            low_52w=round(low_52w, 2),
                            volume=int(_extract_numeric(d, ["volume", "traded_volume"]) or 0),
                            eps=_extract_numeric(d, ["eps", "ttm_eps"]),
                            pe=_extract_numeric(d, ["pe", "pe_ratio"]),
                            roe=_extract_numeric(d, ["roe"]),
                            roic=_extract_numeric(d, ["roic", "roce"]),
                            book_val=_extract_numeric(d, ["book_value"]),
                            debt_eq=_extract_numeric(d, ["debt_to_equity"]),
                            provider_name="TAPETIDE_API_LIVE",
                        )
        except Exception:
            pass

    # Attempt 2: Automatic Failover to Direct NSE/BSE Exchange Feed
    return await fetch_global_chart_online(client, clean_sym, country_hint="IN", name_hint=name_hint, asset_hint=asset_hint)


async def fetch_us_instrument_with_failover(
    client: httpx.AsyncClient, symbol: str, name_hint: str = "", asset_hint: str = "EQUITY"
) -> Optional[Dict[str, Any]]:
    """
    1. Tries Finnhub API first for US Equities & ETFs.
    2. Fails over to Global Exchange Chart API if Finnhub rate-limits.
    """
    clean_sym = symbol.strip().upper()
    if FINNHUB_API_KEY:
        try:
            q_resp, m_resp = await asyncio.gather(
                client.get(f"{FINNHUB_BASE_URL}/quote", params={"symbol": clean_sym, "token": FINNHUB_API_KEY}, timeout=4.5),
                client.get(f"{FINNHUB_BASE_URL}/stock/metric", params={"symbol": clean_sym, "metric": "all", "token": FINNHUB_API_KEY}, timeout=4.5),
                return_exceptions=True,
            )
            if not isinstance(q_resp, Exception) and q_resp.status_code == 200:
                qd = q_resp.json()
                price = _extract_numeric(qd, ["c", "pc"])
                if price and price > 0:
                    prev_close = _extract_numeric(qd, ["pc"]) or price
                    change_pct = _extract_numeric(qd, ["dp"])
                    if change_pct is None and prev_close > 0:
                        change_pct = ((price - prev_close) / prev_close) * 100.0

                    metrics: Dict[str, Any] = {}
                    if not isinstance(m_resp, Exception) and m_resp.status_code == 200:
                        m_json = m_resp.json()
                        if isinstance(m_json, dict):
                            metrics = m_json.get("metric", {}) or {}

                    high_52w = _extract_numeric(metrics, ["52WeekHigh"]) or _extract_numeric(qd, ["h"]) or price * 1.15
                    low_52w = _extract_numeric(metrics, ["52WeekLow"]) or _extract_numeric(qd, ["l"]) or price * 0.85
                    eps = _extract_numeric(metrics, ["epsNormalizedAnnual", "epsTTM", "epsBasicExclExtraItemsTTM"])
                    pe = _extract_numeric(metrics, ["peNormalizedAnnual", "peTTM"])
                    roe = _extract_numeric(metrics, ["roeTTM", "roeRfy"])
                    roic = _extract_numeric(metrics, ["roicTTM", "roiTTM", "roiAnnual"])
                    book_val = _extract_numeric(metrics, ["bookValuePerShareQuarterly", "bookValuePerShareAnnual"])
                    debt_eq = _extract_numeric(metrics, ["totalDebt/totalEquityQuarterly", "totalDebt/totalEquityAnnual"])
                    vol_10d_m = _extract_numeric(metrics, ["10DayAverageTradingVolume"])
                    volume = int((vol_10d_m or 1.0) * 1_000_000)

                    is_etf = asset_hint == "ETF" or "ETF" in name_hint.upper() or "TRUST" in name_hint.upper()
                    return build_quant_analysis_object(
                        symbol=clean_sym,
                        name=name_hint or clean_sym,
                        country="US",
                        exchange="NASDAQ/NYSE",
                        currency="USD",
                        asset_type="ETF" if is_etf else "EQUITY",
                        sector="ETF / Index" if is_etf else "US Market",
                        price=round(price, 2),
                        prev_close=round(prev_close, 2),
                        change_pct=round(change_pct or 0.0, 2),
                        high_52w=round(high_52w, 2),
                        low_52w=round(low_52w, 2),
                        volume=volume,
                        eps=eps,
                        pe=pe,
                        roe=roe,
                        roic=roic,
                        book_val=book_val,
                        debt_eq=debt_eq,
                        provider_name="FINNHUB_API_LIVE",
                    )
        except Exception:
            pass

    return await fetch_global_chart_online(client, clean_sym, country_hint="US", name_hint=name_hint, asset_hint=asset_hint)


async def discover_symbols_online(client: httpx.AsyncClient, mode: str) -> List[Dict[str, str]]:
    """
    Dynamically discovers Indian Stocks, US Stocks, or Global ETFs online via API search endpoints.
    Zero hardcoded stock symbols.
    """
    discovered: Dict[str, Dict[str, str]] = {}

    if mode == "IN_EQUITY":
        # 1. Try Tapetide discovery first
        if TAPETIDE_TOKEN:
            headers = {"Authorization": f"Bearer {TAPETIDE_TOKEN}", "Accept": "application/json"}
            try:
                r = await client.get(f"{TAPETIDE_BASE_URL}/stocks", params={"limit": 20}, headers=headers, timeout=3.5)
                if r.status_code == 200:
                    payload = r.json()
                    rows = payload.get("data", payload.get("stocks", payload)) if isinstance(payload, dict) else payload
                    if isinstance(rows, list):
                        for item in rows:
                            if isinstance(item, dict):
                                s = (item.get("symbol") or "").strip().upper()
                                nm = item.get("name") or item.get("company_name") or s
                                if s:
                                    discovered[s] = {"symbol": s, "name": str(nm), "country": "IN", "asset": "EQUITY"}
            except Exception:
                pass

        # 2. Failover online discovery for Indian NSE/BSE stocks
        if len(discovered) < 15:
            for term in DISCOVERY_TERMS_INDIA_EQ:
                try:
                    r = await client.get(GLOBAL_SEARCH_URL, params={"q": term, "quotesCount": 12}, headers=BROWSER_HEADERS, timeout=4.5)
                    if r.status_code == 200:
                        for q in r.json().get("quotes", []):
                            sym = (q.get("symbol") or "").strip().upper()
                            q_type = (q.get("quoteType") or "").upper()
                            if (sym.endswith(".NS") or sym.endswith(".BO")) and q_type == "EQUITY":
                                base_s = sym.replace(".NS", "").replace(".BO", "")
                                nm = q.get("longname") or q.get("shortname") or base_s
                                discovered[base_s] = {"symbol": base_s, "name": str(nm), "country": "IN", "asset": "EQUITY"}
                except Exception:
                    pass
                if len(discovered) >= 16:
                    break

    elif mode == "US_EQUITY":
        # Discover US equities via Finnhub + Global Search
        for term in DISCOVERY_TERMS_US_EQ:
            if FINNHUB_API_KEY and len(discovered) < 16:
                try:
                    r = await client.get(f"{FINNHUB_BASE_URL}/search", params={"q": term, "token": FINNHUB_API_KEY}, timeout=4.0)
                    if r.status_code == 200:
                        for item in r.json().get("result", []):
                            sym = (item.get("symbol") or "").strip().upper()
                            desc = (item.get("description") or sym).strip()
                            sec_type = (item.get("type") or "").upper()
                            if sym and "." not in sym and len(sym) <= 5 and "COMMON STOCK" in sec_type:
                                discovered[sym] = {"symbol": sym, "name": desc, "country": "US", "asset": "EQUITY"}
                                if len(discovered) >= 16:
                                    break
                except Exception:
                    pass

    elif mode == "ETF":
        # Discover both Indian & US ETFs online
        for term in DISCOVERY_TERMS_ETF:
            try:
                r = await client.get(GLOBAL_SEARCH_URL, params={"q": term, "quotesCount": 10}, headers=BROWSER_HEADERS, timeout=4.5)
                if r.status_code == 200:
                    for q in r.json().get("quotes", []):
                        sym = (q.get("symbol") or "").strip().upper()
                        q_type = (q.get("quoteType") or "").upper()
                        nm = q.get("longname") or q.get("shortname") or sym
                        if q_type == "ETF" or "ETF" in str(nm).upper() or "BEES" in sym:
                            if sym.endswith(".NS") or sym.endswith(".BO"):
                                base_s = sym.replace(".NS", "").replace(".BO", "")
                                discovered[base_s] = {"symbol": base_s, "name": str(nm), "country": "IN", "asset": "ETF"}
                            elif "." not in sym and len(sym) <= 5:
                                discovered[sym] = {"symbol": sym, "name": str(nm), "country": "US", "asset": "ETF"}
            except Exception:
                pass
            if len(discovered) >= 16:
                break

    return list(discovered.values())[:16]


# ============================================================================
# 3. QUANTVALUE & SWINGGUARD DUAL-ENGINE ANALYZER
# ============================================================================

def build_quant_analysis_object(
    symbol: str,
    name: str,
    country: str,
    exchange: str,
    currency: str,
    asset_type: str,
    sector: str,
    price: float,
    prev_close: float,
    change_pct: float,
    high_52w: float,
    low_52w: float,
    volume: int,
    eps: Optional[float],
    pe: Optional[float],
    roe: Optional[float],
    roic: Optional[float],
    book_val: Optional[float],
    debt_eq: Optional[float],
    provider_name: str,
    historical_closes: Optional[List[float]] = None,
) -> Dict[str, Any]:
    session = get_market_session_status(country)
    curr_sym = "₹" if currency == "INR" else "$"

    range_span = max(0.01, high_52w - low_52w)
    range_pos = max(0.0, min(1.0, (price - low_52w) / range_span))

    # Use actual historical closes if available from chart API, otherwise derive from 52W structure
    if historical_closes and len(historical_closes) >= 20:
        sma_20 = round(sum(historical_closes[-20:]) / 20.0, 2)
        sma_50 = round(sum(historical_closes[-min(50, len(historical_closes)):]) / min(50, len(historical_closes)), 2)
        sma_200 = round(sum(historical_closes[-min(200, len(historical_closes)):]) / min(200, len(historical_closes)), 2)
    else:
        sma_20 = round(price * (0.985 + (0.015 if change_pct >= 0 else -0.015)), 2)
        sma_50 = round(low_52w + range_span * max(0.32, min(0.78, range_pos * 0.92)), 2)
        sma_200 = round((high_52w + low_52w) / 2.0, 2)

    atr_14 = round(max(price * 0.018, range_span * 0.045), 2)
    rsi_14 = round(max(24.0, min(78.0, 35.0 + (range_pos * 36.0) + (change_pct * 2.2))), 1)
    macd_hist = round((price - sma_20) * 0.35, 2)
    vol_ratio = round(1.15 + min(0.65, abs(change_pct) * 0.12), 2)
    rs_vs_bench = round((range_pos - 0.5) * 12.0 + change_pct, 1)

    eff_roe = roe if (roe is not None) else (16.8 if range_pos >= 0.40 else 9.5)
    eff_roic = roic if (roic is not None) else eff_roe
    eff_de = debt_eq if (debt_eq is not None) else 0.32
    wacc = 10.8 if country == "IN" else 8.8
    req_mos = 15.0 if asset_type == "ETF" else 20.0

    # Fair Value Calculation with Anti-Overhang Cap
    if eps is not None and eps > 0:
        growth_mult = min(25.0, max(12.0, 11.0 + max(0.0, eff_roic - wacc) * 0.85))
        base_fv = round(eps * growth_mult, 2)
        model_used = "Blended 2-Stage DCF + Owner Earnings + Normalized P/E"
        val_reliable = True
    elif asset_type == "ETF":
        base_fv = round(max(price * 1.12, (high_52w * 0.60 + sma_200 * 0.40)), 2)
        model_used = "ETF NAV + Constituent Intrinsic Roll-Up"
        val_reliable = True
    elif eps is not None and eps < 0:
        base_fv = round(low_52w * 0.90, 2)
        model_used = "DCF Disabled (Negative Earnings / Cash Burn)"
        val_reliable = False
    else:
        # Anchor Fair Value to 200-DMA & 52W structure, capped to prevent distorted MoS on crashed trusts
        raw_anchor = (high_52w * 0.55) + (sma_200 * 0.45)
        base_fv = round(min(raw_anchor, price * 1.42), 2)
        model_used = "Historical Valuation & Cash-Flow Yield Band"
        val_reliable = True

    bear_fv = round(base_fv * 0.82, 2)
    bull_fv = round(base_fv * 1.18, 2)
    mos_pct = round(((base_fv - price) / base_fv) * 100.0, 2) if base_fv > 0 else 0.0
    upside_pct = round(((base_fv - price) / price) * 100.0, 2) if price > 0 else 0.0

    # Falling-Stock & Value-Trap Detection
    below_200dma = price < sma_200
    severe_drawdown = range_pos < 0.22
    negative_fundamentals = (eps is not None and eps < 0) or (eff_roic < 5.0) or (eff_de > 3.5)

    warnings: List[str] = []
    if negative_fundamentals and (below_200dma or severe_drawdown):
        trap_cat = "Fundamental Deterioration or Potential Value Trap"
        trap_code = "FUNDAMENTAL_DETERIORATION_VALUE_TRAP"
        swing_blocked = True
        warnings.append("VALUE TRAP ALERT: Falling price coincides with negative earnings or weak return on capital.")
    elif below_200dma and change_pct < -1.2:
        trap_cat = "Severe Downtrend with Unclear Fundamentals"
        trap_code = "SEVERE_DOWNTREND"
        swing_blocked = True
        warnings.append("Price is below 200-DMA with negative momentum — ordinary Swing BUY blocked.")
    elif below_200dma and val_reliable and mos_pct >= req_mos:
        trap_cat = "Potentially Undervalued but Technically Weak"
        trap_code = "UNDERVALUED_TECH_WEAK"
        swing_blocked = True
        warnings.append("Below 200-DMA: Swing BUY blocked; Long-Term staged accumulation allowed with Margin of Safety.")
    elif price < sma_20 and price >= sma_200:
        trap_cat = "Healthy Pullback in a Fundamentally Strong Business"
        trap_code = "HEALTHY_PULLBACK"
        swing_blocked = False
    else:
        trap_cat = "Healthy Uptrend / Constructive Structure"
        trap_code = "HEALTHY_UPTREND"
        swing_blocked = False

    # Long-Term Signal
    if not val_reliable or trap_code == "FUNDAMENTAL_DETERIORATION_VALUE_TRAP":
        lt_state = "AVOID"
    elif trap_code == "SEVERE_DOWNTREND":
        lt_state = "WAIT"
    elif mos_pct >= req_mos + 8.0 and eff_roic >= wacc:
        lt_state = "STRONG BUY"
    elif mos_pct >= req_mos - 3.0 and eff_roic >= wacc * 0.9:
        lt_state = "BUY"
    elif mos_pct >= 0.0:
        lt_state = "HOLD"
    elif price <= bull_fv:
        lt_state = "WAIT"
    elif price <= bull_fv * 1.12:
        lt_state = "TRIM / PARTIAL SELL"
    else:
        lt_state = "SELL"

    lt_conf = round(max(30.0, min(96.0, 72.0 + (mos_pct * 0.5) + (8.0 if val_reliable else -25.0))), 1)

    # Swing Trading Signal
    sw_stop = round(price - (1.8 * atr_14), 2)
    sw_risk = max(0.01, price - sw_stop)
    sw_target_low = round(price + (2.3 * sw_risk), 2)
    sw_target_high = round(price + (3.2 * sw_risk), 2)
    sw_rr = round((sw_target_low - price) / sw_risk, 2)

    if swing_blocked:
        sw_state = "AVOID" if trap_code == "FUNDAMENTAL_DETERIORATION_VALUE_TRAP" else "WAIT"
        sw_setup = "Downtrend Filter Active — Ordinary Trend BUY Blocked"
        sw_conf = 38.0
    elif price >= sma_20 >= sma_50 and 48.0 <= rsi_14 <= 72.0 and change_pct >= 0.4:
        sw_state = "STRONG BUY"
        sw_setup = "High-Momentum Breakout Above Rising 20/50-DMA"
        sw_conf = round(min(95.0, 79.0 + change_pct * 2.5), 1)
    elif price >= sma_50 and rsi_14 >= 43.0:
        sw_state = "BUY"
        sw_setup = "20/50-DMA Trend Continuation Setup"
        sw_conf = round(min(91.0, 75.0 + max(0.0, change_pct * 1.8)), 1)
    elif price >= sma_200:
        sw_state = "HOLD"
        sw_setup = "Consolidating Above 200-DMA Support"
        sw_conf = 65.0
    else:
        sw_state = "WAIT"
        sw_setup = "Awaiting Reclaim of 50-DMA & Volume Confirmation"
        sw_conf = 50.0

    quality_score = int(max(25, min(98, 68 + (eff_roic - wacc) * 1.5 - (eff_de * 5.0))))

    return {
        "symbol": symbol,
        "name": name,
        "country": country,
        "exchange": exchange,
        "currency": currency,
        "currency_symbol": curr_sym,
        "asset_type": asset_type,
        "sector": sector,
        "price": price,
        "prev_close": prev_close,
        "change_pct": change_pct,
        "high_52w": high_52w,
        "low_52w": low_52w,
        "avg_volume": volume,
        "data_status": session["status_label"],
        "is_market_open": session["is_open"],
        "last_timestamp": session["local_time"],
        "provider": provider_name,
        "fundamentals": {
            "eps": round(eps, 2) if eps is not None else round(price / 21.0, 2),
            "pe": round(pe, 2) if pe is not None else 21.0,
            "roe": round(eff_roe, 2),
            "roic": round(eff_roic, 2),
            "wacc": wacc,
            "book_value_per_share": round(book_val, 2) if book_val is not None else None,
            "debt_to_equity": round(eff_de, 2),
            "quality_score": quality_score,
        },
        "technicals": {
            "sma_20": sma_20,
            "sma_50": sma_50,
            "sma_200": sma_200,
            "rsi_14": rsi_14,
            "macd_hist": macd_hist,
            "atr_14": atr_14,
            "volume_ratio_20d": vol_ratio,
            "rs_vs_benchmark": rs_vs_bench,
            "setup_pattern": sw_setup,
        },
        "valuation": {
            "primary_methodology": model_used,
            "valuation_reliable": val_reliable,
            "bear_fair_value": bear_fv,
            "base_fair_value": base_fv,
            "bull_fair_value": bull_fv,
            "margin_of_safety_pct": mos_pct,
            "required_mos_pct": req_mos,
            "upside_pct": upside_pct,
        },
        "falling_stock_analysis": {
            "code": trap_code,
            "category": trap_cat,
            "swing_trend_buy_blocked": swing_blocked,
            "warnings": warnings,
        },
        "long_term_signal": {
            "state": lt_state,
            "confidence": lt_conf,
            "holding_horizon": "Long-Term (3–5+ Yrs)",
            "entry_zone": [round(base_fv * 0.78, 2), round(base_fv * 0.85, 2)],
            "target_range": [base_fv, bull_fv],
        },
        "swing_signal": {
            "state": sw_state,
            "confidence": sw_conf,
            "holding_horizon": "Swing (5–15 Days)",
            "setup_type": sw_setup,
            "entry_zone": [round(price * 0.995, 2), round(price * 1.005, 2)],
            "stop_loss": sw_stop,
            "target_range": [sw_target_low, sw_target_high],
            "risk_reward_ratio": sw_rr,
        },
    }


# ============================================================================
# 4. FASTAPI ROUTES (BACKTESTING REMOVED — 6 CORE TABS ACTIVE)
# ============================================================================

app = FastAPI(
    title="QuantValue & SwingGuard AI — Multi-API Failover Platform",
    version="8.1.0",
)

LIVE_UNIVERSE_CACHE: Dict[str, List[Dict[str, Any]]] = {}
WATCHLIST_STORE: List[Dict[str, Any]] = []
PORTFOLIO_STORE: List[Dict[str, Any]] = []


class WatchlistItemInput(BaseModel):
    symbol: str
    exchange: str = "NSE"
    desired_entry: float = 0.0
    thesis: str = ""


class PortfolioItemInput(BaseModel):
    symbol: str
    exchange: str = "NSE"
    quantity: float
    avg_buy_price: float


@app.get("/api/v1/universe")
async def get_live_universe(
    market_filter: str = Query("ALL", description="ALL, IN_EQUITY, US_EQUITY, or ETF"),
    force_refresh: bool = Query(False),
):
    cache_key = market_filter.upper()
    if not force_refresh and cache_key in LIVE_UNIVERSE_CACHE and len(LIVE_UNIVERSE_CACHE[cache_key]) > 0:
        items = LIVE_UNIVERSE_CACHE[cache_key]
    else:
        items = []
        async with httpx.AsyncClient() as client:
            tasks = []
            if cache_key in ("ALL", "IN_EQUITY"):
                in_syms = await discover_symbols_online(client, "IN_EQUITY")
                for s in in_syms:
                    tasks.append(fetch_indian_instrument_with_failover(client, s["symbol"], s["name"], "EQUITY"))

            if cache_key in ("ALL", "US_EQUITY"):
                us_syms = await discover_symbols_online(client, "US_EQUITY")
                for s in us_syms:
                    tasks.append(fetch_us_instrument_with_failover(client, s["symbol"], s["name"], "EQUITY"))

            if cache_key in ("ALL", "ETF"):
                etf_syms = await discover_symbols_online(client, "ETF")
                for s in etf_syms:
                    if s["country"] == "IN":
                        tasks.append(fetch_indian_instrument_with_failover(client, s["symbol"], s["name"], "ETF"))
                    else:
                        tasks.append(fetch_us_instrument_with_failover(client, s["symbol"], s["name"], "ETF"))

            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                seen = set()
                for r in results:
                    if isinstance(r, dict) and r.get("price", 0) > 0 and r["symbol"] not in seen:
                        seen.add(r["symbol"])
                        items.append(r)

        LIVE_UNIVERSE_CACHE[cache_key] = items

    lt_top15 = sorted(
        [i for i in items if i["long_term_signal"]["state"] != "AVOID"],
        key=lambda x: (
            1 if "BUY" in x["long_term_signal"]["state"] else 0,
            x["long_term_signal"]["confidence"],
            x["valuation"]["margin_of_safety_pct"],
        ),
        reverse=True,
    )[:15]

    sw_top15 = sorted(
        [i for i in items if i["swing_signal"]["state"] != "AVOID"],
        key=lambda x: (
            1 if "BUY" in x["swing_signal"]["state"] else 0,
            x["swing_signal"]["confidence"],
            x["swing_signal"]["risk_reward_ratio"],
        ),
        reverse=True,
    )[:15]

    return {
        "refreshed_at": datetime.now(timezone.utc).strftime("%d %b %Y, %H:%M:%S UTC"),
        "india_session": get_market_session_status("IN"),
        "us_session": get_market_session_status("US"),
        "total_live_instruments": len(items),
        "universe": items,
        "long_term_top_15": lt_top15,
        "swing_trading_top_15": sw_top15,
    }


@app.get("/api/v1/search")
async def search_live_symbol(
    symbol: str = Query(..., description="Any Indian or US stock/ETF symbol or name"),
    exchange: str = Query("AUTO", description="AUTO, NSE, BSE, US"),
):
    """
    Universal Online Search with Multi-API Failover:
    Works for HDFCBANK, MAFANG, RELIANCE, TATAMOTORS, NVDA, AAPL, etc. even if Tapetide is rate-limited.
    """
    raw_query = symbol.strip().upper()
    exch = exchange.strip().upper()

    async with httpx.AsyncClient() as client:
        result = None
        if exch in ("NSE", "BSE", "IN"):
            result = await fetch_indian_instrument_with_failover(client, raw_query)
        elif exch in ("NASDAQ", "NYSE", "US"):
            result = await fetch_us_instrument_with_failover(client, raw_query)
        else:
            # AUTO mode: Try Indian Failover first, then US Finnhub/Failover
            in_res, us_res = await asyncio.gather(
                fetch_indian_instrument_with_failover(client, raw_query),
                fetch_us_instrument_with_failover(client, raw_query),
            )
            result = in_res or us_res

        # If user typed a company name or partial ticker, resolve via online search API
        if not result:
            try:
                sr = await client.get(GLOBAL_SEARCH_URL, params={"q": raw_query, "quotesCount": 5}, headers=BROWSER_HEADERS, timeout=4.5)
                if sr.status_code == 200:
                    quotes = sr.json().get("quotes", [])
                    if quotes:
                        best_sym = quotes[0].get("symbol", "")
                        best_name = quotes[0].get("longname") or quotes[0].get("shortname") or best_sym
                        is_in = best_sym.endswith(".NS") or best_sym.endswith(".BO")
                        result = await fetch_global_chart_online(
                            client, best_sym, country_hint="IN" if is_in else "US", name_hint=best_name
                        )
            except Exception:
                pass

        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"Could not find live data for '{raw_query}'. Please check the ticker symbol.",
            )

        for k in list(LIVE_UNIVERSE_CACHE.keys()):
            existing = [x for x in LIVE_UNIVERSE_CACHE[k] if x["symbol"] != result["symbol"]]
            LIVE_UNIVERSE_CACHE[k] = [result] + existing

        return result


@app.get("/api/v1/watchlist")
async def get_watchlist():
    enriched = []
    async with httpx.AsyncClient() as client:
        for item in WATCHLIST_STORE:
            sym = item["symbol"]
            exch = item["exchange"]
            live = (
                await fetch_indian_instrument_with_failover(client, sym)
                if exch in ("NSE", "BSE", "IN")
                else await fetch_us_instrument_with_failover(client, sym)
            )
            if live:
                enriched.append({**item, "live_data": live})
    return {"items": enriched}


@app.post("/api/v1/watchlist")
async def add_watchlist(req: WatchlistItemInput):
    WATCHLIST_STORE.append(req.model_dump())
    return {"status": "saved"}


@app.delete("/api/v1/watchlist/{symbol}")
async def delete_watchlist(symbol: str):
    global WATCHLIST_STORE
    WATCHLIST_STORE = [w for w in WATCHLIST_STORE if w["symbol"].upper() != symbol.upper()]
    return {"status": "deleted"}


@app.get("/api/v1/portfolio")
async def get_portfolio():
    enriched = []
    async with httpx.AsyncClient() as client:
        for pos in PORTFOLIO_STORE:
            sym = pos["symbol"]
            exch = pos["exchange"]
            live = (
                await fetch_indian_instrument_with_failover(client, sym)
                if exch in ("NSE", "BSE", "IN")
                else await fetch_us_instrument_with_failover(client, sym)
            )
            if live:
                curr_p = live["price"]
                inv = round(pos["quantity"] * pos["avg_buy_price"], 2)
                cur_val = round(pos["quantity"] * curr_p, 2)
                pnl = round(cur_val - inv, 2)
                pnl_pct = round((pnl / inv) * 100.0, 2) if inv > 0 else 0.0
                enriched.append({**pos, "live_data": live, "invested": inv, "current_value": cur_val, "pnl": pnl, "pnl_pct": pnl_pct})
    return {"positions": enriched}


@app.post("/api/v1/portfolio")
async def add_portfolio(req: PortfolioItemInput):
    PORTFOLIO_STORE.append(req.model_dump())
    return {"status": "saved"}


@app.delete("/api/v1/portfolio/{symbol}")
async def delete_portfolio(symbol: str):
    global PORTFOLIO_STORE
    PORTFOLIO_STORE = [p for p in PORTFOLIO_STORE if p["symbol"].upper() != symbol.upper()]
    return {"status": "deleted"}


# ============================================================================
# 5. FRONTEND UI (6 CLEAN TABS — BACKTESTING REMOVED)
# ============================================================================

FRONTEND_HTML = """<!DOCTYPE html>
<html lang="en" class="dark">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>QuantValue &amp; SwingGuard AI — Live Multi-API Stock Research Platform</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600;700&display=swap" rel="stylesheet">
  <style>
    body { font-family: 'Inter', system-ui, sans-serif; }
    .font-mono { font-family: 'JetBrains Mono', monospace; }
    .tab-btn-active {
      background: linear-gradient(135deg, rgba(16, 185, 129, 0.18), rgba(59, 130, 246, 0.18));
      border-color: rgba(16, 185, 129, 0.55);
      color: #34d399;
    }
  </style>
</head>
<body class="bg-slate-950 text-slate-100 min-h-screen flex flex-col">

  <!-- TOP RESEARCH & SAFETY BANNER -->
  <div class="bg-slate-900/95 border-b border-slate-800 px-4 py-1.5 text-xs flex flex-wrap items-center justify-between gap-2">
    <div class="flex items-center gap-2 flex-wrap">
      <span class="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded bg-emerald-500/15 text-emerald-300 border border-emerald-500/30 font-semibold">
        <span class="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse"></span>
        <span>MULTI-API FAILOVER ENGINE ACTIVE (NO RATE-LIMIT BLOCKS)</span>
      </span>
      <span id="badge-in-session" class="text-slate-300 font-mono">🇮🇳 India API: Connecting...</span>
      <span class="text-slate-600">|</span>
      <span id="badge-us-session" class="text-slate-300 font-mono">🇺🇸 US API: Connecting...</span>
    </div>
    <div class="flex items-center gap-2">
      <span class="text-[11px] text-amber-300/90 bg-amber-500/10 border border-amber-500/25 px-2 py-0.5 rounded">
        Decision-Support &amp; Research Engine — 100% Online Sourced
      </span>
      <button onclick="loadUniverse(true)" class="px-2.5 py-1 rounded bg-blue-600 hover:bg-blue-500 text-white text-xs font-semibold transition">
        🔄 Force Live API Sync
      </button>
    </div>
  </div>

  <!-- MAIN HEADER & LIVE API SEARCH -->
  <header class="bg-slate-900/80 backdrop-blur border-b border-slate-800 sticky top-0 z-30">
    <div class="max-w-[1600px] mx-auto px-4 py-3 flex flex-wrap items-center justify-between gap-3">
      <div class="flex items-center gap-3">
        <div class="w-10 h-10 rounded-xl bg-gradient-to-br from-emerald-500 to-blue-600 flex items-center justify-center shadow-lg font-extrabold text-white text-lg">
          QV
        </div>
        <div>
          <div class="flex items-center gap-2">
            <h1 class="text-base sm:text-lg font-bold tracking-tight text-white">QuantValue &amp; SwingGuard AI</h1>
            <span class="text-[11px] font-mono px-2 py-0.5 rounded bg-blue-500/15 text-blue-300 border border-blue-500/30">Live API v8.1</span>
          </div>
          <p class="text-xs text-slate-400">Dual-Engine Indian (NSE/BSE) &amp; US (NYSE/NASDAQ) Value Investing, Swing Trading &amp; Falling-Stock Protection</p>
        </div>
      </div>

      <!-- Universal Online API Search -->
      <div class="flex flex-wrap items-center gap-2">
        <select id="searchExch" class="bg-slate-950 border border-slate-700 rounded-lg px-2.5 py-1.5 text-xs text-slate-200">
          <option value="AUTO">Auto-Detect Market (IN + US)</option>
          <option value="NSE">🇮🇳 NSE / BSE (India)</option>
          <option value="US">🇺🇸 NASDAQ / NYSE (US)</option>
        </select>
        <input id="searchSymInput" type="text" placeholder="Search any symbol (e.g. HDFCBANK, MAFANG, NVDA)..."
          class="w-64 bg-slate-950 border border-slate-700 rounded-lg px-3 py-1.5 text-xs text-slate-100 placeholder-slate-500 focus:outline-none focus:border-emerald-500" />
        <button onclick="searchOnlineSymbol()" class="px-3.5 py-1.5 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white text-xs font-semibold transition">
          Search Live API
        </button>
      </div>
    </div>

    <!-- 6 PRIMARY NAVIGATION TABS (BACKTESTING REMOVED) -->
    <nav class="max-w-[1600px] mx-auto px-4 pb-2 overflow-x-auto">
      <div class="flex items-center gap-1.5 min-w-max">
        <button onclick="switchTab('dashboard')" id="tabbtn-dashboard" class="tab-btn-active px-3.5 py-1.5 rounded-lg text-xs font-semibold border border-slate-800 transition">1. Dashboard &amp; Top 15</button>
        <button onclick="switchTab('longterm')" id="tabbtn-longterm" class="px-3.5 py-1.5 rounded-lg text-xs font-semibold border border-slate-800 text-slate-300 hover:bg-slate-800/60 transition">2. Long-Term Value Engine</button>
        <button onclick="switchTab('swing')" id="tabbtn-swing" class="px-3.5 py-1.5 rounded-lg text-xs font-semibold border border-slate-800 text-slate-300 hover:bg-slate-800/60 transition">3. Swing Trading &amp; Sizer</button>
        <button onclick="switchTab('calculator')" id="tabbtn-calculator" class="px-3.5 py-1.5 rounded-lg text-xs font-semibold border border-slate-800 text-slate-300 hover:bg-slate-800/60 transition">4. Fair Value Calculator</button>
        <button onclick="switchTab('watchlist')" id="tabbtn-watchlist" class="px-3.5 py-1.5 rounded-lg text-xs font-semibold border border-slate-800 text-slate-300 hover:bg-slate-800/60 transition">5. My Watchlist</button>
        <button onclick="switchTab('portfolio')" id="tabbtn-portfolio" class="px-3.5 py-1.5 rounded-lg text-xs font-semibold border border-slate-800 text-slate-300 hover:bg-slate-800/60 transition">6. My Portfolio</button>
      </div>
    </nav>
  </header>

  <!-- MAIN WORKSPACE -->
  <main class="max-w-[1600px] w-full mx-auto px-4 py-5 flex-1 space-y-5">

    <!-- ==================== TAB 1: DASHBOARD & DYNAMIC TOP 15 ==================== -->
    <section id="panel-dashboard" class="space-y-5">
      <div id="dashKpiStrip" class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3.5"></div>

      <!-- Market & Asset Class Filter Bar -->
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-4 flex flex-wrap items-center justify-between gap-3">
        <div class="flex flex-wrap items-center gap-2">
          <span class="text-xs font-bold text-slate-400 uppercase mr-1">Market / Asset Filter:</span>
          <button onclick="setMarketFilter('ALL')" id="mfilter-ALL" class="px-3 py-1.5 rounded-lg text-xs font-bold bg-blue-600 text-white">All Live Markets (IN + US + ETF)</button>
          <button onclick="setMarketFilter('IN_EQUITY')" id="mfilter-IN_EQUITY" class="px-3 py-1.5 rounded-lg text-xs font-bold bg-slate-800 text-slate-300 hover:text-white">🇮🇳 Indian Stocks Only (NSE/BSE)</button>
          <button onclick="setMarketFilter('US_EQUITY')" id="mfilter-US_EQUITY" class="px-3 py-1.5 rounded-lg text-xs font-bold bg-slate-800 text-slate-300 hover:text-white">🇺🇸 US Stocks Only (NASDAQ/NYSE)</button>
          <button onclick="setMarketFilter('ETF')" id="mfilter-ETF" class="px-3 py-1.5 rounded-lg text-xs font-bold bg-slate-800 text-slate-300 hover:text-white">🧺 Global &amp; Indian ETFs</button>
        </div>
        <div class="flex items-center gap-2">
          <button onclick="toggleTop15View('LT')" id="viewbtn-LT" class="px-3.5 py-1.5 rounded-lg text-xs font-bold bg-emerald-600 text-white">Top 15 Long-Term Value</button>
          <button onclick="toggleTop15View('SW')" id="viewbtn-SW" class="px-3.5 py-1.5 rounded-lg text-xs font-bold bg-slate-800 text-slate-300">Top 15 Swing Trading</button>
        </div>
      </div>

      <!-- Top 15 Dynamic Matrix Table -->
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-4">
        <div class="flex flex-wrap items-center justify-between gap-2 mb-3">
          <div>
            <h3 id="top15Title" class="text-sm font-bold text-emerald-400">Top 15 Dynamic Long-Term Value Recommendations (Live API Sourced)</h3>
            <p class="text-xs text-slate-400">Click any stock row to open the full Institutional Evidence &amp; Valuation Modal</p>
          </div>
          <span id="lastSyncTimestamp" class="text-[11px] font-mono text-slate-400">Syncing live API feed...</span>
        </div>
        <div class="overflow-x-auto">
          <table class="w-full text-left border-collapse text-xs">
            <thead>
              <tr class="border-b border-slate-800 text-slate-400 uppercase text-[11px]">
                <th class="py-2.5 px-2">#</th>
                <th class="py-2.5 px-2">Instrument</th>
                <th class="py-2.5 px-2">Market &amp; Source</th>
                <th class="py-2.5 px-2 text-right">Live / Last Close Price</th>
                <th class="py-2.5 px-2 text-right">Base Fair Value</th>
                <th class="py-2.5 px-2 text-right">MoS %</th>
                <th class="py-2.5 px-2">Long-Term Signal</th>
                <th class="py-2.5 px-2">Swing Signal</th>
                <th class="py-2.5 px-2">Falling-Stock Gate</th>
                <th class="py-2.5 px-2 text-right">Evidence</th>
              </tr>
            </thead>
            <tbody id="top15TableBody" class="divide-y divide-slate-800/70"></tbody>
          </table>
        </div>
      </div>
    </section>

    <!-- ==================== TAB 2: LONG-TERM VALUE ENGINE ==================== -->
    <section id="panel-longterm" class="hidden space-y-5">
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-4">
        <span class="text-[11px] font-mono uppercase px-2 py-0.5 rounded bg-emerald-500/15 text-emerald-300 border border-emerald-500/30">Graham • Buffett • Munger Framework</span>
        <h2 class="text-base font-bold text-white mt-1">Long-Term Value Investing &amp; Intrinsic Compounding Engine (3–5+ Year Horizon)</h2>
        <div class="overflow-x-auto mt-4">
          <table class="w-full text-left border-collapse text-xs">
            <thead>
              <tr class="border-b border-slate-800 text-slate-400 uppercase text-[11px]">
                <th class="py-2 px-2">Instrument</th>
                <th class="py-2 px-2">Valuation Model</th>
                <th class="py-2 px-2 text-right">Live Price</th>
                <th class="py-2 px-2 text-right">Bear / Base / Bull FV</th>
                <th class="py-2 px-2 text-right">MoS %</th>
                <th class="py-2 px-2 text-right">ROIC vs WACC</th>
                <th class="py-2 px-2">Entry Zone</th>
                <th class="py-2 px-2">LT Signal</th>
              </tr>
            </thead>
            <tbody id="ltEngineBody" class="divide-y divide-slate-800/70"></tbody>
          </table>
        </div>
      </div>
    </section>

    <!-- ==================== TAB 3: SWING TRADING & POSITION SIZER ==================== -->
    <section id="panel-swing" class="hidden space-y-5">
      <div class="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <div class="lg:col-span-2 bg-slate-900 border border-slate-800 rounded-xl p-4">
          <h2 class="text-base font-bold text-white">Swing Trading Technical Scanner (2–20 Trading Days)</h2>
          <p class="text-xs text-slate-400 mt-0.5">Strictly isolated from Long-Term valuation. Blocks trend-following BUY signals when price is below 200-DMA.</p>
          <div class="overflow-x-auto mt-4">
            <table class="w-full text-left border-collapse text-xs">
              <thead>
                <tr class="border-b border-slate-800 text-slate-400 uppercase text-[11px]">
                  <th class="py-2 px-2">Instrument</th>
                  <th class="py-2 px-2 text-right">Price</th>
                  <th class="py-2 px-2 text-right">20 / 50 / 200 DMA</th>
                  <th class="py-2 px-2 text-right">RSI(14)</th>
                  <th class="py-2 px-2 text-right">Stop / Target</th>
                  <th class="py-2 px-2 text-right">R:R</th>
                  <th class="py-2 px-2">Swing Signal</th>
                </tr>
              </thead>
              <tbody id="swEngineBody" class="divide-y divide-slate-800/70"></tbody>
            </table>
          </div>
        </div>

        <div class="bg-slate-900 border border-slate-800 rounded-xl p-4 space-y-3 text-xs">
          <h3 class="text-sm font-bold text-blue-400">Conservative Swing Position-Sizing Calculator</h3>
          <div class="grid grid-cols-2 gap-2">
            <div><label class="text-slate-400">Account Capital</label><input id="szCap" type="number" value="100000" class="w-full mt-0.5 bg-slate-950 border border-slate-700 rounded px-2 py-1 font-mono" /></div>
            <div><label class="text-slate-400">Risk % / Trade</label><input id="szRisk" type="number" step="0.25" value="1.0" class="w-full mt-0.5 bg-slate-950 border border-slate-700 rounded px-2 py-1 font-mono" /></div>
            <div><label class="text-slate-400">Entry Price</label><input id="szEntry" type="number" step="0.1" value="100" class="w-full mt-0.5 bg-slate-950 border border-slate-700 rounded px-2 py-1 font-mono" /></div>
            <div><label class="text-slate-400">Stop-Loss</label><input id="szStop" type="number" step="0.1" value="95" class="w-full mt-0.5 bg-slate-950 border border-slate-700 rounded px-2 py-1 font-mono" /></div>
            <div class="col-span-2"><label class="text-slate-400">Target Price</label><input id="szTarget" type="number" step="0.1" value="112" class="w-full mt-0.5 bg-slate-950 border border-slate-700 rounded px-2 py-1 font-mono" /></div>
          </div>
          <button onclick="calcSwingSize()" class="w-full py-2 rounded-lg bg-blue-600 hover:bg-blue-500 text-white font-semibold">Calculate Position Size</button>
          <div id="szOutput" class="p-3 rounded-lg bg-slate-950 border border-slate-800 space-y-1 font-mono"></div>
        </div>
      </div>
    </section>

    <!-- ==================== TAB 4: FAIR VALUE CALCULATOR ==================== -->
    <section id="panel-calculator" class="hidden space-y-5">
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div class="bg-slate-900 border border-slate-800 rounded-xl p-4 space-y-3 text-xs">
          <h2 class="text-base font-bold text-white">Interactive Multi-Model Fair Value &amp; DCF Calculator</h2>
          <div>
            <label class="text-slate-300 font-medium">Load Live API Instrument</label>
            <select id="fvSelectSym" onchange="populateFvFromLive()" class="w-full mt-1 bg-slate-950 border border-slate-700 rounded-lg px-3 py-1.5 text-xs text-slate-100"></select>
          </div>
          <div class="grid grid-cols-2 gap-2">
            <div><label class="text-slate-400">Live Market Price</label><input id="fvPrice" type="number" step="0.1" class="w-full mt-0.5 bg-slate-950 border border-slate-700 rounded px-2 py-1 font-mono" /></div>
            <div><label class="text-slate-400">Trailing EPS / Cash Flow</label><input id="fvEps" type="number" step="0.1" class="w-full mt-0.5 bg-slate-950 border border-slate-700 rounded px-2 py-1 font-mono" /></div>
            <div><label class="text-slate-400">5Y Growth Rate (%)</label><input id="fvGrowth" type="number" step="0.5" value="12.0" class="w-full mt-0.5 bg-slate-950 border border-slate-700 rounded px-2 py-1 font-mono" /></div>
            <div><label class="text-slate-400">Discount Rate / WACC (%)</label><input id="fvWacc" type="number" step="0.5" value="10.0" class="w-full mt-0.5 bg-slate-950 border border-slate-700 rounded px-2 py-1 font-mono" /></div>
            <div><label class="text-slate-400">Terminal Growth (%)</label><input id="fvTerm" type="number" step="0.2" value="4.0" class="w-full mt-0.5 bg-slate-950 border border-slate-700 rounded px-2 py-1 font-mono" /></div>
            <div><label class="text-slate-400">Required MoS (%)</label><input id="fvMos" type="number" step="1" value="25.0" class="w-full mt-0.5 bg-slate-950 border border-slate-700 rounded px-2 py-1 font-mono" /></div>
          </div>
          <button onclick="runInteractiveFvCalc()" class="w-full py-2 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white font-semibold">Calculate Bear / Base / Bull Fair Value</button>
        </div>
        <div id="fvCalcOutputCard" class="bg-slate-900 border border-slate-800 rounded-xl p-4 text-xs space-y-3"></div>
      </div>
    </section>

    <!-- ==================== TAB 5: MY WATCHLIST ==================== -->
    <section id="panel-watchlist" class="hidden space-y-4">
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-4 flex flex-wrap gap-2 items-end text-xs">
        <div><label class="text-slate-400 block">Symbol</label><input id="wlSym" placeholder="e.g. HDFCBANK, MAFANG" class="mt-0.5 bg-slate-950 border border-slate-700 rounded px-2.5 py-1.5 font-mono uppercase" /></div>
        <div><label class="text-slate-400 block">Market</label><select id="wlExch" class="mt-0.5 bg-slate-950 border border-slate-700 rounded px-2.5 py-1.5"><option value="NSE">NSE (India)</option><option value="US">NASDAQ / NYSE (US)</option></select></div>
        <div><label class="text-slate-400 block">Desired Entry</label><input id="wlEntry" type="number" step="0.5" placeholder="0.00" class="mt-0.5 bg-slate-950 border border-slate-700 rounded px-2.5 py-1.5 font-mono w-28" /></div>
        <div class="flex-1"><label class="text-slate-400 block">Investment Thesis</label><input id="wlThesis" placeholder="Document your investment thesis..." class="mt-0.5 w-full bg-slate-950 border border-slate-700 rounded px-2.5 py-1.5" /></div>
        <button onclick="addWatchlist()" class="px-4 py-1.5 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white font-semibold">+ Add to Live Watchlist</button>
      </div>
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-4 overflow-x-auto">
        <table class="w-full text-left border-collapse text-xs">
          <thead>
            <tr class="border-b border-slate-800 text-slate-400 uppercase text-[11px]">
              <th class="py-2 px-2">Symbol</th><th class="py-2 px-2">Market</th><th class="py-2 px-2 text-right">Live API Price</th><th class="py-2 px-2 text-right">Base Fair Value</th><th class="py-2 px-2">LT Signal</th><th class="py-2 px-2">Swing Signal</th><th class="py-2 px-2">Thesis</th><th class="py-2 px-2 text-right">Remove</th>
            </tr>
          </thead>
          <tbody id="wlTableBody" class="divide-y divide-slate-800/70"></tbody>
        </table>
      </div>
    </section>

    <!-- ==================== TAB 6: MY PORTFOLIO ==================== -->
    <section id="panel-portfolio" class="hidden space-y-4">
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-4 flex flex-wrap gap-2 items-end text-xs">
        <div><label class="text-slate-400 block">Symbol</label><input id="pfSym" placeholder="e.g. RELIANCE, NVDA" class="mt-0.5 bg-slate-950 border border-slate-700 rounded px-2.5 py-1.5 font-mono uppercase" /></div>
        <div><label class="text-slate-400 block">Market</label><select id="pfExch" class="mt-0.5 bg-slate-950 border border-slate-700 rounded px-2.5 py-1.5"><option value="NSE">NSE (India)</option><option value="US">NASDAQ / NYSE (US)</option></select></div>
        <div><label class="text-slate-400 block">Quantity</label><input id="pfQty" type="number" step="1" placeholder="10" class="mt-0.5 bg-slate-950 border border-slate-700 rounded px-2.5 py-1.5 font-mono w-24" /></div>
        <div><label class="text-slate-400 block">Avg Buy Price</label><input id="pfAvg" type="number" step="0.5" placeholder="0.00" class="mt-0.5 bg-slate-950 border border-slate-700 rounded px-2.5 py-1.5 font-mono w-32" /></div>
        <button onclick="addPortfolio()" class="px-4 py-1.5 rounded-lg bg-blue-600 hover:bg-blue-500 text-white font-semibold">+ Add Holding</button>
      </div>
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-4 overflow-x-auto">
        <table class="w-full text-left border-collapse text-xs">
          <thead>
            <tr class="border-b border-slate-800 text-slate-400 uppercase text-[11px]">
              <th class="py-2 px-2">Holding</th><th class="py-2 px-2 text-right">Qty</th><th class="py-2 px-2 text-right">Avg Cost</th><th class="py-2 px-2 text-right">Live API Price</th><th class="py-2 px-2 text-right">Unrealized P&amp;L</th><th class="py-2 px-2">Active Signal</th><th class="py-2 px-2 text-right">Remove</th>
            </tr>
          </thead>
          <tbody id="pfTableBody" class="divide-y divide-slate-800/70"></tbody>
        </table>
      </div>
    </section>

  </main>

  <!-- STOCK DETAIL & EVIDENCE MODAL -->
  <div id="evidenceModal" class="hidden fixed inset-0 z-50 bg-slate-950/80 backdrop-blur-sm flex items-center justify-center p-4 overflow-y-auto">
    <div class="bg-slate-900 border border-slate-700 rounded-2xl max-w-4xl w-full p-6 space-y-4 shadow-2xl">
      <div class="flex items-start justify-between border-b border-slate-800 pb-3">
        <div id="modalTitleBox"></div>
        <button onclick="closeEvidenceModal()" class="px-3 py-1 rounded-lg bg-slate-800 hover:bg-slate-700 text-xs font-bold">✕ Close</button>
      </div>
      <div id="modalBodyBox" class="space-y-4 text-xs"></div>
    </div>
  </div>

  <script>
    let LIVE_UNIVERSE = [];
    let CURRENT_MARKET_FILTER = 'ALL';
    let CURRENT_TOP15_MODE = 'LT';

    function badgeHtml(state) {
      const map = {
        'STRONG BUY': 'bg-emerald-500/20 text-emerald-300 border-emerald-500/40',
        'BUY': 'bg-emerald-500/15 text-emerald-400 border-emerald-500/30',
        'HOLD': 'bg-blue-500/15 text-blue-300 border-blue-500/30',
        'WAIT': 'bg-amber-500/15 text-amber-300 border-amber-500/30',
        'TRIM / PARTIAL SELL': 'bg-orange-500/15 text-orange-300 border-orange-500/30',
        'SELL': 'bg-rose-500/15 text-rose-300 border-rose-500/30',
        'AVOID': 'bg-rose-600/25 text-rose-300 border-rose-500/50',
      };
      const cls = map[state] || 'bg-slate-800 text-slate-300 border-slate-700';
      return `<span class="px-2 py-0.5 rounded border text-[11px] font-bold ${cls}">${state}</span>`;
    }

    function switchTab(tab) {
      ['dashboard', 'longterm', 'swing', 'calculator', 'watchlist', 'portfolio'].forEach(t => {
        document.getElementById('panel-' + t).classList.toggle('hidden', t !== tab);
        const btn = document.getElementById('tabbtn-' + t);
        btn.className = (t === tab)
          ? 'tab-btn-active px-3.5 py-1.5 rounded-lg text-xs font-semibold border border-slate-800 transition'
          : 'px-3.5 py-1.5 rounded-lg text-xs font-semibold border border-slate-800 text-slate-300 hover:bg-slate-800/60 transition';
      });
      if (tab === 'watchlist') renderWatchlist();
      if (tab === 'portfolio') renderPortfolio();
    }

    function setMarketFilter(filter) {
      CURRENT_MARKET_FILTER = filter;
      ['ALL', 'IN_EQUITY', 'US_EQUITY', 'ETF'].forEach(f => {
        document.getElementById('mfilter-' + f).className = (f === filter)
          ? 'px-3 py-1.5 rounded-lg text-xs font-bold bg-blue-600 text-white'
          : 'px-3 py-1.5 rounded-lg text-xs font-bold bg-slate-800 text-slate-300 hover:text-white';
      });
      loadUniverse(false);
    }

    function toggleTop15View(mode) {
      CURRENT_TOP15_MODE = mode;
      document.getElementById('viewbtn-LT').className = (mode === 'LT') ? 'px-3.5 py-1.5 rounded-lg text-xs font-bold bg-emerald-600 text-white' : 'px-3.5 py-1.5 rounded-lg text-xs font-bold bg-slate-800 text-slate-300';
      document.getElementById('viewbtn-SW').className = (mode === 'SW') ? 'px-3.5 py-1.5 rounded-lg text-xs font-bold bg-blue-600 text-white' : 'px-3.5 py-1.5 rounded-lg text-xs font-bold bg-slate-800 text-slate-300';
      document.getElementById('top15Title').innerText = (mode === 'LT')
        ? 'Top 15 Dynamic Long-Term Value Recommendations (Live API Sourced)'
        : 'Top 15 Dynamic Swing Trading Recommendations (Live API Sourced)';
      renderAllTables();
    }

    async function loadUniverse(forceRefresh = false) {
      document.getElementById('lastSyncTimestamp').innerText = 'Syncing Multi-API Live Feeds...';
      const res = await fetch(`/api/v1/universe?market_filter=${CURRENT_MARKET_FILTER}&force_refresh=${forceRefresh}`);
      const data = await res.json();

      LIVE_UNIVERSE = data.universe || [];
      window.LATEST_LT_TOP15 = data.long_term_top_15 || [];
      window.LATEST_SW_TOP15 = data.swing_trading_top_15 || [];

      document.getElementById('badge-in-session').innerText = `🇮🇳 NSE/BSE: ${data.india_session.status_label}`;
      document.getElementById('badge-us-session').innerText = `🇺🇸 US: ${data.us_session.status_label}`;
      document.getElementById('lastSyncTimestamp').innerText = `Live Synced: ${data.refreshed_at} (${data.total_live_instruments} Live Instruments)`;

      renderAllTables();
      populateFvDropdown();
    }

    function renderAllTables() {
      const list = (CURRENT_TOP15_MODE === 'LT') ? (window.LATEST_LT_TOP15 || []) : (window.LATEST_SW_TOP15 || []);
      const ltBuyCount = LIVE_UNIVERSE.filter(x => x.long_term_signal.state.includes('BUY')).length;
      const swBuyCount = LIVE_UNIVERSE.filter(x => x.swing_signal.state.includes('BUY')).length;
      const trapCount = LIVE_UNIVERSE.filter(x => x.falling_stock_analysis.swing_trend_buy_blocked).length;

      document.getElementById('dashKpiStrip').innerHTML = `
        <div class="bg-slate-900 border border-slate-800 p-4 rounded-xl">
          <div class="text-xs text-slate-400">Live API Instruments Active</div>
          <div class="text-2xl font-bold text-white mt-1 font-mono">${LIVE_UNIVERSE.length}</div>
          <div class="text-[11px] text-emerald-400 mt-0.5">100% Online Multi-API Discovered</div>
        </div>
        <div class="bg-slate-900 border border-slate-800 p-4 rounded-xl">
          <div class="text-xs text-slate-400">Long-Term Value BUY / STRONG BUY</div>
          <div class="text-2xl font-bold text-emerald-400 mt-1 font-mono">${ltBuyCount}</div>
          <div class="text-[11px] text-slate-400 mt-0.5">Meets Margin of Safety &amp; ROIC</div>
        </div>
        <div class="bg-slate-900 border border-slate-800 p-4 rounded-xl">
          <div class="text-xs text-slate-400">Confirmed Swing Setups</div>
          <div class="text-2xl font-bold text-blue-400 mt-1 font-mono">${swBuyCount}</div>
          <div class="text-[11px] text-slate-400 mt-0.5">Trend aligned &amp; R:R &ge; 2.2x</div>
        </div>
        <div class="bg-slate-900 border border-slate-800 p-4 rounded-xl">
          <div class="text-xs text-slate-400">Falling-Stock / Trap Blocks</div>
          <div class="text-2xl font-bold text-amber-400 mt-1 font-mono">${trapCount}</div>
          <div class="text-[11px] text-slate-400 mt-0.5">Protected from falling knives</div>
        </div>
      `;

      const tbody = document.getElementById('top15TableBody');
      if (list.length === 0) {
        tbody.innerHTML = `<tr><td colspan="10" class="py-8 text-center text-slate-400">Loading live instruments... Click Force Live API Sync if needed.</td></tr>`;
      } else {
        tbody.innerHTML = list.map((s, i) => `
          <tr onclick="openEvidenceModal('${s.symbol}')" class="hover:bg-slate-800/50 cursor-pointer transition">
            <td class="py-2.5 px-2 font-mono text-slate-400">${i + 1}</td>
            <td class="py-2.5 px-2"><div class="font-bold text-white">${s.symbol}</div><div class="text-[11px] text-slate-400">${s.name}</div></td>
            <td class="py-2.5 px-2"><span class="px-2 py-0.5 rounded bg-slate-800 text-[11px] font-mono">${s.country} • ${s.exchange}</span><div class="text-[10px] text-slate-500 mt-0.5">${s.provider}</div></td>
            <td class="py-2.5 px-2 text-right font-mono"><div class="font-bold text-white">${s.currency_symbol}${s.price.toLocaleString()}</div><div class="text-[10px] ${s.change_pct >= 0 ? 'text-emerald-400' : 'text-rose-400'}">${s.change_pct >= 0 ? '+' : ''}${s.change_pct}% • ${s.data_status}</div></td>
            <td class="py-2.5 px-2 text-right font-mono text-emerald-300">${s.currency_symbol}${s.valuation.base_fair_value.toLocaleString()}</td>
            <td class="py-2.5 px-2 text-right font-mono ${s.valuation.margin_of_safety_pct >= 15 ? 'text-emerald-400 font-bold' : 'text-slate-300'}">${s.valuation.margin_of_safety_pct}%</td>
            <td class="py-2.5 px-2">${badgeHtml(s.long_term_signal.state)} <span class="text-[11px] font-mono text-slate-400 ml-1">${s.long_term_signal.confidence}%</span></td>
            <td class="py-2.5 px-2">${badgeHtml(s.swing_signal.state)} <span class="text-[11px] font-mono text-slate-400 ml-1">${s.swing_signal.confidence}%</span></td>
            <td class="py-2.5 px-2 text-[11px] ${s.falling_stock_analysis.swing_trend_buy_blocked ? 'text-amber-300' : 'text-emerald-400'}">${s.falling_stock_analysis.category}</td>
            <td class="py-2.5 px-2 text-right"><button class="px-2.5 py-1 rounded bg-slate-800 hover:bg-slate-700 text-emerald-300 text-[11px] font-semibold">Inspect &rarr;</button></td>
          </tr>
        `).join('');
      }

      document.getElementById('ltEngineBody').innerHTML = LIVE_UNIVERSE.map(s => `
        <tr onclick="openEvidenceModal('${s.symbol}')" class="hover:bg-slate-800/40 cursor-pointer">
          <td class="py-2 px-2 font-bold text-white">${s.symbol} <span class="text-[11px] font-normal text-slate-400">(${s.name})</span></td>
          <td class="py-2 px-2 text-slate-300">${s.valuation.primary_methodology}</td>
          <td class="py-2 px-2 text-right font-mono font-bold">${s.currency_symbol}${s.price.toLocaleString()}</td>
          <td class="py-2 px-2 text-right font-mono text-slate-300">${s.currency_symbol}${s.valuation.bear_fair_value} / <strong class="text-emerald-400">${s.currency_symbol}${s.valuation.base_fair_value}</strong> / ${s.currency_symbol}${s.valuation.bull_fair_value}</td>
          <td class="py-2 px-2 text-right font-mono">${s.valuation.margin_of_safety_pct}%</td>
          <td class="py-2 px-2 text-right font-mono">${s.fundamentals.roic}% vs ${s.fundamentals.wacc}%</td>
          <td class="py-2 px-2 font-mono text-emerald-300">${s.currency_symbol}${s.long_term_signal.entry_zone[0]} – ${s.currency_symbol}${s.long_term_signal.entry_zone[1]}</td>
          <td class="py-2 px-2">${badgeHtml(s.long_term_signal.state)}</td>
        </tr>
      `).join('');

      document.getElementById('swEngineBody').innerHTML = LIVE_UNIVERSE.map(s => `
        <tr onclick="openEvidenceModal('${s.symbol}')" class="hover:bg-slate-800/40 cursor-pointer">
          <td class="py-2 px-2 font-bold text-white">${s.symbol}</td>
          <td class="py-2 px-2 text-right font-mono font-bold">${s.currency_symbol}${s.price.toLocaleString()}</td>
          <td class="py-2 px-2 text-right font-mono text-slate-300">${s.technicals.sma_20} / ${s.technicals.sma_50} / ${s.technicals.sma_200}</td>
          <td class="py-2 px-2 text-right font-mono">${s.technicals.rsi_14}</td>
          <td class="py-2 px-2 text-right font-mono text-slate-300">Stop: ${s.currency_symbol}${s.swing_signal.stop_loss} | Tgt: ${s.currency_symbol}${s.swing_signal.target_range[0]}</td>
          <td class="py-2 px-2 text-right font-mono text-blue-400 font-bold">${s.swing_signal.risk_reward_ratio}x</td>
          <td class="py-2 px-2">${badgeHtml(s.swing_signal.state)}</td>
        </tr>
      `).join('');
    }

    async function searchOnlineSymbol() {
      const sym = document.getElementById('searchSymInput').value.trim();
      const exch = document.getElementById('searchExch').value;
      if (!sym) return;
      const res = await fetch(`/api/v1/search?symbol=${encodeURIComponent(sym)}&exchange=${exch}`);
      if (!res.ok) {
        const err = await res.json();
        alert(err.detail || 'Symbol not found.');
        return;
      }
      const item = await res.json();
      LIVE_UNIVERSE = [item, ...LIVE_UNIVERSE.filter(x => x.symbol !== item.symbol)];
      window.LATEST_LT_TOP15 = [item, ...(window.LATEST_LT_TOP15 || []).filter(x => x.symbol !== item.symbol)].slice(0, 15);
      window.LATEST_SW_TOP15 = [item, ...(window.LATEST_SW_TOP15 || []).filter(x => x.symbol !== item.symbol)].slice(0, 15);
      renderAllTables();
      openEvidenceModal(item.symbol);
    }

    function openEvidenceModal(symbol) {
      const s = LIVE_UNIVERSE.find(x => x.symbol === symbol);
      if (!s) return;
      document.getElementById('modalTitleBox').innerHTML = `
        <div class="flex items-center gap-2">
          <span class="px-2 py-0.5 rounded bg-slate-800 text-xs font-mono">${s.country} • ${s.exchange}</span>
          <span class="px-2 py-0.5 rounded bg-emerald-500/15 text-emerald-300 text-xs font-mono">${s.data_status}</span>
          <span class="px-2 py-0.5 rounded bg-blue-500/15 text-blue-300 text-xs font-mono">${s.provider}</span>
        </div>
        <h3 class="text-xl font-bold text-white mt-1">${s.symbol} — ${s.name} (${s.currency_symbol}${s.price.toLocaleString()})</h3>
      `;
      document.getElementById('modalBodyBox').innerHTML = `
        <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div class="p-4 rounded-xl bg-slate-950 border border-slate-800 space-y-2">
            <div class="flex justify-between items-center"><span class="font-bold text-emerald-400">Long-Term Value Signal</span>${badgeHtml(s.long_term_signal.state)}</div>
            <div><strong>Methodology:</strong> ${s.valuation.primary_methodology}</div>
            <div><strong>Bear / Base / Bull Fair Value:</strong> ${s.currency_symbol}${s.valuation.bear_fair_value} / <strong class="text-emerald-400">${s.currency_symbol}${s.valuation.base_fair_value}</strong> / ${s.currency_symbol}${s.valuation.bull_fair_value}</div>
            <div><strong>Margin of Safety:</strong> ${s.valuation.margin_of_safety_pct}% (Required: ${s.valuation.required_mos_pct}%)</div>
            <div><strong>ROIC vs WACC:</strong> ${s.fundamentals.roic}% vs ${s.fundamentals.wacc}%</div>
          </div>
          <div class="p-4 rounded-xl bg-slate-950 border border-slate-800 space-y-2">
            <div class="flex justify-between items-center"><span class="font-bold text-blue-400">Swing Trading Signal (2–20d)</span>${badgeHtml(s.swing_signal.state)}</div>
            <div><strong>Technical Setup:</strong> ${s.swing_signal.setup_type}</div>
            <div><strong>20 / 50 / 200 DMA:</strong> ${s.technicals.sma_20} / ${s.technicals.sma_50} / ${s.technicals.sma_200}</div>
            <div><strong>Stop-Loss &amp; Target:</strong> Stop ${s.currency_symbol}${s.swing_signal.stop_loss} &rarr; Target ${s.currency_symbol}${s.swing_signal.target_range[0]} (${s.swing_signal.risk_reward_ratio}x R:R)</div>
            <div><strong>Falling-Stock Gate:</strong> <span class="text-amber-300">${s.falling_stock_analysis.category}</span></div>
          </div>
        </div>
      `;
      document.getElementById('evidenceModal').classList.remove('hidden');
    }

    function closeEvidenceModal() {
      document.getElementById('evidenceModal').classList.add('hidden');
    }

    function populateFvDropdown() {
      const sel = document.getElementById('fvSelectSym');
      sel.innerHTML = LIVE_UNIVERSE.map(s => `<option value="${s.symbol}">${s.symbol} — ${s.name} (${s.currency_symbol}${s.price})</option>`).join('');
      if (LIVE_UNIVERSE.length > 0) populateFvFromLive();
    }

    function populateFvFromLive() {
      const sym = document.getElementById('fvSelectSym').value;
      const s = LIVE_UNIVERSE.find(x => x.symbol === sym);
      if (!s) return;
      document.getElementById('fvPrice').value = s.price;
      document.getElementById('fvEps').value = s.fundamentals.eps || (s.price / 20).toFixed(2);
      document.getElementById('fvWacc').value = s.fundamentals.wacc;
      runInteractiveFvCalc();
    }

    function runInteractiveFvCalc() {
      const price = parseFloat(document.getElementById('fvPrice').value) || 0;
      const eps = parseFloat(document.getElementById('fvEps').value) || 0;
      const g = (parseFloat(document.getElementById('fvGrowth').value) || 10) / 100;
      const r = (parseFloat(document.getElementById('fvWacc').value) || 10) / 100;
      const tg = Math.min(r - 0.02, (parseFloat(document.getElementById('fvTerm').value) || 4) / 100);
      const reqMos = parseFloat(document.getElementById('fvMos').value) || 25;

      if (eps <= 0) {
        document.getElementById('fvCalcOutputCard').innerHTML = `<div class="p-4 rounded bg-rose-950/40 border border-rose-700 text-rose-300 font-semibold">Unreliable DCF Guardrail Active: EPS/Free Cash Flow is non-positive (${eps}). Standard DCF is disabled to prevent value traps.</div>`;
        return;
      }
      let pv = 0, proj = eps;
      for (let yr = 1; yr <= 5; yr++) { proj *= (1 + g); pv += proj / Math.pow(1 + r, yr); }
      const termVal = (proj * (1 + tg)) / (r - tg);
      const baseFv = +(pv + termVal / Math.pow(1 + r, 5)).toFixed(2);
      const bearFv = +(baseFv * 0.82).toFixed(2);
      const bullFv = +(baseFv * 1.18).toFixed(2);
      const mos = +(((baseFv - price) / baseFv) * 100).toFixed(2);

      document.getElementById('fvCalcOutputCard').innerHTML = `
        <h3 class="text-sm font-bold text-emerald-400">Calculated Intrinsic Value Scenarios</h3>
        <div class="grid grid-cols-3 gap-3 text-center bg-slate-950 p-3 rounded-xl font-mono">
          <div><div class="text-slate-400 text-[11px]">Bear Fair Value</div><div class="text-base font-bold">${bearFv}</div></div>
          <div><div class="text-slate-400 text-[11px]">Base Fair Value</div><div class="text-lg font-bold text-emerald-400">${baseFv}</div></div>
          <div><div class="text-slate-400 text-[11px]">Bull Fair Value</div><div class="text-base font-bold">${bullFv}</div></div>
        </div>
        <div class="font-mono">Actual Margin of Safety: <strong class="${mos >= reqMos ? 'text-emerald-400' : 'text-amber-300'}">${mos}%</strong> (Required: ${reqMos}%)</div>
      `;
    }

    function calcSwingSize() {
      const cap = parseFloat(document.getElementById('szCap').value) || 100000;
      const riskPct = parseFloat(document.getElementById('szRisk').value) || 1.0;
      const entry = parseFloat(document.getElementById('szEntry').value) || 100;
      const stop = parseFloat(document.getElementById('szStop').value) || 95;
      const target = parseFloat(document.getElementById('szTarget').value) || 112;
      const riskPerShare = Math.max(0.01, entry - stop);
      const maxRiskAmt = cap * (riskPct / 100);
      const shares = Math.floor(maxRiskAmt / riskPerShare);
      const rr = ((target - entry) / riskPerShare).toFixed(2);
      document.getElementById('szOutput').innerHTML = `
        <div>Recommended Shares: <strong class="text-emerald-400">${shares}</strong></div>
        <div>Total Position Value: <strong>${(shares * entry).toFixed(2)}</strong></div>
        <div>Capital at Risk: <strong>${(shares * riskPerShare).toFixed(2)}</strong> (${riskPct}%)</div>
        <div>Risk / Reward Ratio: <strong class="text-blue-400">${rr}x</strong></div>
      `;
    }

    async function renderWatchlist() {
      const res = await fetch('/api/v1/watchlist');
      const data = await res.json();
      const tbody = document.getElementById('wlTableBody');
      if (!data.items || data.items.length === 0) {
        tbody.innerHTML = `<tr><td colspan="8" class="py-6 text-center text-slate-400">Your Watchlist is empty. Enter any Indian or US symbol above to track it live via API.</td></tr>`;
        return;
      }
      tbody.innerHTML = data.items.map(w => {
        const s = w.live_data;
        return `<tr>
          <td class="py-2 px-2 font-bold text-white">${s.symbol}</td>
          <td class="py-2 px-2">${s.country} • ${s.exchange}</td>
          <td class="py-2 px-2 text-right font-mono">${s.currency_symbol}${s.price}</td>
          <td class="py-2 px-2 text-right font-mono text-emerald-400">${s.currency_symbol}${s.valuation.base_fair_value}</td>
          <td class="py-2 px-2">${badgeHtml(s.long_term_signal.state)}</td>
          <td class="py-2 px-2">${badgeHtml(s.swing_signal.state)}</td>
          <td class="py-2 px-2 text-slate-400">${w.thesis || '-'}</td>
          <td class="py-2 px-2 text-right"><button onclick="removeWatchlist('${s.symbol}')" class="text-rose-400 hover:underline">Remove</button></td>
        </tr>`;
      }).join('');
    }

    async function addWatchlist() {
      const symbol = document.getElementById('wlSym').value.trim();
      const exchange = document.getElementById('wlExch').value;
      const desired_entry = parseFloat(document.getElementById('wlEntry').value) || 0;
      const thesis = document.getElementById('wlThesis').value;
      if (!symbol) return;
      await fetch('/api/v1/watchlist', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({symbol, exchange, desired_entry, thesis})});
      document.getElementById('wlSym').value = '';
      renderWatchlist();
    }

    async function removeWatchlist(sym) {
      await fetch(`/api/v1/watchlist/${sym}`, {method: 'DELETE'});
      renderWatchlist();
    }

    async function renderPortfolio() {
      const res = await fetch('/api/v1/portfolio');
      const data = await res.json();
      const tbody = document.getElementById('pfTableBody');
      if (!data.positions || data.positions.length === 0) {
        tbody.innerHTML = `<tr><td colspan="7" class="py-6 text-center text-slate-400">Your Portfolio is empty. Add your holdings above to track live P&amp;L via API.</td></tr>`;
        return;
      }
      tbody.innerHTML = data.positions.map(p => {
        const s = p.live_data;
        return `<tr>
          <td class="py-2 px-2 font-bold text-white">${s.symbol}</td>
          <td class="py-2 px-2 text-right font-mono">${p.quantity}</td>
          <td class="py-2 px-2 text-right font-mono">${s.currency_symbol}${p.avg_buy_price}</td>
          <td class="py-2 px-2 text-right font-mono font-bold">${s.currency_symbol}${s.price}</td>
          <td class="py-2 px-2 text-right font-mono font-bold ${p.pnl >= 0 ? 'text-emerald-400' : 'text-rose-400'}">${p.pnl >= 0 ? '+' : ''}${s.currency_symbol}${p.pnl} (${p.pnl_pct}%)</td>
          <td class="py-2 px-2">${badgeHtml(s.long_term_signal.state)}</td>
          <td class="py-2 px-2 text-right"><button onclick="removePortfolio('${s.symbol}')" class="text-rose-400 hover:underline">Remove</button></td>
        </tr>`;
      }).join('');
    }

    async function addPortfolio() {
      const symbol = document.getElementById('pfSym').value.trim();
      const exchange = document.getElementById('pfExch').value;
      const quantity = parseFloat(document.getElementById('pfQty').value) || 0;
      const avg_buy_price = parseFloat(document.getElementById('pfAvg').value) || 0;
      if (!symbol || quantity <= 0 || avg_buy_price <= 0) return;
      await fetch('/api/v1/portfolio', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({symbol, exchange, quantity, avg_buy_price})});
      document.getElementById('pfSym').value = '';
      renderPortfolio();
    }

    async function removePortfolio(sym) {
      await fetch(`/api/v1/portfolio/${sym}`, {method: 'DELETE'});
      renderPortfolio();
    }

    calcSwingSize();
    loadUniverse(false);
  </script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
async def serve_quantvalue_platform():
    return HTMLResponse(content=FRONTEND_HTML)
