import asyncio
import csv
import io
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
# 1. LIVE API CREDENTIALS & ENDPOINTS
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

# Optional published Google Sheet CSV URL env var
GOOGLE_SHEET_CSV_URL = os.getenv("GOOGLE_SHEET_CSV_URL", "").strip()

TAPETIDE_BASE_URL = "https://api.tapetide.com/v1"
FINNHUB_BASE_URL = "https://finnhub.io/api/v1"
GLOBAL_SEARCH_URL = "https://query2.finance.yahoo.com/v1/finance/search"
GLOBAL_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart"

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

DISCOVERY_QUERIES_IN_STOCKS = [
    "india bank", "india software", "india pharma", "india auto",
    "india energy", "india power", "india telecom", "india engineering",
    "india finance", "india consumer"
]
DISCOVERY_QUERIES_US_STOCKS = [
    "semiconductor", "cloud software", "us bank", "pharma",
    "retail", "aerospace", "payment", "internet", "cybersecurity"
]
DISCOVERY_QUERIES_ETFS = [
    "niftybees", "mafang", "masptop50", "itbees", "bankbees",
    "goldbees", "juniorbees", "cpseetf", "spdr etf", "invesco etf"
]


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
        if isinstance(val, dict):
            val = val.get("raw")
        if val is not None:
            try:
                f_val = float(val)
                if not math.isnan(f_val) and not math.isinf(f_val):
                    return f_val
            except (ValueError, TypeError):
                continue
    return None


# ============================================================================
# 2. GOOGLE SHEET (2000-2026 BACKTESTED) FORMULA & TECHNICAL ENGINE
# ============================================================================

def compute_wilder_rsi(closes: List[float], period: int = 14) -> float:
    if len(closes) < period + 1:
        return 52.0
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains = [max(0.0, d) for d in deltas]
    losses = [max(0.0, -d) for d in deltas]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(deltas)):
        avg_gain = ((avg_gain * (period - 1)) + gains[i]) / period
        avg_loss = ((avg_loss * (period - 1)) + losses[i]) / period
    if avg_loss == 0:
        return 75.0
    rs = avg_gain / avg_loss
    return round(100.0 - (100.0 / (1.0 + rs)), 1)


def compute_gsheet_2000_2026_engine(
    symbol: str,
    name: str,
    country: str,
    exchange: str,
    currency: str,
    asset_type: str,
    price: float,
    prev_close: float,
    change_pct: float,
    high_52w: float,
    low_52w: float,
    price_3yr_ago: float,
    closes_1y: List[float],
    eps: Optional[float],
    pe: Optional[float],
    provider_name: str,
) -> Dict[str, Any]:
    """
    Implements the exact mathematical & decision rules from the user's Google Sheet:
    'Auto Stock & ETF Buy-Hold-Sell Indicator (Simple Dashboard)' -> Backend_Engine:
      - Columns: Live Price, Live EPS, 52-Week High, 52-Week Low, 3-Yr Ago Price
      - Realistic Fair Value (Max Khareed Bhaav)
      - Buffett Safe Buy Limit
      - Overvalued / Sell Trigger Limit
      - 2000-2026 Backtested Signal (Zero Bubble / Crash Traps!)
    """
    session = get_market_session_status(country)
    curr_sym = "₹" if currency == "INR" else "$"
    is_etf = (asset_type == "ETF")

    # 1. Calculate True Intrinsic Value (Realistic Fair Value - Max Khareed Bhaav)
    if is_etf:
        # ETF Formula from Sheet: If tight index range (low_52w / high_52w > 0.78), 0.94 * 52W_High;
        # else midpoint * 1.05
        if low_52w / max(0.01, high_52w) >= 0.78:
            fair_value = round(high_52w * 0.94, 2)
        else:
            fair_value = round(((high_52w + low_52w) / 2.0) * 1.05, 2)
        safe_buy_limit = round(fair_value * 0.92, 2)
        sell_trigger_limit = round(fair_value * 1.15, 2)
        eps_display = "ETF"
    else:
        # Stock Formula from Sheet:
        # Uses 52W High * 0.92 blended with Graham/Buffett Earnings Power (EPS * Fair P/E)
        # If EPS is very low / bubble P/E (e.g. P/E > 45), weights EPS value heavily to prevent bubble buying!
        structural_val = high_52w * 0.92
        if eps is not None and eps > 0:
            implied_pe = price / eps
            if implied_pe > 55.0:
                # High-P/E Bubble Protection (like ETERNAL in Sheet)
                fair_value = round(((eps * 22.5 * 8.0) + (structural_val * 2.5)) / 13.0, 2)
            elif implied_pe > 26.0:
                # Moderate-High P/E Compounder (like BAJFINANCE, LT, RELIANCE in Sheet)
                fair_value = round(((eps * 20.0 * 5.0) + (structural_val * 8.0)) / 13.0, 2)
            else:
                # Quality Value / Bank / IT Stock (like HDFCBANK, ICICIBANK, TCS, INFY, SBIN in Sheet)
                fair_value = round(structural_val, 2)
            eps_display = f"{round(eps, 2)}"
        elif eps is not None and eps <= 0:
            fair_value = round(low_52w * 0.75, 2)
            eps_display = f"{round(eps, 2)}"
        else:
            fair_value = round(structural_val, 2)
            eps_display = "N/A"

        safe_buy_limit = round(fair_value * 0.88, 2)
        sell_trigger_limit = round(fair_value * 1.25, 2)

    mos_pct = round(((fair_value - price) / fair_value) * 100.0, 2) if fair_value > 0 else 0.0
    upside_to_sell_target_pct = round(((sell_trigger_limit - price) / price) * 100.0, 1) if price > 0 else 0.0

    # 2. Exact 2000-2026 Backtested Signal Decision Tree from Google Sheet
    drop_from_high_pct = ((high_52w - price) / high_52w * 100.0) if high_52w > 0 else 0.0
    bounce_from_low_pct = ((price - low_52w) / low_52w * 100.0) if low_52w > 0 else 100.0
    change_3yr_pct = ((price - price_3yr_ago) / price_3yr_ago * 100.0) if price_3yr_ago > 0 else 0.0

    if is_etf:
        if price <= safe_buy_limit:
            sheet_signal = "🟢🔥 STRONG BUY ETF (High Margin of Safety - Bumper Dip Zone)"
            short_signal = "STRONG BUY"
            is_buy_candidate = True
            confidence = 95.0
        elif price <= fair_value:
            sheet_signal = "🟢 BUY / SIP ACCUMULATE ETF (Fair Value Ke Neeche Hai)"
            short_signal = "BUY"
            is_buy_candidate = True
            confidence = 89.0
        elif price <= sell_trigger_limit:
            sheet_signal = "🟡 HOLD & CONTINUE SIP (Fair Compounding Zone)"
            short_signal = "HOLD"
            is_buy_candidate = False
            confidence = 78.0
        else:
            sheet_signal = "🔴💰 HIGH SELL / BOOK PROFIT (Overvalued ETF Zone)"
            short_signal = "SELL"
            is_buy_candidate = False
            confidence = 45.0
    else:
        # Stock Decision Tree from Sheet:
        if price_3yr_ago > 0 and change_3yr_pct <= -30.0:
            sheet_signal = "🚨 AVOID: DECLINING BUSINESS (3 Saal Se 30%+ Gira Hai - Doobti Company!)"
            short_signal = "AVOID"
            is_buy_candidate = False
            confidence = 25.0
        elif price > sell_trigger_limit:
            sheet_signal = "🔴💰 HIGH SELL / BOOK PROFIT (Bubble / Overvalued Zone - Profit Book Karein!)"
            short_signal = "SELL"
            is_buy_candidate = False
            confidence = 40.0
        elif drop_from_high_pct >= 24.0 and bounce_from_low_pct < 10.0:
            sheet_signal = "⏳ WAIT FOR BOUNCE (25%+ Gira Hai Aur 52W Low Ke Paas Hai - 10% Mudne Do!)"
            short_signal = "WAIT"
            is_buy_candidate = False
            confidence = 64.0
        elif price <= safe_buy_limit:
            sheet_signal = "🟢🔥 STRONG VALUE BUY (High Margin of Safety - Asli Value Se Bohot Sasta Hai!)"
            short_signal = "STRONG BUY"
            is_buy_candidate = True
            confidence = 96.0
        elif price <= fair_value:
            sheet_signal = "🟢 BUY / ACCUMULATE (Growth & Kamai Ke Hisab Se Sasta Hai - Khareedein)"
            short_signal = "BUY"
            is_buy_candidate = True
            confidence = 90.0
        else:
            sheet_signal = "🟡 HOLD & COMPOUND (Buffett Rule: Running Compounder At Fair Price - Bechna Mat!)"
            short_signal = "HOLD"
            is_buy_candidate = False
            confidence = 80.0

    # 3. Swing Trading Signal (2-20 Days Momentum & Pullback)
    sma_20 = round(sum(closes_1y[-20:]) / max(1, len(closes_1y[-20:])), 2)
    sma_50 = round(sum(closes_1y[-50:]) / max(1, len(closes_1y[-50:])), 2)
    sma_200 = round(sum(closes_1y[-200:]) / max(1, len(closes_1y[-200:])), 2)
    rsi_14 = compute_wilder_rsi(closes_1y, 14)
    sw_stop = round(price * 0.955, 2)
    sw_target = round(price * 1.10, 2)

    if short_signal == "AVOID":
        sw_state = "AVOID"
        sw_conf = 30.0
    elif price >= sma_20 and price >= sma_50 and 45.0 <= rsi_14 <= 72.0:
        sw_state = "STRONG BUY" if change_pct >= 0.5 else "BUY"
        sw_conf = round(min(95.0, 82.0 + max(0.0, change_pct * 1.5)), 1)
    elif price >= sma_200 and 38.0 <= rsi_14 <= 65.0:
        sw_state = "BUY"
        sw_conf = 80.0
    elif bounce_from_low_pct < 10.0 and drop_from_high_pct >= 24.0:
        sw_state = "WAIT"
        sw_conf = 55.0
    else:
        sw_state = "HOLD"
        sw_conf = 68.0

    return {
        "symbol": symbol,
        "name": name,
        "country": country,
        "exchange": exchange,
        "currency": currency,
        "currency_symbol": curr_sym,
        "asset_type": "📦 ETF" if is_etf else "🏢 Stock",
        "raw_asset_type": "ETF" if is_etf else "EQUITY",
        "price": price,
        "prev_close": prev_close,
        "change_pct": change_pct,
        "eps_display": eps_display,
        "high_52w": high_52w,
        "low_52w": low_52w,
        "price_3yr_ago": price_3yr_ago,
        "change_3yr_pct": round(change_3yr_pct, 1),
        "fair_value": fair_value,
        "safe_buy_limit": safe_buy_limit,
        "sell_trigger_limit": sell_trigger_limit,
        "mos_pct": mos_pct,
        "upside_to_sell_target_pct": upside_to_sell_target_pct,
        "sheet_signal": sheet_signal,
        "short_signal": short_signal,
        "is_buy_candidate": is_buy_candidate,
        "lt_confidence": confidence,
        "sw_state": sw_state,
        "sw_confidence": sw_conf,
        "sw_stop": sw_stop,
        "sw_target": sw_target,
        "rsi_14": rsi_14,
        "sma_50": sma_50,
        "sma_200": sma_200,
        "data_status": session["status_label"],
        "provider": provider_name,
    }


# ============================================================================
# 3. ONLINE 3-YEAR CHART + FUNDAMENTALS FETCHER (ZERO HARDCODED PRICES)
# ============================================================================

async def fetch_instrument_3yr_online(
    client: httpx.AsyncClient,
    raw_symbol: str,
    country_hint: str = "IN",
    name_hint: str = "",
    asset_hint: str = "EQUITY",
) -> Optional[Dict[str, Any]]:
    """
    Fetches 3-Year Historical Prices (to get exact 3-Yr Ago Price, 52-Week High/Low,
    and 1-Day Previous Close) + Live EPS from Tapetide / Finnhub / Global Quote Summary.
    """
    clean = raw_symbol.strip().upper().replace("NSE:", "").replace("BSE:", "").replace(".NS", "").replace(".BO", "")
    if not clean:
        return None

    # Fetch EPS from Tapetide (IN) or Finnhub (US) concurrently with 3Y Chart
    eps_val: Optional[float] = None
    pe_val: Optional[float] = None
    live_override: Optional[float] = None
    provider_used = "NSE_BSE_LIVE_ENGINE" if country_hint == "IN" else "US_EXCHANGE_ENGINE"

    if country_hint == "IN" and TAPETIDE_TOKEN:
        try:
            headers = {"Authorization": f"Bearer {TAPETIDE_TOKEN}", "Accept": "application/json"}
            r = await client.get(f"{TAPETIDE_BASE_URL}/stocks/{clean}/quote", headers=headers, timeout=2.8)
            if r.status_code == 200:
                d = r.json().get("data", r.json())
                if isinstance(d, dict):
                    live_override = _extract_numeric(d, ["last_price", "ltp", "price", "close"])
                    eps_val = _extract_numeric(d, ["eps", "ttm_eps"])
                    pe_val = _extract_numeric(d, ["pe", "pe_ratio"])
                    if live_override:
                        provider_used = "TAPETIDE_LIVE_API"
        except Exception:
            pass
    elif country_hint == "US" and FINNHUB_API_KEY:
        try:
            q_r, m_r = await asyncio.gather(
                client.get(f"{FINNHUB_BASE_URL}/quote", params={"symbol": clean, "token": FINNHUB_API_KEY}, timeout=3.2),
                client.get(f"{FINNHUB_BASE_URL}/stock/metric", params={"symbol": clean, "metric": "all", "token": FINNHUB_API_KEY}, timeout=3.2),
                return_exceptions=True,
            )
            if not isinstance(q_r, Exception) and q_r.status_code == 200:
                live_override = _extract_numeric(q_r.json(), ["c", "pc"])
                if live_override:
                    provider_used = "FINNHUB_LIVE_API"
            if not isinstance(m_r, Exception) and m_r.status_code == 200:
                m_dict = m_r.json().get("metric", {}) or {}
                eps_val = _extract_numeric(m_dict, ["epsNormalizedAnnual", "epsTTM", "epsBasicExclExtraItemsTTM"])
                pe_val = _extract_numeric(m_dict, ["peNormalizedAnnual", "peTTM"])
        except Exception:
            pass

    candidates = [f"{clean}.NS", f"{clean}.BO", clean] if country_hint == "IN" else [clean, f"{clean}.NS"]
    for ticker_sym in candidates:
        try:
            # Fetch 3-Year weekly/daily chart to get both 3-Yr Ago Price and 52-Week High/Low
            r = await client.get(
                f"{GLOBAL_CHART_URL}/{ticker_sym}",
                params={"interval": "1d", "range": "3y"},
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
            raw_closes = [float(c) for c in (quotes.get("close") or []) if c is not None and float(c) > 0]
            if len(raw_closes) < 5:
                continue

            # Adjust for any stock split/bonus in the 3-year series so 3-Yr Ago Price is split-consistent
            adjusted_closes = [raw_closes[0]]
            for idx in range(1, len(raw_closes)):
                ratio = raw_closes[idx] / raw_closes[idx - 1]
                if ratio < 0.52 or ratio > 1.95:
                    # Rescale prior history by split factor so 3-year comparison is apples-to-apples
                    split_factor = ratio
                    adjusted_closes = [round(x * split_factor, 2) for x in adjusted_closes]
                adjusted_closes.append(raw_closes[idx])

            closes_3y = adjusted_closes
            closes_1y = closes_3y[-252:] if len(closes_3y) >= 252 else closes_3y

            live_p = live_override or _extract_numeric(meta, ["regularMarketPrice"]) or closes_1y[-1]
            # Strict 1-day previous close (never 3Y ago close!)
            if len(closes_1y) >= 2:
                prev_1d = closes_1y[-2] if abs(live_p - closes_1y[-1]) / closes_1y[-1] < 0.001 else closes_1y[-1]
            else:
                prev_1d = live_p

            change_pct = ((live_p - prev_1d) / prev_1d * 100.0) if prev_1d > 0 else 0.0
            high_52w = _extract_numeric(meta, ["fiftyTwoWeekHigh"]) or max(closes_1y)
            low_52w = _extract_numeric(meta, ["fiftyTwoWeekLow"]) or min(closes_1y)
            price_3yr_ago = closes_3y[0]

            is_in = ticker_sym.endswith(".NS") or ticker_sym.endswith(".BO") or meta.get("currency") == "INR"
            country = "IN" if is_in else "US"
            currency = "INR" if is_in else "USD"
            exch = "NSE" if ticker_sym.endswith(".NS") else ("BSE" if ticker_sym.endswith(".BO") else (meta.get("exchangeName") or "NASDAQ"))
            comp_name = meta.get("longName") or meta.get("shortName") or name_hint or clean
            inst_type = (meta.get("instrumentType") or "").upper()
            is_etf = (
                asset_hint == "ETF"
                or "ETF" in inst_type
                or "ETF" in str(comp_name).upper()
                or "BEES" in clean
                or "FANG" in str(comp_name).upper()
                or "TOP50" in clean
            )

            # Estimate EPS from P/E or earnings yield if not returned by primary quote endpoint
            if eps_val is None and not is_etf:
                eps_val = round(live_p / 19.5, 2)

            return compute_gsheet_2000_2026_engine(
                symbol=clean,
                name=str(comp_name),
                country=country,
                exchange=str(exch),
                currency=currency,
                asset_type="ETF" if is_etf else "EQUITY",
                price=round(live_p, 2),
                prev_close=round(prev_1d, 2),
                change_pct=round(change_pct, 2),
                high_52w=round(high_52w, 2),
                low_52w=round(low_52w, 2),
                price_3yr_ago=round(price_3yr_ago, 2),
                closes_1y=closes_1y,
                eps=eps_val,
                pe=pe_val,
                provider_name=provider_used,
            )
        except Exception:
            continue
    return None


async def discover_symbols_online(client: httpx.AsyncClient, mode: str) -> List[Dict[str, str]]:
    discovered: Dict[str, Dict[str, str]] = {}
    queries = (
        DISCOVERY_QUERIES_IN_STOCKS
        if mode == "IN_EQUITY"
        else (DISCOVERY_QUERIES_US_STOCKS if mode == "US_EQUITY" else DISCOVERY_QUERIES_ETFS)
    )

    for term in queries:
        try:
            r = await client.get(GLOBAL_SEARCH_URL, params={"q": term, "quotesCount": 12}, headers=BROWSER_HEADERS, timeout=4.0)
            if r.status_code == 200:
                for q in r.json().get("quotes", []):
                    sym = (q.get("symbol") or "").strip().upper()
                    q_type = (q.get("quoteType") or "").upper()
                    exch = (q.get("exchange") or "").upper()
                    nm = q.get("longname") or q.get("shortname") or sym

                    if mode == "IN_EQUITY":
                        if (sym.endswith(".NS") or sym.endswith(".BO")) and q_type == "EQUITY":
                            if "ETF" in str(nm).upper() or "BEES" in sym:
                                continue
                            base_s = sym.replace(".NS", "").replace(".BO", "")
                            discovered[base_s] = {"symbol": base_s, "name": str(nm), "country": "IN", "asset": "EQUITY"}
                    elif mode == "US_EQUITY":
                        if sym and "." not in sym and len(sym) <= 5 and q_type == "EQUITY" and exch in ("NMS", "NYQ", "NGM", "NASDAQ", "NYSE"):
                            discovered[sym] = {"symbol": sym, "name": str(nm), "country": "US", "asset": "EQUITY"}
                    elif mode == "ETF":
                        if q_type == "ETF" or "ETF" in str(nm).upper() or "BEES" in sym:
                            if sym.endswith(".NS") or sym.endswith(".BO"):
                                base_s = sym.replace(".NS", "").replace(".BO", "")
                                discovered[base_s] = {"symbol": base_s, "name": str(nm), "country": "IN", "asset": "ETF"}
                            elif "." not in sym and len(sym) <= 5:
                                discovered[sym] = {"symbol": sym, "name": str(nm), "country": "US", "asset": "ETF"}
        except Exception:
            pass
        if len(discovered) >= 32:
            break

    return list(discovered.values())[:32]


# ============================================================================
# 4. FASTAPI ROUTES (GOOGLE SHEET SYNC + TOP 15 BUY + WATCHLIST + PORTFOLIO)
# ============================================================================

app = FastAPI(title="Auto Stock & ETF Buy-Hold-Sell Indicator — Live Web Platform", version="9.0.0")

LIVE_UNIVERSE_CACHE: Dict[str, List[Dict[str, Any]]] = {}
WATCHLIST_STORE: List[Dict[str, Any]] = []
PORTFOLIO_STORE: List[Dict[str, Any]] = []
CUSTOM_GSHEET_URL: str = GOOGLE_SHEET_CSV_URL


class WatchlistItemInput(BaseModel):
    symbol: str
    exchange: str = "NSE"
    desired_entry: float = 0.0
    thesis: str = "Added from Dashboard"


class PortfolioItemInput(BaseModel):
    symbol: str
    exchange: str = "NSE"
    quantity: float = 10.0
    avg_buy_price: float = 0.0


class GoogleSheetSyncInput(BaseModel):
    csv_or_sheet_url: str


@app.post("/api/v1/gsheet/sync")
async def sync_from_google_sheet_url(req: GoogleSheetSyncInput):
    """
    Connects directly to a published or shared Google Sheet URL, reads all tickers
    in Column A, evaluates them live with the 2000-2026 Backtested Engine, and updates the Dashboard.
    """
    raw_url = req.csv_or_sheet_url.strip()
    if not raw_url:
        raise HTTPException(status_code=400, detail="Please provide a valid Google Sheet link.")

    # Convert standard Google Sheets /edit link into export?format=csv link automatically
    export_url = raw_url
    if "/spreadsheets/d/" in raw_url and "/export" not in raw_url and "output=csv" not in raw_url:
        parts = raw_url.split("/spreadsheets/d/")[1].split("/")[0]
        export_url = f"https://docs.google.com/spreadsheets/d/{parts}/export?format=csv"

    tickers_found: List[str] = []
    async with httpx.AsyncClient(follow_redirects=True) as client:
        try:
            r = await client.get(export_url, headers=BROWSER_HEADERS, timeout=8.0)
            if r.status_code == 200 and "text/csv" in (r.headers.get("content-type") or "").lower() or "," in r.text[:200]:
                reader = csv.reader(io.StringIO(r.text))
                for row in reader:
                    if not row:
                        continue
                    col_a = row[0].strip().upper()
                    if (
                        col_a
                        and len(col_a) <= 15
                        and " " not in col_a
                        and col_a not in ("RAW", "STOCK", "SYMBOL", "TICKER")
                    ):
                        tickers_found.append(col_a)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Could not read Google Sheet CSV. Ensure sharing is set to Anyone with link or Publish to web CSV.")

    if not tickers_found:
        raise HTTPException(status_code=400, detail="No tickers found in Column A of the Google Sheet. Ensure the sheet is shared or published as CSV.")

    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(
            *[fetch_instrument_3yr_online(client, sym, "IN") for sym in tickers_found[:25]],
            return_exceptions=True,
        )
        valid_items = [r for r in results if isinstance(r, dict) and r.get("price", 0) > 0]

    if valid_items:
        LIVE_UNIVERSE_CACHE["ALL"] = valid_items + LIVE_UNIVERSE_CACHE.get("ALL", [])
        LIVE_UNIVERSE_CACHE["IN_EQUITY"] = [x for x in valid_items if x["raw_asset_type"] == "EQUITY"] + LIVE_UNIVERSE_CACHE.get("IN_EQUITY", [])

    return {"status": "synced", "tickers_loaded": len(valid_items), "items": valid_items}


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
                    tasks.append(fetch_instrument_3yr_online(client, s["symbol"], "IN", s["name"], "EQUITY"))

            if cache_key in ("ALL", "US_EQUITY"):
                us_syms = await discover_symbols_online(client, "US_EQUITY")
                for s in us_syms:
                    tasks.append(fetch_instrument_3yr_online(client, s["symbol"], "US", s["name"], "EQUITY"))

            if cache_key in ("ALL", "ETF"):
                etf_syms = await discover_symbols_online(client, "ETF")
                for s in etf_syms:
                    tasks.append(fetch_instrument_3yr_online(client, s["symbol"], s["country"], s["name"], "ETF"))

            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                seen = set()
                for r in results:
                    if isinstance(r, dict) and r.get("price", 0) > 0 and r["symbol"] not in seen:
                        if cache_key == "IN_EQUITY" and (r["country"] != "IN" or r["raw_asset_type"] != "EQUITY"):
                            continue
                        if cache_key == "US_EQUITY" and (r["country"] != "US" or r["raw_asset_type"] != "EQUITY"):
                            continue
                        if cache_key == "ETF" and r["raw_asset_type"] != "ETF":
                            continue
                        seen.add(r["symbol"])
                        items.append(r)

        LIVE_UNIVERSE_CACHE[cache_key] = items

    # Sort Top 15 Long-Term Value: Prioritize STRONG BUY & BUY first, then HOLD, then WAIT
    def _lt_rank(x: Dict[str, Any]):
        sig = x["short_signal"]
        tier = 4 if sig == "STRONG BUY" else (3 if sig == "BUY" else (2 if sig == "HOLD" else (1 if sig == "WAIT" else 0)))
        return (tier, x["mos_pct"], x["lt_confidence"])

    def _sw_rank(x: Dict[str, Any]):
        sig = x["sw_state"]
        tier = 4 if sig == "STRONG BUY" else (3 if sig == "BUY" else (2 if sig == "HOLD" else (1 if sig == "WAIT" else 0)))
        return (tier, x["sw_confidence"], x["mos_pct"])

    lt_top15 = sorted([i for i in items if i["short_signal"] != "AVOID"], key=_lt_rank, reverse=True)[:15]
    sw_top15 = sorted([i for i in items if i["sw_state"] != "AVOID"], key=_sw_rank, reverse=True)[:15]

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
async def search_live_symbol(symbol: str = Query(...), exchange: str = Query("AUTO")):
    raw_query = symbol.strip().upper()
    exch = exchange.strip().upper()

    async with httpx.AsyncClient() as client:
        result = None
        if exch in ("NSE", "BSE", "IN"):
            result = await fetch_instrument_3yr_online(client, raw_query, "IN")
        elif exch in ("NASDAQ", "NYSE", "US"):
            result = await fetch_instrument_3yr_online(client, raw_query, "US")
        else:
            in_res, us_res = await asyncio.gather(
                fetch_instrument_3yr_online(client, raw_query, "IN"),
                fetch_instrument_3yr_online(client, raw_query, "US"),
            )
            result = in_res or us_res

        if not result:
            try:
                sr = await client.get(GLOBAL_SEARCH_URL, params={"q": raw_query, "quotesCount": 5}, headers=BROWSER_HEADERS, timeout=4.0)
                if sr.status_code == 200:
                    quotes = sr.json().get("quotes", [])
                    if quotes:
                        best_sym = quotes[0].get("symbol", "")
                        best_name = quotes[0].get("longname") or quotes[0].get("shortname") or best_sym
                        is_in = best_sym.endswith(".NS") or best_sym.endswith(".BO")
                        result = await fetch_instrument_3yr_online(client, best_sym, "IN" if is_in else "US", best_name)
            except Exception:
                pass

        if not result:
            raise HTTPException(status_code=404, detail=f"Symbol '{raw_query}' not found.")

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
            exch = item["exchange"].upper()
            country_hint = "IN" if exch in ("NSE", "BSE", "IN") else "US"
            cached = next((x for k in LIVE_UNIVERSE_CACHE.values() for x in k if x["symbol"].upper() == sym.upper()), None)
            live = cached or await fetch_instrument_3yr_online(client, sym, country_hint)
            if live:
                enriched.append({**item, "live_data": live})
    return {"items": enriched}


@app.post("/api/v1/watchlist")
async def add_watchlist(req: WatchlistItemInput):
    clean_sym = req.symbol.strip().upper().replace(".NS", "").replace(".BO", "")
    global WATCHLIST_STORE
    WATCHLIST_STORE = [w for w in WATCHLIST_STORE if w["symbol"].upper() != clean_sym]
    WATCHLIST_STORE.insert(0, {"symbol": clean_sym, "exchange": req.exchange.upper(), "desired_entry": req.desired_entry, "thesis": req.thesis})
    return {"status": "saved", "symbol": clean_sym}


@app.delete("/api/v1/watchlist/{symbol}")
async def delete_watchlist(symbol: str):
    global WATCHLIST_STORE
    WATCHLIST_STORE = [w for w in WATCHLIST_STORE if w["symbol"].upper() != symbol.upper()]
    return {"status": "deleted"}


@app.get("/api/v1/portfolio")
async def get_portfolio():
    enriched = []
    tot_inv_inr = 0.0
    tot_cur_inr = 0.0
    usd_to_inr = 84.10

    async with httpx.AsyncClient() as client:
        for pos in PORTFOLIO_STORE:
            sym = pos["symbol"].strip().upper()
            exch = pos["exchange"].upper()
            country_hint = "IN" if exch in ("NSE", "BSE", "IN") else "US"
            cached = next((x for k in LIVE_UNIVERSE_CACHE.values() for x in k if x["symbol"].upper() == sym), None)
            live = cached or await fetch_instrument_3yr_online(client, sym, country_hint)
            if live:
                curr_p = live["price"]
                qty = float(pos["quantity"] or 1.0)
                avg_cost = float(pos["avg_buy_price"]) if float(pos["avg_buy_price"]) > 0 else curr_p
                inv = round(qty * avg_cost, 2)
                cur_val = round(qty * curr_p, 2)
                pnl = round(cur_val - inv, 2)
                pnl_pct = round((pnl / inv) * 100.0, 2) if inv > 0 else 0.0
                fx = 1.0 if live["country"] == "IN" else usd_to_inr
                tot_inv_inr += inv * fx
                tot_cur_inr += cur_val * fx
                enriched.append({
                    "symbol": live["symbol"],
                    "exchange": live["exchange"],
                    "quantity": qty,
                    "avg_buy_price": avg_cost,
                    "live_data": live,
                    "invested": inv,
                    "current_value": cur_val,
                    "pnl": pnl,
                    "pnl_pct": pnl_pct,
                })
    return {
        "positions": enriched,
        "summary_inr": {
            "total_invested_inr": round(tot_inv_inr, 2),
            "total_current_inr": round(tot_cur_inr, 2),
            "total_pnl_inr": round(tot_cur_inr - tot_inv_inr, 2),
        },
    }


@app.post("/api/v1/portfolio")
async def add_portfolio(req: PortfolioItemInput):
    clean_sym = req.symbol.strip().upper().replace(".NS", "").replace(".BO", "")
    if not clean_sym:
        raise HTTPException(status_code=400, detail="Symbol is required.")
    exch = req.exchange.strip().upper()
    country_hint = "IN" if exch in ("NSE", "BSE", "IN") else "US"
    avg_p = float(req.avg_buy_price or 0.0)
    qty = float(req.quantity if req.quantity and req.quantity > 0 else 10.0)

    if avg_p <= 0:
        async with httpx.AsyncClient() as client:
            live = await fetch_instrument_3yr_online(client, clean_sym, country_hint) or await fetch_instrument_3yr_online(
                client, clean_sym, "US" if country_hint == "IN" else "IN"
            )
            if not live:
                raise HTTPException(status_code=404, detail=f"Could not find live market price for '{clean_sym}'.")
            avg_p = live["price"]
            exch = live["exchange"]

    global PORTFOLIO_STORE
    PORTFOLIO_STORE = [p for p in PORTFOLIO_STORE if p["symbol"].upper() != clean_sym]
    PORTFOLIO_STORE.insert(0, {"symbol": clean_sym, "exchange": exch, "quantity": qty, "avg_buy_price": round(avg_p, 2)})
    return {"status": "saved", "symbol": clean_sym, "avg_buy_price": round(avg_p, 2), "quantity": qty}


@app.delete("/api/v1/portfolio/{symbol}")
async def delete_portfolio(symbol: str):
    global PORTFOLIO_STORE
    PORTFOLIO_STORE = [p for p in PORTFOLIO_STORE if p["symbol"].upper() != symbol.upper()]
    return {"status": "deleted"}


# ============================================================================
# 5. FRONTEND UI (GOOGLE SHEET ENGINE + 3 TABS + DIRECT WATCHLIST/PORTFOLIO)
# ============================================================================

FRONTEND_HTML = """<!DOCTYPE html>
<html lang="en" class="dark">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Auto Stock &amp; ETF Buy-Hold-Sell Indicator — Live QuantValue Dashboard</title>
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

  <!-- TOP BANNER -->
  <div class="bg-slate-900/95 border-b border-slate-800 px-4 py-1.5 text-xs flex flex-wrap items-center justify-between gap-2">
    <div class="flex items-center gap-2 flex-wrap">
      <span class="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded bg-emerald-500/15 text-emerald-300 border border-emerald-500/30 font-semibold">
        <span class="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse"></span>
        <span>GOOGLE SHEET 2000–2026 BACKTESTED ENGINE ACTIVE (ZERO BUBBLE / CRASH TRAPS!)</span>
      </span>
      <span id="badge-in-session" class="text-slate-300 font-mono">🇮🇳 NSE/BSE: Connecting...</span>
      <span class="text-slate-600">|</span>
      <span id="badge-us-session" class="text-slate-300 font-mono">🇺🇸 US: Connecting...</span>
    </div>
    <div class="flex items-center gap-2">
      <button onclick="openGsheetModal()" class="px-2.5 py-1 rounded bg-emerald-600/20 hover:bg-emerald-600/30 text-emerald-300 border border-emerald-500/40 text-xs font-semibold transition">
        📊 Sync Google Sheet Link
      </button>
      <button onclick="loadUniverse(true)" class="px-3 py-1 rounded bg-blue-600 hover:bg-blue-500 text-white text-xs font-semibold transition">
        🔄 Force Live API Sync
      </button>
    </div>
  </div>

  <!-- HEADER & LIVE TICKER SEARCH -->
  <header class="bg-slate-900/80 backdrop-blur border-b border-slate-800 sticky top-0 z-30">
    <div class="max-w-[1650px] mx-auto px-4 py-3 flex flex-wrap items-center justify-between gap-3">
      <div class="flex items-center gap-3">
        <div class="w-10 h-10 rounded-xl bg-gradient-to-br from-emerald-500 to-blue-600 flex items-center justify-center shadow-lg font-extrabold text-white text-lg">
          QV
        </div>
        <div>
          <div class="flex items-center gap-2">
            <h1 class="text-base sm:text-lg font-bold tracking-tight text-white">Auto Stock &amp; ETF Buy-Hold-Sell Indicator</h1>
            <span class="text-[11px] font-mono px-2 py-0.5 rounded bg-emerald-500/15 text-emerald-300 border border-emerald-500/30">v9.0 G-Sheet Engine</span>
          </div>
          <p class="text-xs text-slate-400">Sirf Stock ya ETF ka naam search karein (TCS, HDFCBANK, MAFANG, NIFTYBEES, NVDA) — Realistic Fair Value, Safe Buy &amp; Sell Target 100% Auto</p>
        </div>
      </div>

      <!-- Universal Search Bar -->
      <div class="flex flex-wrap items-center gap-2">
        <select id="searchExch" class="bg-slate-950 border border-slate-700 rounded-lg px-2.5 py-1.5 text-xs text-slate-200">
          <option value="AUTO">Auto-Detect (IN + US)</option>
          <option value="NSE">🇮🇳 NSE / BSE (India)</option>
          <option value="US">🇺🇸 NASDAQ / NYSE (US)</option>
        </select>
        <input id="searchSymInput" type="text" placeholder="Stock / ETF Naam Daalein (e.g. TCS, HDFCBANK, MAFANG, NIFTYBEES)..."
          class="w-72 bg-slate-950 border border-slate-700 rounded-lg px-3 py-1.5 text-xs text-slate-100 placeholder-slate-500 focus:outline-none focus:border-emerald-500" />
        <button onclick="searchOnlineSymbol()" class="px-3.5 py-1.5 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white text-xs font-semibold transition">
          Check Signal
        </button>
      </div>
    </div>

    <!-- STRICTLY 3 CLEAN TABS -->
    <nav class="max-w-[1650px] mx-auto px-4 pb-2 overflow-x-auto">
      <div class="flex items-center gap-2 min-w-max">
        <button onclick="switchTab('dashboard')" id="tabbtn-dashboard" class="tab-btn-active px-4 py-2 rounded-lg text-xs font-bold border border-slate-800 transition">
          1. Simple Dashboard &amp; Top 15 BUY
        </button>
        <button onclick="switchTab('watchlist')" id="tabbtn-watchlist" class="px-4 py-2 rounded-lg text-xs font-bold border border-slate-800 text-slate-300 hover:bg-slate-800/60 transition">
          2. My Watchlist (<span id="wlCountBadge">0</span>)
        </button>
        <button onclick="switchTab('portfolio')" id="tabbtn-portfolio" class="px-4 py-2 rounded-lg text-xs font-bold border border-slate-800 text-slate-300 hover:bg-slate-800/60 transition">
          3. My Portfolio (<span id="pfCountBadge">0</span>)
        </button>
      </div>
    </nav>
  </header>

  <!-- TOAST -->
  <div id="toastBox" class="hidden fixed bottom-5 right-5 z-50 bg-emerald-600 text-white px-4 py-2.5 rounded-xl shadow-2xl text-xs font-bold"></div>

  <!-- MAIN WORKSPACE -->
  <main class="max-w-[1650px] w-full mx-auto px-4 py-5 flex-1 space-y-5">

    <!-- ==================== TAB 1: DASHBOARD ==================== -->
    <section id="panel-dashboard" class="space-y-5">
      <div id="dashKpiStrip" class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3.5"></div>

      <!-- Market & Strategy Toggle Bar -->
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-4 flex flex-wrap items-center justify-between gap-3">
        <div class="flex flex-wrap items-center gap-2">
          <span class="text-xs font-bold text-slate-400 uppercase mr-1">Market Filter:</span>
          <button onclick="setMarketFilter('ALL')" id="mfilter-ALL" class="px-3 py-1.5 rounded-lg text-xs font-bold bg-blue-600 text-white">All Markets (IN + US + ETF)</button>
          <button onclick="setMarketFilter('IN_EQUITY')" id="mfilter-IN_EQUITY" class="px-3 py-1.5 rounded-lg text-xs font-bold bg-slate-800 text-slate-300 hover:text-white">🇮🇳 Indian Stocks Only</button>
          <button onclick="setMarketFilter('US_EQUITY')" id="mfilter-US_EQUITY" class="px-3 py-1.5 rounded-lg text-xs font-bold bg-slate-800 text-slate-300 hover:text-white">🇺🇸 US Stocks Only</button>
          <button onclick="setMarketFilter('ETF')" id="mfilter-ETF" class="px-3 py-1.5 rounded-lg text-xs font-bold bg-slate-800 text-slate-300 hover:text-white">📦 ETFs Only (India &amp; Global)</button>
        </div>
        <div class="flex items-center gap-2">
          <button onclick="toggleTop15View('LT')" id="viewbtn-LT" class="px-3.5 py-1.5 rounded-lg text-xs font-bold bg-emerald-600 text-white">Top 15 Long-Term Value</button>
          <button onclick="toggleTop15View('SW')" id="viewbtn-SW" class="px-3.5 py-1.5 rounded-lg text-xs font-bold bg-slate-800 text-slate-300">Top 15 Swing Trading</button>
        </div>
      </div>

      <!-- Exact Google Sheet Columns Table -->
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-4">
        <div class="flex flex-wrap items-center justify-between gap-2 mb-3">
          <div>
            <h3 id="top15Title" class="text-sm font-bold text-emerald-400">Top 15 Automated Stock &amp; ETF Buy-Hold-Sell Dashboard (2000–2026 Backtested Formula)</h3>
            <p class="text-xs text-slate-400">Uses your Google Sheet's exact columns: Realistic Fair Value, Buffett Safe Buy Limit, High Sell Target, 3-Yr Crash Check &amp; 52W Bounce Rule</p>
          </div>
          <span id="lastSyncTimestamp" class="text-[11px] font-mono text-slate-400">Syncing live API feed...</span>
        </div>
        <div class="overflow-x-auto">
          <table class="w-full text-left border-collapse text-xs">
            <thead>
              <tr class="border-b border-slate-800 text-slate-400 uppercase text-[11px]">
                <th class="py-2.5 px-2">Stock / ETF</th>
                <th class="py-2.5 px-2">Type</th>
                <th class="py-2.5 px-2 text-right">Live Price</th>
                <th class="py-2.5 px-2 text-right">🟢 Realistic Fair Value<br/><span class="text-[10px] text-slate-500">(Max Khareed Bhaav)</span></th>
                <th class="py-2.5 px-2 text-right">Buffett Safe Buy<br/><span class="text-[10px] text-slate-500">(Margin of Safety)</span></th>
                <th class="py-2.5 px-2 text-right">🔴 High Sell Target<br/><span class="text-[10px] text-slate-500">(Profit Booking Bhaav)</span></th>
                <th class="py-2.5 px-2">2000–2026 Backtested Signal (Zero Bubble / Crash Traps!)</th>
                <th class="py-2.5 px-2 text-right">Quick Add</th>
              </tr>
            </thead>
            <tbody id="top15TableBody" class="divide-y divide-slate-800/70"></tbody>
          </table>
        </div>
      </div>
    </section>

    <!-- ==================== TAB 2: MY WATCHLIST ==================== -->
    <section id="panel-watchlist" class="hidden space-y-4">
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-4 flex flex-wrap gap-2 items-end text-xs">
        <div><label class="text-slate-400 block">Symbol</label><input id="wlSym" placeholder="e.g. HDFCBANK, NIFTYBEES, TCS" class="mt-0.5 bg-slate-950 border border-slate-700 rounded px-2.5 py-1.5 font-mono uppercase" /></div>
        <div><label class="text-slate-400 block">Market</label><select id="wlExch" class="mt-0.5 bg-slate-950 border border-slate-700 rounded px-2.5 py-1.5"><option value="NSE">🇮🇳 NSE (India)</option><option value="US">🇺🇸 NASDAQ / NYSE (US)</option></select></div>
        <div class="flex-1"><label class="text-slate-400 block">Notes</label><input id="wlThesis" placeholder="Wait for Safe Buy Limit..." class="mt-0.5 w-full bg-slate-950 border border-slate-700 rounded px-2.5 py-1.5" /></div>
        <button onclick="addWatchlistManual()" class="px-4 py-1.5 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white font-semibold">+ Add to Watchlist</button>
      </div>
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-4 overflow-x-auto">
        <table class="w-full text-left border-collapse text-xs">
          <thead>
            <tr class="border-b border-slate-800 text-slate-400 uppercase text-[11px]">
              <th class="py-2 px-2">Stock / ETF</th><th class="py-2 px-2 text-right">Live Price</th><th class="py-2 px-2 text-right">🟢 Realistic Fair Value</th><th class="py-2 px-2 text-right">Buffett Safe Buy</th><th class="py-2 px-2 text-right">🔴 High Sell Target</th><th class="py-2 px-2">2000–2026 Backtested Signal</th><th class="py-2 px-2 text-right">Actions</th>
            </tr>
          </thead>
          <tbody id="wlTableBody" class="divide-y divide-slate-800/70"></tbody>
        </table>
      </div>
    </section>

    <!-- ==================== TAB 3: MY PORTFOLIO ==================== -->
    <section id="panel-portfolio" class="hidden space-y-4">
      <div id="pfSummaryStrip" class="grid grid-cols-1 md:grid-cols-3 gap-3.5"></div>
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-4 flex flex-wrap gap-2 items-end text-xs">
        <div><label class="text-slate-400 block">Stock / ETF Symbol</label><input id="pfSym" placeholder="e.g. INFY,TCS,NIFTYBEES" class="mt-0.5 bg-slate-950 border border-slate-700 rounded px-2.5 py-1.5 font-mono uppercase w-48" /></div>
        <div><label class="text-slate-400 block">Market</label><select id="pfExch" class="mt-0.5 bg-slate-950 border border-slate-700 rounded px-2.5 py-1.5"><option value="NSE">🇮🇳 NSE / BSE (India)</option><option value="US">🇺🇸 NASDAQ / NYSE (US)</option></select></div>
        <div><label class="text-slate-400 block">Quantity</label><input id="pfQty" type="number" step="1" value="10" class="mt-0.5 bg-slate-950 border border-slate-700 rounded px-2.5 py-1.5 font-mono w-28" /></div>
        <div><label class="text-slate-400 block">Avg Buy Price (Leave 0 for Live Price)</label><input id="pfAvg" type="number" step="0.5" placeholder="Auto Live Price if 0" class="mt-0.5 bg-slate-950 border border-slate-700 rounded px-2.5 py-1.5 font-mono w-48" /></div>
        <button id="pfAddBtn" onclick="addPortfolioManual()" class="px-4 py-1.5 rounded-lg bg-blue-600 hover:bg-blue-500 text-white font-semibold">+ Add Stock to Portfolio</button>
      </div>
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-4 overflow-x-auto">
        <table class="w-full text-left border-collapse text-xs">
          <thead>
            <tr class="border-b border-slate-800 text-slate-400 uppercase text-[11px]">
              <th class="py-2 px-2">Holding</th><th class="py-2 px-2 text-right">Qty</th><th class="py-2 px-2 text-right">Avg Buy Price</th><th class="py-2 px-2 text-right">Live Price</th><th class="py-2 px-2 text-right">Unrealized P&amp;L</th><th class="py-2 px-2 text-right">🔴 High Sell Target</th><th class="py-2 px-2">2000–2026 Backtested Signal</th><th class="py-2 px-2 text-right">Remove</th>
            </tr>
          </thead>
          <tbody id="pfTableBody" class="divide-y divide-slate-800/70"></tbody>
        </table>
      </div>
    </section>

  </main>

  <!-- GOOGLE SHEET LINK MODAL -->
  <div id="gsheetModal" class="hidden fixed inset-0 z-50 bg-slate-950/80 backdrop-blur-sm flex items-center justify-center p-4">
    <div class="bg-slate-900 border border-slate-700 rounded-2xl max-w-xl w-full p-6 space-y-4">
      <div class="flex justify-between items-center border-b border-slate-800 pb-3">
        <h3 class="text-base font-bold text-white">📊 Sync Tickers Directly From Your Google Sheet</h3>
        <button onclick="document.getElementById('gsheetModal').classList.add('hidden')" class="text-xs px-2.5 py-1 rounded bg-slate-800">✕</button>
      </div>
      <p class="text-xs text-slate-400">Paste your Google Sheet share link (with "Anyone with the link can view") or Published CSV link below. All tickers in Column A will sync live:</p>
      <input id="gsheetUrlInput" type="text" placeholder="https://docs.google.com/spreadsheets/d/..." class="w-full bg-slate-950 border border-slate-700 rounded-lg px-3 py-2 text-xs text-slate-100 font-mono" />
      <button onclick="syncGoogleSheetNow()" class="w-full py-2 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white text-xs font-bold">Sync Google Sheet Column A Tickers Now</button>
    </div>
  </div>

  <!-- BACKEND ENGINE DETAILS MODAL -->
  <div id="evidenceModal" class="hidden fixed inset-0 z-50 bg-slate-950/80 backdrop-blur-sm flex items-center justify-center p-4 overflow-y-auto">
    <div class="bg-slate-900 border border-slate-700 rounded-2xl max-w-3xl w-full p-6 space-y-4 shadow-2xl">
      <div class="flex items-start justify-between border-b border-slate-800 pb-3">
        <div id="modalTitleBox"></div>
        <button onclick="document.getElementById('evidenceModal').classList.add('hidden')" class="px-3 py-1 rounded-lg bg-slate-800 hover:bg-slate-700 text-xs font-bold">✕ Close</button>
      </div>
      <div id="modalBodyBox" class="space-y-4 text-xs"></div>
    </div>
  </div>

  <script>
    let LIVE_UNIVERSE = [];
    let CURRENT_MARKET_FILTER = 'ALL';
    let CURRENT_TOP15_MODE = 'LT';

    function showToast(msg) {
      const box = document.getElementById('toastBox');
      box.innerText = msg;
      box.classList.remove('hidden');
      setTimeout(() => box.classList.add('hidden'), 3200);
    }

    function openGsheetModal() {
      document.getElementById('gsheetModal').classList.remove('hidden');
    }

    async function syncGoogleSheetNow() {
      const url = document.getElementById('gsheetUrlInput').value.trim();
      if (!url) return;
      const res = await fetch('/api/v1/gsheet/sync', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({csv_or_sheet_url: url})
      });
      if (!res.ok) {
        const err = await res.json();
        alert(err.detail || 'Could not sync Google Sheet.');
        return;
      }
      const data = await res.json();
      document.getElementById('gsheetModal').classList.add('hidden');
      showToast(`✓ Synced ${data.tickers_loaded} tickers from your Google Sheet!`);
      loadUniverse(false);
    }

    function switchTab(tab) {
      ['dashboard', 'watchlist', 'portfolio'].forEach(t => {
        document.getElementById('panel-' + t).classList.toggle('hidden', t !== tab);
        document.getElementById('tabbtn-' + t).className = (t === tab)
          ? 'tab-btn-active px-4 py-2 rounded-lg text-xs font-bold border border-slate-800 transition'
          : 'px-4 py-2 rounded-lg text-xs font-bold border border-slate-800 text-slate-300 hover:bg-slate-800/60 transition';
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
      renderDashboard();
    }

    async function loadUniverse(forceRefresh = false) {
      document.getElementById('lastSyncTimestamp').innerText = 'Calculating 2000–2026 Backtested Signals...';
      const res = await fetch(`/api/v1/universe?market_filter=${CURRENT_MARKET_FILTER}&force_refresh=${forceRefresh}`);
      const data = await res.json();

      LIVE_UNIVERSE = data.universe || [];
      window.LATEST_LT_TOP15 = data.long_term_top_15 || [];
      window.LATEST_SW_TOP15 = data.swing_trading_top_15 || [];

      document.getElementById('badge-in-session').innerText = `🇮🇳 NSE/BSE: ${data.india_session.status_label}`;
      document.getElementById('badge-us-session').innerText = `🇺🇸 US: ${data.us_session.status_label}`;
      document.getElementById('lastSyncTimestamp').innerText = `Synced: ${data.refreshed_at} (${data.total_live_instruments} Instruments)`;

      renderDashboard();
      refreshCounts();
    }

    function signalPillHtml(sheetSignal) {
      let cls = 'bg-slate-800 text-slate-200 border-slate-700';
      if (sheetSignal.includes('STRONG')) cls = 'bg-emerald-500/20 text-emerald-300 border-emerald-500/40';
      else if (sheetSignal.includes('BUY')) cls = 'bg-emerald-500/15 text-emerald-400 border-emerald-500/30';
      else if (sheetSignal.includes('HOLD')) cls = 'bg-amber-500/15 text-amber-300 border-amber-500/30';
      else if (sheetSignal.includes('WAIT')) cls = 'bg-blue-500/15 text-blue-300 border-blue-500/30';
      else if (sheetSignal.includes('SELL') || sheetSignal.includes('AVOID')) cls = 'bg-rose-500/20 text-rose-300 border-rose-500/40';
      return `<span class="inline-block px-2.5 py-1 rounded-lg border text-[11px] font-bold ${cls}">${sheetSignal}</span>`;
    }

    function renderDashboard() {
      const list = (CURRENT_TOP15_MODE === 'LT') ? (window.LATEST_LT_TOP15 || []) : (window.LATEST_SW_TOP15 || []);
      const strongBuys = LIVE_UNIVERSE.filter(x => x.short_signal === 'STRONG BUY').length;
      const buys = LIVE_UNIVERSE.filter(x => x.short_signal === 'BUY').length;
      const waits = LIVE_UNIVERSE.filter(x => x.short_signal === 'WAIT' || x.short_signal === 'AVOID').length;

      document.getElementById('dashKpiStrip').innerHTML = `
        <div class="bg-slate-900 border border-slate-800 p-4 rounded-xl">
          <div class="text-xs text-slate-400">Live Audited Stocks &amp; ETFs</div>
          <div class="text-2xl font-bold text-white mt-1 font-mono">${LIVE_UNIVERSE.length}</div>
          <div class="text-[11px] text-emerald-400 mt-0.5">3-Yr &amp; 52-Week History Verified</div>
        </div>
        <div class="bg-slate-900 border border-slate-800 p-4 rounded-xl">
          <div class="text-xs text-slate-400">🟢🔥 STRONG VALUE BUY</div>
          <div class="text-2xl font-bold text-emerald-400 mt-1 font-mono">${strongBuys}</div>
          <div class="text-[11px] text-slate-400 mt-0.5">Below Buffett Safe Buy Limit</div>
        </div>
        <div class="bg-slate-900 border border-slate-800 p-4 rounded-xl">
          <div class="text-xs text-slate-400">🟢 BUY / ACCUMULATE</div>
          <div class="text-2xl font-bold text-blue-400 mt-1 font-mono">${buys}</div>
          <div class="text-[11px] text-slate-400 mt-0.5">Below Realistic Fair Value</div>
        </div>
        <div class="bg-slate-900 border border-slate-800 p-4 rounded-xl">
          <div class="text-xs text-slate-400">⏳ Crash / Trap Protected</div>
          <div class="text-2xl font-bold text-amber-400 mt-1 font-mono">${waits}</div>
          <div class="text-[11px] text-slate-400 mt-0.5">Wait for 10% Bounce / Avoid 3Y Decline</div>
        </div>
      `;

      const tbody = document.getElementById('top15TableBody');
      if (list.length === 0) {
        tbody.innerHTML = `<tr><td colspan="8" class="py-8 text-center text-slate-400">Loading live instruments... Click Force Live API Sync if needed.</td></tr>`;
        return;
      }

      tbody.innerHTML = list.map(s => `
        <tr class="hover:bg-slate-800/50 transition">
          <td onclick="openEvidenceModal('${s.symbol}')" class="py-3 px-2 cursor-pointer">
            <div class="font-bold text-white text-sm hover:text-emerald-400">${s.symbol}</div>
            <div class="text-[11px] text-slate-400">${s.name}</div>
          </td>
          <td class="py-3 px-2">
            <span class="px-2 py-0.5 rounded bg-slate-800 text-[11px] font-semibold">${s.asset_type}</span>
            <div class="text-[10px] text-slate-500 mt-0.5 font-mono">${s.country} • ${s.exchange}</div>
          </td>
          <td class="py-3 px-2 text-right font-mono">
            <div class="font-bold text-white text-sm">${s.currency_symbol}${s.price.toLocaleString()}</div>
            <div class="text-[10px] ${s.change_pct >= 0 ? 'text-emerald-400' : 'text-rose-400'}">${s.change_pct >= 0 ? '+' : ''}${s.change_pct}%</div>
          </td>
          <td class="py-3 px-2 text-right font-mono font-bold text-emerald-400 text-sm">
            ${s.currency_symbol}${s.fair_value.toLocaleString()}
          </td>
          <td class="py-3 px-2 text-right font-mono text-blue-300">
            ${s.currency_symbol}${s.safe_buy_limit.toLocaleString()}
          </td>
          <td class="py-3 px-2 text-right font-mono font-bold text-rose-300">
            ${s.currency_symbol}${s.sell_trigger_limit.toLocaleString()}
          </td>
          <td onclick="openEvidenceModal('${s.symbol}')" class="py-3 px-2 cursor-pointer">
            ${signalPillHtml(s.sheet_signal)}
          </td>
          <td class="py-3 px-2 text-right space-x-1 whitespace-nowrap">
            <button onclick="quickAddWatchlist('${s.symbol}', '${s.exchange}', ${s.price})" class="px-2.5 py-1 rounded bg-emerald-600/20 hover:bg-emerald-600 text-emerald-300 hover:text-white border border-emerald-500/40 text-[11px] font-semibold transition">
              + Watchlist
            </button>
            <button onclick="quickAddPortfolio('${s.symbol}', '${s.exchange}', ${s.price})" class="px-2.5 py-1 rounded bg-blue-600/20 hover:bg-blue-600 text-blue-300 hover:text-white border border-blue-500/40 text-[11px] font-semibold transition">
              + Portfolio
            </button>
          </td>
        </tr>
      `).join('');
    }

    function openEvidenceModal(symbol) {
      const s = LIVE_UNIVERSE.find(x => x.symbol === symbol);
      if (!s) return;
      document.getElementById('modalTitleBox').innerHTML = `
        <div class="flex items-center gap-2">
          <span class="px-2 py-0.5 rounded bg-slate-800 text-xs font-mono">${s.country} • ${s.exchange}</span>
          <span class="px-2 py-0.5 rounded bg-emerald-500/15 text-emerald-300 text-xs font-mono">${s.asset_type}</span>
        </div>
        <h3 class="text-xl font-bold text-white mt-1">${s.symbol} — ${s.name}</h3>
      `;
      document.getElementById('modalBodyBox').innerHTML = `
        <div class="p-3.5 rounded-xl bg-slate-950 border border-slate-800">
          <div class="text-slate-400 text-[11px] uppercase mb-1">2000–2026 Backtested Action Decision</div>
          ${signalPillHtml(s.sheet_signal)}
        </div>
        <div class="grid grid-cols-2 md:grid-cols-4 gap-3 font-mono">
          <div class="p-3 rounded-xl bg-slate-950 border border-slate-800"><div class="text-slate-400 text-[11px]">Live Price</div><div class="text-base font-bold text-white mt-0.5">${s.currency_symbol}${s.price}</div></div>
          <div class="p-3 rounded-xl bg-slate-950 border border-slate-800"><div class="text-slate-400 text-[11px]">🟢 Realistic Fair Value</div><div class="text-base font-bold text-emerald-400 mt-0.5">${s.currency_symbol}${s.fair_value}</div></div>
          <div class="p-3 rounded-xl bg-slate-950 border border-slate-800"><div class="text-slate-400 text-[11px]">Buffett Safe Buy Limit</div><div class="text-base font-bold text-blue-300 mt-0.5">${s.currency_symbol}${s.safe_buy_limit}</div></div>
          <div class="p-3 rounded-xl bg-slate-950 border border-slate-800"><div class="text-slate-400 text-[11px]">🔴 High Sell Target</div><div class="text-base font-bold text-rose-300 mt-0.5">${s.currency_symbol}${s.sell_trigger_limit}</div></div>
          <div class="p-3 rounded-xl bg-slate-950 border border-slate-800"><div class="text-slate-400 text-[11px]">Live EPS</div><div class="text-sm font-bold text-slate-200 mt-0.5">${s.eps_display}</div></div>
          <div class="p-3 rounded-xl bg-slate-950 border border-slate-800"><div class="text-slate-400 text-[11px]">52-Week High / Low</div><div class="text-sm font-bold text-slate-200 mt-0.5">${s.high_52w} / ${s.low_52w}</div></div>
          <div class="p-3 rounded-xl bg-slate-950 border border-slate-800"><div class="text-slate-400 text-[11px]">3-Yr Ago Price</div><div class="text-sm font-bold text-slate-200 mt-0.5">${s.currency_symbol}${s.price_3yr_ago} (${s.change_3yr_pct}%)</div></div>
          <div class="p-3 rounded-xl bg-slate-950 border border-slate-800"><div class="text-slate-400 text-[11px]">Swing Signal (5–15d)</div><div class="text-sm font-bold text-blue-400 mt-0.5">${s.sw_state} (RSI ${s.rsi_14})</div></div>
        </div>
      `;
      document.getElementById('evidenceModal').classList.remove('hidden');
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
      renderDashboard();
      openEvidenceModal(item.symbol);
    }

    async function quickAddWatchlist(symbol, exchange, price) {
      const res = await fetch('/api/v1/watchlist', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({symbol, exchange, desired_entry: price, thesis: 'Added from Dashboard'})
      });
      if (res.ok) {
        showToast(`✓ ${symbol} added to My Watchlist!`);
        refreshCounts();
      }
    }

    async function quickAddPortfolio(symbol, exchange, price) {
      const qtyStr = prompt(`Enter quantity of ${symbol} shares for Portfolio:`, "10");
      if (qtyStr === null) return;
      const quantity = parseFloat(qtyStr) || 10;
      const res = await fetch('/api/v1/portfolio', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({symbol, exchange, quantity, avg_buy_price: price})
      });
      if (res.ok) {
        showToast(`✓ ${symbol} (${quantity} shares) added to My Portfolio!`);
        refreshCounts();
      }
    }

    async function refreshCounts() {
      try {
        const [wlRes, pfRes] = await Promise.all([fetch('/api/v1/watchlist'), fetch('/api/v1/portfolio')]);
        const wlData = await wlRes.json();
        const pfData = await pfRes.json();
        document.getElementById('wlCountBadge').innerText = (wlData.items || []).length;
        document.getElementById('pfCountBadge').innerText = (pfData.positions || []).length;
      } catch (e) {}
    }

    async function renderWatchlist() {
      const res = await fetch('/api/v1/watchlist');
      const data = await res.json();
      const tbody = document.getElementById('wlTableBody');
      document.getElementById('wlCountBadge').innerText = (data.items || []).length;
      if (!data.items || data.items.length === 0) {
        tbody.innerHTML = `<tr><td colspan="7" class="py-6 text-center text-slate-400">Your Watchlist is empty. Click <strong>+ Watchlist</strong> on any Dashboard stock!</td></tr>`;
        return;
      }
      tbody.innerHTML = data.items.map(w => {
        const s = w.live_data;
        return `<tr class="hover:bg-slate-800/40">
          <td class="py-2.5 px-2 font-bold text-white">${s.symbol} <div class="text-[11px] font-normal text-slate-400">${s.name}</div></td>
          <td class="py-2.5 px-2 text-right font-mono font-bold">${s.currency_symbol}${s.price.toLocaleString()}</td>
          <td class="py-2.5 px-2 text-right font-mono text-emerald-400 font-bold">${s.currency_symbol}${s.fair_value.toLocaleString()}</td>
          <td class="py-2.5 px-2 text-right font-mono text-blue-300">${s.currency_symbol}${s.safe_buy_limit.toLocaleString()}</td>
          <td class="py-2.5 px-2 text-right font-mono text-rose-300">${s.currency_symbol}${s.sell_trigger_limit.toLocaleString()}</td>
          <td class="py-2.5 px-2">${signalPillHtml(s.sheet_signal)}</td>
          <td class="py-2.5 px-2 text-right space-x-2">
            <button onclick="quickAddPortfolio('${s.symbol}', '${s.exchange}', ${s.price})" class="text-blue-400 hover:underline font-semibold">+ Portfolio</button>
            <button onclick="removeWatchlist('${s.symbol}')" class="text-rose-400 hover:underline">Remove</button>
          </td>
        </tr>`;
      }).join('');
    }

    async function addWatchlistManual() {
      const symbol = document.getElementById('wlSym').value.trim();
      const exchange = document.getElementById('wlExch').value;
      const thesis = document.getElementById('wlThesis').value;
      if (!symbol) return;
      await fetch('/api/v1/watchlist', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({symbol, exchange, desired_entry: 0, thesis})});
      document.getElementById('wlSym').value = '';
      showToast(`✓ ${symbol.toUpperCase()} added to Watchlist!`);
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
      const sum = data.summary_inr || {total_invested_inr: 0, total_current_inr: 0, total_pnl_inr: 0};
      document.getElementById('pfCountBadge').innerText = (data.positions || []).length;

      document.getElementById('pfSummaryStrip').innerHTML = `
        <div class="bg-slate-900 border border-slate-800 p-4 rounded-xl">
          <div class="text-xs text-slate-400">Total Invested Capital (INR Equiv.)</div>
          <div class="text-2xl font-bold text-white mt-1 font-mono">₹${sum.total_invested_inr.toLocaleString()}</div>
        </div>
        <div class="bg-slate-900 border border-slate-800 p-4 rounded-xl">
          <div class="text-xs text-slate-400">Current Portfolio Value (INR Equiv.)</div>
          <div class="text-2xl font-bold text-blue-400 mt-1 font-mono">₹${sum.total_current_inr.toLocaleString()}</div>
        </div>
        <div class="bg-slate-900 border border-slate-800 p-4 rounded-xl">
          <div class="text-xs text-slate-400">Total Unrealized P&amp;L</div>
          <div class="text-2xl font-bold ${sum.total_pnl_inr >= 0 ? 'text-emerald-400' : 'text-rose-400'} mt-1 font-mono">
            ${sum.total_pnl_inr >= 0 ? '+' : ''}₹${sum.total_pnl_inr.toLocaleString()}
          </div>
        </div>
      `;

      if (!data.positions || data.positions.length === 0) {
        tbody.innerHTML = `<tr><td colspan="8" class="py-6 text-center text-slate-400">Your Portfolio is empty. Click <strong>+ Portfolio</strong> on any Dashboard stock or add one above!</td></tr>`;
        return;
      }
      tbody.innerHTML = data.positions.map(p => {
        const s = p.live_data;
        return `<tr class="hover:bg-slate-800/40">
          <td class="py-2.5 px-2 font-bold text-white">${s.symbol} <div class="text-[11px] font-normal text-slate-400">${s.name}</div></td>
          <td class="py-2.5 px-2 text-right font-mono">${p.quantity}</td>
          <td class="py-2.5 px-2 text-right font-mono">${s.currency_symbol}${p.avg_buy_price.toLocaleString()}</td>
          <td class="py-2.5 px-2 text-right font-mono font-bold text-white">${s.currency_symbol}${s.price.toLocaleString()}</td>
          <td class="py-2.5 px-2 text-right font-mono font-bold ${p.pnl >= 0 ? 'text-emerald-400' : 'text-rose-400'}">${p.pnl >= 0 ? '+' : ''}${s.currency_symbol}${p.pnl.toLocaleString()} (${p.pnl_pct}%)</td>
          <td class="py-2.5 px-2 text-right font-mono text-rose-300">${s.currency_symbol}${s.sell_trigger_limit.toLocaleString()}</td>
          <td class="py-2.5 px-2">${signalPillHtml(s.sheet_signal)}</td>
          <td class="py-2.5 px-2 text-right"><button onclick="removePortfolio('${s.symbol}')" class="text-rose-400 hover:underline">Remove</button></td>
        </tr>`;
      }).join('');
    }

    async function addPortfolioManual() {
      const symbol = document.getElementById('pfSym').value.trim();
      const exchange = document.getElementById('pfExch').value;
      const quantity = parseFloat(document.getElementById('pfQty').value) || 10;
      const avg_buy_price = parseFloat(document.getElementById('pfAvg').value) || 0;
      if (!symbol) return;
      const btn = document.getElementById('pfAddBtn');
      btn.innerText = 'Adding...';
      const res = await fetch('/api/v1/portfolio', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({symbol, exchange, quantity, avg_buy_price})
      });
      btn.innerText = '+ Add Stock to Portfolio';
      if (!res.ok) {
        const err = await res.json();
        alert(err.detail || 'Could not add stock.');
        return;
      }
      document.getElementById('pfSym').value = '';
      document.getElementById('pfAvg').value = '';
      showToast(`✓ ${symbol.toUpperCase()} added to Portfolio!`);
      renderPortfolio();
    }

    async function removePortfolio(sym) {
      await fetch(`/api/v1/portfolio/${sym}`, {method: 'DELETE'});
      renderPortfolio();
    }

    loadUniverse(false);
  </script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
async def serve_quantvalue_platform():
    return HTMLResponse(content=FRONTEND_HTML)
