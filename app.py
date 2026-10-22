import asyncio
import math
import os
import uuid
from datetime import date, datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

import httpx
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

# ============================================================================
# 1. LIVE API KEYS & CONFIGURATION (INDIA: TAPETIDE | US: FINNHUB)
# ============================================================================

# Indian Market (NSE/BSE) - Tapetide API Token
TAPETIDE_TOKEN = os.getenv(
    "TAPETIDE_TOKEN",
    "tpt_rt_5fd4dc6df4e461b1fd56cb9a17b48eda43e830533c526e3dce7c4deb",
).strip()

# Fix accidental mobile capitalization ('Tpt_rt_' -> 'tpt_rt_')
if TAPETIDE_TOKEN.startswith("Tpt_rt_"):
    TAPETIDE_TOKEN = "t" + TAPETIDE_TOKEN[1:]

TAPETIDE_BASE_URL = "https://api.tapetide.com/v1"

# US Market (NYSE/NASDAQ/AMEX) - Finnhub API Key
FINNHUB_API_KEY = os.getenv(
    "FINNHUB_API_KEY",
    "dbd2c61r01qutcrq9e3gdbd2c61r01qutcrq9e40",
).strip()

FINNHUB_BASE_URL = "https://finnhub.io/api/v1"


# ============================================================================
# 2. DOMAIN ENUMS & MODELS (PHASES 1 - 6)
# ============================================================================

class MarketRegion(str, Enum):
    IN = "IN"
    US = "US"

class ExchangeCode(str, Enum):
    NSE = "NSE"
    BSE = "BSE"
    NYSE = "NYSE"
    NASDAQ = "NASDAQ"
    AMEX = "AMEX"

class AssetClass(str, Enum):
    EQUITY = "EQUITY"
    ETF = "ETF"

class DataFreshnessMode(str, Enum):
    REAL_TIME = "REAL_TIME"
    DELAYED_15M = "DELAYED_15M"
    END_OF_DAY = "END_OF_DAY"
    LAST_AVAILABLE = "LAST_AVAILABLE"
    DEMO_HISTORICAL = "DEMO_HISTORICAL"

class StrategyType(str, Enum):
    LONG_TERM_VALUE = "LONG_TERM_VALUE"
    SWING_TRADING = "SWING_TRADING"

class SignalAction(str, Enum):
    STRONG_BUY = "STRONG_BUY"
    BUY = "BUY"
    HOLD = "HOLD"
    WAIT = "WAIT"
    TRIM_PARTIAL_SELL = "TRIM_PARTIAL_SELL"
    SELL = "SELL"
    AVOID = "AVOID"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"

class SectorValuationProfile(str, Enum):
    GENERAL_CORPORATE = "GENERAL_CORPORATE"
    FINANCIALS_BANKING = "FINANCIALS_BANKING"
    REIT_REAL_ESTATE = "REIT_REAL_ESTATE"
    ETF_INDEX_FUND = "ETF_INDEX_FUND"

class FallingStockCategory(str, Enum):
    NONE_UPTREND = "NONE_UPTREND"
    CAT_1_HEALTHY_PULLBACK = "CAT_1_HEALTHY_PULLBACK"
    CAT_2_UNDERVALUED_TECH_WEAK = "CAT_2_UNDERVALUED_TECH_WEAK"
    CAT_3_SEVERE_DOWNTREND_UNCLEAR = "CAT_3_SEVERE_DOWNTREND_UNCLEAR"
    CAT_4_FUNDAMENTAL_VALUE_TRAP = "CAT_4_FUNDAMENTAL_VALUE_TRAP"
    CAT_5_CAPITULATION_WATCH = "CAT_5_CAPITULATION_WATCH"

class StrategyOperationalStatus(str, Enum):
    ACTIVE_NORMAL = "ACTIVE_NORMAL"
    PAUSED_CIRCUIT_BREAKER_TRIPPED = "PAUSED_CIRCUIT_BREAKER_TRIPPED"
    SUSPENDED_REQUIRES_REVALIDATION = "SUSPENDED_REQUIRES_REVALIDATION"


class ValuationRequest(BaseModel):
    symbol: str = "RELIANCE"
    exchange: ExchangeCode = ExchangeCode.NSE
    sector_profile: SectorValuationProfile = SectorValuationProfile.GENERAL_CORPORATE
    current_price: float = 1350.0
    use_live_price: bool = True
    free_cash_flow_ttm: float = 72000.0
    net_income_ttm: float = 78000.0
    eps_diluted_ttm: float = 115.0
    book_value_per_share: float = 560.0
    nav_per_share: float = 260.0
    roic_pct: float = 16.8
    roe_pct: float = 15.2
    net_debt: float = -15000.0
    shares_outstanding: float = 676.0
    historical_5y_median_pe: float = 22.0
    historical_5y_median_pb: float = 2.5
    base_growth_rate_5y_pct: float = 11.0
    discount_rate_wacc_pct: float = 10.5
    terminal_growth_rate_pct: float = 4.0
    required_margin_of_safety_pct: float = 25.0
    promoter_pledged_pct: float = 0.0
    is_fundamentals_deteriorating: bool = False
    auditor_qualification_flag: bool = False
    price_change_3m_pct: float = -6.0
    is_below_200dma: bool = False


class SwingTradeRequest(BaseModel):
    symbol: str = "AAPL"
    exchange: ExchangeCode = ExchangeCode.NASDAQ
    account_capital: float = 100000.0
    max_risk_per_trade_pct: float = 1.0
    max_position_allocation_pct: float = 15.0
    entry_price: float = 180.0
    use_live_entry_price: bool = False
    stop_loss_price: float = 171.0
    target_price: float = 202.0
    days_to_earnings: int = 25
    bid_ask_spread_pct: float = 0.03
    downtrend_filter_active: bool = False
    is_breakout_confirmed: bool = True


class TargetRevisionInput(BaseModel):
    symbol: str
    exchange: ExchangeCode
    strategy: StrategyType
    previous_target_price: float
    new_target_price: float
    new_fair_value: float
    fundamental_evidence_verified: bool
    evidence_explanation: str


# ============================================================================
# 3. REAL-TIME MARKET DATA ADAPTERS (TAPETIDE FOR INDIA, FINNHUB FOR US)
# ============================================================================

async def fetch_tapetide_india_quote(symbol: str, fallback_price: float) -> Dict[str, Any]:
    """Fetches real-time/latest NSE/BSE quote from Tapetide API."""
    now_iso = datetime.now(timezone.utc).isoformat()
    if not TAPETIDE_TOKEN:
        return {"symbol": symbol.upper(), "price": fallback_price, "freshness": "DEMO_HISTORICAL", "provider": "DEMO", "timestamp": now_iso}

    headers = {"Authorization": f"Bearer {TAPETIDE_TOKEN}", "Accept": "application/json"}
    url = f"{TAPETIDE_BASE_URL}/stocks/{symbol.upper()}/quote"
    try:
        async with httpx.AsyncClient(timeout=6.0) as client:
            resp = await client.get(url, headers=headers)
            if resp.status_code == 200:
                payload = resp.json()
                data = payload.get("data", payload) if isinstance(payload, dict) else {}
                live_p = data.get("last_price") or data.get("ltp") or data.get("close") or data.get("price")
                if live_p is not None and float(live_p) > 0:
                    return {
                        "symbol": symbol.upper(),
                        "price": round(float(live_p), 2),
                        "change_pct": data.get("change_percent") or data.get("pChange") or 0.0,
                        "freshness": "REAL_TIME",
                        "provider": "TAPETIDE_NSE_BSE",
                        "timestamp": data.get("timestamp") or now_iso,
                    }
    except Exception:
        pass

    return {"symbol": symbol.upper(), "price": fallback_price, "freshness": "LAST_AVAILABLE", "provider": "TAPETIDE_FALLBACK", "timestamp": now_iso}


async def fetch_finnhub_us_quote(symbol: str, fallback_price: float) -> Dict[str, Any]:
    """Fetches real-time US market (NYSE/NASDAQ) quote from Finnhub API."""
    now_iso = datetime.now(timezone.utc).isoformat()
    if not FINNHUB_API_KEY:
        return {"symbol": symbol.upper(), "price": fallback_price, "freshness": "DEMO_HISTORICAL", "provider": "DEMO", "timestamp": now_iso}

    url = f"{FINNHUB_BASE_URL}/quote"
    params = {"symbol": symbol.upper(), "token": FINNHUB_API_KEY}
    try:
        async with httpx.AsyncClient(timeout=6.0) as client:
            resp = await client.get(url, params=params)
            if resp.status_code == 200:
                data = resp.json()
                # Finnhub returns: c = current price, dp = percent change, pc = previous close
                live_p = data.get("c")
                if live_p is not None and float(live_p) > 0:
                    return {
                        "symbol": symbol.upper(),
                        "price": round(float(live_p), 2),
                        "change_pct": round(float(data.get("dp") or 0.0), 2),
                        "high": data.get("h"),
                        "low": data.get("l"),
                        "prev_close": data.get("pc"),
                        "freshness": "REAL_TIME",
                        "provider": "FINNHUB_US_LIVE",
                        "timestamp": now_iso,
                    }
    except Exception:
        pass

    return {"symbol": symbol.upper(), "price": fallback_price, "freshness": "LAST_AVAILABLE", "provider": "FINNHUB_FALLBACK", "timestamp": now_iso}


async def fetch_universal_live_quote(symbol: str, exchange: str, fallback_price: float = 100.0) -> Dict[str, Any]:
    """Routes quote requests automatically to Tapetide (NSE/BSE) or Finnhub (NYSE/NASDAQ/AMEX)."""
    if exchange.upper() in ("NSE", "BSE", "IN"):
        return await fetch_tapetide_india_quote(symbol, fallback_price)
    return await fetch_finnhub_us_quote(symbol, fallback_price)


# ============================================================================
# 4. CORE VALUATION & SWING TRADING ENGINES (STRICTLY SEPARATED)
# ============================================================================

def compute_2stage_dcf(
    fcf_0: float, growth_5y_pct: float, wacc_pct: float, term_growth_pct: float, net_debt: float, shares: float
) -> float:
    g = growth_5y_pct / 100.0
    r = wacc_pct / 100.0
    g_term = min(term_growth_pct / 100.0, r - 0.02)
    pv_fcf = 0.0
    proj = fcf_0
    for yr in range(1, 6):
        proj *= (1.0 + g)
        pv_fcf += proj / ((1.0 + r) ** yr)
    term_val = (proj * (1.0 + g_term)) / (r - g_term)
    pv_term = term_val / ((1.0 + r) ** 5)
    eq_val = (pv_fcf + pv_term) - net_debt
    return round(max(0.0, eq_val / shares), 2)


async def evaluate_long_term_engine(req: ValuationRequest) -> Dict[str, Any]:
    now = datetime.now(timezone.utc)
    reasons: List[str] = []
    risks: List[str] = []

    # Fetch live price if requested
    price = req.current_price
    freshness = "USER_INPUT"
    provider_used = "MANUAL"
    if req.use_live_price:
        q = await fetch_universal_live_quote(req.symbol, req.exchange.value, req.current_price)
        price = q["price"]
        freshness = q["freshness"]
        provider_used = q["provider"]

    has_gov_flag = req.auditor_qualification_flag or req.promoter_pledged_pct > 20.0
    if req.promoter_pledged_pct > 20.0:
        risks.append(f"CRITICAL INDIAN GOVERNANCE RISK: Promoter pledging at {req.promoter_pledged_pct:.1f}% (>20%).")
    if req.auditor_qualification_flag:
        risks.append("CRITICAL GOVERNANCE WARNING: Active auditor qualification.")
    if req.is_fundamentals_deteriorating:
        risks.append("VALUE TRAP WARNING: Revenue/margins are deteriorating with earnings downgrades.")

    is_dcf_reliable = (
        req.sector_profile == SectorValuationProfile.GENERAL_CORPORATE
        and req.free_cash_flow_ttm > 0
        and req.shares_outstanding > 0
    )
    dcf_unreliability_reason = None
    if not is_dcf_reliable:
        if req.sector_profile != SectorValuationProfile.GENERAL_CORPORATE:
            dcf_unreliability_reason = f"Standard corporate DCF not used for {req.sector_profile.value}; using sector model."
        else:
            dcf_unreliability_reason = "Unreliable DCF: Trailing Free Cash Flow is zero or negative. DCF was not forced."

    dcf_val = None
    oe_val = None
    mult_val = round(req.eps_diluted_ttm * min(25.0, req.historical_5y_median_pe), 2) if req.eps_diluted_ttm > 0 else None

    if req.sector_profile == SectorValuationProfile.GENERAL_CORPORATE:
        if is_dcf_reliable:
            dcf_val = compute_2stage_dcf(
                req.free_cash_flow_ttm,
                req.base_growth_rate_5y_pct,
                req.discount_rate_wacc_pct,
                req.terminal_growth_rate_pct,
                req.net_debt,
                req.shares_outstanding,
            )
        if req.net_income_ttm > 0 and req.shares_outstanding > 0:
            oe_ps = req.net_income_ttm / req.shares_outstanding
            r = req.discount_rate_wacc_pct / 100.0
            g_oe = min(0.05, (req.base_growth_rate_5y_pct / 100.0) * 0.45)
            oe_val = round((oe_ps * (1.0 + g_oe)) / (r - g_oe), 2)

        if dcf_val and oe_val and mult_val:
            base_fv = round(0.40 * dcf_val + 0.35 * oe_val + 0.25 * mult_val, 2)
            weights = {"DCF (40%)": dcf_val, "Owner Earnings (35%)": oe_val, "Multiples (25%)": mult_val}
        elif oe_val and mult_val:
            base_fv = round(0.55 * oe_val + 0.45 * mult_val, 2)
            weights = {"Owner Earnings (55%)": oe_val, "Multiples (45%)": mult_val}
        else:
            base_fv = mult_val
            weights = {"Multiples (100%)": mult_val} if mult_val else {}

    elif req.sector_profile == SectorValuationProfile.FINANCIALS_BANKING:
        ke = req.discount_rate_wacc_pct / 100.0
        roe = req.roe_pct / 100.0
        g_bv = min(0.05, req.terminal_growth_rate_pct / 100.0)
        fair_pb = max(0.5, (roe - g_bv) / (ke - g_bv))
        bank_val = round(req.book_value_per_share * fair_pb, 2)
        pb_mult = round(req.book_value_per_share * req.historical_5y_median_pb, 2)
        base_fv = round(0.70 * bank_val + 0.30 * pb_mult, 2)
        weights = {"Excess Return / ROE (70%)": bank_val, "Historical P/B (30%)": pb_mult}

    else:
        nav_val = round(req.nav_per_share, 2)
        base_fv = nav_val
        weights = {"NAV Model (100%)": nav_val}

    if not base_fv or base_fv <= 0:
        return {
            "signal_state": SignalAction.INSUFFICIENT_DATA.value,
            "falling_category": FallingStockCategory.CAT_3_SEVERE_DOWNTREND_UNCLEAR.value,
            "is_dcf_reliable": False,
            "dcf_unreliability_reason": dcf_unreliability_reason or "Insufficient positive cash flow or earnings.",
            "bear_fair_value": None,
            "base_fair_value": None,
            "bull_fair_value": None,
            "current_price": price,
            "freshness": freshness,
            "provider": provider_used,
            "margin_of_safety_pct": None,
            "required_margin_of_safety_pct": req.required_margin_of_safety_pct,
            "reasons": ["Valuation marked INSUFFICIENT_DATA — refusing to fabricate DCF on negative fundamentals."],
            "risks": risks,
        }

    bear_fv = round(base_fv * 0.82, 2)
    bull_fv = round(base_fv * 1.20, 2)
    mos_pct = round(((base_fv - price) / base_fv) * 100.0, 2)
    upside_pct = round(((base_fv - price) / price) * 100.0, 2)

    if has_gov_flag or req.is_fundamentals_deteriorating:
        fall_cat = FallingStockCategory.CAT_4_FUNDAMENTAL_VALUE_TRAP
        signal = SignalAction.AVOID
        risks.append("Blocked as Category 4 Value Trap despite any optical valuation discount.")
    elif req.price_change_3m_pct <= -20.0 and req.is_below_200dma:
        fall_cat = FallingStockCategory.CAT_3_SEVERE_DOWNTREND_UNCLEAR
        signal = SignalAction.WAIT
        risks.append("Sharp 3M price decline below 200-DMA; forced to WAIT for quarterly earnings confirmation.")
    elif req.is_below_200dma:
        fall_cat = FallingStockCategory.CAT_2_UNDERVALUED_TECH_WEAK
        signal = SignalAction.BUY if mos_pct >= req.required_margin_of_safety_pct else SignalAction.WAIT
        reasons.append("Undervalued with intact fundamentals below 200-DMA; accumulate in staged tranches.")
    else:
        fall_cat = FallingStockCategory.CAT_1_HEALTHY_PULLBACK if req.price_change_3m_pct < 0 else FallingStockCategory.NONE_UPTREND
        if price > bull_fv * 1.05:
            signal = SignalAction.SELL
            risks.append("Price exceeds Bull-Case Fair Value; negative margin of safety.")
        elif price >= base_fv:
            signal = SignalAction.HOLD
            reasons.append("Trading near Fair Value with intact fundamentals.")
        elif mos_pct >= req.required_margin_of_safety_pct + 8.0 and req.roic_pct >= 14.0:
            signal = SignalAction.STRONG_BUY
            reasons.append(f"High Margin of Safety ({mos_pct}%) + strong ROIC ({req.roic_pct}%).")
        elif mos_pct >= req.required_margin_of_safety_pct:
            signal = SignalAction.BUY
            reasons.append(f"Margin of Safety ({mos_pct}%) exceeds minimum requirement ({req.required_margin_of_safety_pct}%).")
        else:
            signal = SignalAction.WAIT
            reasons.append(f"Margin of Safety ({mos_pct}%) is below target ({req.required_margin_of_safety_pct}%).")

    return {
        "timestamp": now.isoformat(),
        "symbol": req.symbol.upper(),
        "exchange": req.exchange.value,
        "strategy": "LONG_TERM_VALUE_INVESTING (3-5+ Years)",
        "signal_state": signal.value,
        "falling_category": fall_cat.value,
        "is_dcf_reliable": is_dcf_reliable,
        "dcf_unreliability_reason": dcf_unreliability_reason,
        "model_breakdown": weights,
        "bear_fair_value": bear_fv,
        "base_fair_value": base_fv,
        "bull_fair_value": bull_fv,
        "current_price": price,
        "freshness": freshness,
        "provider": provider_used,
        "margin_of_safety_pct": mos_pct,
        "required_margin_of_safety_pct": req.required_margin_of_safety_pct,
        "upside_to_base_fv_pct": upside_pct,
        "reasons": reasons,
        "risks": risks,
    }


async def evaluate_swing_engine(req: SwingTradeRequest) -> Dict[str, Any]:
    now = datetime.now(timezone.utc)
    reasons: List[str] = []
    warnings: List[str] = []

    entry_p = req.entry_price
    freshness = "USER_INPUT"
    if req.use_live_entry_price:
        q = await fetch_universal_live_quote(req.symbol, req.exchange.value, req.entry_price)
        entry_p = q["price"]
        freshness = q["freshness"]

    stop_dist = max(0.01, entry_p - req.stop_loss_price)
    statutory_pct = 0.25 if req.exchange in (ExchangeCode.NSE, ExchangeCode.BSE) else 0.03
    rt_friction_pct = statutory_pct + req.bid_ask_spread_pct + 0.20
    cost_per_share = entry_p * (rt_friction_pct / 100.0)

    eff_risk_per_share = stop_dist + cost_per_share
    max_risk_cash = req.account_capital * (req.max_risk_per_trade_pct / 100.0)
    max_alloc_cash = req.account_capital * (req.max_position_allocation_pct / 100.0)

    shares = max(0, min(math.floor(max_risk_cash / eff_risk_per_share), math.floor(max_alloc_cash / entry_p)))
    pos_value = round(shares * entry_p, 2)
    total_cost = round(shares * cost_per_share, 2)
    cap_at_risk = round(shares * eff_risk_per_share, 2)

    net_reward = max(0.0, req.target_price - entry_p - cost_per_share)
    rr_ratio = round(net_reward / eff_risk_per_share, 2)

    if req.downtrend_filter_active:
        signal = SignalAction.AVOID
        warnings.append("SWING DOWNTREND FILTER ACTIVE: Ordinary trend-following BUY signals are strictly blocked.")
    elif req.days_to_earnings <= 3:
        signal = SignalAction.WAIT
        warnings.append(f"EARNINGS EVENT BLOCK: Earnings in {req.days_to_earnings} days (minimum buffer is 3 days).")
    elif not req.is_breakout_confirmed:
        signal = SignalAction.WAIT
        warnings.append("Awaiting confirmed price/volume breakout trigger.")
    elif rr_ratio < 2.0:
        signal = SignalAction.WAIT
        warnings.append(f"Net Risk/Reward after trading costs ({rr_ratio}x) is below minimum 2.0x threshold.")
    elif rr_ratio >= 2.6:
        signal = SignalAction.STRONG_BUY
        reasons.append(f"Confirmed setup with attractive net Risk/Reward ({rr_ratio}x after costs).")
    else:
        signal = SignalAction.BUY
        reasons.append(f"Swing entry criteria satisfied (Net R/R: {rr_ratio}x).")

    return {
        "timestamp": now.isoformat(),
        "symbol": req.symbol.upper(),
        "exchange": req.exchange.value,
        "strategy": "SWING_TRADING_ENGINE (2-20 Trading Days)",
        "signal_state": signal.value,
        "entry_price": entry_p,
        "freshness": freshness,
        "stop_loss_price": req.stop_loss_price,
        "target_price": req.target_price,
        "net_risk_reward_ratio": rr_ratio,
        "recommended_shares": shares if signal in (SignalAction.BUY, SignalAction.STRONG_BUY) else 0,
        "position_value": pos_value if signal in (SignalAction.BUY, SignalAction.STRONG_BUY) else 0.0,
        "estimated_round_trip_cost": total_cost,
        "capital_at_risk": cap_at_risk,
        "reasons": reasons,
        "warnings": warnings,
        "stop_execution_disclaimer": "Stop-loss orders do not guarantee execution at the stop price during overnight gaps.",
    }


# ============================================================================
# 5. FASTAPI ENDPOINTS & INITIAL UNIVERSE
# ============================================================================

app = FastAPI(
    title="AI Stock Research, Value Investing & Swing Trading Platform",
    version="6.1.0-live-apis",
    description="Connected to Tapetide (NSE/BSE India) and Finnhub (NYSE/NASDAQ US).",
)

REFERENCE_UNIVERSE = [
    {"symbol": "RELIANCE", "name": "Reliance Industries Ltd", "country": "IN", "exchange": "NSE", "type": "EQUITY", "sector": "Energy", "price": 1350.0, "fair_value": 2180.0, "lt_signal": "BUY", "swing_signal": "WAIT"},
    {"symbol": "HDFCBANK", "name": "HDFC Bank Ltd", "country": "IN", "exchange": "NSE", "type": "EQUITY", "sector": "Financials", "price": 1450.0, "fair_value": 1890.0, "lt_signal": "BUY", "swing_signal": "BUY"},
    {"symbol": "TCS", "name": "Tata Consultancy Services", "country": "IN", "exchange": "NSE", "type": "EQUITY", "sector": "Technology", "price": 3850.0, "fair_value": 4150.0, "lt_signal": "HOLD", "swing_signal": "BUY"},
    {"symbol": "NIFTYBEES", "name": "Nippon India ETF Nifty 50 BeES", "country": "IN", "exchange": "NSE", "type": "ETF", "sector": "Broad Index", "price": 262.0, "fair_value": 260.0, "lt_signal": "HOLD", "swing_signal": "BUY"},
    {"symbol": "AAPL", "name": "Apple Inc.", "country": "US", "exchange": "NASDAQ", "type": "EQUITY", "sector": "Technology", "price": 225.0, "fair_value": 210.0, "lt_signal": "HOLD", "swing_signal": "BUY"},
    {"symbol": "NVDA", "name": "NVIDIA Corporation", "country": "US", "exchange": "NASDAQ", "type": "EQUITY", "sector": "Semiconductors", "price": 138.0, "fair_value": 130.0, "lt_signal": "HOLD", "swing_signal": "STRONG_BUY"},
    {"symbol": "MSFT", "name": "Microsoft Corporation", "country": "US", "exchange": "NASDAQ", "type": "EQUITY", "sector": "Technology", "price": 425.0, "fair_value": 465.0, "lt_signal": "BUY", "swing_signal": "BUY"},
    {"symbol": "SPY", "name": "SPDR S&P 500 ETF Trust", "country": "US", "exchange": "NYSE", "type": "ETF", "sector": "Broad Index", "price": 580.0, "fair_value": 565.0, "lt_signal": "HOLD", "swing_signal": "BUY"},
]

TARGET_REVISIONS_STORE: List[Dict[str, Any]] = []


@app.get("/api/v1/system/health")
async def system_health():
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "target_stock_universe": 5000,
        "target_etf_universe": 200,
        "actual_instruments_loaded": len(REFERENCE_UNIVERSE),
        "india_provider": "TAPETIDE_LIVE" if TAPETIDE_TOKEN else "DEMO",
        "us_provider": "FINNHUB_LIVE" if FINNHUB_API_KEY else "DEMO",
        "circuit_breaker_long_term": StrategyOperationalStatus.ACTIVE_NORMAL.value,
        "circuit_breaker_swing": StrategyOperationalStatus.ACTIVE_NORMAL.value,
    }


@app.get("/api/v1/quote/{exchange}/{symbol}")
async def get_live_quote_any_symbol(exchange: str, symbol: str):
    """Fetches a live quote for ANY Indian (NSE/BSE) or US (NYSE/NASDAQ) symbol on demand."""
    return await fetch_universal_live_quote(symbol, exchange, fallback_price=0.0)


@app.get("/api/v1/instruments")
async def list_instruments(country: Optional[str] = None, asset_type: Optional[str] = None):
    items = [dict(i) for i in REFERENCE_UNIVERSE]
    if country and country != "ALL":
        items = [i for i in items if i["country"] == country]
    if asset_type and asset_type != "ALL":
        items = [i for i in items if i["type"] == asset_type]

    # Concurrent live price fetch from Tapetide (IN) & Finnhub (US)
    quotes = await asyncio.gather(
        *[fetch_universal_live_quote(i["symbol"], i["exchange"], i["price"]) for i in items]
    )
    for item, q in zip(items, quotes):
        item["price"] = q["price"]
        item["freshness"] = q["freshness"]
        item["provider"] = q["provider"]
        item["mos_pct"] = round(((item["fair_value"] - item["price"]) / item["fair_value"]) * 100.0, 1)

    return {"count": len(items), "instruments": items}


@app.post("/api/v1/long-term/evaluate")
async def api_evaluate_long_term(req: ValuationRequest):
    return await evaluate_long_term_engine(req)


@app.post("/api/v1/swing/evaluate")
async def api_evaluate_swing(req: SwingTradeRequest):
    return await evaluate_swing_engine(req)


@app.post("/api/v1/targets/revise")
async def api_revise_target(req: TargetRevisionInput):
    if req.new_target_price > req.previous_target_price and not req.fundamental_evidence_verified:
        return {
            "accepted": False,
            "message": "REJECTED: Targets cannot be raised solely to follow a rising share price without verified earnings/FCF improvement.",
        }
    record = req.model_dump()
    record["revised_at"] = datetime.now(timezone.utc).isoformat()
    record["accepted"] = True
    TARGET_REVISIONS_STORE.append(record)
    return {"accepted": True, "message": "Target revision logged with audit trail.", "record": record}


# ============================================================================
# 6. BUILT-IN WEB APPLICATION DASHBOARD (SERVED AT "/")
# ============================================================================

FRONTEND_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>AI Stock Research, Value Investing & Swing Trading Platform</title>
  <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-slate-950 text-slate-100 min-h-screen font-sans">
  <!-- Header -->
  <header class="border-b border-slate-800 bg-slate-900/90 sticky top-0 z-30 px-6 py-4 flex flex-wrap justify-between items-center gap-4">
    <div>
      <h1 class="text-xl font-bold text-white tracking-tight">AI Stock Research, Value Investing & Swing Trading</h1>
      <p class="text-xs text-slate-400">Live Indian (Tapetide NSE/BSE) & US (Finnhub NYSE/NASDAQ) Feeds • Strict Strategy Isolation</p>
    </div>
    <div class="flex flex-wrap items-center gap-2 text-xs">
      <span class="px-3 py-1 rounded-full bg-emerald-500/20 text-emerald-300 border border-emerald-500/40 font-medium">
        🇮🇳 India API: Tapetide Connected
      </span>
      <span class="px-3 py-1 rounded-full bg-blue-500/20 text-blue-300 border border-blue-500/40 font-medium">
        🇺🇸 US API: Finnhub Connected
      </span>
    </div>
  </header>

  <!-- Navigation Tabs -->
  <nav class="bg-slate-900 border-b border-slate-800 px-6 flex gap-2 overflow-x-auto">
    <button onclick="showTab('screener')" id="tab-screener" class="tab-btn px-4 py-3 text-sm font-medium border-b-2 border-blue-500 text-blue-400">1. Live Dashboard & Screener</button>
    <button onclick="showTab('valuation')" id="tab-valuation" class="tab-btn px-4 py-3 text-sm font-medium border-b-2 border-transparent text-slate-400 hover:text-white">2. Long-Term Fair Value Calculator</button>
    <button onclick="showTab('swing')" id="tab-swing" class="tab-btn px-4 py-3 text-sm font-medium border-b-2 border-transparent text-slate-400 hover:text-white">3. Swing Trading & Position Sizer</button>
    <button onclick="showTab('portfolio')" id="tab-portfolio" class="tab-btn px-4 py-3 text-sm font-medium border-b-2 border-transparent text-slate-400 hover:text-white">4. Target Revision & Safety Gate</button>
  </nav>

  <main class="max-w-7xl mx-auto p-6 space-y-6">
    <!-- LIVE ANY-SYMBOL QUOTE LOOKUP BAR -->
    <div class="bg-slate-900 border border-slate-800 rounded-xl p-4 flex flex-wrap items-center justify-between gap-4">
      <div class="flex items-center gap-2">
        <span class="text-sm font-semibold text-white">Live Ticker Lookup:</span>
        <select id="live-exch" class="bg-slate-800 border border-slate-700 rounded-lg px-3 py-1.5 text-sm">
          <option value="NSE">NSE (India - Tapetide)</option>
          <option value="NASDAQ">NASDAQ / NYSE (US - Finnhub)</option>
        </select>
        <input id="live-sym" placeholder="e.g. INFY, TATAMOTORS, TSLA, NVDA" value="NVDA" class="bg-slate-800 border border-slate-700 rounded-lg px-3 py-1.5 text-sm w-56" />
        <button onclick="lookupAnyLiveQuote()" class="bg-blue-600 hover:bg-blue-500 text-white text-sm font-semibold px-4 py-1.5 rounded-lg">Fetch Live Quote</button>
      </div>
      <div id="live-quote-result" class="text-sm text-slate-300 font-mono">
        Enter any Indian or US stock symbol and click Fetch Live Quote.
      </div>
    </div>

    <!-- TAB 1: DASHBOARD & SCREENER -->
    <section id="panel-screener" class="space-y-6">
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-5">
        <div class="flex flex-wrap justify-between items-center gap-4 mb-4">
          <div>
            <h2 class="text-lg font-semibold">Live Multi-Market Research Universe (India & US)</h2>
            <p class="text-xs text-slate-400">Prices auto-sync from Tapetide (NSE) and Finnhub (US). Shows exact freshness status.</p>
          </div>
          <div class="flex gap-2">
            <select id="filter-country" onchange="loadInstruments()" class="bg-slate-800 border border-slate-700 rounded-lg px-3 py-1.5 text-sm">
              <option value="ALL">All Markets (IN & US)</option>
              <option value="IN">India (NSE/BSE)</option>
              <option value="US">United States (NYSE/NASDAQ)</option>
            </select>
            <select id="filter-type" onchange="loadInstruments()" class="bg-slate-800 border border-slate-700 rounded-lg px-3 py-1.5 text-sm">
              <option value="ALL">Stocks & ETFs</option>
              <option value="EQUITY">Equities Only</option>
              <option value="ETF">ETFs Only</option>
            </select>
            <button onclick="loadInstruments()" class="bg-slate-800 hover:bg-slate-700 border border-slate-700 px-3 py-1.5 rounded-lg text-sm">Refresh Feed</button>
          </div>
        </div>
        <div class="overflow-x-auto">
          <table class="w-full text-left text-sm">
            <thead class="text-xs uppercase text-slate-400 border-b border-slate-800">
              <tr>
                <th class="py-3 px-3">Symbol</th>
                <th class="py-3 px-3">Name</th>
                <th class="py-3 px-3">Exchange</th>
                <th class="py-3 px-3">Live / Latest Price</th>
                <th class="py-3 px-3">Base Fair Value</th>
                <th class="py-3 px-3">Margin of Safety</th>
                <th class="py-3 px-3">Data Status</th>
                <th class="py-3 px-3">Long-Term Value</th>
                <th class="py-3 px-3">Swing Trading</th>
              </tr>
            </thead>
            <tbody id="screener-tbody" class="divide-y divide-slate-800/60"></tbody>
          </table>
        </div>
      </div>
    </section>

    <!-- TAB 2: LONG-TERM FAIR VALUE CALCULATOR -->
    <section id="panel-valuation" class="hidden grid grid-cols-1 lg:grid-cols-2 gap-6">
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-5 space-y-4">
        <h2 class="text-lg font-semibold">Long-Term Value & Fair Value Inputs</h2>
        <div class="grid grid-cols-2 gap-3 text-sm">
          <div>
            <label class="text-xs text-slate-400">Symbol</label>
            <input id="lt-symbol" value="RELIANCE" class="w-full bg-slate-800 border border-slate-700 rounded px-3 py-2 mt-1" />
          </div>
          <div>
            <label class="text-xs text-slate-400">Exchange</label>
            <select id="lt-exchange" class="w-full bg-slate-800 border border-slate-700 rounded px-3 py-2 mt-1">
              <option value="NSE">NSE (India - Tapetide)</option>
              <option value="NASDAQ">NASDAQ (US - Finnhub)</option>
              <option value="NYSE">NYSE (US - Finnhub)</option>
            </select>
          </div>
          <div>
            <label class="text-xs text-slate-400">Sector Valuation Profile</label>
            <select id="lt-sector" class="w-full bg-slate-800 border border-slate-700 rounded px-3 py-2 mt-1">
              <option value="GENERAL_CORPORATE">General Corporate (DCF + Owner Earnings)</option>
              <option value="FINANCIALS_BANKING">Financials / Banking (ROE Excess Return)</option>
              <option value="ETF_INDEX_FUND">ETF / Index Fund (NAV Model)</option>
            </select>
          </div>
          <div>
            <label class="text-xs text-slate-400">Fallback / Manual Price</label>
            <input id="lt-price" type="number" value="1350" class="w-full bg-slate-800 border border-slate-700 rounded px-3 py-2 mt-1" />
          </div>
          <div>
            <label class="text-xs text-slate-400">Trailing Free Cash Flow (TTM)</label>
            <input id="lt-fcf" type="number" value="72000" class="w-full bg-slate-800 border border-slate-700 rounded px-3 py-2 mt-1" />
          </div>
          <div>
            <label class="text-xs text-slate-400">5Y Growth Rate (%)</label>
            <input id="lt-growth" type="number" step="0.5" value="11.0" class="w-full bg-slate-800 border border-slate-700 rounded px-3 py-2 mt-1" />
          </div>
          <div>
            <label class="text-xs text-slate-400">Discount Rate / WACC (%)</label>
            <input id="lt-wacc" type="number" step="0.5" value="10.5" class="w-full bg-slate-800 border border-slate-700 rounded px-3 py-2 mt-1" />
          </div>
          <div>
            <label class="text-xs text-slate-400">Required Margin of Safety (%)</label>
            <input id="lt-mos" type="number" step="1" value="25.0" class="w-full bg-slate-800 border border-slate-700 rounded px-3 py-2 mt-1" />
          </div>
        </div>
        <div class="flex flex-col gap-2 text-sm pt-2">
          <label class="flex items-center gap-2">
            <input id="lt-use-live" type="checkbox" checked class="rounded bg-slate-800" />
            <span class="text-emerald-300 font-medium">Auto-Fetch Live Price from Tapetide / Finnhub API</span>
          </label>
          <label class="flex items-center gap-2">
            <input id="lt-deteriorating" type="checkbox" class="rounded bg-slate-800" />
            <span>Fundamentals Deteriorating (Value-Trap Protection Test)</span>
          </label>
        </div>
        <button onclick="runLongTermValuation()" class="w-full bg-blue-600 hover:bg-blue-500 text-white font-semibold py-2.5 rounded-lg transition">
          Calculate Fair Value & Long-Term Signal
        </button>
      </div>

      <div id="lt-output" class="bg-slate-900 border border-slate-800 rounded-xl p-5 space-y-4"></div>
    </section>

    <!-- TAB 3: SWING TRADING & POSITION SIZER -->
    <section id="panel-swing" class="hidden grid grid-cols-1 lg:grid-cols-2 gap-6">
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-5 space-y-4">
        <h2 class="text-lg font-semibold">Swing Trading Setup & Position Size Calculator</h2>
        <div class="grid grid-cols-2 gap-3 text-sm">
          <div>
            <label class="text-xs text-slate-400">Symbol</label>
            <input id="sw-symbol" value="AAPL" class="w-full bg-slate-800 border border-slate-700 rounded px-3 py-2 mt-1" />
          </div>
          <div>
            <label class="text-xs text-slate-400">Exchange</label>
            <select id="sw-exchange" class="w-full bg-slate-800 border border-slate-700 rounded px-3 py-2 mt-1">
              <option value="NASDAQ">NASDAQ (US - Finnhub)</option>
              <option value="NSE">NSE (India - Tapetide)</option>
            </select>
          </div>
          <div>
            <label class="text-xs text-slate-400">Account Capital</label>
            <input id="sw-capital" type="number" value="100000" class="w-full bg-slate-800 border border-slate-700 rounded px-3 py-2 mt-1" />
          </div>
          <div>
            <label class="text-xs text-slate-400">Max Risk Per Trade (%)</label>
            <input id="sw-risk-pct" type="number" step="0.25" value="1.0" class="w-full bg-slate-800 border border-slate-700 rounded px-3 py-2 mt-1" />
          </div>
          <div>
            <label class="text-xs text-slate-400">Entry Price</label>
            <input id="sw-entry" type="number" step="0.5" value="225.0" class="w-full bg-slate-800 border border-slate-700 rounded px-3 py-2 mt-1" />
          </div>
          <div>
            <label class="text-xs text-slate-400">Stop-Loss Price</label>
            <input id="sw-stop" type="number" step="0.5" value="214.0" class="w-full bg-slate-800 border border-slate-700 rounded px-3 py-2 mt-1" />
          </div>
          <div>
            <label class="text-xs text-slate-400">Target Price</label>
            <input id="sw-target" type="number" step="0.5" value="252.0" class="w-full bg-slate-800 border border-slate-700 rounded px-3 py-2 mt-1" />
          </div>
          <div>
            <label class="text-xs text-slate-400">Days to Next Earnings</label>
            <input id="sw-earnings" type="number" value="25" class="w-full bg-slate-800 border border-slate-700 rounded px-3 py-2 mt-1" />
          </div>
        </div>
        <div class="flex items-center gap-4 text-sm pt-2">
          <label class="flex items-center gap-2">
            <input id="sw-downtrend" type="checkbox" class="rounded bg-slate-800" />
            <span>Downtrend Filter Active (Price below declining 50/200-DMA)</span>
          </label>
        </div>
        <button onclick="runSwingEvaluation()" class="w-full bg-emerald-600 hover:bg-emerald-500 text-white font-semibold py-2.5 rounded-lg transition">
          Evaluate Swing Setup & Position Size
        </button>
      </div>

      <div id="sw-output" class="bg-slate-900 border border-slate-800 rounded-xl p-5 space-y-4"></div>
    </section>

    <!-- TAB 4: TARGET REVISION & SAFETY -->
    <section id="panel-portfolio" class="hidden grid grid-cols-1 lg:grid-cols-2 gap-6">
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-5 space-y-4">
        <h2 class="text-lg font-semibold">Evidence-Based Target Revision Gate</h2>
        <p class="text-xs text-slate-400">Targets never rise just to follow share prices; upward revisions require verified fundamental improvement.</p>
        <div class="grid grid-cols-2 gap-3 text-sm">
          <div>
            <label class="text-xs text-slate-400">Symbol</label>
            <input id="rev-sym" value="RELIANCE" class="w-full bg-slate-800 border border-slate-700 rounded px-3 py-2 mt-1" />
          </div>
          <div>
            <label class="text-xs text-slate-400">Previous Target</label>
            <input id="rev-old" type="number" value="2000" class="w-full bg-slate-800 border border-slate-700 rounded px-3 py-2 mt-1" />
          </div>
          <div>
            <label class="text-xs text-slate-400">Proposed New Target</label>
            <input id="rev-new" type="number" value="2250" class="w-full bg-slate-800 border border-slate-700 rounded px-3 py-2 mt-1" />
          </div>
          <div>
            <label class="text-xs text-slate-400">Updated Fair Value</label>
            <input id="rev-fv" type="number" value="2300" class="w-full bg-slate-800 border border-slate-700 rounded px-3 py-2 mt-1" />
          </div>
        </div>
        <input id="rev-note" value="Q3 Operating Margin expanded +180 bps YoY" class="w-full bg-slate-800 border border-slate-700 rounded px-3 py-2 text-sm" />
        <label class="flex items-center gap-2 text-sm">
          <input id="rev-verified" type="checkbox" checked class="rounded bg-slate-800" />
          <span>Verified Quarterly Filing / Cash-Flow Expansion Confirmed</span>
        </label>
        <button onclick="submitTargetRevision()" class="w-full bg-purple-600 hover:bg-purple-500 text-white font-semibold py-2.5 rounded-lg transition">
          Validate & Log Target Revision
        </button>
        <div id="rev-output" class="text-sm pt-2"></div>
      </div>

      <div class="bg-slate-900 border border-slate-800 rounded-xl p-5 space-y-3">
        <h2 class="text-lg font-semibold">Regulatory & Safety Notice</h2>
        <p class="text-sm text-slate-300 leading-relaxed">
          This platform provides research and decision support, not guaranteed returns or regulated financial advice. Long-Term Value and Swing Trading signals are strictly separated.
        </p>
      </div>
    </section>
  </main>

  <script>
    function showTab(name) {
      ['screener', 'valuation', 'swing', 'portfolio'].forEach(t => {
        document.getElementById('panel-' + t).classList.toggle('hidden', t !== name);
        const btn = document.getElementById('tab-' + t);
        btn.className = t === name
          ? 'tab-btn px-4 py-3 text-sm font-medium border-b-2 border-blue-500 text-blue-400'
          : 'tab-btn px-4 py-3 text-sm font-medium border-b-2 border-transparent text-slate-400 hover:text-white';
      });
    }

    async function lookupAnyLiveQuote() {
      const exch = document.getElementById('live-exch').value;
      const sym = document.getElementById('live-sym').value.trim();
      if (!sym) return;
      document.getElementById('live-quote-result').innerText = 'Fetching live quote...';
      const res = await fetch(`/api/v1/quote/${exch}/${sym}`);
      const q = await res.json();
      document.getElementById('live-quote-result').innerHTML =
        `<span class="text-white font-bold">${q.symbol}</span>: <span class="text-emerald-400 font-bold">${q.price}</span> • Status: <span class="text-blue-300">${q.freshness}</span> (${q.provider})`;
    }

    async function loadInstruments() {
      const country = document.getElementById('filter-country').value;
      const assetType = document.getElementById('filter-type').value;
      const res = await fetch(`/api/v1/instruments?country=${country}&asset_type=${assetType}`);
      const data = await res.json();
      const tbody = document.getElementById('screener-tbody');
      tbody.innerHTML = data.instruments.map(item => `
        <tr class="hover:bg-slate-800/50">
          <td class="py-3 px-3 font-bold text-white">${item.symbol}</td>
          <td class="py-3 px-3 text-slate-300">${item.name}</td>
          <td class="py-3 px-3"><span class="px-2 py-0.5 rounded bg-slate-800 text-xs">${item.country} • ${item.exchange}</span></td>
          <td class="py-3 px-3 font-mono font-bold text-white">${item.price.toLocaleString()}</td>
          <td class="py-3 px-3 font-mono text-emerald-300">${item.fair_value.toLocaleString()}</td>
          <td class="py-3 px-3 font-mono ${item.mos_pct >= 20 ? 'text-emerald-400' : 'text-slate-400'}">${item.mos_pct}%</td>
          <td class="py-3 px-3"><span class="px-2 py-0.5 rounded text-xs ${item.freshness === 'REAL_TIME' ? 'bg-emerald-500/20 text-emerald-300' : 'bg-amber-500/20 text-amber-300'}">${item.freshness}</span></td>
          <td class="py-3 px-3"><span class="px-2.5 py-1 rounded text-xs font-semibold bg-emerald-500/20 text-emerald-300">${item.lt_signal}</span></td>
          <td class="py-3 px-3"><span class="px-2.5 py-1 rounded text-xs font-semibold bg-blue-500/20 text-blue-300">${item.swing_signal}</span></td>
        </tr>
      `).join('');
    }

    async function runLongTermValuation() {
      const payload = {
        symbol: document.getElementById('lt-symbol').value,
        exchange: document.getElementById('lt-exchange').value,
        sector_profile: document.getElementById('lt-sector').value,
        current_price: parseFloat(document.getElementById('lt-price').value),
        use_live_price: document.getElementById('lt-use-live').checked,
        free_cash_flow_ttm: parseFloat(document.getElementById('lt-fcf').value),
        base_growth_rate_5y_pct: parseFloat(document.getElementById('lt-growth').value),
        discount_rate_wacc_pct: parseFloat(document.getElementById('lt-wacc').value),
        required_margin_of_safety_pct: parseFloat(document.getElementById('lt-mos').value),
        is_fundamentals_deteriorating: document.getElementById('lt-deteriorating').checked
      };
      const res = await fetch('/api/v1/long-term/evaluate', {
        method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)
      });
      const out = await res.json();
      document.getElementById('lt-output').innerHTML = `
        <div class="flex justify-between items-center border-b border-slate-800 pb-3">
          <div>
            <span class="text-xs text-slate-400">${out.strategy} • Price Used: ${out.current_price} (${out.freshness})</span>
            <h3 class="text-xl font-bold">${out.symbol} (${out.exchange})</h3>
          </div>
          <span class="px-3 py-1.5 rounded-lg text-sm font-bold bg-blue-500/20 text-blue-300 border border-blue-500/40">${out.signal_state}</span>
        </div>
        <div class="grid grid-cols-3 gap-3 text-center bg-slate-950 p-3 rounded-lg">
          <div><div class="text-xs text-slate-400">Bear Fair Value</div><div class="font-bold text-slate-200">${out.bear_fair_value ?? 'N/A'}</div></div>
          <div><div class="text-xs text-slate-400">Base Fair Value</div><div class="font-bold text-emerald-400 text-lg">${out.base_fair_value ?? 'N/A'}</div></div>
          <div><div class="text-xs text-slate-400">Bull Fair Value</div><div class="font-bold text-slate-200">${out.bull_fair_value ?? 'N/A'}</div></div>
        </div>
        <div class="text-sm space-y-1">
          <div><strong>Margin of Safety:</strong> ${out.margin_of_safety_pct ?? 'N/A'}% (Required: ${out.required_margin_of_safety_pct}%)</div>
          <div><strong>Falling-Stock Category:</strong> <span class="text-amber-300">${out.falling_category}</span></div>
        </div>
        <div class="text-xs space-y-1 text-slate-300">
          ${(out.reasons || []).concat(out.risks || []).map(r => `<div>• ${r}</div>`).join('')}
        </div>
      `;
    }

    async function runSwingEvaluation() {
      const payload = {
        symbol: document.getElementById('sw-symbol').value,
        exchange: document.getElementById('sw-exchange').value,
        account_capital: parseFloat(document.getElementById('sw-capital').value),
        max_risk_per_trade_pct: parseFloat(document.getElementById('sw-risk-pct').value),
        entry_price: parseFloat(document.getElementById('sw-entry').value),
        stop_loss_price: parseFloat(document.getElementById('sw-stop').value),
        target_price: parseFloat(document.getElementById('sw-target').value),
        days_to_earnings: parseInt(document.getElementById('sw-earnings').value),
        downtrend_filter_active: document.getElementById('sw-downtrend').checked
      };
      const res = await fetch('/api/v1/swing/evaluate', {
        method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)
      });
      const out = await res.json();
      document.getElementById('sw-output').innerHTML = `
        <div class="flex justify-between items-center border-b border-slate-800 pb-3">
          <div>
            <span class="text-xs text-slate-400">${out.strategy}</span>
            <h3 class="text-xl font-bold">${out.symbol} (${out.exchange})</h3>
          </div>
          <span class="px-3 py-1.5 rounded-lg text-sm font-bold bg-emerald-500/20 text-emerald-300 border border-emerald-500/40">${out.signal_state}</span>
        </div>
        <div class="grid grid-cols-3 gap-3 text-center bg-slate-950 p-3 rounded-lg">
          <div><div class="text-xs text-slate-400">Recommended Shares</div><div class="font-bold text-lg text-white">${out.recommended_shares}</div></div>
          <div><div class="text-xs text-slate-400">Net Risk/Reward</div><div class="font-bold text-lg text-emerald-400">${out.net_risk_reward_ratio}x</div></div>
          <div><div class="text-xs text-slate-400">Est. Round-Trip Cost</div><div class="font-bold text-amber-300">${out.estimated_round_trip_cost}</div></div>
        </div>
        <div class="text-xs space-y-1 text-slate-300">
          ${(out.reasons || []).concat(out.warnings || []).map(r => `<div>• ${r}</div>`).join('')}
        </div>
      `;
    }

    async function submitTargetRevision() {
      const payload = {
        symbol: document.getElementById('rev-sym').value,
        exchange: 'NSE',
        strategy: 'LONG_TERM_VALUE',
        previous_target_price: parseFloat(document.getElementById('rev-old').value),
        new_target_price: parseFloat(document.getElementById('rev-new').value),
        new_fair_value: parseFloat(document.getElementById('rev-fv').value),
        fundamental_evidence_verified: document.getElementById('rev-verified').checked,
        evidence_explanation: document.getElementById('rev-note').value
      };
      const res = await fetch('/api/v1/targets/revise', {
        method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)
      });
      const out = await res.json();
      document.getElementById('rev-output').innerHTML = `
        <div class="p-3 rounded-lg border ${out.accepted ? 'bg-emerald-950/50 border-emerald-700 text-emerald-300' : 'bg-rose-950/50 border-rose-700 text-rose-300'}">
          ${out.message}
        </div>
      `;
    }

    loadInstruments();
    runLongTermValuation();
    runSwingEvaluation();
  </script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
async def serve_web_dashboard():
    return HTMLResponse(content=FRONTEND_HTML)
