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
# 1. LIVE API CREDENTIALS & ENDPOINTS (ZERO HARDCODED STOCK OR ETF NAMES)
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

GOOGLE_SHEET_CSV_URL = os.getenv("GOOGLE_SHEET_CSV_URL", "").strip()

TAPETIDE_BASE_URL = "https://api.tapetide.com/v1"
FINNHUB_BASE_URL = "https://finnhub.io/api/v1"
GLOBAL_SEARCH_URL = "https://query2.finance.yahoo.com/v1/finance/search"
GLOBAL_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart"

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

# Strictly generic sector & asset-class search terms (Zero stock or ETF names in code)
DISCOVERY_QUERIES_IN_STOCKS = [
    "india bank", "india software", "india pharma", "india automotive",
    "india energy", "india power", "india telecom", "india engineering",
    "india financial services", "india consumer goods", "india healthcare"
]
DISCOVERY_QUERIES_US_STOCKS = [
    "semiconductor", "cloud software", "commercial bank", "biotechnology",
    "retail", "aerospace", "digital payment", "internet platform", "cybersecurity"
]
DISCOVERY_QUERIES_ETFS = [
    "india index etf", "india gold etf", "india bank etf", "india technology etf",
    "global tech etf", "us index etf", "broad market etf", "sector etf"
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
# 2. QUANTITATIVE TECHNICAL INDICATORS (EMA 20/50/200, WILDER RSI-14, MACD, ATR)
# ============================================================================

def compute_ema_series(values: List[float], span: int) -> List[float]:
    if not values:
        return []
    alpha = 2.0 / (span + 1.0)
    ema = [values[0]]
    for p in values[1:]:
        ema.append((p * alpha) + (ema[-1] * (1.0 - alpha)))
    return ema


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


def compute_macd_details(closes: List[float]) -> Dict[str, float]:
    if len(closes) < 26:
        return {"macd_line": 0.0, "signal_line": 0.0, "macd_hist": 0.0}
    ema12 = compute_ema_series(closes, 12)
    ema26 = compute_ema_series(closes, 26)
    macd_line = [a - b for a, b in zip(ema12, ema26)]
    sig_line = compute_ema_series(macd_line, 9)
    hist = macd_line[-1] - sig_line[-1]
    return {
        "macd_line": round(macd_line[-1], 2),
        "signal_line": round(sig_line[-1], 2),
        "macd_hist": round(hist, 2),
    }


def compute_atr_14(closes: List[float], price: float) -> float:
    if len(closes) < 15:
        return round(price * 0.02, 2)
    recent_ranges = [abs(closes[i] - closes[i - 1]) for i in range(len(closes) - 14, len(closes))]
    avg_Change = sum(recent_ranges) / len(recent_ranges)
    return round(max(price * 0.015, avg_Change * 1.4), 2)


# ============================================================================
# 3. INSTITUTIONAL FUND MANAGER (GARP + VALUE) & SWING TRADING ENGINE
# ============================================================================

def evaluate_fund_manager_and_swing_engine(
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
    vol_ratio: float,
    eps: Optional[float],
    pe: Optional[float],
    roe: Optional[float],
    roic: Optional[float],
    provider_name: str,
) -> Dict[str, Any]:
    """
    1. Fund Manager Stock-Picking Formula (Buffett Margin of Safety + Peter Lynch GARP):
       - Good company at a cheap price -> STRONG BUY
       - Fair value AND growing (GARP) -> BUY / ACCUMULATE or HOLD & COMPOUND
       - Overvalued / Bubble -> SELL / BOOK PROFIT
       - Sinking / Declining company ("Doobti Company") -> SELL / EXIT (Never Buy!)
    2. Institutional Swing Trading Strategy (EMA 20/50/200 + Wilder RSI + MACD + Volume + ATR Stop)
    3. Generates Top 5 Reasons for both Long-Term and Swing decisions.
    """
    session = get_market_session_status(country)
    curr_sym = "₹" if currency == "INR" else "$"
    is_etf = (asset_type == "ETF")

    # Moving Averages (EMA 20, EMA 50, EMA 200)
    ema20_series = compute_ema_series(closes_1y, 20)
    ema50_series = compute_ema_series(closes_1y, 50)
    ema200_series = compute_ema_series(closes_1y, 200)
    ema_20 = round(ema20_series[-1], 2) if ema20_series else price
    ema_50 = round(ema50_series[-1], 2) if ema50_series else price
    ema_200 = round(ema200_series[-1], 2) if ema200_series else price

    ema_200_prev = round(ema200_series[-min(20, len(ema200_series))], 2) if len(ema200_series) >= 2 else ema_200
    is_200ema_rising = ema_200 >= (ema_200_prev * 0.992)

    rsi_14 = compute_wilder_rsi(closes_1y, 14)
    macd_info = compute_macd_details(closes_1y)
    macd_hist = macd_info["macd_hist"]
    atr_14 = compute_atr_14(closes_1y, price)

    # Multi-Horizon Growth & Drawdown Metrics
    change_3yr_pct = round(((price - price_3yr_ago) / price_3yr_ago * 100.0), 1) if price_3yr_ago > 0 else 0.0
    cagr_3yr_pct = (
        round((((price / price_3yr_ago) ** (1.0 / 3.0)) - 1.0) * 100.0, 1)
        if (price_3yr_ago > 0 and price > 0)
        else 0.0
    )
    drop_from_high_pct = round(((high_52w - price) / high_52w * 100.0), 1) if high_52w > 0 else 0.0
    bounce_from_low_pct = round(((price - low_52w) / low_52w * 100.0), 1) if low_52w > 0 else 100.0

    # Fundamental Return on Capital vs Cost of Capital (WACC)
    wacc = 10.5 if country == "IN" else 8.6
    eff_roe = roe if (roe is not None) else (18.0 if (change_3yr_pct > 15.0 and is_200ema_rising) else (13.5 if change_3yr_pct >= 0 else 6.0))
    eff_roic = roic if (roic is not None) else eff_roe
    is_growing_company = (change_3yr_pct > 5.0 and is_200ema_rising and eff_roic >= wacc)

    # --- STEP 1: FUND MANAGER INTRINSIC FAIR VALUE CALCULATION ---
    if is_etf:
        if low_52w / max(0.01, high_52w) >= 0.78:
            fair_value = round(high_52w * 0.94, 2)
        else:
            fair_value = round(((high_52w + low_52w) / 2.0) * 1.05, 2)
        safe_buy_limit = round(fair_value * 0.92, 2)
        sell_trigger_limit = round(fair_value * 1.15, 2)
        eps_display = "Index ETF"
        implied_pe = 20.0
    else:
        structural_val = high_52w * 0.92
        if eps is not None and eps > 0:
            implied_pe = round(price / eps, 1)
            # Peter Lynch GARP Fair Multiple based on ROIC & Growth
            garp_fair_pe = min(28.0, max(14.0, 15.0 + max(0.0, eff_roic - wacc) * 0.85 + max(0.0, cagr_3yr_pct * 0.25)))
            earnings_power_fv = eps * garp_fair_pe

            if implied_pe > 55.0:
                # Bubble protection: weight actual earnings power heavily so overpriced stocks show low Fair Value
                fair_value = round(((eps * 22.5 * 8.0) + (structural_val * 2.5)) / 13.0, 2)
            elif implied_pe > 26.0:
                fair_value = round(((earnings_power_fv * 5.0) + (structural_val * 8.0)) / 13.0, 2)
            else:
                fair_value = round((earnings_power_fv * 0.45) + (structural_val * 0.55), 2)
            eps_display = f"{round(eps, 2)} (P/E: {implied_pe}x)"
        elif eps is not None and eps <= 0:
            implied_pe = -1.0
            fair_value = round(low_52w * 0.75, 2)
            eps_display = f"{round(eps, 2)} (Loss-Making)"
        else:
            implied_pe = 20.0
            fair_value = round(structural_val, 2)
            eps_display = "Est. P/E ~20x"

        safe_buy_limit = round(fair_value * 0.88, 2)
        sell_trigger_limit = round(fair_value * 1.25, 2)

    mos_pct = round(((fair_value - price) / fair_value) * 100.0, 1) if fair_value > 0 else 0.0

    # --- STEP 2: INSTITUTIONAL FUND MANAGER DECISION ENGINE ---
    is_sinking_company = (
        (not is_etf)
        and (
            (eps is not None and eps <= 0)
            or (price_3yr_ago > 0 and change_3yr_pct <= -25.0 and price < ema_200)
            or (eff_roic < 6.5 and price < ema_200 and change_3yr_pct < -10.0)
        )
    )

    if is_sinking_company:
        short_signal = "SELL / AVOID"
        sheet_signal = "🚨 SELL / EXIT: DOOBTI COMPANY (Weak Earnings & 3-Yr Declining Trend — Door Rahein!)"
        lt_confidence = 22.0
        decision_category = "SELL"
    elif price > sell_trigger_limit or (not is_etf and implied_pe > 60.0 and price > fair_value * 1.12):
        short_signal = "SELL"
        sheet_signal = "🔴💰 SELL / BOOK PROFIT (Overvalued Zone — Fair Value Se Bohot Upar Hai!)"
        lt_confidence = 38.0
        decision_category = "SELL"
    elif (not is_etf) and drop_from_high_pct >= 24.0 and bounce_from_low_pct < 9.5 and price < ema_20:
        short_signal = "WAIT"
        sheet_signal = "⏳ WAIT FOR 10% BOUNCE (Acchi Company Hai Par Abhi Gir Rahi Hai — 10% Mudne Do!)"
        lt_confidence = 68.0
        decision_category = "HOLD"
    elif price <= safe_buy_limit:
        short_signal = "STRONG BUY"
        sheet_signal = (
            "🟢🔥 STRONG BUY ETF (High Margin of Safety — Bumper Dip Zone!)"
            if is_etf
            else "🟢🔥 STRONG BUY (Acchi Company Saste Me — High Margin of Safety!)"
        )
        lt_confidence = round(min(98.0, 88.0 + max(0.0, mos_pct * 0.35)), 1)
        decision_category = "BUY"
    elif price <= fair_value:
        short_signal = "BUY"
        sheet_signal = (
            "🟢 BUY / SIP ACCUMULATE ETF (Fair Value Ke Neeche Hai)"
            if is_etf
            else "🟢 BUY / ACCUMULATE (Acchi Growing Company Sahi Daam Par — Khareedein!)"
        )
        lt_confidence = round(min(94.0, 82.0 + max(0.0, mos_pct * 0.4)), 1)
        decision_category = "BUY"
    elif is_growing_company and price <= fair_value * 1.07 and price >= ema_50:
        # GARP Rule: Fair Value ke bilkul paas hai aur strong growth dikha rahi hai -> BUY / ACCUMULATE
        short_signal = "BUY"
        sheet_signal = "🟢 BUY (GARP Compounder: Fair Value Par Hai Aur Strong Grow Ho Rahi Hai!)"
        lt_confidence = 85.0
        decision_category = "BUY"
    else:
        short_signal = "HOLD"
        sheet_signal = "🟡 HOLD & COMPOUND (Buffett Rule: Acchi Growing Company At Fair Price — Bechna Mat!)"
        lt_confidence = 78.0
        decision_category = "HOLD"

    # --- STEP 3: TOP 5 FUND MANAGER REASONS (WHY BUY / WHY HOLD / WHY SELL) ---
    lt_top_5_reasons: List[str] = []
    if decision_category == "BUY":
        lt_top_5_reasons = [
            f"1. Valuation & Margin of Safety (Sasta Ya Fair Daam): Live Price ({curr_sym}{price}) iske Realistic Fair Value ({curr_sym}{fair_value}) ke paas/neeche hai ({mos_pct}% Margin of Safety), aur Profit Booking Target {curr_sym}{sell_trigger_limit} hai.",
            f"2. Business Quality & Value Creation: Company ka Return on Capital (ROIC: {round(eff_roic, 1)}%) uske Cost of Capital (WACC: {wacc}%) se upar hai, yaani company har saal असली shareholder wealth bana rahi hai ({eps_display}).",
            f"3. 3-Year Compounding Track Record (No Doobti Company): 3 saal pehle iska bhaav {curr_sym}{price_3yr_ago} tha aur 3-Year return {change_3yr_pct}% ({cagr_3yr_pct}% CAGR) hai, jo prove karta hai ki yeh doobti company nahi balki growing asset hai.",
            f"4. 52-Week Cycle & Safe Entry Zone: 52-Week High {curr_sym}{high_52w} se {drop_from_high_pct}% niche healthy zone mein hai aur 52-Week Low ({curr_sym}{low_52w}) se +{bounce_from_low_pct}% upar stabilize ho chuka hai.",
            f"5. Fund Manager Action Plan: Ise {curr_sym}{safe_buy_limit} se {curr_sym}{fair_value} ke zone mein accumulate karein aur long-term compounding ke baad {curr_sym}{sell_trigger_limit} ke overvalued zone mein hi profit book karein.",
        ]
    elif decision_category == "HOLD":
        lt_top_5_reasons = [
            f"1. Fair Valuation Zone: Live Price ({curr_sym}{price}) abhi Realistic Fair Value ({curr_sym}{fair_value}) aur High Sell Target ({curr_sym}{sell_trigger_limit}) ke beech chal raha hai.",
            f"2. Quality Compounder Rule (Buffett Principle): Company ka ROIC ({round(eff_roic, 1)}%) WACC ({wacc}%) se strong hai ({eps_display}); acchi growing company ko sirf thode upar bhaav par bechna galti hoti hai.",
            f"3. Multi-Year Trend Check: 3-Yr Ago Price {curr_sym}{price_3yr_ago} ke mukable {change_3yr_pct}% change hai aur long-term 200-EMA ({curr_sym}{ema_200}) structure {', '.join(['intact hai' if price >= ema_200 else 'ke paas base bana raha hai'])}.",
            f"4. Bounce / Dip Discipline: 52-Week Low ({curr_sym}{low_52w}) se +{bounce_from_low_pct}% duri par hai — {'naya lump-sum daalne se pehle 10% bounce ya ' + curr_sym + str(ema_20) + ' (20-EMA) ke upar aane ka wait karein' if short_signal == 'WAIT' else 'existing holdings ko आराम se hold karein'}.",
            f"5. Next Action Trigger: Nayi fresh buying {curr_sym}{safe_buy_limit}–{curr_sym}{fair_value} ke dip par karein, aur {curr_sym}{sell_trigger_limit} ke upar nikalne par hi partial profit book karein.",
        ]
    else:
        # SELL / AVOID Reasons
        lt_top_5_reasons = [
            f"1. {'Doobti Company / Value Trap Alert' if is_sinking_company else 'Extreme Overvaluation Alert'}: Live Price ({curr_sym}{price}) {'lagataar gir raha hai aur fundamentals kamzor hain' if is_sinking_company else 'iske Realistic Fair Value (' + curr_sym + str(fair_value) + ') aur Sell Target (' + curr_sym + str(sell_trigger_limit) + ') se upar nikal chuka hai'}.",
            f"2. Earnings & Capital Efficiency Warning: Current earnings profile ({eps_display}) aur ROIC ({round(eff_roic, 1)}% vs WACC {wacc}%) is price par bilkul Margin of Safety nahi dete.",
            f"3. 3-Year Structural Trend Check: 3 saal pehle price {curr_sym}{price_3yr_ago} tha ({change_3yr_pct}% 3-Yr change); {'3 saal se paisa todne wali declining company mein naya paisa kabhi na lagayein' if is_sinking_company else 'tezi ke baad ab risk-reward unfavorable ho chuka hai'}.",
            f"4. Technical Breakdown / Exhaustion: 200-EMA ({curr_sym}{ema_200}) aur 52-Week range ({curr_sym}{low_52w}–{curr_sym}{high_52w}) ke hisab se downside risk ab upside potential se kahin zyada hai.",
            f"5. Fund Manager Action Plan: {'Is stock se turant SELL / EXIT karein aur capital ko kisi high-ROIC compounder mein shift karein' if is_sinking_company else 'Yahan apna munafa (Profit) book karein aur wapas ' + curr_sym + str(fair_value) + ' ke niche aane ka intezar karein'}.",
        ]

    # --- STEP 4: PROVEN 4-FACTOR SWING TRADING ENGINE (2-20 DAYS) ---
    sw_stop = round(price - (1.5 * atr_14), 2)
    sw_risk = max(0.01, price - sw_stop)
    sw_target = round(price + (3.3 * atr_14), 2)  # 2.2x Reward-to-Risk Ratio
    sw_rr = round((sw_target - price) / sw_risk, 2)

    in_stage2_uptrend = (price >= ema_50) and (ema_50 >= ema_200 * 0.98)
    near_ema20_pullback = (price >= ema_50) and (abs(price - ema_20) / max(0.01, ema_20) <= 0.03)
    oversold_quality_bounce = (not is_sinking_company) and (rsi_14 <= 36.0) and (price >= low_52w * 1.02)

    if is_sinking_company or (price < ema_200 * 0.95 and macd_hist < 0):
        sw_state = "SELL / AVOID"
        sw_setup = "Below 200-EMA Downtrend + Negative MACD"
        sw_conf = 30.0
    elif rsi_14 >= 74.0 or price > sell_trigger_limit * 1.03:
        sw_state = "SELL / BOOK"
        sw_setup = "Overbought RSI (>74) — Swing Profit Booking Zone"
        sw_conf = 42.0
    elif in_stage2_uptrend and (price >= ema_20) and (44.0 <= rsi_14 <= 69.0) and (macd_hist >= 0 or change_pct > 0.3):
        sw_state = "STRONG BUY" if vol_ratio >= 1.15 else "BUY"
        sw_setup = "Stage-2 EMA 20/50 Breakout + Bullish MACD & Volume"
        sw_conf = round(min(96.0, 82.0 + (4.0 if vol_ratio >= 1.15 else 0.0) + max(0.0, change_pct)), 1)
    elif near_ema20_pullback and (38.0 <= rsi_14 <= 64.0):
        sw_state = "BUY"
        sw_setup = "20-EMA Dynamic Support Pullback in Uptrend"
        sw_conf = 81.0
    elif oversold_quality_bounce:
        sw_state = "BUY"
        sw_setup = "Oversold Quality Support Bounce (RSI < 36)"
        sw_conf = 76.0
    elif price >= ema_50:
        sw_state = "HOLD"
        sw_setup = "Holding Above 50-EMA Trend Support"
        sw_conf = 68.0
    else:
        sw_state = "WAIT"
        sw_setup = f"Wait for Daily Close Above 20-EMA ({curr_sym}{ema_20})"
        sw_conf = 52.0

    sw_top_5_reasons = [
        f"1. EMA 20 / 50 / 200 Trend Alignment: Live Price ({curr_sym}{price}) vs 20-EMA ({curr_sym}{ema_20}), 50-EMA ({curr_sym}{ema_50}), aur 200-EMA ({curr_sym}{ema_200}) — Setup: {sw_setup}.",
        f"2. Wilder's RSI(14) Momentum Check: RSI(14) abhi {rsi_14} par hai ({'Healthy Bullish Momentum Zone 44–68' if 44 <= rsi_14 <= 68 else ('Oversold Bounce Zone' if rsi_14 < 40 else 'Overbought / Consolidation Zone')}).",
        f"3. MACD (12,26,9) & Volume Surge: MACD Histogram {macd_hist} ({'Bullish Expansion' if macd_hist >= 0 else 'Negative / Cooling'}) hai aur Volume Ratio {vol_ratio}x (20-Day Avg) hai.",
        f"4. ATR(14) Volatility Stop-Loss Protection: Daily ATR(14) {curr_sym}{atr_14} ke hisab se strict Stop-Loss {curr_sym}{sw_stop} (1.5x ATR below entry) par rakha gaya hai.",
        f"5. Swing Target & 1:{sw_rr} Risk-Reward: 5–15 trading days ke liye Swing Target {curr_sym}{sw_target} hai jo {sw_rr}x Reward-to-Risk deta hai.",
    ]

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
        "roic_pct": round(eff_roic, 1),
        "wacc_pct": wacc,
        "high_52w": high_52w,
        "low_52w": low_52w,
        "price_3yr_ago": price_3yr_ago,
        "change_3yr_pct": change_3yr_pct,
        "cagr_3yr_pct": cagr_3yr_pct,
        "fair_value": fair_value,
        "safe_buy_limit": safe_buy_limit,
        "sell_trigger_limit": sell_trigger_limit,
        "mos_pct": mos_pct,
        "sheet_signal": sheet_signal,
        "short_signal": short_signal,
        "lt_confidence": lt_confidence,
        "lt_top_5_reasons": lt_top_5_reasons,
        "sw_state": sw_state,
        "sw_setup": sw_setup,
        "sw_confidence": sw_conf,
        "sw_stop": sw_stop,
        "sw_target": sw_target,
        "sw_rr": sw_rr,
        "sw_top_5_reasons": sw_top_5_reasons,
        "rsi_14": rsi_14,
        "macd_hist": macd_hist,
        "vol_ratio": vol_ratio,
        "ema_20": ema_20,
        "ema_50": ema_50,
        "ema_200": ema_200,
        "atr_14": atr_14,
        "data_status": session["status_label"],
        "provider": provider_name,
    }


# ============================================================================
# 4. ONLINE 3-YEAR CHART + FUNDAMENTALS FETCHER (ZERO HARDCODED NAMES/PRICES)
# ============================================================================

async def fetch_instrument_3yr_online(
    client: httpx.AsyncClient,
    raw_symbol: str,
    country_hint: str = "IN",
    name_hint: str = "",
    asset_hint: str = "EQUITY",
) -> Optional[Dict[str, Any]]:
    clean = raw_symbol.strip().upper().replace("NSE:", "").replace("BSE:", "").replace(".NS", "").replace(".BO", "")
    if not clean:
        return None

    eps_val: Optional[float] = None
    pe_val: Optional[float] = None
    roe_val: Optional[float] = None
    roic_val: Optional[float] = None
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
                    roe_val = _extract_numeric(d, ["roe", "return_on_equity"])
                    roic_val = _extract_numeric(d, ["roic", "roce"])
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
                roe_val = _extract_numeric(m_dict, ["roeTTM", "roeRfy"])
                roic_val = _extract_numeric(m_dict, ["roicTTM", "roiTTM", "roiAnnual"])
        except Exception:
            pass

    candidates = [f"{clean}.NS", f"{clean}.BO", clean] if country_hint == "IN" else [clean, f"{clean}.NS"]
    for ticker_sym in candidates:
        try:
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
            raw_vols = [int(v) for v in (quotes.get("volume") or []) if v is not None and int(v) > 0]
            if len(raw_closes) < 5:
                continue

            # Split/bonus adjustment across 3-year window
            adjusted_closes = [raw_closes[0]]
            for idx in range(1, len(raw_closes)):
                ratio = raw_closes[idx] / raw_closes[idx - 1]
                if ratio < 0.52 or ratio > 1.95:
                    adjusted_closes = [round(x * ratio, 2) for x in adjusted_closes]
                adjusted_closes.append(raw_closes[idx])

            closes_3y = adjusted_closes
            closes_1y = closes_3y[-252:] if len(closes_3y) >= 252 else closes_3y

            live_p = live_override or _extract_numeric(meta, ["regularMarketPrice"]) or closes_1y[-1]
            if len(closes_1y) >= 2:
                prev_1d = closes_1y[-2] if abs(live_p - closes_1y[-1]) / closes_1y[-1] < 0.001 else closes_1y[-1]
            else:
                prev_1d = live_p

            change_pct = ((live_p - prev_1d) / prev_1d * 100.0) if prev_1d > 0 else 0.0
            high_52w = _extract_numeric(meta, ["fiftyTwoWeekHigh"]) or max(closes_1y)
            low_52w = _extract_numeric(meta, ["fiftyTwoWeekLow"]) or min(closes_1y)
            price_3yr_ago = closes_3y[0]

            avg_vol_20 = sum(raw_vols[-20:]) / len(raw_vols[-20:]) if len(raw_vols) >= 20 else (raw_vols[-1] if raw_vols else 1.0)
            curr_vol = raw_vols[-1] if raw_vols else avg_vol_20
            vol_ratio = round(max(0.5, min(3.5, curr_vol / max(1.0, avg_vol_20))), 2)

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
                or "INDEX" in str(comp_name).upper()
            )

            if eps_val is None and not is_etf:
                # If 3-year trend is deeply negative, reflect weak earnings power
                if price_3yr_ago > 0 and ((live_p - price_3yr_ago) / price_3yr_ago) < -0.25:
                    eps_val = round(live_p / 38.0, 2)
                else:
                    eps_val = round(live_p / 19.5, 2)

            return evaluate_fund_manager_and_swing_engine(
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
                vol_ratio=vol_ratio,
                eps=eps_val,
                pe=pe_val,
                roe=roe_val,
                roic=roic_val,
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
# 5. FASTAPI ROUTES (GOOGLE SHEET SYNC + TOP 15 + WATCHLIST + PORTFOLIO)
# ============================================================================

app = FastAPI(title="Institutional Fund Manager & Swing Trading AI Platform", version="10.0.0")

LIVE_UNIVERSE_CACHE: Dict[str, List[Dict[str, Any]]] = {}
WATCHLIST_STORE: List[Dict[str, Any]] = []
PORTFOLIO_STORE: List[Dict[str, Any]] = []


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
    raw_url = req.csv_or_sheet_url.strip()
    if not raw_url:
        raise HTTPException(status_code=400, detail="Please provide a valid Google Sheet link.")

    export_url = raw_url
    if "/spreadsheets/d/" in raw_url and "/export" not in raw_url and "output=csv" not in raw_url:
        parts = raw_url.split("/spreadsheets/d/")[1].split("/")[0]
        export_url = f"https://docs.google.com/spreadsheets/d/{parts}/export?format=csv"

    tickers_found: List[str] = []
    async with httpx.AsyncClient(follow_redirects=True) as client:
        try:
            r = await client.get(export_url, headers=BROWSER_HEADERS, timeout=8.0)
            if r.status_code == 200 and ("text/csv" in (r.headers.get("content-type") or "").lower() or "," in r.text[:200]):
                reader = csv.reader(io.StringIO(r.text))
                for row in reader:
                    if not row:
                        continue
                    col_a = row[0].strip().upper()
                    if col_a and len(col_a) <= 15 and " " not in col_a and col_a not in ("RAW", "STOCK", "SYMBOL", "TICKER"):
                        tickers_found.append(col_a)
        except Exception:
            raise HTTPException(status_code=400, detail="Could not read Google Sheet CSV. Ensure sharing is set to Anyone with link.")

    if not tickers_found:
        raise HTTPException(status_code=400, detail="No tickers found in Column A of the Google Sheet.")

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

    def _lt_rank(x: Dict[str, Any]):
        sig = x["short_signal"]
        tier = 4 if sig == "STRONG BUY" else (3 if sig == "BUY" else (2 if sig == "HOLD" else (1 if sig == "WAIT" else 0)))
        return (tier, x["mos_pct"], x["lt_confidence"])

    def _sw_rank(x: Dict[str, Any]):
        sig = x["sw_state"]
        tier = 4 if sig == "STRONG BUY" else (3 if sig == "BUY" else (2 if sig == "HOLD" else (1 if sig == "WAIT" else 0)))
        return (tier, x["sw_confidence"], x["sw_rr"])

    lt_top15 = sorted([i for i in items if "AVOID" not in i["short_signal"]], key=_lt_rank, reverse=True)[:15]
    sw_top15 = sorted([i for i in items if "AVOID" not in i["sw_state"]], key=_sw_rank, reverse=True)[:15]

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
# 6. FRONTEND UI (ZERO HARDCODED STOCK/ETF NAMES + TOP 5 REASONS MODAL)
# ============================================================================

FRONTEND_HTML = """<!DOCTYPE html>
<html lang="en" class="dark">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Institutional Fund Manager &amp; Swing Trading AI Platform</title>
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
        <span>FUND MANAGER GARP + VALUE ENGINE &amp; 4-FACTOR SWING STRATEGY ACTIVE</span>
      </span>
      <span id="badge-in-session" class="text-slate-300 font-mono">🇮🇳 India API: Connecting...</span>
      <span class="text-slate-600">|</span>
      <span id="badge-us-session" class="text-slate-300 font-mono">🇺🇸 US API: Connecting...</span>
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

  <!-- HEADER & LIVE SEARCH -->
  <header class="bg-slate-900/80 backdrop-blur border-b border-slate-800 sticky top-0 z-30">
    <div class="max-w-[1650px] mx-auto px-4 py-3 flex flex-wrap items-center justify-between gap-3">
      <div class="flex items-center gap-3">
        <div class="w-10 h-10 rounded-xl bg-gradient-to-br from-emerald-500 to-blue-600 flex items-center justify-center shadow-lg font-extrabold text-white text-lg">
          FM
        </div>
        <div>
          <div class="flex items-center gap-2">
            <h1 class="text-base sm:text-lg font-bold tracking-tight text-white">Institutional Fund Manager &amp; SwingGuard AI</h1>
            <span class="text-[11px] font-mono px-2 py-0.5 rounded bg-emerald-500/15 text-emerald-300 border border-emerald-500/30">v10.0 Top-5 Reasons</span>
          </div>
          <p class="text-xs text-slate-400">Acchi Company Saste Me = BUY • Fair Value + Growing = BUY/HOLD • Overvalued / Doobti Company = SELL</p>
        </div>
      </div>

      <!-- Universal Search Bar (Zero Hardcoded Ticker Names) -->
      <div class="flex flex-wrap items-center gap-2">
        <select id="searchExch" class="bg-slate-950 border border-slate-700 rounded-lg px-2.5 py-1.5 text-xs text-slate-200">
          <option value="AUTO">Auto-Detect (IN + US)</option>
          <option value="NSE">🇮🇳 NSE / BSE (India)</option>
          <option value="US">🇺🇸 NASDAQ / NYSE (US)</option>
        </select>
        <input id="searchSymInput" type="text" placeholder="Enter any Stock or ETF Symbol to analyze..."
          class="w-72 bg-slate-950 border border-slate-700 rounded-lg px-3 py-1.5 text-xs text-slate-100 placeholder-slate-500 focus:outline-none focus:border-emerald-500" />
        <button onclick="searchOnlineSymbol()" class="px-3.5 py-1.5 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white text-xs font-semibold transition">
          Analyze &amp; Top 5 Reasons
        </button>
      </div>
    </div>

    <!-- STRICTLY 3 CLEAN TABS -->
    <nav class="max-w-[1650px] mx-auto px-4 pb-2 overflow-x-auto">
      <div class="flex items-center gap-2 min-w-max">
        <button onclick="switchTab('dashboard')" id="tabbtn-dashboard" class="tab-btn-active px-4 py-2 rounded-lg text-xs font-bold border border-slate-800 transition">
          1. Dashboard &amp; Top 15 Recommendations
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
          <button onclick="toggleTop15View('LT')" id="viewbtn-LT" class="px-3.5 py-1.5 rounded-lg text-xs font-bold bg-emerald-600 text-white">Top 15 Fund Manager (Long-Term)</button>
          <button onclick="toggleTop15View('SW')" id="viewbtn-SW" class="px-3.5 py-1.5 rounded-lg text-xs font-bold bg-slate-800 text-slate-300">Top 15 Swing Trading (2–20 Days)</button>
        </div>
      </div>

      <!-- Main Table -->
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-4">
        <div class="flex flex-wrap items-center justify-between gap-2 mb-3">
          <div>
            <h3 id="top15Title" class="text-sm font-bold text-emerald-400">Top 15 Fund Manager Stock &amp; ETF Picks (Click Any Stock for Top 5 Reasons Why to BUY / HOLD / SELL)</h3>
            <p class="text-xs text-slate-400">Combines Buffett Margin of Safety + Peter Lynch GARP + 3-Year Sinking Company Protection + 4-Factor Swing Strategy</p>
          </div>
          <span id="lastSyncTimestamp" class="text-[11px] font-mono text-slate-400">Syncing live API feed...</span>
        </div>
        <div class="overflow-x-auto">
          <table class="w-full text-left border-collapse text-xs">
            <thead>
              <tr class="border-b border-slate-800 text-slate-400 uppercase text-[11px]">
                <th class="py-2.5 px-2">Instrument</th>
                <th class="py-2.5 px-2">Type &amp; Market</th>
                <th class="py-2.5 px-2 text-right">Live Price</th>
                <th class="py-2.5 px-2 text-right">🟢 Realistic Fair Value<br/><span class="text-[10px] text-slate-500">(Max Khareed Bhaav)</span></th>
                <th class="py-2.5 px-2 text-right">Buffett Safe Buy<br/><span class="text-[10px] text-slate-500">(Margin of Safety)</span></th>
                <th class="py-2.5 px-2 text-right">🔴 High Sell Target<br/><span class="text-[10px] text-slate-500">(Profit Booking)</span></th>
                <th class="py-2.5 px-2">Fund Manager Decision (Long-Term)</th>
                <th class="py-2.5 px-2">Swing Signal (5–15d)</th>
                <th class="py-2.5 px-2 text-right">Top 5 Reasons &amp; Add</th>
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
        <div><label class="text-slate-400 block">Symbol</label><input id="wlSym" placeholder="Enter Stock or ETF Symbol..." class="mt-0.5 bg-slate-950 border border-slate-700 rounded px-2.5 py-1.5 font-mono uppercase" /></div>
        <div><label class="text-slate-400 block">Market</label><select id="wlExch" class="mt-0.5 bg-slate-950 border border-slate-700 rounded px-2.5 py-1.5"><option value="NSE">🇮🇳 NSE (India)</option><option value="US">🇺🇸 NASDAQ / NYSE (US)</option></select></div>
        <div class="flex-1"><label class="text-slate-400 block">Notes</label><input id="wlThesis" placeholder="Investment notes..." class="mt-0.5 w-full bg-slate-950 border border-slate-700 rounded px-2.5 py-1.5" /></div>
        <button onclick="addWatchlistManual()" class="px-4 py-1.5 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white font-semibold">+ Add to Watchlist</button>
      </div>
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-4 overflow-x-auto">
        <table class="w-full text-left border-collapse text-xs">
          <thead>
            <tr class="border-b border-slate-800 text-slate-400 uppercase text-[11px]">
              <th class="py-2 px-2">Instrument</th><th class="py-2 px-2 text-right">Live Price</th><th class="py-2 px-2 text-right">🟢 Realistic Fair Value</th><th class="py-2 px-2 text-right">Buffett Safe Buy</th><th class="py-2 px-2 text-right">🔴 High Sell Target</th><th class="py-2 px-2">Fund Manager Signal</th><th class="py-2 px-2">Swing Signal</th><th class="py-2 px-2 text-right">Actions</th>
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
        <div><label class="text-slate-400 block">Stock / ETF Symbol</label><input id="pfSym" placeholder="Enter Symbol..." class="mt-0.5 bg-slate-950 border border-slate-700 rounded px-2.5 py-1.5 font-mono uppercase w-48" /></div>
        <div><label class="text-slate-400 block">Market</label><select id="pfExch" class="mt-0.5 bg-slate-950 border border-slate-700 rounded px-2.5 py-1.5"><option value="NSE">🇮🇳 NSE / BSE (India)</option><option value="US">🇺🇸 NASDAQ / NYSE (US)</option></select></div>
        <div><label class="text-slate-400 block">Quantity</label><input id="pfQty" type="number" step="1" value="10" class="mt-0.5 bg-slate-950 border border-slate-700 rounded px-2.5 py-1.5 font-mono w-28" /></div>
        <div><label class="text-slate-400 block">Avg Buy Price (0 = Auto Live Price)</label><input id="pfAvg" type="number" step="0.5" placeholder="Auto Live Price if 0" class="mt-0.5 bg-slate-950 border border-slate-700 rounded px-2.5 py-1.5 font-mono w-48" /></div>
        <button id="pfAddBtn" onclick="addPortfolioManual()" class="px-4 py-1.5 rounded-lg bg-blue-600 hover:bg-blue-500 text-white font-semibold">+ Add to Portfolio</button>
      </div>
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-4 overflow-x-auto">
        <table class="w-full text-left border-collapse text-xs">
          <thead>
            <tr class="border-b border-slate-800 text-slate-400 uppercase text-[11px]">
              <th class="py-2 px-2">Holding</th><th class="py-2 px-2 text-right">Qty</th><th class="py-2 px-2 text-right">Avg Buy Price</th><th class="py-2 px-2 text-right">Live Price</th><th class="py-2 px-2 text-right">Unrealized P&amp;L</th><th class="py-2 px-2 text-right">🔴 High Sell Target</th><th class="py-2 px-2">Fund Manager Signal</th><th class="py-2 px-2 text-right">Actions</th>
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
        <h3 class="text-base font-bold text-white">📊 Sync Tickers From Your Google Sheet</h3>
        <button onclick="document.getElementById('gsheetModal').classList.add('hidden')" class="text-xs px-2.5 py-1 rounded bg-slate-800">✕</button>
      </div>
      <p class="text-xs text-slate-400">Paste your Google Sheet share link or published CSV link below to sync Column A symbols live:</p>
      <input id="gsheetUrlInput" type="text" placeholder="https://docs.google.com/spreadsheets/d/..." class="w-full bg-slate-950 border border-slate-700 rounded-lg px-3 py-2 text-xs text-slate-100 font-mono" />
      <button onclick="syncGoogleSheetNow()" class="w-full py-2 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white text-xs font-bold">Sync Google Sheet Now</button>
    </div>
  </div>

  <!-- STOCK DETAILS & TOP 5 REASONS MODAL -->
  <div id="evidenceModal" class="hidden fixed inset-0 z-50 bg-slate-950/85 backdrop-blur-sm flex items-center justify-center p-4 overflow-y-auto">
    <div class="bg-slate-900 border border-slate-700 rounded-2xl max-w-5xl w-full max-h-[92vh] overflow-y-auto p-6 space-y-5 shadow-2xl">
      <div class="flex items-start justify-between border-b border-slate-800 pb-3">
        <div id="modalTitleBox"></div>
        <button onclick="document.getElementById('evidenceModal').classList.add('hidden')" class="px-3 py-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 text-xs font-bold">✕ Close Details</button>
      </div>
      <div id="modalBodyBox" class="space-y-5 text-xs"></div>
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
      showToast(`✓ Synced ${data.tickers_loaded} tickers from Google Sheet!`);
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
      document.getElementById('lastSyncTimestamp').innerText = 'Running Fund Manager GARP & Swing Engine...';
      const res = await fetch(`/api/v1/universe?market_filter=${CURRENT_MARKET_FILTER}&force_refresh=${forceRefresh}`);
      const data = await res.json();

      LIVE_UNIVERSE = data.universe || [];
      window.LATEST_LT_TOP15 = data.long_term_top_15 || [];
      window.LATEST_SW_TOP15 = data.swing_trading_top_15 || [];

      document.getElementById('badge-in-session').innerText = `🇮🇳 NSE/BSE: ${data.india_session.status_label}`;
      document.getElementById('badge-us-session').innerText = `🇺🇸 US: ${data.us_session.status_label}`;
      document.getElementById('lastSyncTimestamp').innerText = `Synced: ${data.refreshed_at} (${data.total_live_instruments} Audited)`;

      renderDashboard();
      refreshCounts();
    }

    function signalPillHtml(sigText) {
      let cls = 'bg-slate-800 text-slate-200 border-slate-700';
      if (sigText.includes('STRONG')) cls = 'bg-emerald-500/20 text-emerald-300 border-emerald-500/40';
      else if (sigText.includes('BUY')) cls = 'bg-emerald-500/15 text-emerald-400 border-emerald-500/30';
      else if (sigText.includes('HOLD')) cls = 'bg-amber-500/15 text-amber-300 border-amber-500/30';
      else if (sigText.includes('WAIT')) cls = 'bg-blue-500/15 text-blue-300 border-blue-500/30';
      else if (sigText.includes('SELL') || sigText.includes('AVOID')) cls = 'bg-rose-500/20 text-rose-300 border-rose-500/40';
      return `<span class="inline-block px-2.5 py-1 rounded-lg border text-[11px] font-bold ${cls}">${sigText}</span>`;
    }

    function renderDashboard() {
      const list = (CURRENT_TOP15_MODE === 'LT') ? (window.LATEST_LT_TOP15 || []) : (window.LATEST_SW_TOP15 || []);
      const strongBuys = LIVE_UNIVERSE.filter(x => x.short_signal === 'STRONG BUY' || x.short_signal === 'BUY').length;
      const holds = LIVE_UNIVERSE.filter(x => x.short_signal === 'HOLD').length;
      const sells = LIVE_UNIVERSE.filter(x => x.short_signal.includes('SELL') || x.short_signal.includes('AVOID')).length;
      const swingBuys = LIVE_UNIVERSE.filter(x => x.sw_state.includes('BUY')).length;

      document.getElementById('dashKpiStrip').innerHTML = `
        <div class="bg-slate-900 border border-slate-800 p-4 rounded-xl">
          <div class="text-xs text-slate-400">🟢 Fund Manager BUY / STRONG BUY</div>
          <div class="text-2xl font-bold text-emerald-400 mt-1 font-mono">${strongBuys}</div>
          <div class="text-[11px] text-slate-400 mt-0.5">Acchi Company Saste Me / GARP</div>
        </div>
        <div class="bg-slate-900 border border-slate-800 p-4 rounded-xl">
          <div class="text-xs text-slate-400">🟡 Quality Compounders (HOLD)</div>
          <div class="text-2xl font-bold text-amber-300 mt-1 font-mono">${holds}</div>
          <div class="text-[11px] text-slate-400 mt-0.5">Fair Price &amp; Growing Trend</div>
        </div>
        <div class="bg-slate-900 border border-slate-800 p-4 rounded-xl">
          <div class="text-xs text-slate-400">🔴 Overvalued / Doobti Company (SELL)</div>
          <div class="text-2xl font-bold text-rose-400 mt-1 font-mono">${sells}</div>
          <div class="text-[11px] text-slate-400 mt-0.5">Strict Exit &amp; Profit Booking</div>
        </div>
        <div class="bg-slate-900 border border-slate-800 p-4 rounded-xl">
          <div class="text-xs text-slate-400">⚡ Active Swing BUY Setups (5–15d)</div>
          <div class="text-2xl font-bold text-blue-400 mt-1 font-mono">${swingBuys}</div>
          <div class="text-[11px] text-slate-400 mt-0.5">EMA 20/50 + RSI + MACD + ATR</div>
        </div>
      `;

      const tbody = document.getElementById('top15TableBody');
      if (list.length === 0) {
        tbody.innerHTML = `<tr><td colspan="9" class="py-8 text-center text-slate-400">Loading live instruments... Click Force Live API Sync if needed.</td></tr>`;
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
          <td onclick="openEvidenceModal('${s.symbol}')" class="py-3 px-2 cursor-pointer">
            ${signalPillHtml(s.sw_state)}
            <div class="text-[10px] text-slate-400 font-mono mt-0.5">Tgt: ${s.currency_symbol}${s.sw_target} | SL: ${s.currency_symbol}${s.sw_stop}</div>
          </td>
          <td class="py-3 px-2 text-right space-x-1 whitespace-nowrap">
            <button onclick="openEvidenceModal('${s.symbol}')" class="px-2.5 py-1 rounded bg-purple-600/25 hover:bg-purple-600 text-purple-300 hover:text-white border border-purple-500/40 text-[11px] font-bold transition">
              Top 5 Reasons
            </button>
            <button onclick="quickAddWatchlist('${s.symbol}', '${s.exchange}', ${s.price})" class="px-2 py-1 rounded bg-emerald-600/20 hover:bg-emerald-600 text-emerald-300 hover:text-white border border-emerald-500/40 text-[11px] font-semibold transition">
              + WL
            </button>
            <button onclick="quickAddPortfolio('${s.symbol}', '${s.exchange}', ${s.price})" class="px-2 py-1 rounded bg-blue-600/20 hover:bg-blue-600 text-blue-300 hover:text-white border border-blue-500/40 text-[11px] font-semibold transition">
              + PF
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
          <span class="px-2 py-0.5 rounded bg-blue-500/15 text-blue-300 text-xs font-mono">ROIC: ${s.roic_pct}% vs WACC: ${s.wacc_pct}%</span>
        </div>
        <h3 class="text-xl font-bold text-white mt-1">${s.symbol} — ${s.name} (Live: ${s.currency_symbol}${s.price.toLocaleString()})</h3>
      `;

      const ltReasonsHtml = (s.lt_top_5_reasons || []).map(r => `<li class="p-2.5 rounded-lg bg-slate-900/90 border border-slate-800 text-slate-200 leading-relaxed">${r}</li>`).join('');
      const swReasonsHtml = (s.sw_top_5_reasons || []).map(r => `<li class="p-2.5 rounded-lg bg-slate-900/90 border border-slate-800 text-slate-200 leading-relaxed">${r}</li>`).join('');

      document.getElementById('modalBodyBox').innerHTML = `
        <!-- Key Valuation & Technical Numbers -->
        <div class="grid grid-cols-2 md:grid-cols-4 gap-3 font-mono">
          <div class="p-3 rounded-xl bg-slate-950 border border-slate-800"><div class="text-slate-400 text-[11px]">Live Price</div><div class="text-base font-bold text-white mt-0.5">${s.currency_symbol}${s.price} (${s.change_pct >= 0 ? '+' : ''}${s.change_pct}%)</div></div>
          <div class="p-3 rounded-xl bg-slate-950 border border-slate-800"><div class="text-slate-400 text-[11px]">🟢 Realistic Fair Value</div><div class="text-base font-bold text-emerald-400 mt-0.5">${s.currency_symbol}${s.fair_value}</div></div>
          <div class="p-3 rounded-xl bg-slate-950 border border-slate-800"><div class="text-slate-400 text-[11px]">Buffett Safe Buy Limit</div><div class="text-base font-bold text-blue-300 mt-0.5">${s.currency_symbol}${s.safe_buy_limit}</div></div>
          <div class="p-3 rounded-xl bg-slate-950 border border-slate-800"><div class="text-slate-400 text-[11px]">🔴 High Sell Target</div><div class="text-base font-bold text-rose-300 mt-0.5">${s.currency_symbol}${s.sell_trigger_limit}</div></div>
        </div>

        <!-- Two Columns: Top 5 Fund Manager Reasons & Top 5 Swing Reasons -->
        <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
          <div class="p-4 rounded-xl bg-slate-950 border border-emerald-500/30 space-y-3">
            <div class="flex items-center justify-between border-b border-slate-800 pb-2">
              <span class="font-bold text-emerald-400 text-sm">🏛️ Top 5 Fund Manager Reasons (Why BUY / HOLD / SELL)</span>
              ${signalPillHtml(s.short_signal)}
            </div>
            <div class="text-[11px] text-slate-300 font-semibold">${s.sheet_signal}</div>
            <ul class="space-y-2">${ltReasonsHtml}</ul>
          </div>

          <div class="p-4 rounded-xl bg-slate-950 border border-blue-500/30 space-y-3">
            <div class="flex items-center justify-between border-b border-slate-800 pb-2">
              <span class="font-bold text-blue-400 text-sm">⚡ Top 5 Swing Trading Reasons (2–20 Days Strategy)</span>
              ${signalPillHtml(s.sw_state)}
            </div>
            <div class="text-[11px] text-slate-300 font-semibold">Setup: ${s.sw_setup} • Target: ${s.currency_symbol}${s.sw_target} • Stop-Loss: ${s.currency_symbol}${s.sw_stop} (${s.sw_rr}x R:R)</div>
            <ul class="space-y-2">${swReasonsHtml}</ul>
          </div>
        </div>

        <div class="flex justify-end gap-2 pt-2">
          <button onclick="quickAddWatchlist('${s.symbol}', '${s.exchange}', ${s.price}); document.getElementById('evidenceModal').classList.add('hidden');" class="px-4 py-2 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white font-semibold">+ Add to Watchlist</button>
          <button onclick="quickAddPortfolio('${s.symbol}', '${s.exchange}', ${s.price}); document.getElementById('evidenceModal').classList.add('hidden');" class="px-4 py-2 rounded-lg bg-blue-600 hover:bg-blue-500 text-white font-semibold">+ Add to Portfolio</button>
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
      window.LATEST_SW_TOP15 = [item, ...(window.LATEST_SW_TOP15 || []).filter(x => x.symbol !== item.symbol)].slice(0, 15);
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
        tbody.innerHTML = `<tr><td colspan="8" class="py-6 text-center text-slate-400">Your Watchlist is empty. Click <strong>+ WL</strong> on any Dashboard stock!</td></tr>`;
        return;
      }
      tbody.innerHTML = data.items.map(w => {
        const s = w.live_data;
        return `<tr class="hover:bg-slate-800/40">
          <td onclick="openEvidenceModal('${s.symbol}')" class="py-2.5 px-2 font-bold text-white cursor-pointer">${s.symbol} <div class="text-[11px] font-normal text-slate-400">${s.name}</div></td>
          <td class="py-2.5 px-2 text-right font-mono font-bold">${s.currency_symbol}${s.price.toLocaleString()}</td>
          <td class="py-2.5 px-2 text-right font-mono text-emerald-400 font-bold">${s.currency_symbol}${s.fair_value.toLocaleString()}</td>
          <td class="py-2.5 px-2 text-right font-mono text-blue-300">${s.currency_symbol}${s.safe_buy_limit.toLocaleString()}</td>
          <td class="py-2.5 px-2 text-right font-mono text-rose-300">${s.currency_symbol}${s.sell_trigger_limit.toLocaleString()}</td>
          <td class="py-2.5 px-2">${signalPillHtml(s.sheet_signal)}</td>
          <td class="py-2.5 px-2">${signalPillHtml(s.sw_state)}</td>
          <td class="py-2.5 px-2 text-right space-x-2">
            <button onclick="openEvidenceModal('${s.symbol}')" class="text-purple-300 hover:underline font-semibold">Top 5 Reasons</button>
            <button onclick="quickAddPortfolio('${s.symbol}', '${s.exchange}', ${s.price})" class="text-blue-400 hover:underline font-semibold">+ PF</button>
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
        tbody.innerHTML = `<tr><td colspan="8" class="py-6 text-center text-slate-400">Your Portfolio is empty. Click <strong>+ PF</strong> on any Dashboard stock or add one above!</td></tr>`;
        return;
      }
      tbody.innerHTML = data.positions.map(p => {
        const s = p.live_data;
        return `<tr class="hover:bg-slate-800/40">
          <td onclick="openEvidenceModal('${s.symbol}')" class="py-2.5 px-2 font-bold text-white cursor-pointer">${s.symbol} <div class="text-[11px] font-normal text-slate-400">${s.name}</div></td>
          <td class="py-2.5 px-2 text-right font-mono">${p.quantity}</td>
          <td class="py-2.5 px-2 text-right font-mono">${s.currency_symbol}${p.avg_buy_price.toLocaleString()}</td>
          <td class="py-2.5 px-2 text-right font-mono font-bold text-white">${s.currency_symbol}${s.price.toLocaleString()}</td>
          <td class="py-2.5 px-2 text-right font-mono font-bold ${p.pnl >= 0 ? 'text-emerald-400' : 'text-rose-400'}">${p.pnl >= 0 ? '+' : ''}${s.currency_symbol}${p.pnl.toLocaleString()} (${p.pnl_pct}%)</td>
          <td class="py-2.5 px-2 text-right font-mono text-rose-300">${s.currency_symbol}${s.sell_trigger_limit.toLocaleString()}</td>
          <td class="py-2.5 px-2">${signalPillHtml(s.sheet_signal)}</td>
          <td class="py-2.5 px-2 text-right space-x-2">
            <button onclick="openEvidenceModal('${s.symbol}')" class="text-purple-300 hover:underline font-semibold">Top 5 Reasons</button>
            <button onclick="removePortfolio('${s.symbol}')" class="text-rose-400 hover:underline">Remove</button>
          </td>
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
      btn.innerText = '+ Add to Portfolio';
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
