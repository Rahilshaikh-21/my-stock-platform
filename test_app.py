import asyncio
import math
import unittest
from fastapi.testclient import TestClient

import app as platform


class TestFinancialValidationAndDiscrepancyEngine(unittest.TestCase):
    """Tests Sections 3, 4, 5, 6, 7 & 12: Financial formulas, edge cases, and AI discrepancy engine."""

    def setUp(self):
        platform.AUDIT_TRAIL_STORE.clear()
        platform.SYMBOL_CACHE.clear()
        platform.FUNDAMENTALS_CACHE.clear()
        platform.UNIVERSE_CACHE.clear()
        platform.USER_STOCK_OVERRIDES.clear()
        platform.STOCK_CHAT_HISTORY.clear()

    def test_parse_optional_float_distinguishes_zero_from_missing(self):
        self.assertEqual(platform._parse_optional_float("0"), 0.0)
        self.assertEqual(platform._parse_optional_float("0.00%"), 0.0)
        self.assertEqual(platform._parse_optional_float("₹0"), 0.0)
        self.assertEqual(platform._parse_optional_float(0), 0.0)
        self.assertEqual(platform._parse_optional_float("117."), 117.0)
        self.assertEqual(platform._parse_optional_float("(₹1,250.50)"), -1250.50)
        for missing_val in (None, "", "-", "—", "–", "N/A", "null", "NaN", float("nan"), float("inf")):
            self.assertIsNone(platform._parse_optional_float(missing_val))

    def test_unverified_fundamentals_never_fabricated(self):
        closes = [100.0 + i * 0.5 for i in range(300)]
        res = platform.validate_and_reconcile_fundamentals(
            symbol="UNVERIFIEDCO",
            company_name="Unverified Corp Ltd",
            country="IN",
            live_price=250.0,
            closes_3y=closes,
            raw_metrics={},
            source_tag="UNVERIFIED",
        )
        self.assertFalse(res["fundamentals_verified"])
        self.assertIsNone(res["pe"])
        self.assertIsNone(res["eps"])
        self.assertIsNone(res["roic"])
        self.assertIsNone(res["roe"])

        eval_out = platform.evaluate_institutional_engine(
            symbol="UNVERIFIEDCO",
            name="Unverified Corp Ltd",
            country="IN",
            exchange="NSE",
            currency="INR",
            asset_type="EQUITY",
            price=250.0,
            prev_close=248.0,
            change_pct=0.8,
            closes_3y=closes,
            timestamps_3y=[],
            vol_ratio=1.1,
            real_fundamentals=res,
            provider_name="NSE_BSE_LIVE_API",
        )
        self.assertFalse(eval_out["fundamentals_verified"])
        self.assertNotIn("BUY", eval_out["short_signal"])
        self.assertEqual(eval_out["decision_category"], "HOLD")
        self.assertIn("Unverified", eval_out["eps_display"])

    def test_embedded_fundamentals_registry_is_disabled(self):
        # Production metrics must come from live providers, never bundled snapshots.
        self.assertEqual(platform.INSTITUTIONAL_FUNDAMENTALS_REGISTRY, {})


    def test_negative_or_zero_earnings_invalidates_positive_pe(self):
        closes = [200.0 - i * 0.3 for i in range(260)]
        res = platform.validate_and_reconcile_fundamentals(
            symbol="LOSSCO",
            company_name="Loss Making Industries",
            country="IN",
            live_price=120.0,
            closes_3y=closes,
            raw_metrics={"eps": -12.5, "pe": 18.4, "roic": -4.2, "roe": -8.0, "book_value": 85.0},
            source_tag="SCREENER_LIVE",
        )
        self.assertIsNone(res["pe"])
        self.assertEqual(res["eps"], -12.5)
        codes = [d["code"] for d in res["discrepancies"]]
        self.assertIn("LOSS_MAKING_POSITIVE_PE", codes)

        eval_out = platform.evaluate_institutional_engine(
            symbol="LOSSCO",
            name="Loss Making Industries",
            country="IN",
            exchange="NSE",
            currency="INR",
            asset_type="EQUITY",
            price=120.0,
            prev_close=122.0,
            change_pct=-1.6,
            closes_3y=closes,
            timestamps_3y=[],
            vol_ratio=1.0,
            real_fundamentals=res,
            provider_name="NSE_BSE_LIVE_API",
        )
        self.assertEqual(eval_out["decision_category"], "SELL")
        self.assertIn("SELL", eval_out["short_signal"])

    def test_negative_shareholder_equity_invalidates_false_positive_roe(self):
        closes = [150.0] * 260
        res = platform.validate_and_reconcile_fundamentals(
            symbol="NEGEQ",
            company_name="Distressed Equity Corp",
            country="US",
            live_price=150.0,
            closes_3y=closes,
            raw_metrics={"eps": -5.0, "book_value": -20.0, "roe": 25.0, "pb_ratio": 3.5},
            source_tag="FINNHUB_LIVE",
        )
        self.assertTrue(res["negative_equity"])
        self.assertIsNone(res["roe"])
        self.assertIsNone(res["pb_ratio"])
        codes = [d["code"] for d in res["discrepancies"]]
        self.assertIn("NEGATIVE_EQUITY_FALSE_ROE", codes)

    def test_price_eps_pe_discrepancy_detected_and_corrected(self):
        closes = [900.0 + i * 0.4 for i in range(260)]
        res = platform.validate_and_reconcile_fundamentals(
            symbol="TATASTEEL",
            company_name="Tata Steel Ltd",
            country="IN",
            live_price=1000.0,
            closes_3y=closes,
            raw_metrics={"eps": 50.0, "pe": 32.0, "roic": 18.0, "roe": 16.5, "book_value": 310.0},
            source_tag="SCREENER_LIVE",
        )
        self.assertEqual(res["pe"], 20.0)
        self.assertEqual(res["original_values"]["pe"], 32.0)
        codes = [d["code"] for d in res["discrepancies"]]
        self.assertIn("PRICE_EPS_PE_DISCREPANCY", codes)
        self.assertEqual(res["provenance"]["pe"]["status"], "DISCREPANCY_CORRECTED")

    def test_market_cap_and_dividend_yield_validation(self):
        closes = [500.0] * 260
        res = platform.validate_and_reconcile_fundamentals(
            symbol="MCAPTEST",
            company_name="Mcap Test Co",
            country="IN",
            live_price=500.0,
            closes_3y=closes,
            raw_metrics={
                "eps": 25.0,
                "pe": 20.0,
                "roic": 16.0,
                "shares_outstanding": 100.0,
                "market_cap": 75000.0,
                "dividend_yield": 42.0,
            },
            source_tag="SCREENER_LIVE",
        )
        self.assertEqual(res["market_cap"], 50000.0)
        self.assertIsNone(res["dividend_yield"])
        codes = [d["code"] for d in res["discrepancies"]]
        self.assertIn("MCAP_SHARES_PRICE_MISMATCH", codes)
        self.assertIn("DIVIDEND_YIELD_OUT_OF_BOUNDS", codes)

    def test_sector_aware_bank_valuation_and_value_trap_detection(self):
        closes = [650.0 + i * 0.2 for i in range(300)]
        bank_fund = platform.validate_and_reconcile_fundamentals(
            symbol="HDFCBANK",
            company_name="HDFC Bank Ltd",
            country="IN",
            live_price=700.0,
            closes_3y=closes,
            raw_metrics={"eps": 45.0, "pe": 15.56, "roe": 16.8, "book_value": 295.0},
            source_tag="SCREENER_LIVE",
        )
        self.assertTrue(bank_fund["is_bank_or_fin"])
        bank_eval = platform.evaluate_institutional_engine(
            symbol="HDFCBANK",
            name="HDFC Bank Ltd",
            country="IN",
            exchange="NSE",
            currency="INR",
            asset_type="EQUITY",
            price=700.0,
            prev_close=695.0,
            change_pct=0.72,
            closes_3y=closes,
            timestamps_3y=[],
            vol_ratio=1.15,
            real_fundamentals=bank_fund,
            provider_name="NSE_BSE_LIVE_API",
        )
        self.assertEqual(
            bank_eval["valuation_assumptions"]["valuation_method"],
            "FINANCIALS_EXCESS_RETURN_JUSTIFIED_PB_AND_EARNINGS",
        )
        self.assertTrue(bank_eval["safe_buy_limit"] < bank_eval["fair_value"] < bank_eval["sell_trigger_limit"])

        declining_closes = [300.0 - i * 0.4 for i in range(300)]
        trap_fund = platform.validate_and_reconcile_fundamentals(
            symbol="TRAPCO",
            company_name="High Debt Cyclical Trap Ltd",
            country="IN",
            live_price=180.0,
            closes_3y=declining_closes,
            raw_metrics={"eps": 24.0, "pe": 7.5, "roic": 4.5, "roe": 5.0, "debt_to_equity": 2.1},
            source_tag="SCREENER_LIVE",
        )
        trap_eval = platform.evaluate_institutional_engine(
            symbol="TRAPCO",
            name="High Debt Cyclical Trap Ltd",
            country="IN",
            exchange="NSE",
            currency="INR",
            asset_type="EQUITY",
            price=180.0,
            prev_close=182.0,
            change_pct=-1.1,
            closes_3y=declining_closes,
            timestamps_3y=[],
            vol_ratio=0.9,
            real_fundamentals=trap_fund,
            provider_name="NSE_BSE_LIVE_API",
        )
        self.assertTrue(trap_eval["is_value_trap"])
        self.assertEqual(trap_eval["decision_category"], "SELL")
        self.assertIn("TRAP", trap_eval["short_signal"])

    def test_high_quality_compounder_at_expensive_price_is_not_confused_with_buy(self):
        closes = [1000.0 + i * 2.5 for i in range(300)]
        expensive_fund = platform.validate_and_reconcile_fundamentals(
            symbol="EXPENSIVECOMP",
            company_name="Quality Compounder Ltd",
            country="IN",
            live_price=1920.0,
            closes_3y=closes,
            raw_metrics={"eps": 40.0, "pe": 48.0, "roic": 28.0, "roe": 26.0, "debt_to_equity": 0.1},
            source_tag="SCREENER_LIVE",
        )
        eval_out = platform.evaluate_institutional_engine(
            symbol="EXPENSIVECOMP",
            name="Quality Compounder Ltd",
            country="IN",
            exchange="NSE",
            currency="INR",
            asset_type="EQUITY",
            price=1920.0,
            prev_close=1910.0,
            change_pct=0.5,
            closes_3y=closes,
            timestamps_3y=[],
            vol_ratio=1.0,
            real_fundamentals=expensive_fund,
            provider_name="NSE_BSE_LIVE_API",
        )
        self.assertLess(eval_out["mos_pct"], 0.0)
        self.assertIn(eval_out["decision_category"], ("HOLD", "SELL"))

    def test_hdfcbank_screenshot_fair_value_and_swing_rsi_consistency(self):
        closes = [753.03 + (i % 9 - 4) * 3.5 for i in range(500)] + [788.93 - i * 0.45 for i in range(195)] + [690.0, 692.0, 694.0, 693.5, 695.25]
        fund = platform.validate_and_reconcile_fundamentals(
            symbol="HDFCBANK",
            company_name="HDFC Bank Limited",
            country="IN",
            live_price=695.25,
            closes_3y=closes,
            raw_metrics={"pe": 13.4, "roe": 13.6, "book_value": 391.0},
            source_tag="SCREENER_LIVE",
        )
        out = platform.evaluate_institutional_engine(
            symbol="HDFCBANK",
            name="HDFC Bank Limited",
            country="IN",
            exchange="NSE",
            currency="INR",
            asset_type="EQUITY",
            price=695.25,
            prev_close=694.07,
            change_pct=0.17,
            closes_3y=closes,
            timestamps_3y=[],
            vol_ratio=0.5,
            real_fundamentals=fund,
            provider_name="NSE_BSE_LIVE_API",
        )
        self.assertGreater(out["valuation_assumptions"]["assumed_stage1_growth_pct"], 8.0)
        self.assertGreater(out["fair_value"], 800.0)
        self.assertGreater(out["mos_pct"], 12.0)
        self.assertEqual(out["decision_category"], "BUY")
        self.assertIn("BUY", out["short_signal"])
        self.assertLess(out["rsi_14"], 74.0)
        self.assertNotIn("Overbought RSI", out["sw_setup"])

    def test_closed_loop_gemini_verifier_corrects_wrong_buy_sell_and_fair_value(self):
        buggy_hdfc_payload = {
            "symbol": "HDFCBANK",
            "name": "HDFC Bank Limited",
            "country": "IN",
            "exchange": "NSE",
            "currency": "INR",
            "currency_symbol": "₹",
            "raw_asset_type": "EQUITY",
            "price": 695.25,
            "fair_value": 495.63,
            "safe_buy_limit": 421.29,
            "sell_trigger_limit": 604.67,
            "mos_pct": -40.3,
            "pe_ratio": 13.4,
            "pb_ratio": 1.78,
            "book_value": 391.0,
            "roic_pct": 13.6,
            "roe_pct": 13.6,
            "debt_to_equity": 0.0,
            "wacc_pct": 11.24,
            "cost_of_equity_pct": 11.24,
            "is_bank_or_fin": True,
            "is_value_trap": False,
            "fundamentals_verified": True,
            "short_signal": "SELL / BOOK PROFIT",
            "action_badge": "🔴💰 SELL / BOOK PROFIT",
            "decision_category": "SELL",
            "lt_confidence": 38.0,
            "sw_state": "SELL / BOOK",
            "sw_setup": "Overbought RSI >= 74 / Momentum Exhaustion",
            "sw_confidence": 40.0,
            "sw_stop": 672.69,
            "sw_target": 744.88,
            "rsi_14": 41.9,
            "macd_hist": -1.29,
            "ema_20": 711.17,
            "ema_50": 721.41,
            "ema_200": 788.93,
            "change_pct": 0.17,
            "discrepancies": [],
            "valuation_assumptions": {},
        }
        corrected = platform.run_gemini_closed_loop_signal_verifier(buggy_hdfc_payload)
        self.assertGreater(corrected["fair_value"], 780.0)
        self.assertEqual(corrected["decision_category"], "BUY")
        self.assertIn("BUY", corrected["short_signal"])
        self.assertNotIn("Overbought RSI", corrected["sw_setup"])
        self.assertGreaterEqual(corrected["ai_verification"]["auto_corrections_count"], 2)
        codes = [c["code"] for c in corrected["ai_verification"]["auto_corrections"]]
        self.assertIn("VALUATION_UNDERESTIMATE_AUTO_CORRECTED", codes)
        self.assertIn("SWING_RSI_LABEL_CONTRADICTION_FIXED", codes)

        false_buy_payload = {
            "symbol": "OVERVALUEDCO",
            "name": "Overvalued Corp",
            "country": "IN",
            "exchange": "NSE",
            "currency": "INR",
            "currency_symbol": "₹",
            "raw_asset_type": "EQUITY",
            "price": 1500.0,
            "fair_value": 950.0,
            "safe_buy_limit": 807.5,
            "sell_trigger_limit": 1159.0,
            "mos_pct": -57.9,
            "pe_ratio": 55.0,
            "roic_pct": 14.0,
            "roe_pct": 14.0,
            "wacc_pct": 11.0,
            "cost_of_equity_pct": 11.0,
            "is_bank_or_fin": False,
            "is_value_trap": False,
            "fundamentals_verified": True,
            "short_signal": "STRONG BUY",
            "action_badge": "🟢🔥 STRONG BUY",
            "decision_category": "BUY",
            "lt_confidence": 90.0,
            "sw_state": "STRONG BUY",
            "sw_setup": "Breakout",
            "sw_confidence": 85.0,
            "sw_stop": 1440.0,
            "sw_target": 1620.0,
            "rsi_14": 78.5,
            "macd_hist": 2.1,
            "ema_20": 1420.0,
            "ema_50": 1360.0,
            "ema_200": 1200.0,
            "change_pct": 1.5,
            "discrepancies": [],
            "valuation_assumptions": {},
        }
        fixed_overval = platform.run_gemini_closed_loop_signal_verifier(false_buy_payload)
        self.assertEqual(fixed_overval["decision_category"], "SELL")
        self.assertNotIn("BUY", fixed_overval["short_signal"])
        self.assertNotIn("BUY", fixed_overval["sw_state"])
        overval_codes = [c["code"] for c in fixed_overval["ai_verification"]["auto_corrections"]]
        self.assertIn("FALSE_BUY_SIGNAL_PREVENTED", overval_codes)
        self.assertIn("SWING_OVERBOUGHT_BUY_PREVENTED", overval_codes)


class TestTechnicalIndicatorsAndZeroLookaheadBacktest(unittest.TestCase):
    """Tests Section 6 & 12: Split adjuster, Wilder RSI, True ATR(14), and Zero-Lookahead Backtesting."""

    def test_split_adjuster_preserves_genuine_crash_and_adjusts_exact_split(self):
        split_series = [1000.0, 1010.0, 1020.0, 510.0, 515.0, 520.0]
        adj = platform.clean_and_split_adjust_series(split_series, 520.0)
        self.assertAlmostEqual(adj[0], 500.0, places=1)
        self.assertAlmostEqual(adj[2], 510.0, places=1)

        crash_series = [1000.0, 1005.0, 1000.0, 620.0, 615.0, 610.0]
        adj_crash = platform.clean_and_split_adjust_series(crash_series, 610.0)
        self.assertAlmostEqual(adj_crash[2], 1000.0, places=1)
        self.assertAlmostEqual(adj_crash[3], 620.0, places=1)

        glitch_series = [500.0, 501.0, 820.0, 502.0, 503.0]
        adj_glitch = platform.clean_and_split_adjust_series(glitch_series, 503.0)
        self.assertAlmostEqual(adj_glitch[2], 501.5, places=1)

    def test_wilder_rsi_and_true_ohlc_atr_edge_cases(self):
        flat = [100.0] * 30
        self.assertEqual(platform.compute_wilder_rsi_series(flat, 14)[-1], 50.0)
        rising = [100.0 + i for i in range(30)]
        self.assertEqual(platform.compute_wilder_rsi_series(rising, 14)[-1], 100.0)
        falling = [200.0 - i for i in range(30)]
        self.assertEqual(platform.compute_wilder_rsi_series(falling, 14)[-1], 0.0)

        highs = [105.0] * 30
        lows = [95.0] * 30
        atr_ohlc = platform.compute_atr_14(flat, 100.0, highs=highs, lows=lows)
        self.assertAlmostEqual(atr_ohlc, 10.0, places=1)

    def test_zero_lookahead_backtest_invariance_and_friction_costs(self):
        closes = [
            round(200.0 + 25.0 * math.sin(i / 14.0) + (i * 0.25), 2)
            for i in range(350)
        ]
        bt_1 = platform.run_daily_frame_signal_audit(closes[:260], [], False, "₹")
        closes_mutated = closes[:260] + [9999.0] * 90
        bt_2 = platform.run_daily_frame_signal_audit(closes_mutated[:260], [], False, "₹")
        self.assertEqual(bt_1["signal_log"], bt_2["signal_log"])
        self.assertGreater(bt_1["round_trip_cost_pct"], 0.0)
        self.assertIn("max_drawdown_pct", bt_1)
        self.assertIn("profit_factor", bt_1)
        self.assertIn("out_of_sample_win_rate_pct", bt_1)


class TestPortfolioAccountingAndGSheetSync(unittest.TestCase):
    """Tests Section 8 & 12: Weighted-average cost basis, partial sales, fees, splits, and GSheet CSV parsing."""

    def test_weighted_average_cost_partial_sales_fees_and_splits(self):
        txs = [
            {"date": "2026-01-10", "type": "BUY", "symbol": "INFY", "quantity": 10, "price": 1400.0, "fees": 20.0},
            {"date": "2026-02-15", "type": "BUY", "symbol": "INFY", "quantity": 10, "price": 1600.0, "fees": 20.0},
            {"date": "2026-02-15", "type": "BUY", "symbol": "INFY", "quantity": 10, "price": 1600.0, "fees": 20.0, "row_id": 1},
            {"date": "2026-03-20", "type": "SELL", "symbol": "INFY", "quantity": 5, "price": 1800.0, "fees": 10.0},
            {"date": "2026-04-01", "type": "SPLIT", "symbol": "INFY", "quantity": 1, "price": 0.0, "split_ratio": 2.0},
            {"date": "2026-05-01", "type": "DIVIDEND", "symbol": "INFY", "quantity": 500.0, "price": 1.0, "fees": 0.0},
        ]
        res = platform.calculate_portfolio_from_transactions(txs)
        self.assertEqual(res["duplicates_skipped"], 1)
        self.assertEqual(len(res["positions"]), 1)
        pos = res["positions"][0]
        self.assertEqual(pos["quantity"], 30.0)
        self.assertAlmostEqual(pos["avg_buy_price"], 751.0, places=2)
        self.assertAlmostEqual(pos["realized_pnl"], 1480.0, places=2)
        self.assertAlmostEqual(pos["realized_dividends"], 500.0, places=2)
        self.assertAlmostEqual(pos["total_fees_paid"], 50.0, places=2)

    def test_gsheet_instruments_and_transactions_csv_parsing(self):
        instruments_csv = (
            "Instruments,,\n"
            'TOTAL,,,,,,,,,"₹49,913","₹48,312","-₹1,601"\n'
            "Symbol,Name,Type,Company,Google Finance ticker,Manual price,Current price,Qty held,Avg buy cost (₹)\n"
            "NIFTYBEES,Nippon ETF,ETF,,NSE:NIFTYBEES,₹276.76,₹255.91,117.,₹263.89\n"
            "COALINDIA,Coal India,Stock,Coal India,NSE:COALINDIA,,₹391.55,20.,₹410.50\n"
            "HDFCBANK,HDFC Bank,Stock,HDFC Bank,NSE:HDFCBANK,,₹693.15,15.,₹712.40\n"
            "ITBEES,Nippon IT ETF,ETF,,NSE:ITBEES,,₹35.61,4.,₹35.34\n"
            'ICICIBANK,ICICI Bank,Stock,ICICI Bank,NSE:ICICIBANK,,"₹1,380.80",-,\n'
        )
        positions, has_cost = platform.parse_gsheet_instruments_csv(instruments_csv)
        self.assertTrue(has_cost)
        self.assertEqual(len(positions), 4)
        by_sym = {p["symbol"]: p for p in positions}
        self.assertEqual(by_sym["NIFTYBEES"]["quantity"], 117.0)
        self.assertEqual(by_sym["NIFTYBEES"]["avg_buy_price"], 263.89)
        self.assertEqual(by_sym["COALINDIA"]["quantity"], 20.0)
        self.assertEqual(by_sym["HDFCBANK"]["avg_buy_price"], 712.40)

        tx_csv = (
            "Transactions (yahan apni har BUY / SELL entry karo),,,,,,,\n"
            "Date,Type (BUY/SELL),Symbol,Qty,Price per unit (₹),Fees (₹) optional,Amount (₹)\n"
            '31-Aug-2026,BUY,NIFTYBEES,18.,₹274.57,-,"₹4,942.26"\n'
            '30-Sep-2026,BUY,NIFTYBEES,19.,₹260.15,,"₹4,942.85"\n'
            "14-Oct-2026,BUY,ITBEES,1.,₹33.58,,₹33.58\n"
            "21-Oct-2026,BUY,ITBEES,3.,₹35.93,,₹107.79\n"
            '21-Oct-2026,BUY,HDFCBANK,15.,₹712.40,,"₹10,686.00"\n'
            '21-Oct-2026,BUY,NIFTYBEES,80.,₹262.38,,"₹20,990.40"\n'
            '21-Oct-2026,BUY,COALINDIA,20.,₹410.50,,"₹8,210.00"\n'
        )
        parsed_txs = platform.parse_gsheet_transactions_csv(tx_csv)
        self.assertEqual(len(parsed_txs), 7)
        ledger_calc = platform.calculate_portfolio_from_transactions(parsed_txs)
        pos_map = {p["symbol"]: p for p in ledger_calc["positions"]}
        self.assertEqual(pos_map["NIFTYBEES"]["quantity"], 117.0)
        self.assertAlmostEqual(pos_map["NIFTYBEES"]["avg_buy_price"], 263.8932, places=3)
        self.assertEqual(pos_map["ITBEES"]["quantity"], 4.0)
        self.assertAlmostEqual(pos_map["ITBEES"]["avg_buy_price"], 35.3425, places=3)


class TestFastAPIEndpointsAndSecurity(unittest.TestCase):
    """Tests API endpoints, Stock AI Chatbox & live override, BUY-only Top 15, and UI cleanliness."""

    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(platform.app)

    def test_health_check_and_10500_audit_endpoints(self):
        r_health = self.client.get("/api/v1/system/health")
        self.assertEqual(r_health.status_code, 200)
        h_data = r_health.json()
        self.assertEqual(h_data["status"], "healthy")
        self.assertEqual(h_data["audit_engine"]["layers_active"], 8)
        self.assertEqual(r_health.headers.get("X-Content-Type-Options"), "nosniff")

        r_audit = self.client.get("/api/v1/audit/10500")
        self.assertEqual(r_audit.status_code, 200)
        a_data = r_audit.json()
        self.assertEqual(a_data["total_instruments_tested"], 10500)
        self.assertEqual(a_data["mathematical_or_nan_errors"], 0)
        self.assertEqual(a_data["buy_fair_sell_ordering_errors"], 0)
        self.assertEqual(a_data["swing_stop_target_errors"], 0)

    def test_frontend_html_removes_backtest_and_stress_test_and_includes_stock_chatbox(self):
        r_home = self.client.get("/")
        self.assertEqual(r_home.status_code, 200)
        html = r_home.text
        # Must NOT contain hardcoded secrets
        self.assertNotIn("f0200da2-cba1-419c-a955-0d7671783ff7", html)
        self.assertNotIn("eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9", html)
        # Must NOT contain Backtest Win Rate, Sahi/Galat table, or 10,500 Stress-Test window in UI
        self.assertNotIn("Zero-Lookahead BUY &amp; SELL Backtest Log", html)
        self.assertNotIn("Sahi:", html)
        self.assertNotIn("Galat:", html)
        self.assertNotIn("openAudit10500Modal", html)
        self.assertNotIn("audit10500Modal", html)
        # MUST contain the interactive Stock AI Chatbot and Top 15 BUY Signals UI
        self.assertIn("Interactive Stock AI Chatbot", html)
        self.assertIn("sendStockChatMessage", html)
        self.assertIn("Top 15 Verified BUY Signal Picks", html)

    def test_stock_ai_chatbot_answers_questions_and_applies_live_overrides(self):
        # Seed HDFCBANK into SYMBOL_CACHE so offline test environment doesn't need external Yahoo network
        closes = [700.0 + (i % 5) * 2.0 for i in range(300)]
        fund = platform.validate_and_reconcile_fundamentals(
            symbol="HDFCBANK",
            company_name="HDFC Bank Limited",
            country="IN",
            live_price=691.20,
            closes_3y=closes,
            raw_metrics={"eps": 48.0, "pe": 14.4, "roe": 16.0, "roic": 16.0, "book_value": 300.0},
            source_tag="TEST_FIXTURE",
        )
        seeded = platform.evaluate_institutional_engine(
            symbol="HDFCBANK",
            name="HDFC Bank Limited",
            country="IN",
            exchange="NSE",
            currency="INR",
            asset_type="EQUITY",
            price=691.20,
            prev_close=694.0,
            change_pct=-0.41,
            closes_3y=closes,
            timestamps_3y=[],
            vol_ratio=1.0,
            real_fundamentals=fund,
            provider_name="NSE_BSE_LIVE_API",
        )
        platform.SYMBOL_CACHE.set("HDFCBANK", seeded)

        # 1. Ask a question about Fair Value calculation
        r_q = self.client.post(
            "/api/v1/stock/HDFCBANK/chat",
            json={"message": "How is the Fair Value of HDFCBANK calculated?", "exchange": "NSE"},
        )
        self.assertEqual(r_q.status_code, 200)
        q_data = r_q.json()
        self.assertIn("Fair Value", q_data["reply"])
        self.assertEqual(q_data["corrections_applied"], {})

        # 2. Override Fair Value and Signal via natural language command in the chatbox
        def _mock_search(symbol: str = "HDFCBANK", exchange: str = "AUTO"):
            # Re-evaluate with USER_STOCK_OVERRIDES applied
            f_upd = asyncio.run(platform.fetch_real_fundamentals_online("HDFCBANK", "HDFC Bank Limited", "IN", 691.20, closes))
            ev_upd = platform.evaluate_institutional_engine(
                symbol="HDFCBANK",
                name="HDFC Bank Limited",
                country="IN",
                exchange="NSE",
                currency="INR",
                asset_type="EQUITY",
                price=691.20,
                prev_close=694.0,
                change_pct=-0.41,
                closes_3y=closes,
                timestamps_3y=[],
                vol_ratio=1.0,
                real_fundamentals=f_upd,
                provider_name="NSE_BSE_LIVE_API",
            )
            platform.SYMBOL_CACHE.set("HDFCBANK", ev_upd)
            return ev_upd

        parsed_ovr = platform.parse_user_chat_corrections("Set fair value to 910 and pe to 14.5")
        self.assertEqual(parsed_ovr["fair_value"], 910.0)
        self.assertEqual(parsed_ovr["pe"], 14.5)

        platform.USER_STOCK_OVERRIDES["HDFCBANK"] = parsed_ovr
        updated_stock = _mock_search()
        self.assertEqual(updated_stock["fair_value"], 910.0)
        self.assertEqual(updated_stock["decision_category"], "BUY")
        self.assertEqual(updated_stock["short_signal"], "STRONG BUY")
        platform.USER_STOCK_OVERRIDES.clear()

    def test_ssrf_protection_on_broker_connect(self):
        r_bad = self.client.post(
            "/api/v1/broker/connect",
            json={"portfolio_gsheet_url": "http://169.254.169.254/latest/meta-data/"},
        )
        self.assertEqual(r_bad.status_code, 400)

    def test_portfolio_missing_cost_basis_not_confused_with_live_price(self):
        r_add = self.client.post(
            "/api/v1/portfolio",
            json={"symbol": "RELIANCE", "exchange": "NSE", "quantity": 12, "avg_buy_price": 0},
        )
        self.assertEqual(r_add.status_code, 200)
        out = r_add.json()
        self.assertFalse(out["has_cost_basis"])
        self.assertIsNone(out["avg_buy_price"])
        self.client.delete("/api/v1/portfolio/RELIANCE")

    def test_watchlist_and_transaction_ledger_endpoints(self):
        r_wl = self.client.post(
            "/api/v1/watchlist",
            json={"symbol": "TCS", "exchange": "NSE", "desired_entry": 3500.0, "thesis": "Core IT compounder"},
        )
        self.assertEqual(r_wl.status_code, 200)
        self.assertEqual(r_wl.json()["symbol"], "TCS")

        r_bad_sym = self.client.post(
            "/api/v1/watchlist",
            json={"symbol": "<script>alert(1)</script>", "exchange": "NSE"},
        )
        self.assertEqual(r_bad_sym.status_code, 422)

        r_tx = self.client.post(
            "/api/v1/portfolio/transaction",
            json={
                "date": "2026-10-22",
                "type": "BUY",
                "symbol": "HDFCBANK",
                "exchange": "NSE",
                "quantity": 10.0,
                "price": 710.0,
                "fees": 15.0,
            },
        )
        self.assertEqual(r_tx.status_code, 200)
        self.assertEqual(r_tx.json()["status"], "recorded")

        r_list_tx = self.client.get("/api/v1/portfolio/transactions")
        self.assertEqual(r_list_tx.status_code, 200)
        self.assertGreaterEqual(len(r_list_tx.json()["transactions"]), 1)

    def test_bounded_ttl_cache_eviction(self):
        cache = platform.BoundedTTLCache(maxsize=10, ttl_seconds=5.0)
        for i in range(15):
            cache.set(f"K{i}", i, now_ts=100.0)
        self.assertEqual(len(cache), 10)
        self.assertIsNone(cache.get("K0", now_ts=101.0))
        self.assertEqual(cache.get("K14", now_ts=101.0), 14)
        self.assertIsNone(cache.get("K14", now_ts=106.0))


if __name__ == "__main__":
    unittest.main(verbosity=2)
