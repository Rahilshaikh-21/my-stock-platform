# Institutional Value & Growth Matrix (Graham, Buffett, Lynch)

A fully dynamic, real-time stock analysis and portfolio tracking web application where **no stock names, prices, values, or metrics are hardcoded**. All data—including live quotes, market prices, company fundamentals, and portfolio values—is fetched dynamically using live financial data sources.

## Core Pillars & Philosophy Engine

The application synthesizes the investment wisdom of the three greatest investors into a unified Scoring & Signal Matrix:

1. **Benjamin Graham (The Father of Value Investing)**
   - **Margin of Safety**: Calculates discounts against the Graham Intrinsic Number ($\sqrt{22.5 \times \text{EPS} \times \text{BVPS}}$).
   - **Valuation Filter**: Conservative $P/B \le 1.5$ and $(P/E \times P/B \le 22.5)$.
   - **Balance Sheet Conservatism**: Debt-to-Equity $< 0.60$, Current Ratio $\ge 1.50$, and positive trailing earnings stability.

2. **Warren Buffett (Economic Moats & Quality Compounding)**
   - **Economic Moats**: Classifies companies into Wide Moat, Narrow Moat, and No Moat based on Return on Equity (ROE $\ge 15\%$) and Operating Profit Margins ($\ge 15\%$).
   - **Owner Earnings DCF**: Discounted Cash Flow valuation using conservative discount rates (cost of equity/capital) and normalized cash flows.
   - **Pricing Discipline**: Evaluates whether compounders are priced at a bargain, fair price, or excessive premium.

3. **Peter Lynch (Growth at a Reasonable Price & Categorization)**
   - **PEG Ratio**: Price/Earnings-to-Growth (values under $1.0$ indicate exceptional growth bargains).
   - **Categorization Engine**: Classifies businesses into Fast Growers ($>20\%$ earnings growth), Stalwarts (large-cap compounders), Slow Growers, Cyclicals, Turnarounds, and Asset Plays.
   - **Balance Sheet Buffer**: Cash-to-Debt and cash per share evaluation.

## Key Features

- **Live Data Streaming**: Real-time ticker search with autocomplete for global equities (US NASDAQ/NYSE, Indian NSE/BSE, and ETFs).
- **Three Legends Dashboard**: Live market screener with filter presets (Verified BUYs, Graham Deep Value, Buffett Wide Moats, Lynch PEG $< 1.0$).
- **Deep Stock Analyzer & Sensitivity Studio**:
  - Live price action chart with 1M, 6M, 1Y views.
  - Granular breakdown of passing/failing tests for each legend.
  - Interactive DCF & P/E slider studio to recalculate fair value and Margin of Safety live.
- **Live Portfolio Tracker**:
  - Add real positions with purchase price and share quantities.
  - Real-time total portfolio valuation, cost basis, unrealized P&L ($ and %).
  - Aggregate Portfolio Philosophy Health Rating (weighted Graham, Buffett, and Lynch scores).
- **Margin of Safety Watchlist**:
  - Tracks live market prices against your desired buy entry limits.
  - Alert indicator when stock enters the value buy zone.
