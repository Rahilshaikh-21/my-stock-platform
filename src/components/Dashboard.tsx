import React, { useState } from 'react';
import { Award, ShieldCheck, Zap, TrendingUp, Sparkles, Filter, Plus, ArrowUpRight, Search } from 'lucide-react';
import { StockQuote, StockFundamentals } from '../types';
import { evaluateStockPhilosophy } from '../services/philosophyEngine';

interface DashboardProps {
  quotes: StockQuote[];
  onSelectStock: (symbol: string) => void;
  onQuickAddToPortfolio: (quote: StockQuote) => void;
  onQuickAddToWatchlist: (quote: StockQuote) => void;
  isLoading: boolean;
  onSearchSymbol: (symbol: string) => void;
}

export const Dashboard: React.FC<DashboardProps> = ({
  quotes,
  onSelectStock,
  onQuickAddToPortfolio,
  onQuickAddToWatchlist,
  isLoading,
  onSearchSymbol,
}) => {
  const [philosophyFilter, setPhilosophyFilter] = useState<'ALL' | 'GRAHAM' | 'BUFFETT' | 'LYNCH' | 'BUYS'>('BUYS');
  const [marketFilter, setMarketFilter] = useState<'ALL' | 'US' | 'IN'>('ALL');
  const [quickInput, setQuickInput] = useState('');

  // Evaluate all quotes with the 3 philosophies
  const enrichedStocks = quotes.map((quote) => {
    const fundamentals = evaluateStockPhilosophy(quote);
    return { quote, fundamentals };
  });

  // Apply filters
  const filteredStocks = enrichedStocks.filter(({ quote, fundamentals }) => {
    // Market filter
    if (marketFilter === 'US' && quote.exchange === 'NSE') return false;
    if (marketFilter === 'IN' && quote.exchange !== 'NSE') return false;

    // Philosophy filter
    if (philosophyFilter === 'BUYS') {
      return fundamentals.overallSignal === 'STRONG_BUY' || fundamentals.overallSignal === 'BUY';
    }
    if (philosophyFilter === 'GRAHAM') {
      return fundamentals.grahamMarginOfSafetyPercent >= 15 || fundamentals.grahamScore >= 70;
    }
    if (philosophyFilter === 'BUFFETT') {
      return fundamentals.buffettScore >= 70 || fundamentals.moatStrength === 'Wide Moat';
    }
    if (philosophyFilter === 'LYNCH') {
      return fundamentals.pegRatio <= 1.2 || fundamentals.lynchScore >= 70;
    }
    return true;
  });

  // Sort primarily by Composite Score descending
  const sortedStocks = [...filteredStocks].sort((a, b) => b.fundamentals.compositeScore - a.fundamentals.compositeScore);

  // Compute Dashboard Aggregate KPIs
  const totalAnalyzed = enrichedStocks.length;
  const verifiedBuys = enrichedStocks.filter(
    (x) => x.fundamentals.overallSignal === 'STRONG_BUY' || x.fundamentals.overallSignal === 'BUY'
  ).length;

  const avgMos =
    enrichedStocks.length > 0
      ? Math.round(
          (enrichedStocks.reduce((sum, x) => sum + x.fundamentals.grahamMarginOfSafetyPercent, 0) /
            enrichedStocks.length) *
            10
        ) / 10
      : 0;

  const wideMoatsCount = enrichedStocks.filter((x) => x.fundamentals.moatStrength === 'Wide Moat').length;
  const lynchBargainsCount = enrichedStocks.filter((x) => x.fundamentals.pegRatio <= 1.0).length;

  const getSignalBadge = (signal: string) => {
    switch (signal) {
      case 'STRONG_BUY':
        return 'bg-emerald-500/20 text-emerald-300 border-emerald-500/40';
      case 'BUY':
        return 'bg-emerald-500/15 text-emerald-400 border-emerald-500/30';
      case 'HOLD':
        return 'bg-blue-500/15 text-blue-300 border-blue-500/30';
      case 'WAIT_TRIM':
        return 'bg-amber-500/15 text-amber-300 border-amber-500/30';
      case 'SELL':
      case 'AVOID':
        return 'bg-rose-500/20 text-rose-300 border-rose-500/40';
      default:
        return 'bg-slate-800 text-slate-300 border-slate-700';
    }
  };

  const handleQuickLookup = (e: React.FormEvent) => {
    e.preventDefault();
    if (quickInput.trim()) {
      onSearchSymbol(quickInput.trim().toUpperCase());
      setQuickInput('');
    }
  };

  return (
    <div className="space-y-6">
      {/* KPI Ribbon */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <div className="bg-slate-900 border border-slate-800 p-4 rounded-2xl relative overflow-hidden">
          <div className="flex items-center justify-between text-xs text-slate-400">
            <span>Verified BUY Conviction</span>
            <span className="p-1.5 rounded-lg bg-emerald-500/10 text-emerald-400">
              <Award className="w-4 h-4" />
            </span>
          </div>
          <div className="text-3xl font-black font-mono text-emerald-400 mt-2">
            {verifiedBuys}
            <span className="text-xs text-slate-500 font-normal ml-1.5">/ {totalAnalyzed} Stocks</span>
          </div>
          <div className="text-[11px] text-slate-400 mt-1">Met Graham, Buffett &amp; Lynch thresholds</div>
        </div>

        <div className="bg-slate-900 border border-slate-800 p-4 rounded-2xl relative overflow-hidden">
          <div className="flex items-center justify-between text-xs text-slate-400">
            <span>Avg Graham Margin of Safety</span>
            <span className="p-1.5 rounded-lg bg-emerald-500/10 text-emerald-400">
              <ShieldCheck className="w-4 h-4" />
            </span>
          </div>
          <div className={`text-3xl font-black font-mono mt-2 ${avgMos >= 0 ? 'text-emerald-300' : 'text-slate-300'}`}>
            {avgMos > 0 ? `+${avgMos}%` : `${avgMos}%`}
          </div>
          <div className="text-[11px] text-slate-400 mt-1">Discount vs Graham Intrinsic Number</div>
        </div>

        <div className="bg-slate-900 border border-slate-800 p-4 rounded-2xl relative overflow-hidden">
          <div className="flex items-center justify-between text-xs text-slate-400">
            <span>Buffett Wide Moat Leaders</span>
            <span className="p-1.5 rounded-lg bg-blue-500/10 text-blue-400">
              <Sparkles className="w-4 h-4" />
            </span>
          </div>
          <div className="text-3xl font-black font-mono text-blue-400 mt-2">
            {wideMoatsCount}
          </div>
          <div className="text-[11px] text-slate-400 mt-1">High ROE (&gt;20%) &amp; High Operating Margin</div>
        </div>

        <div className="bg-slate-900 border border-slate-800 p-4 rounded-2xl relative overflow-hidden">
          <div className="flex items-center justify-between text-xs text-slate-400">
            <span>Peter Lynch Bargains</span>
            <span className="p-1.5 rounded-lg bg-amber-500/10 text-amber-400">
              <Zap className="w-4 h-4" />
            </span>
          </div>
          <div className="text-3xl font-black font-mono text-amber-400 mt-2">
            {lynchBargainsCount}
          </div>
          <div className="text-[11px] text-slate-400 mt-1">PEG Ratio &lt; 1.0 (Growth at a Discount)</div>
        </div>
      </div>

      {/* Philosophy Screener Presets & Market Controls */}
      <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 flex flex-col xl:flex-row xl:items-center justify-between gap-4">
        {/* Philosophy Presets */}
        <div className="flex items-center gap-2 flex-wrap">
          <span className="text-xs font-bold text-slate-400 uppercase tracking-wider mr-1">
            Philosophy Screener:
          </span>
          <button
            onClick={() => setPhilosophyFilter('BUYS')}
            className={`px-3 py-1.5 rounded-xl text-xs font-bold transition ${
              philosophyFilter === 'BUYS'
                ? 'bg-emerald-600 text-white shadow-sm'
                : 'bg-slate-950 text-slate-300 hover:bg-slate-800 border border-slate-800'
            }`}
          >
            🟢 Verified BUYs Only
          </button>
          <button
            onClick={() => setPhilosophyFilter('ALL')}
            className={`px-3 py-1.5 rounded-xl text-xs font-bold transition ${
              philosophyFilter === 'ALL'
                ? 'bg-emerald-600 text-white shadow-sm'
                : 'bg-slate-950 text-slate-300 hover:bg-slate-800 border border-slate-800'
            }`}
          >
            All Discoveries
          </button>
          <button
            onClick={() => setPhilosophyFilter('GRAHAM')}
            className={`px-3 py-1.5 rounded-xl text-xs font-bold transition ${
              philosophyFilter === 'GRAHAM'
                ? 'bg-emerald-600 text-white shadow-sm'
                : 'bg-slate-950 text-slate-300 hover:bg-slate-800 border border-slate-800'
            }`}
          >
            🛡️ Graham Value &amp; Safety
          </button>
          <button
            onClick={() => setPhilosophyFilter('BUFFETT')}
            className={`px-3 py-1.5 rounded-xl text-xs font-bold transition ${
              philosophyFilter === 'BUFFETT'
                ? 'bg-blue-600 text-white shadow-sm'
                : 'bg-slate-950 text-slate-300 hover:bg-slate-800 border border-slate-800'
            }`}
          >
            🏰 Buffett Wide Moats
          </button>
          <button
            onClick={() => setPhilosophyFilter('LYNCH')}
            className={`px-3 py-1.5 rounded-xl text-xs font-bold transition ${
              philosophyFilter === 'LYNCH'
                ? 'bg-amber-600 text-white shadow-sm'
                : 'bg-slate-950 text-slate-300 hover:bg-slate-800 border border-slate-800'
            }`}
          >
            ⚡ Lynch Fast Growth (PEG &lt; 1.0)
          </button>
        </div>

        {/* Market Filter & Quick Lookup */}
        <div className="flex items-center gap-2 flex-wrap">
          <div className="flex items-center gap-1 bg-slate-950 p-1 rounded-xl border border-slate-800">
            <button
              onClick={() => setMarketFilter('ALL')}
              className={`px-2.5 py-1 rounded-lg text-xs font-bold ${
                marketFilter === 'ALL' ? 'bg-slate-800 text-white' : 'text-slate-400 hover:text-white'
              }`}
            >
              All Regions
            </button>
            <button
              onClick={() => setMarketFilter('US')}
              className={`px-2.5 py-1 rounded-lg text-xs font-bold ${
                marketFilter === 'US' ? 'bg-slate-800 text-white' : 'text-slate-400 hover:text-white'
              }`}
            >
              🇺🇸 US
            </button>
            <button
              onClick={() => setMarketFilter('IN')}
              className={`px-2.5 py-1 rounded-lg text-xs font-bold ${
                marketFilter === 'IN' ? 'bg-slate-800 text-white' : 'text-slate-400 hover:text-white'
              }`}
            >
              🇮🇳 India
            </button>
          </div>

          <form onSubmit={handleQuickLookup} className="flex items-center gap-1.5">
            <input
              type="text"
              value={quickInput}
              onChange={(e) => setQuickInput(e.target.value)}
              placeholder="Lookup symbol..."
              className="w-32 bg-slate-950 border border-slate-700 rounded-lg px-2.5 py-1 text-xs text-white uppercase placeholder-slate-500 focus:outline-none"
            />
            <button
              type="submit"
              className="px-2.5 py-1 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white text-xs font-bold"
            >
              Go
            </button>
          </form>
        </div>
      </div>

      {/* Main Discoveries Table */}
      <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 overflow-hidden shadow-lg">
        <div className="flex items-center justify-between mb-4">
          <div>
            <h2 className="text-sm font-bold text-white flex items-center gap-2">
              <span>Dynamic Market Universe</span>
              <span className="text-xs font-mono text-emerald-400 font-bold px-2 py-0.2 rounded bg-emerald-500/10">
                {sortedStocks.length} Assets Found
              </span>
            </h2>
            <p className="text-xs text-slate-400">
              Evaluated dynamically via Graham Margin of Safety, Buffett Moat, and Lynch PEG
            </p>
          </div>
          <span className="text-[11px] text-slate-500 font-mono">
            {isLoading ? 'Fetching live quotes...' : 'Live Data Stream Active'}
          </span>
        </div>

        <div className="overflow-x-auto -mx-4 px-4">
          <table className="w-full min-w-[980px] text-left border-collapse text-xs">
            <thead>
              <tr className="border-b border-slate-800 text-slate-400 uppercase text-[11px] font-mono">
                <th className="py-2.5 px-3">#</th>
                <th className="py-2.5 px-3">Company &amp; Symbol</th>
                <th className="py-2.5 px-3 text-right">Live Price</th>
                <th className="py-2.5 px-3 text-right">Graham Value &amp; MoS</th>
                <th className="py-2.5 px-3 text-right">Buffett Moat &amp; ROE</th>
                <th className="py-2.5 px-3 text-right">Lynch PEG &amp; Growth</th>
                <th className="py-2.5 px-3 text-center">Composite Score</th>
                <th className="py-2.5 px-3">Overall Signal</th>
                <th className="py-2.5 px-3 text-right">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/60 font-sans">
              {sortedStocks.length > 0 ? (
                sortedStocks.map(({ quote, fundamentals }, idx) => {
                  const isPositiveMoS = fundamentals.grahamMarginOfSafetyPercent > 0;
                  return (
                    <tr
                      key={quote.symbol}
                      className="hover:bg-slate-800/40 transition group cursor-pointer"
                      onClick={() => onSelectStock(quote.symbol)}
                    >
                      <td className="py-3 px-3 font-mono text-slate-500 font-bold">{idx + 1}</td>

                      <td className="py-3 px-3">
                        <div className="flex items-center gap-1.5">
                          <span className="font-bold text-white text-sm group-hover:text-emerald-400 transition">
                            {quote.symbol}
                          </span>
                          <span className="text-[10px] px-1.5 py-0.2 rounded bg-slate-800 text-slate-400 font-mono">
                            {quote.exchange}
                          </span>
                        </div>
                        <div className="text-[11px] text-slate-400 truncate max-w-xs">{quote.name}</div>
                      </td>

                      <td className="py-3 px-3 text-right font-mono">
                        <div className="font-bold text-white text-sm">
                          {quote.currency}{quote.price}
                        </div>
                        <div
                          className={`text-[10px] ${
                            quote.change >= 0 ? 'text-emerald-400' : 'text-rose-400'
                          }`}
                        >
                          {quote.change >= 0 ? '+' : ''}{quote.change} ({quote.changePercent}%)
                        </div>
                      </td>

                      <td className="py-3 px-3 text-right font-mono">
                        <div className="font-bold text-emerald-400 text-xs">
                          {quote.currency}{fundamentals.grahamNumber}
                        </div>
                        <div
                          className={`text-[10px] ${
                            isPositiveMoS ? 'text-emerald-300' : 'text-rose-400'
                          }`}
                        >
                          MoS: {isPositiveMoS ? '+' : ''}{fundamentals.grahamMarginOfSafetyPercent}%
                        </div>
                      </td>

                      <td className="py-3 px-3 text-right font-mono">
                        <div className="font-bold text-blue-300 text-xs">
                          {fundamentals.moatStrength}
                        </div>
                        <div className="text-[10px] text-slate-400">
                          ROE: {fundamentals.roe}%
                        </div>
                      </td>

                      <td className="py-3 px-3 text-right font-mono">
                        <div
                          className={`font-bold text-xs ${
                            fundamentals.pegRatio <= 1.0 ? 'text-emerald-400' : 'text-amber-300'
                          }`}
                        >
                          PEG {fundamentals.pegRatio}x
                        </div>
                        <div className="text-[10px] text-slate-400">
                          +{fundamentals.earningsGrowthRate}% YoY
                        </div>
                      </td>

                      <td className="py-3 px-3 text-center font-mono">
                        <div className="inline-block px-2 py-0.5 rounded-lg bg-slate-950 border border-slate-800 font-black text-sm text-emerald-400">
                          {fundamentals.compositeScore}
                        </div>
                      </td>

                      <td className="py-3 px-3">
                        <span
                          className={`inline-block px-2.5 py-0.5 rounded-full text-[11px] font-bold border ${getSignalBadge(
                            fundamentals.overallSignal
                          )}`}
                        >
                          {fundamentals.overallSignal.replace('_', ' ')}
                        </span>
                      </td>

                      <td
                        className="py-3 px-3 text-right space-x-1.5 whitespace-nowrap"
                        onClick={(e) => e.stopPropagation()}
                      >
                        <button
                          onClick={() => onSelectStock(quote.symbol)}
                          className="px-2.5 py-1 rounded-lg bg-emerald-600/20 hover:bg-emerald-600 text-emerald-300 hover:text-white border border-emerald-500/40 text-[11px] font-bold transition"
                        >
                          Analyze
                        </button>
                        <button
                          onClick={() => onQuickAddToPortfolio(quote)}
                          className="px-2 py-1 rounded-lg bg-blue-600/20 hover:bg-blue-600 text-blue-300 hover:text-white border border-blue-500/40 text-[11px] font-semibold transition"
                          title="Add to Portfolio"
                        >
                          + PF
                        </button>
                        <button
                          onClick={() => onQuickAddToWatchlist(quote)}
                          className="px-2 py-1 rounded-lg bg-slate-800 hover:bg-emerald-600 text-slate-300 hover:text-white border border-slate-700 text-[11px] font-semibold transition"
                          title="Add to Watchlist"
                        >
                          + WL
                        </button>
                      </td>
                    </tr>
                  );
                })
              ) : (
                <tr>
                  <td colSpan={9} className="py-8 text-center text-slate-400">
                    No stocks matching the selected filter criteria. Try changing filters or search a stock above!
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};
