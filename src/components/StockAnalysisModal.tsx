import React, { useState, useEffect } from 'react';
import { X, TrendingUp, TrendingDown, DollarSign, Layers, PlusCircle, Bookmark, Sliders, Info, ShieldCheck, Sparkles } from 'lucide-react';
import { StockQuote, StockFundamentals, HistoricalPricePoint } from '../types';
import { PhilosophyCard } from './PhilosophyCard';
import { fetchHistoricalPrices } from '../services/marketApi';
import { evaluateStockPhilosophy } from '../services/philosophyEngine';

interface StockAnalysisModalProps {
  quote: StockQuote;
  onClose: () => void;
  onAddToPortfolio: (quote: StockQuote, shares: number, cost: number) => void;
  onAddToWatchlist: (quote: StockQuote, targetPrice: number, notes: string) => void;
}

export const StockAnalysisModal: React.FC<StockAnalysisModalProps> = ({
  quote,
  onClose,
  onAddToPortfolio,
  onAddToWatchlist,
}) => {
  const [chartRange, setChartRange] = useState<'1mo' | '6mo' | '1y'>('1mo');
  const [chartPoints, setChartPoints] = useState<HistoricalPricePoint[]>([]);
  const [isLoadingChart, setIsLoadingChart] = useState(false);

  // Interactive Valuation Overrides State
  const [growthOverride, setGrowthOverride] = useState<number | null>(null);
  const [hurdleOverride, setHurdleOverride] = useState<number>(9.5);
  const [peOverride, setPeOverride] = useState<number | null>(null);
  const [showOverrideStudio, setShowOverrideStudio] = useState(false);

  // Portfolio & Watchlist Quick Input Dialogs
  const [showPortfolioDialog, setShowPortfolioDialog] = useState(false);
  const [portfolioShares, setPortfolioShares] = useState(10);
  const [portfolioCost, setPortfolioCost] = useState(quote.price);

  const [showWatchlistDialog, setShowWatchlistDialog] = useState(false);
  const [watchlistTarget, setWatchlistTarget] = useState(Math.round(quote.price * 0.85 * 100) / 100);
  const [watchlistNotes, setWatchlistNotes] = useState('Watching for entry below fair value with Margin of Safety.');

  // Fetch real price history
  useEffect(() => {
    let isMounted = true;
    setIsLoadingChart(true);
    fetchHistoricalPrices(quote.symbol, chartRange).then((points) => {
      if (isMounted) {
        setChartPoints(points);
        setIsLoadingChart(false);
      }
    });
    return () => {
      isMounted = false;
    };
  }, [quote.symbol, chartRange]);

  // Compute live fundamentals based on standard or user-overridden values
  const fundamentals: StockFundamentals = evaluateStockPhilosophy(quote, {
    pe: peOverride !== null ? peOverride : undefined,
    earningsGrowthRate: growthOverride !== null ? growthOverride : undefined,
  });

  const getSignalBadgeColor = (signal: string) => {
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

  // Canvas Simple SVG Chart
  const minPrice = chartPoints.length > 0 ? Math.min(...chartPoints.map((p) => p.price)) : 100;
  const maxPrice = chartPoints.length > 0 ? Math.max(...chartPoints.map((p) => p.price)) : 200;
  const priceSpread = Math.max(0.1, maxPrice - minPrice);

  const svgWidth = 720;
  const svgHeight = 160;

  const pointsString = chartPoints
    .map((pt, i) => {
      const x = (i / Math.max(1, chartPoints.length - 1)) * (svgWidth - 20) + 10;
      const y = svgHeight - 15 - ((pt.price - minPrice) / priceSpread) * (svgHeight - 35);
      return `${x},${y}`;
    })
    .join(' ');

  return (
    <div className="fixed inset-0 z-50 bg-slate-950/80 backdrop-blur-md flex items-center justify-center p-3 md:p-6 overflow-y-auto">
      <div className="bg-slate-900 border border-slate-800 rounded-3xl max-w-5xl w-full max-h-[92vh] overflow-y-auto shadow-2xl space-y-6 p-5 md:p-7">
        {/* Top Bar: Title & Actions */}
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 border-b border-slate-800 pb-4">
          <div>
            <div className="flex items-center gap-2 flex-wrap">
              <span className="text-xl md:text-2xl font-black text-white">{quote.symbol}</span>
              <span className="text-xs px-2 py-0.5 rounded bg-slate-800 text-slate-300 font-mono">
                {quote.exchange}
              </span>
              <span
                className={`text-xs px-2.5 py-0.5 rounded-full font-bold border ${getSignalBadgeColor(
                  fundamentals.overallSignal
                )}`}
              >
                {fundamentals.overallSignal.replace('_', ' ')}
              </span>
            </div>
            <h2 className="text-sm text-slate-400 mt-0.5 font-medium">{quote.name}</h2>
          </div>

          <div className="flex items-center gap-2">
            <button
              onClick={() => setShowPortfolioDialog(true)}
              className="flex items-center gap-1.5 px-3 py-1.5 rounded-xl bg-blue-600 hover:bg-blue-500 text-white text-xs font-semibold shadow transition"
            >
              <PlusCircle className="w-3.5 h-3.5" />
              <span>+ Add to Portfolio</span>
            </button>
            <button
              onClick={() => setShowWatchlistDialog(true)}
              className="flex items-center gap-1.5 px-3 py-1.5 rounded-xl bg-slate-800 hover:bg-slate-700 text-slate-200 border border-slate-700 text-xs font-semibold transition"
            >
              <Bookmark className="w-3.5 h-3.5 text-emerald-400" />
              <span>+ Watchlist</span>
            </button>
            <button
              onClick={onClose}
              className="p-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-400 hover:text-white transition"
            >
              <X className="w-5 h-5" />
            </button>
          </div>
        </div>

        {/* Live Price & Key Fundamentals Ribbon */}
        <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3">
          <div className="bg-slate-950 p-3 rounded-xl border border-slate-800">
            <span className="text-[11px] text-slate-400 block">Live Price</span>
            <div className="text-lg font-bold font-mono text-white">
              {quote.currency}{quote.price}
            </div>
            <span
              className={`text-[11px] font-mono flex items-center gap-0.5 ${
                quote.change >= 0 ? 'text-emerald-400' : 'text-rose-400'
              }`}
            >
              {quote.change >= 0 ? <TrendingUp className="w-3 h-3" /> : <TrendingDown className="w-3 h-3" />}
              {quote.change >= 0 ? '+' : ''}{quote.change} ({quote.changePercent}%)
            </span>
          </div>

          <div className="bg-slate-950 p-3 rounded-xl border border-slate-800">
            <span className="text-[11px] text-slate-400 block">Triple-Legend Score</span>
            <div className="text-lg font-bold font-mono text-emerald-400">
              {fundamentals.compositeScore}
              <span className="text-xs text-slate-500 font-normal">/100</span>
            </div>
            <span className="text-[11px] text-slate-400">Weighted Consensus</span>
          </div>

          <div className="bg-slate-950 p-3 rounded-xl border border-slate-800">
            <span className="text-[11px] text-slate-400 block">P/E Ratio</span>
            <div className="text-lg font-bold font-mono text-slate-200">
              {quote.pe ? `${quote.pe}x` : 'N/A'}
            </div>
            <span className="text-[11px] text-slate-400">Trailing TTM</span>
          </div>

          <div className="bg-slate-950 p-3 rounded-xl border border-slate-800">
            <span className="text-[11px] text-slate-400 block">EPS (TTM)</span>
            <div className="text-lg font-bold font-mono text-slate-200">
              {quote.currency}{quote.eps || 'N/A'}
            </div>
            <span className="text-[11px] text-slate-400">Net Diluted</span>
          </div>

          <div className="bg-slate-950 p-3 rounded-xl border border-slate-800">
            <span className="text-[11px] text-slate-400 block">Price to Book (P/B)</span>
            <div className="text-lg font-bold font-mono text-slate-200">
              {quote.pb ? `${quote.pb}x` : 'N/A'}
            </div>
            <span className="text-[11px] text-slate-400">Graham cap 1.5x</span>
          </div>

          <div className="bg-slate-950 p-3 rounded-xl border border-slate-800">
            <span className="text-[11px] text-slate-400 block">52W Range</span>
            <div className="text-xs font-bold font-mono text-slate-200 truncate mt-1">
              {quote.currency}{quote.fiftyTwoWeekLow} – {quote.currency}{quote.fiftyTwoWeekHigh}
            </div>
            <span className="text-[11px] text-slate-400">Annual Volatility</span>
          </div>
        </div>

        {/* Real-time SVG Historical Price Chart */}
        <div className="bg-slate-950 border border-slate-800 rounded-2xl p-4 space-y-2">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <span className="text-xs font-bold text-slate-300 uppercase tracking-wide">
                Live Price Action &amp; Trend
              </span>
              <span className="text-[10px] text-slate-500 font-mono">
                High: {quote.currency}{maxPrice} | Low: {quote.currency}{minPrice}
              </span>
            </div>
            <div className="flex items-center gap-1 bg-slate-900 p-0.5 rounded-lg border border-slate-800">
              {(['1mo', '6mo', '1y'] as const).map((r) => (
                <button
                  key={r}
                  onClick={() => setChartRange(r)}
                  className={`px-2.5 py-0.5 rounded text-[11px] font-bold transition ${
                    chartRange === r
                      ? 'bg-emerald-600 text-white shadow-sm'
                      : 'text-slate-400 hover:text-white'
                  }`}
                >
                  {r.toUpperCase()}
                </button>
              ))}
            </div>
          </div>

          <div className="relative h-40 w-full flex items-center justify-center">
            {isLoadingChart ? (
              <span className="text-xs text-slate-400 animate-pulse">Streaming live price series...</span>
            ) : chartPoints.length > 1 ? (
              <svg className="w-full h-full overflow-visible" viewBox={`0 0 ${svgWidth} ${svgHeight}`}>
                <defs>
                  <linearGradient id="chartGradient" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor="#10b981" stopOpacity="0.3" />
                    <stop offset="100%" stopColor="#10b981" stopOpacity="0.0" />
                  </linearGradient>
                </defs>
                {/* Area fill */}
                <polygon
                  points={`10,${svgHeight - 15} ${pointsString} ${svgWidth - 10},${svgHeight - 15}`}
                  fill="url(#chartGradient)"
                />
                {/* Main line */}
                <polyline
                  fill="none"
                  stroke="#10b981"
                  strokeWidth="2.5"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  points={pointsString}
                />
              </svg>
            ) : (
              <span className="text-xs text-slate-500">Live chart initializing...</span>
            )}
          </div>
        </div>

        {/* Synthesis Thesis Callout Box */}
        <div className="bg-emerald-950/20 border border-emerald-500/30 rounded-2xl p-4 flex items-start gap-3">
          <div className="p-2 rounded-xl bg-emerald-500/20 text-emerald-400 mt-0.5">
            <Sparkles className="w-5 h-5" />
          </div>
          <div>
            <div className="flex items-center gap-2">
              <h3 className="text-sm font-bold text-white">Three-Legend Synthesis &amp; Verdict</h3>
              <span className="text-[10px] font-mono px-2 py-0.2 rounded bg-emerald-500/20 text-emerald-300">
                Composite Score: {fundamentals.compositeScore}/100
              </span>
            </div>
            <p className="text-xs text-slate-300 mt-1 leading-relaxed">
              {fundamentals.synthesisThesis}
            </p>
            <div className="mt-2 text-xs flex flex-wrap gap-4 font-mono text-slate-400">
              <span>
                Conservative Value: <strong className="text-slate-200">{quote.currency}{fundamentals.intrinsicValueRange.conservative}</strong>
              </span>
              <span>
                Base Appraised Value: <strong className="text-emerald-400">{quote.currency}{fundamentals.intrinsicValueRange.base}</strong>
              </span>
              <span>
                Growth Scenario: <strong className="text-slate-200">{quote.currency}{fundamentals.intrinsicValueRange.optimistic}</strong>
              </span>
            </div>
          </div>
        </div>

        {/* The 3 Philosophy Deep-Dives: Benjamin Graham, Warren Buffett, Peter Lynch */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
          <PhilosophyCard
            type="graham"
            fundamentals={fundamentals}
            price={quote.price}
            currency={quote.currency}
          />
          <PhilosophyCard
            type="buffett"
            fundamentals={fundamentals}
            price={quote.price}
            currency={quote.currency}
          />
          <PhilosophyCard
            type="lynch"
            fundamentals={fundamentals}
            price={quote.price}
            currency={quote.currency}
          />
        </div>

        {/* Interactive Valuation Sensitivity Studio Toggle */}
        <div className="border border-slate-800 rounded-2xl bg-slate-950 p-4 space-y-4">
          <div className="flex items-center justify-between cursor-pointer" onClick={() => setShowOverrideStudio(!showOverrideStudio)}>
            <div className="flex items-center gap-2">
              <Sliders className="w-4 h-4 text-emerald-400" />
              <h4 className="text-xs font-bold text-slate-200 uppercase tracking-wider">
                Interactive Valuation Sensitivity Studio
              </h4>
              <span className="text-[10px] px-2 py-0.5 rounded bg-slate-800 text-slate-400">
                {showOverrideStudio ? 'Hide Custom Sliders' : 'Tweak Assumptions & Recalculate Live'}
              </span>
            </div>
            <button className="text-xs text-emerald-400 hover:underline">
              {showOverrideStudio ? 'Collapse' : 'Expand'}
            </button>
          </div>

          {showOverrideStudio && (
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4 pt-3 border-t border-slate-800/80 text-xs">
              <div className="space-y-1.5">
                <div className="flex justify-between">
                  <span className="text-slate-400">Expected EPS Growth Rate</span>
                  <span className="font-mono font-bold text-emerald-400">
                    {growthOverride !== null ? growthOverride : fundamentals.earningsGrowthRate}%
                  </span>
                </div>
                <input
                  type="range"
                  min="2"
                  max="45"
                  step="1"
                  value={growthOverride !== null ? growthOverride : fundamentals.earningsGrowthRate}
                  onChange={(e) => setGrowthOverride(parseFloat(e.target.value))}
                  className="w-full accent-emerald-500"
                />
                <span className="text-[10px] text-slate-500 block">Controls DCF and Lynch PEG Fair Value</span>
              </div>

              <div className="space-y-1.5">
                <div className="flex justify-between">
                  <span className="text-slate-400">Custom P/E Multiple</span>
                  <span className="font-mono font-bold text-blue-400">
                    {peOverride !== null ? peOverride : quote.pe || 18}x
                  </span>
                </div>
                <input
                  type="range"
                  min="5"
                  max="60"
                  step="1"
                  value={peOverride !== null ? peOverride : quote.pe || 18}
                  onChange={(e) => setPeOverride(parseFloat(e.target.value))}
                  className="w-full accent-blue-500"
                />
                <span className="text-[10px] text-slate-500 block">Simulate Multiple Expansion/Compression</span>
              </div>

              <div className="flex items-end">
                <button
                  onClick={() => {
                    setGrowthOverride(null);
                    setPeOverride(null);
                  }}
                  className="w-full py-2 rounded-xl bg-slate-800 hover:bg-slate-700 text-slate-300 font-semibold text-xs border border-slate-700 transition"
                >
                  ↺ Reset to Market Defaults
                </button>
              </div>
            </div>
          )}
        </div>

        {/* Quick Add to Portfolio Modal */}
        {showPortfolioDialog && (
          <div className="fixed inset-0 z-60 bg-slate-950/70 backdrop-blur-sm flex items-center justify-center p-4">
            <div className="bg-slate-900 border border-slate-700 rounded-2xl max-w-sm w-full p-5 space-y-4 shadow-2xl">
              <div className="flex justify-between items-center border-b border-slate-800 pb-2">
                <h4 className="font-bold text-white text-sm">Add {quote.symbol} to Portfolio</h4>
                <button onClick={() => setShowPortfolioDialog(false)} className="text-slate-400 hover:text-white">✕</button>
              </div>
              <div className="space-y-3 text-xs">
                <div>
                  <label className="text-slate-400 block mb-1">Number of Shares</label>
                  <input
                    type="number"
                    min="1"
                    value={portfolioShares}
                    onChange={(e) => setPortfolioShares(Math.max(1, parseFloat(e.target.value) || 1))}
                    className="w-full bg-slate-950 border border-slate-700 rounded-lg px-3 py-2 font-mono text-white"
                  />
                </div>
                <div>
                  <label className="text-slate-400 block mb-1">Average Purchase Price ({quote.currency})</label>
                  <input
                    type="number"
                    step="0.01"
                    min="0"
                    value={portfolioCost}
                    onChange={(e) => setPortfolioCost(parseFloat(e.target.value) || 0)}
                    className="w-full bg-slate-950 border border-slate-700 rounded-lg px-3 py-2 font-mono text-white"
                  />
                </div>
                <div className="p-2.5 rounded-lg bg-slate-950 text-slate-400">
                  Total Investment: <strong className="text-white">{quote.currency}{(portfolioShares * portfolioCost).toFixed(2)}</strong>
                </div>
              </div>
              <div className="flex gap-2">
                <button
                  onClick={() => setShowPortfolioDialog(false)}
                  className="flex-1 py-2 rounded-lg bg-slate-800 text-slate-300 font-bold text-xs"
                >
                  Cancel
                </button>
                <button
                  onClick={() => {
                    onAddToPortfolio(quote, portfolioShares, portfolioCost);
                    setShowPortfolioDialog(false);
                  }}
                  className="flex-1 py-2 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white font-bold text-xs"
                >
                  Save Position
                </button>
              </div>
            </div>
          </div>
        )}

        {/* Quick Add to Watchlist Modal */}
        {showWatchlistDialog && (
          <div className="fixed inset-0 z-60 bg-slate-950/70 backdrop-blur-sm flex items-center justify-center p-4">
            <div className="bg-slate-900 border border-slate-700 rounded-2xl max-w-sm w-full p-5 space-y-4 shadow-2xl">
              <div className="flex justify-between items-center border-b border-slate-800 pb-2">
                <h4 className="font-bold text-white text-sm">Add {quote.symbol} to Watchlist</h4>
                <button onClick={() => setShowWatchlistDialog(false)} className="text-slate-400 hover:text-white">✕</button>
              </div>
              <div className="space-y-3 text-xs">
                <div>
                  <label className="text-slate-400 block mb-1">Target Entry Price ({quote.currency})</label>
                  <input
                    type="number"
                    step="0.01"
                    value={watchlistTarget}
                    onChange={(e) => setWatchlistTarget(parseFloat(e.target.value) || 0)}
                    className="w-full bg-slate-950 border border-slate-700 rounded-lg px-3 py-2 font-mono text-white"
                  />
                  <span className="text-[10px] text-slate-500 block mt-0.5">
                    Live price: {quote.currency}{quote.price} (Target is {Math.round(((watchlistTarget - quote.price) / quote.price) * 100)}% discount)
                  </span>
                </div>
                <div>
                  <label className="text-slate-400 block mb-1">Investment Thesis Notes</label>
                  <textarea
                    rows={2}
                    value={watchlistNotes}
                    onChange={(e) => setWatchlistNotes(e.target.value)}
                    className="w-full bg-slate-950 border border-slate-700 rounded-lg p-2.5 text-slate-100"
                  />
                </div>
              </div>
              <div className="flex gap-2">
                <button
                  onClick={() => setShowWatchlistDialog(false)}
                  className="flex-1 py-2 rounded-lg bg-slate-800 text-slate-300 font-bold text-xs"
                >
                  Cancel
                </button>
                <button
                  onClick={() => {
                    onAddToWatchlist(quote, watchlistTarget, watchlistNotes);
                    setShowWatchlistDialog(false);
                  }}
                  className="flex-1 py-2 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white font-bold text-xs"
                >
                  Save to Watchlist
                </button>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};
