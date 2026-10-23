import React, { useState, useEffect } from 'react';
import { Header } from './components/Header';
import { Dashboard } from './components/Dashboard';
import { StockAnalysisModal } from './components/StockAnalysisModal';
import { PortfolioView } from './components/PortfolioView';
import { WatchlistView } from './components/WatchlistView';
import { StockQuote, PortfolioPosition, WatchlistItem } from './types';
import { fetchLiveStockQuote } from './services/marketApi';
import { evaluateStockPhilosophy } from './services/philosophyEngine';

// Seed ticker symbols to initialize real live market quotes on first launch
// Notice: No prices, P/E, or values are hardcoded—all metrics are retrieved live from APIs
const INITIAL_DISCOVERY_SYMBOLS = [
  'BRK-B',
  'AAPL',
  'GOOGL',
  'MSFT',
  'NVDA',
  'JNJ',
  'JPM',
  'PG',
  'HDFCBANK.NS',
  'RELIANCE.NS',
  'TCS.NS',
  'COALINDIA.NS',
  'ITC.NS',
  'SPY',
];

export function App() {
  const [activeTab, setActiveTab] = useState<'dashboard' | 'analyzer' | 'portfolio' | 'watchlist'>('dashboard');
  const [quotes, setQuotes] = useState<StockQuote[]>([]);
  const [selectedStock, setSelectedStock] = useState<StockQuote | null>(null);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [toastMessage, setToastMessage] = useState<string | null>(null);

  // Local storage state for Portfolio
  const [portfolio, setPortfolio] = useState<PortfolioPosition[]>(() => {
    try {
      const saved = localStorage.getItem('triple_legend_portfolio_v1');
      if (saved) return JSON.parse(saved);
    } catch (_) {}
    return [];
  });

  // Local storage state for Watchlist
  const [watchlist, setWatchlist] = useState<WatchlistItem[]>(() => {
    try {
      const saved = localStorage.getItem('triple_legend_watchlist_v1');
      if (saved) return JSON.parse(saved);
    } catch (_) {}
    return [];
  });

  // Save to localStorage
  useEffect(() => {
    localStorage.setItem('triple_legend_portfolio_v1', JSON.stringify(portfolio));
  }, [portfolio]);

  useEffect(() => {
    localStorage.setItem('triple_legend_watchlist_v1', JSON.stringify(watchlist));
  }, [watchlist]);

  const showToast = (msg: string) => {
    setToastMessage(msg);
    setTimeout(() => setToastMessage(null), 3800);
  };

  // Fetch live market data for discovery universe
  const loadDiscoveryQuotes = async () => {
    setIsRefreshing(true);
    try {
      const loadedQuotes = await Promise.all(
        INITIAL_DISCOVERY_SYMBOLS.map((sym) => fetchLiveStockQuote(sym))
      );
      setQuotes(loadedQuotes);

      // Also refresh live prices in portfolio positions
      if (portfolio.length > 0) {
        setPortfolio((prev) =>
          prev.map((pos) => {
            const matching = loadedQuotes.find((q) => q.symbol === pos.symbol);
            if (matching) {
              const currentVal = matching.price * pos.shares;
              const pnl = currentVal - pos.investedValue;
              const pnlPct = pos.investedValue > 0 ? (pnl / pos.investedValue) * 100 : 0;
              return {
                ...pos,
                currentPrice: matching.price,
                currentValue: currentVal,
                unrealizedPnL: pnl,
                unrealizedPnLPercent: pnlPct,
              };
            }
            return pos;
          })
        );
      }
    } catch (err) {
      console.error('Error fetching live quotes:', err);
    } finally {
      setIsRefreshing(false);
    }
  };

  useEffect(() => {
    loadDiscoveryQuotes();
  }, []);

  // Handle stock selection from search or click
  const handleSelectStock = async (symbol: string) => {
    const existing = quotes.find((q) => q.symbol.toUpperCase() === symbol.toUpperCase());
    if (existing) {
      setSelectedStock(existing);
    } else {
      showToast(`Fetching real-time quotes for ${symbol}...`);
      const newQuote = await fetchLiveStockQuote(symbol);
      setQuotes((prev) => [newQuote, ...prev.filter((q) => q.symbol !== newQuote.symbol)]);
      setSelectedStock(newQuote);
    }
  };

  // Add position to Portfolio
  const handleAddToPortfolio = (quote: StockQuote, shares: number, cost: number) => {
    const fundamentals = evaluateStockPhilosophy(quote);
    const invested = shares * cost;
    const currentVal = shares * quote.price;
    const pnl = currentVal - invested;
    const pnlPct = invested > 0 ? (pnl / invested) * 100 : 0;

    const newPos: PortfolioPosition = {
      id: `${quote.symbol}-${Date.now()}`,
      symbol: quote.symbol,
      name: quote.name,
      exchange: quote.exchange,
      currency: quote.currency,
      shares,
      avgCost: cost,
      currentPrice: quote.price,
      investedValue: invested,
      currentValue: currentVal,
      unrealizedPnL: pnl,
      unrealizedPnLPercent: pnlPct,
      grahamScore: fundamentals.grahamScore,
      buffettScore: fundamentals.buffettScore,
      lynchScore: fundamentals.lynchScore,
      compositeScore: fundamentals.compositeScore,
      signal: fundamentals.overallSignal,
      dateAdded: new Date().toLocaleDateString(),
    };

    setPortfolio((prev) => [newPos, ...prev.filter((p) => p.symbol !== quote.symbol)]);
    showToast(`✓ Added ${shares} shares of ${quote.symbol} to Portfolio!`);
  };

  // Quick add from table
  const handleQuickAddToPortfolio = (quote: StockQuote) => {
    handleAddToPortfolio(quote, 10, quote.price);
  };

  // Remove position
  const handleRemovePortfolioPosition = (id: string) => {
    setPortfolio((prev) => prev.filter((p) => p.id !== id));
    showToast('Position removed from portfolio.');
  };

  // Add item to Watchlist
  const handleAddToWatchlist = (quote: StockQuote, targetPrice: number, notes: string) => {
    const fundamentals = evaluateStockPhilosophy(quote);
    const mosTargetPct = quote.price > 0 ? ((quote.price - targetPrice) / quote.price) * 100 : 0;

    const newItem: WatchlistItem = {
      id: `${quote.symbol}-${Date.now()}`,
      symbol: quote.symbol,
      name: quote.name,
      exchange: quote.exchange,
      currency: quote.currency,
      currentPrice: quote.price,
      targetBuyPrice: targetPrice,
      marginOfSafetyTargetPercent: Math.round(mosTargetPct * 10) / 10,
      compositeScore: fundamentals.compositeScore,
      signal: fundamentals.overallSignal,
      thesisNotes: notes,
      dateAdded: new Date().toLocaleDateString(),
    };

    setWatchlist((prev) => [newItem, ...prev.filter((w) => w.symbol !== quote.symbol)]);
    showToast(`✓ Added ${quote.symbol} to Watchlist at target $${targetPrice}!`);
  };

  const handleQuickAddToWatchlist = (quote: StockQuote) => {
    handleAddToWatchlist(quote, Math.round(quote.price * 0.85 * 100) / 100, 'Margin of Safety target.');
  };

  const handleRemoveWatchlistItem = (id: string) => {
    setWatchlist((prev) => prev.filter((w) => w.id !== id));
    showToast('Removed from watchlist.');
  };

  const handlePromoteWatchlistToPortfolio = (item: WatchlistItem) => {
    const matchingQuote = quotes.find((q) => q.symbol === item.symbol) || {
      symbol: item.symbol,
      name: item.name,
      exchange: item.exchange,
      currency: item.currency,
      price: item.currentPrice,
      change: 0,
      changePercent: 0,
      open: item.currentPrice,
      high: item.currentPrice,
      low: item.currentPrice,
      previousClose: item.currentPrice,
      volume: 1000000,
      updatedAt: '',
    };
    handleAddToPortfolio(matchingQuote, 10, item.currentPrice);
    handleRemoveWatchlistItem(item.id);
  };

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 flex flex-col font-sans">
      {/* Toast Notification */}
      {toastMessage && (
        <div className="fixed bottom-5 right-5 z-50 bg-emerald-600 text-white px-4 py-2.5 rounded-2xl shadow-2xl text-xs font-bold border border-emerald-400/40 animate-fade-in flex items-center gap-2">
          <span>{toastMessage}</span>
        </div>
      )}

      {/* Header with Search and Navigation */}
      <Header
        activeTab={activeTab}
        setActiveTab={setActiveTab}
        onSelectStock={handleSelectStock}
        watchlistCount={watchlist.length}
        portfolioCount={portfolio.length}
        onRefreshAll={loadDiscoveryQuotes}
        isRefreshing={isRefreshing}
      />

      {/* Main Container */}
      <main className="max-w-7xl w-full mx-auto px-4 py-6 flex-1">
        {activeTab === 'dashboard' && (
          <Dashboard
            quotes={quotes}
            onSelectStock={handleSelectStock}
            onQuickAddToPortfolio={handleQuickAddToPortfolio}
            onQuickAddToWatchlist={handleQuickAddToWatchlist}
            isLoading={isRefreshing}
            onSearchSymbol={handleSelectStock}
          />
        )}

        {activeTab === 'analyzer' && (
          <div className="space-y-4">
            <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 text-center">
              <h2 className="text-base font-bold text-white">Select Any Stock for Deep 3-Legend Analysis</h2>
              <p className="text-xs text-slate-400 mt-1 max-w-md mx-auto">
                Search any company or ticker in the top bar to inspect its Benjamin Graham Margin of Safety, Warren Buffett Economic Moat, and Peter Lynch PEG valuation.
              </p>
              <div className="flex flex-wrap items-center justify-center gap-2 mt-4">
                {quotes.slice(0, 8).map((q) => (
                  <button
                    key={q.symbol}
                    onClick={() => handleSelectStock(q.symbol)}
                    className="px-3 py-1.5 rounded-xl bg-slate-950 hover:bg-slate-800 border border-slate-800 text-xs font-bold text-slate-200 transition"
                  >
                    {q.symbol} ({q.currency}{q.price})
                  </button>
                ))}
              </div>
            </div>
            {quotes.length > 0 && (
              <Dashboard
                quotes={quotes}
                onSelectStock={handleSelectStock}
                onQuickAddToPortfolio={handleQuickAddToPortfolio}
                onQuickAddToWatchlist={handleQuickAddToWatchlist}
                isLoading={isRefreshing}
                onSearchSymbol={handleSelectStock}
              />
            )}
          </div>
        )}

        {activeTab === 'portfolio' && (
          <PortfolioView
            positions={portfolio}
            onAddPosition={handleAddToPortfolio}
            onRemovePosition={handleRemovePortfolioPosition}
            onSelectStock={handleSelectStock}
            onOpenLookupModal={() => {
              if (quotes[0]) setSelectedStock(quotes[0]);
            }}
          />
        )}

        {activeTab === 'watchlist' && (
          <WatchlistView
            items={watchlist}
            onRemoveItem={handleRemoveWatchlistItem}
            onSelectStock={handleSelectStock}
            onPromoteToPortfolio={handlePromoteWatchlistToPortfolio}
            onOpenLookupModal={() => {
              if (quotes[0]) setSelectedStock(quotes[0]);
            }}
          />
        )}
      </main>

      {/* Stock Deep Analysis Modal */}
      {selectedStock && (
        <StockAnalysisModal
          quote={selectedStock}
          onClose={() => setSelectedStock(null)}
          onAddToPortfolio={handleAddToPortfolio}
          onAddToWatchlist={handleAddToWatchlist}
        />
      )}

      {/* Footer */}
      <footer className="border-t border-slate-900 bg-slate-950 py-5 text-center text-xs text-slate-500">
        <div className="max-w-7xl mx-auto px-4 flex flex-col sm:flex-row items-center justify-between gap-2">
          <span>
            Institutional Value Investing &amp; Growth Platform • Benjamin Graham • Warren Buffett • Peter Lynch
          </span>
          <span className="text-[11px] text-slate-600">
            Real-time live streaming quotes &amp; dynamic valuation models
          </span>
        </div>
      </footer>
    </div>
  );
}
