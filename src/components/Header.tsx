import React, { useState, useEffect, useRef } from 'react';
import { Search, ShieldAlert, Award, TrendingUp, Sparkles, RefreshCw, Globe, ChevronRight } from 'lucide-react';
import { searchLiveSymbols, SearchResultItem } from '../services/marketApi';

interface HeaderProps {
  activeTab: 'dashboard' | 'analyzer' | 'portfolio' | 'watchlist';
  setActiveTab: (tab: 'dashboard' | 'analyzer' | 'portfolio' | 'watchlist') => void;
  onSelectStock: (symbol: string) => void;
  watchlistCount: number;
  portfolioCount: number;
  onRefreshAll: () => void;
  isRefreshing: boolean;
}

export const Header: React.FC<HeaderProps> = ({
  activeTab,
  setActiveTab,
  onSelectStock,
  watchlistCount,
  portfolioCount,
  onRefreshAll,
  isRefreshing,
}) => {
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState<SearchResultItem[]>([]);
  const [isSearching, setIsSearching] = useState(false);
  const [isOpenDropdown, setIsOpenDropdown] = useState(false);
  const searchRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const timer = setTimeout(async () => {
      if (searchQuery.trim().length >= 1) {
        setIsSearching(true);
        try {
          const results = await searchLiveSymbols(searchQuery);
          setSearchResults(results);
          setIsOpenDropdown(true);
        } catch (_) {
          setSearchResults([]);
        } finally {
          setIsSearching(false);
        }
      } else {
        setSearchResults([]);
        setIsOpenDropdown(false);
      }
    }, 280);

    return () => clearTimeout(timer);
  }, [searchQuery]);

  // Click outside to close dropdown
  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (searchRef.current && !searchRef.current.contains(event.target as Node)) {
        setIsOpenDropdown(false);
      }
    }
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const handleSelectSymbol = (symbol: string) => {
    setIsOpenDropdown(false);
    setSearchQuery('');
    onSelectStock(symbol);
  };

  const handleFormSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (searchQuery.trim()) {
      handleSelectSymbol(searchQuery.trim().toUpperCase());
    }
  };

  return (
    <header className="sticky top-0 z-40 bg-slate-900/95 backdrop-blur-md border-b border-slate-800">
      {/* Top Banner: Real-time Market Status & Legends Pill */}
      <div className="px-4 py-1.5 bg-slate-950/80 border-b border-slate-800/80 text-xs flex flex-wrap items-center justify-between gap-2">
        <div className="flex items-center gap-2 flex-wrap">
          <span className="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full bg-emerald-500/15 text-emerald-300 border border-emerald-500/30 font-medium">
            <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse"></span>
            Triple-Legend Value Engine Active
          </span>
          <span className="text-slate-400 text-[11px] hidden md:inline">
            Graham (Safety) • Buffett (Moat) • Lynch (PEG)
          </span>
        </div>
        <div className="flex items-center gap-3 text-[11px] text-slate-400">
          <span className="flex items-center gap-1 text-slate-300">
            <Globe className="w-3.5 h-3.5 text-blue-400" />
            <span>US &amp; Global Markets Active</span>
          </span>
          <button
            onClick={onRefreshAll}
            disabled={isRefreshing}
            className="flex items-center gap-1.5 px-2 py-0.5 rounded bg-slate-800 hover:bg-slate-700 text-slate-200 border border-slate-700 transition"
          >
            <RefreshCw className={`w-3 h-3 text-emerald-400 ${isRefreshing ? 'animate-spin' : ''}`} />
            <span>{isRefreshing ? 'Syncing...' : 'Live Refresh'}</span>
          </button>
        </div>
      </div>

      {/* Main Header Row */}
      <div className="max-w-7xl mx-auto px-4 py-3 flex flex-col md:flex-row md:items-center justify-between gap-3">
        {/* Brand */}
        <div className="flex items-center gap-3 cursor-pointer" onClick={() => setActiveTab('dashboard')}>
          <div className="w-10 h-10 rounded-xl bg-gradient-to-tr from-emerald-600 via-teal-500 to-blue-600 flex items-center justify-center font-black text-white text-lg shadow-lg shadow-emerald-500/20">
            3L
          </div>
          <div>
            <div className="flex items-center gap-2">
              <h1 className="font-extrabold text-base md:text-lg text-white tracking-tight">
                Value Investing &amp; Growth Matrix
              </h1>
              <span className="px-1.5 py-0.5 rounded bg-emerald-500/20 text-emerald-300 text-[10px] font-mono font-bold">
                PRO
              </span>
            </div>
            <p className="text-xs text-slate-400">
              Benjamin Graham • Warren Buffett • Peter Lynch Intelligence
            </p>
          </div>
        </div>

        {/* Dynamic Universal Search Bar */}
        <div className="relative flex-1 max-w-md" ref={searchRef}>
          <form onSubmit={handleFormSubmit} className="relative">
            <Search className="w-4 h-4 absolute left-3.5 top-1/2 -translate-y-1/2 text-slate-400" />
            <input
              type="text"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              onFocus={() => searchQuery.trim() && setIsOpenDropdown(true)}
              placeholder="Search any real stock or ETF (e.g. AAPL, BRK-B, HDFCBANK.NS, NVDA)..."
              className="w-full bg-slate-950/90 border border-slate-700 focus:border-emerald-500 rounded-xl pl-9 pr-9 py-2 text-xs text-slate-100 placeholder-slate-500 focus:outline-none shadow-inner"
            />
            {isSearching && (
              <RefreshCw className="w-3.5 h-3.5 absolute right-3.5 top-1/2 -translate-y-1/2 text-emerald-400 animate-spin" />
            )}
          </form>

          {/* Autocomplete Dropdown */}
          {isOpenDropdown && (
            <div className="absolute left-0 right-0 top-full mt-1.5 bg-slate-900 border border-slate-700/80 rounded-xl shadow-2xl overflow-hidden z-50 divide-y divide-slate-800">
              {searchResults.length > 0 ? (
                searchResults.map((item) => (
                  <button
                    key={item.symbol}
                    type="button"
                    onClick={() => handleSelectSymbol(item.symbol)}
                    className="w-full px-3.5 py-2.5 text-left hover:bg-slate-800/80 flex items-center justify-between text-xs transition"
                  >
                    <div>
                      <div className="font-bold text-white flex items-center gap-1.5">
                        <span>{item.symbol}</span>
                        <span className="text-[10px] px-1.5 py-0.2 rounded bg-slate-800 text-slate-400 font-mono">
                          {item.exchange}
                        </span>
                      </div>
                      <div className="text-[11px] text-slate-400 truncate max-w-xs">{item.name}</div>
                    </div>
                    <ChevronRight className="w-4 h-4 text-slate-500" />
                  </button>
                ))
              ) : searchQuery.trim().length >= 1 ? (
                <button
                  type="button"
                  onClick={() => handleSelectSymbol(searchQuery.trim().toUpperCase())}
                  className="w-full px-3.5 py-2.5 text-left hover:bg-slate-800 flex items-center justify-between text-xs"
                >
                  <span className="text-slate-300">
                    Analyze symbol <strong>{searchQuery.trim().toUpperCase()}</strong>
                  </span>
                  <ChevronRight className="w-4 h-4 text-emerald-400" />
                </button>
              ) : null}
            </div>
          )}
        </div>
      </div>

      {/* Navigation Tabs */}
      <div className="max-w-7xl mx-auto px-4 overflow-x-auto flex items-center gap-2 border-t border-slate-800/60 pt-2 pb-2">
        <button
          onClick={() => setActiveTab('dashboard')}
          className={`flex items-center gap-1.5 px-3.5 py-1.5 rounded-lg text-xs font-bold transition whitespace-nowrap ${
            activeTab === 'dashboard'
              ? 'bg-emerald-600/20 text-emerald-400 border border-emerald-500/40 shadow-sm'
              : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/50'
          }`}
        >
          <Award className="w-3.5 h-3.5" />
          <span>1. Three Legends Dashboard</span>
        </button>

        <button
          onClick={() => setActiveTab('analyzer')}
          className={`flex items-center gap-1.5 px-3.5 py-1.5 rounded-lg text-xs font-bold transition whitespace-nowrap ${
            activeTab === 'analyzer'
              ? 'bg-emerald-600/20 text-emerald-400 border border-emerald-500/40 shadow-sm'
              : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/50'
          }`}
        >
          <Sparkles className="w-3.5 h-3.5" />
          <span>2. Deep Stock Analyzer &amp; DCF</span>
        </button>

        <button
          onClick={() => setActiveTab('portfolio')}
          className={`flex items-center gap-1.5 px-3.5 py-1.5 rounded-lg text-xs font-bold transition whitespace-nowrap ${
            activeTab === 'portfolio'
              ? 'bg-emerald-600/20 text-emerald-400 border border-emerald-500/40 shadow-sm'
              : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/50'
          }`}
        >
          <TrendingUp className="w-3.5 h-3.5" />
          <span>3. Live Portfolio Tracker</span>
          <span className="ml-1 px-1.5 py-0.2 rounded-full bg-slate-800 text-[10px] text-slate-300">
            {portfolioCount}
          </span>
        </button>

        <button
          onClick={() => setActiveTab('watchlist')}
          className={`flex items-center gap-1.5 px-3.5 py-1.5 rounded-lg text-xs font-bold transition whitespace-nowrap ${
            activeTab === 'watchlist'
              ? 'bg-emerald-600/20 text-emerald-400 border border-emerald-500/40 shadow-sm'
              : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/50'
          }`}
        >
          <ShieldAlert className="w-3.5 h-3.5" />
          <span>4. Watchlist &amp; MoS Alerts</span>
          <span className="ml-1 px-1.5 py-0.2 rounded-full bg-slate-800 text-[10px] text-slate-300">
            {watchlistCount}
          </span>
        </button>
      </div>
    </header>
  );
};
