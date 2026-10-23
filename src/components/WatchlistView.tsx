import React from 'react';
import { ShieldCheck, PlusCircle, Trash2, ArrowUpRight, TrendingDown } from 'lucide-react';
import { WatchlistItem, StockQuote } from '../types';

interface WatchlistViewProps {
  items: WatchlistItem[];
  onRemoveItem: (id: string) => void;
  onSelectStock: (symbol: string) => void;
  onPromoteToPortfolio: (item: WatchlistItem) => void;
  onOpenLookupModal: () => void;
}

export const WatchlistView: React.FC<WatchlistViewProps> = ({
  items,
  onRemoveItem,
  onSelectStock,
  onPromoteToPortfolio,
  onOpenLookupModal,
}) => {
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

  return (
    <div className="space-y-6">
      {/* Control Bar */}
      <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 className="text-sm font-bold text-white flex items-center gap-2">
            <span>Value Watchlist &amp; Margin of Safety Radar</span>
            <span className="px-2 py-0.5 rounded bg-slate-800 text-xs font-mono text-slate-300">
              {items.length} items
            </span>
          </h2>
          <p className="text-xs text-slate-400">
            Monitoring market prices against your predetermined target buy entry points
          </p>
        </div>

        <button
          onClick={onOpenLookupModal}
          className="flex items-center gap-1.5 px-3.5 py-2 rounded-xl bg-emerald-600 hover:bg-emerald-500 text-white text-xs font-bold transition shadow-sm"
        >
          <PlusCircle className="w-4 h-4" />
          <span>+ Add Stock to Watchlist</span>
        </button>
      </div>

      {/* Watchlist Table */}
      <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 overflow-hidden shadow-lg">
        <div className="overflow-x-auto -mx-4 px-4">
          <table className="w-full min-w-[850px] text-left border-collapse text-xs">
            <thead>
              <tr className="border-b border-slate-800 text-slate-400 uppercase text-[11px] font-mono">
                <th className="py-2.5 px-3">Asset</th>
                <th className="py-2.5 px-3 text-right">Live Price</th>
                <th className="py-2.5 px-3 text-right">Target Buy Limit</th>
                <th className="py-2.5 px-3 text-right">Discount to Target</th>
                <th className="py-2.5 px-3">Thesis &amp; Notes</th>
                <th className="py-2.5 px-3 text-center">Score</th>
                <th className="py-2.5 px-3">Philosophy Signal</th>
                <th className="py-2.5 px-3 text-right">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/60 font-sans">
              {items.length > 0 ? (
                items.map((item) => {
                  const discountPct =
                    item.currentPrice > 0
                      ? Math.round(((item.targetBuyPrice - item.currentPrice) / item.currentPrice) * 1000) / 10
                      : 0;
                  const isReadyToBuy = item.currentPrice <= item.targetBuyPrice;

                  return (
                    <tr
                      key={item.id}
                      className="hover:bg-slate-800/40 transition group cursor-pointer"
                      onClick={() => onSelectStock(item.symbol)}
                    >
                      <td className="py-3 px-3">
                        <div className="font-bold text-white text-sm group-hover:text-emerald-400 transition">
                          {item.symbol}
                        </div>
                        <div className="text-[11px] text-slate-400 truncate max-w-xs">{item.name}</div>
                      </td>

                      <td className="py-3 px-3 text-right font-mono font-bold text-white text-sm">
                        {item.currency}{item.currentPrice.toFixed(2)}
                      </td>

                      <td className="py-3 px-3 text-right font-mono text-emerald-400 font-bold">
                        {item.currency}{item.targetBuyPrice.toFixed(2)}
                      </td>

                      <td className="py-3 px-3 text-right font-mono">
                        <span
                          className={`font-bold text-xs ${
                            isReadyToBuy ? 'text-emerald-400' : 'text-slate-300'
                          }`}
                        >
                          {isReadyToBuy ? 'IN BUY ZONE' : `${discountPct}% away`}
                        </span>
                      </td>

                      <td className="py-3 px-3 text-slate-300 max-w-xs truncate">
                        {item.thesisNotes || 'Long-term compounding target.'}
                      </td>

                      <td className="py-3 px-3 text-center font-mono">
                        <span className="px-2 py-0.5 rounded bg-slate-950 border border-slate-800 font-bold text-emerald-400">
                          {item.compositeScore}
                        </span>
                      </td>

                      <td className="py-3 px-3">
                        <span
                          className={`inline-block px-2.5 py-0.5 rounded-full text-[11px] font-bold border ${getSignalBadge(
                            item.signal
                          )}`}
                        >
                          {item.signal.replace('_', ' ')}
                        </span>
                      </td>

                      <td
                        className="py-3 px-3 text-right space-x-1.5 whitespace-nowrap"
                        onClick={(e) => e.stopPropagation()}
                      >
                        <button
                          onClick={() => onPromoteToPortfolio(item)}
                          className="px-2 py-1 rounded bg-blue-600/20 hover:bg-blue-600 text-blue-300 hover:text-white border border-blue-500/40 text-[11px] font-semibold"
                          title="Buy & Move to Portfolio"
                        >
                          + Buy / PF
                        </button>
                        <button
                          onClick={() => onRemoveItem(item.id)}
                          className="p-1 rounded bg-rose-500/10 hover:bg-rose-500/20 text-rose-400 border border-rose-500/30"
                          title="Remove from watchlist"
                        >
                          <Trash2 className="w-3.5 h-3.5" />
                        </button>
                      </td>
                    </tr>
                  );
                })
              ) : (
                <tr>
                  <td colSpan={8} className="py-8 text-center text-slate-400">
                    Your watchlist is currently empty. Add stocks with custom target buy prices to track your Margin of Safety!
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
