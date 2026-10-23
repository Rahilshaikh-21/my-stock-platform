import React, { useState } from 'react';
import { TrendingUp, TrendingDown, DollarSign, PlusCircle, Trash2, ShieldCheck, Award, Zap, PieChart, Info } from 'lucide-react';
import { PortfolioPosition, StockQuote } from '../types';

interface PortfolioViewProps {
  positions: PortfolioPosition[];
  onAddPosition: (quote: StockQuote, shares: number, cost: number) => void;
  onRemovePosition: (id: string) => void;
  onSelectStock: (symbol: string) => void;
  onOpenLookupModal: () => void;
}

export const PortfolioView: React.FC<PortfolioViewProps> = ({
  positions,
  onAddPosition,
  onRemovePosition,
  onSelectStock,
  onOpenLookupModal,
}) => {
  // Aggregate portfolio metrics
  const totalInvested = positions.reduce((acc, p) => acc + p.investedValue, 0);
  const totalCurrentValue = positions.reduce((acc, p) => acc + p.currentValue, 0);
  const totalPnL = totalCurrentValue - totalInvested;
  const totalPnLPercent = totalInvested > 0 ? (totalPnL / totalInvested) * 100 : 0;

  // Weighted philosophy scores based on position weight
  let weightedGraham = 0;
  let weightedBuffett = 0;
  let weightedLynch = 0;
  let weightedComposite = 0;

  if (totalCurrentValue > 0) {
    positions.forEach((p) => {
      const weight = p.currentValue / totalCurrentValue;
      weightedGraham += p.grahamScore * weight;
      weightedBuffett += p.buffettScore * weight;
      weightedLynch += p.lynchScore * weight;
      weightedComposite += p.compositeScore * weight;
    });
  } else {
    weightedGraham = 50;
    weightedBuffett = 50;
    weightedLynch = 50;
    weightedComposite = 50;
  }

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
      {/* Portfolio Financial & Philosophy Summary Strips */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <div className="bg-slate-900 border border-slate-800 p-4 rounded-2xl">
          <span className="text-xs text-slate-400 block">Total Portfolio Valuation</span>
          <div className="text-2xl font-black font-mono text-white mt-1">
            ${totalCurrentValue.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
          </div>
          <span className="text-[11px] text-slate-400">
            Cost Basis: ${totalInvested.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
          </span>
        </div>

        <div className="bg-slate-900 border border-slate-800 p-4 rounded-2xl">
          <span className="text-xs text-slate-400 block">Total Unrealized Returns</span>
          <div
            className={`text-2xl font-black font-mono mt-1 ${
              totalPnL >= 0 ? 'text-emerald-400' : 'text-rose-400'
            }`}
          >
            {totalPnL >= 0 ? '+' : ''}
            ${totalPnL.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
          </div>
          <span
            className={`text-[11px] font-mono flex items-center gap-1 ${
              totalPnLPercent >= 0 ? 'text-emerald-400' : 'text-rose-400'
            }`}
          >
            {totalPnLPercent >= 0 ? <TrendingUp className="w-3 h-3" /> : <TrendingDown className="w-3 h-3" />}
            {totalPnLPercent >= 0 ? '+' : ''}
            {totalPnLPercent.toFixed(2)}% overall return
          </span>
        </div>

        <div className="bg-slate-900 border border-slate-800 p-4 rounded-2xl">
          <span className="text-xs text-slate-400 block">Aggregate Philosophy Health</span>
          <div className="text-2xl font-black font-mono text-emerald-400 mt-1">
            {Math.round(weightedComposite)}
            <span className="text-xs text-slate-500 font-normal">/100</span>
          </div>
          <span className="text-[11px] text-slate-400">
            Weighted 3-Legend Health Rating
          </span>
        </div>

        <div className="bg-slate-900 border border-slate-800 p-4 rounded-2xl">
          <span className="text-xs text-slate-400 block">Philosophy Sub-Indices</span>
          <div className="text-xs font-mono text-slate-200 mt-1.5 space-y-0.5">
            <div className="flex justify-between">
              <span className="text-slate-400">Graham Safety:</span>
              <span className="text-emerald-400 font-bold">{Math.round(weightedGraham)}/100</span>
            </div>
            <div className="flex justify-between">
              <span className="text-slate-400">Buffett Moat:</span>
              <span className="text-blue-400 font-bold">{Math.round(weightedBuffett)}/100</span>
            </div>
            <div className="flex justify-between">
              <span className="text-slate-400">Lynch Growth:</span>
              <span className="text-amber-400 font-bold">{Math.round(weightedLynch)}/100</span>
            </div>
          </div>
        </div>
      </div>

      {/* Control Bar */}
      <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 className="text-sm font-bold text-white flex items-center gap-2">
            <span>Active Holdings Ledger</span>
            <span className="px-2 py-0.5 rounded bg-slate-800 text-xs font-mono text-slate-300">
              {positions.length} positions
            </span>
          </h2>
          <p className="text-xs text-slate-400">
            Real-time live prices, position P&amp;L, and position-specific legend health signals
          </p>
        </div>

        <button
          onClick={onOpenLookupModal}
          className="flex items-center gap-1.5 px-3.5 py-2 rounded-xl bg-blue-600 hover:bg-blue-500 text-white text-xs font-bold transition shadow-sm"
        >
          <PlusCircle className="w-4 h-4" />
          <span>+ Add Position</span>
        </button>
      </div>

      {/* Holdings Table */}
      <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 overflow-hidden shadow-lg">
        <div className="overflow-x-auto -mx-4 px-4">
          <table className="w-full min-w-[900px] text-left border-collapse text-xs">
            <thead>
              <tr className="border-b border-slate-800 text-slate-400 uppercase text-[11px] font-mono">
                <th className="py-2.5 px-3">Holding</th>
                <th className="py-2.5 px-3 text-right">Shares</th>
                <th className="py-2.5 px-3 text-right">Avg Purchase Cost</th>
                <th className="py-2.5 px-3 text-right">Live Price</th>
                <th className="py-2.5 px-3 text-right">Current Value</th>
                <th className="py-2.5 px-3 text-right">Unrealized P&amp;L</th>
                <th className="py-2.5 px-3 text-center">Score</th>
                <th className="py-2.5 px-3">Philosophy Signal</th>
                <th className="py-2.5 px-3 text-right">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/60 font-sans">
              {positions.length > 0 ? (
                positions.map((pos) => {
                  const isGain = pos.unrealizedPnL >= 0;
                  return (
                    <tr
                      key={pos.id}
                      className="hover:bg-slate-800/40 transition group cursor-pointer"
                      onClick={() => onSelectStock(pos.symbol)}
                    >
                      <td className="py-3 px-3">
                        <div className="font-bold text-white text-sm group-hover:text-emerald-400 transition">
                          {pos.symbol}
                        </div>
                        <div className="text-[11px] text-slate-400 truncate max-w-xs">{pos.name}</div>
                      </td>

                      <td className="py-3 px-3 text-right font-mono text-slate-200">
                        {pos.shares} sh
                      </td>

                      <td className="py-3 px-3 text-right font-mono text-slate-300">
                        {pos.currency}{pos.avgCost.toFixed(2)}
                      </td>

                      <td className="py-3 px-3 text-right font-mono font-bold text-white">
                        {pos.currency}{pos.currentPrice.toFixed(2)}
                      </td>

                      <td className="py-3 px-3 text-right font-mono font-bold text-slate-100">
                        {pos.currency}{pos.currentValue.toFixed(2)}
                      </td>

                      <td className="py-3 px-3 text-right font-mono">
                        <div className={`font-bold ${isGain ? 'text-emerald-400' : 'text-rose-400'}`}>
                          {isGain ? '+' : ''}{pos.currency}{pos.unrealizedPnL.toFixed(2)}
                        </div>
                        <div className={`text-[10px] ${isGain ? 'text-emerald-400' : 'text-rose-400'}`}>
                          {isGain ? '+' : ''}{pos.unrealizedPnLPercent.toFixed(2)}%
                        </div>
                      </td>

                      <td className="py-3 px-3 text-center font-mono">
                        <span className="px-2 py-0.5 rounded bg-slate-950 border border-slate-800 font-bold text-emerald-400">
                          {pos.compositeScore}
                        </span>
                      </td>

                      <td className="py-3 px-3">
                        <span
                          className={`inline-block px-2.5 py-0.5 rounded-full text-[11px] font-bold border ${getSignalBadge(
                            pos.signal
                          )}`}
                        >
                          {pos.signal.replace('_', ' ')}
                        </span>
                      </td>

                      <td
                        className="py-3 px-3 text-right space-x-1"
                        onClick={(e) => e.stopPropagation()}
                      >
                        <button
                          onClick={() => onSelectStock(pos.symbol)}
                          className="px-2.5 py-1 rounded bg-slate-800 hover:bg-slate-700 text-slate-300 text-[11px] font-semibold"
                        >
                          Analyze
                        </button>
                        <button
                          onClick={() => onRemovePosition(pos.id)}
                          className="p-1 rounded bg-rose-500/10 hover:bg-rose-500/20 text-rose-400 border border-rose-500/30"
                          title="Remove holding"
                        >
                          <Trash2 className="w-3.5 h-3.5" />
                        </button>
                      </td>
                    </tr>
                  );
                })
              ) : (
                <tr>
                  <td colSpan={9} className="py-8 text-center text-slate-400">
                    No holdings in portfolio yet. Click <strong>+ Add Position</strong> or search any stock to build your value portfolio!
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
