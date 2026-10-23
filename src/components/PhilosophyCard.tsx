import React from 'react';
import { ShieldCheck, CheckCircle2, XCircle, Award, Compass, Zap } from 'lucide-react';
import { StockFundamentals } from '../types';

interface PhilosophyCardProps {
  type: 'graham' | 'buffett' | 'lynch';
  fundamentals: StockFundamentals;
  price: number;
  currency: string;
}

export const PhilosophyCard: React.FC<PhilosophyCardProps> = ({
  type,
  fundamentals,
  price,
  currency,
}) => {
  if (type === 'graham') {
    const isUndervalued = fundamentals.grahamMarginOfSafetyPercent > 0;
    return (
      <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 space-y-4 hover:border-slate-700 transition">
        {/* Header */}
        <div className="flex items-center justify-between border-b border-slate-800 pb-3">
          <div className="flex items-center gap-2.5">
            <div className="w-8 h-8 rounded-lg bg-emerald-500/15 border border-emerald-500/30 flex items-center justify-center text-emerald-400">
              <ShieldCheck className="w-4 h-4" />
            </div>
            <div>
              <h3 className="font-bold text-white text-sm">Benjamin Graham</h3>
              <p className="text-[11px] text-slate-400">The Father of Value Investing</p>
            </div>
          </div>
          <div className="text-right">
            <div className="text-xl font-black font-mono text-emerald-400">
              {fundamentals.grahamScore}
              <span className="text-xs text-slate-500 font-normal">/100</span>
            </div>
            <span className="text-[10px] uppercase font-bold text-slate-400">Graham Score</span>
          </div>
        </div>

        {/* Core Formula & Fair Value Metric */}
        <div className="grid grid-cols-2 gap-2.5 bg-slate-950 p-3 rounded-xl border border-slate-800/80 font-mono text-xs">
          <div>
            <span className="text-[11px] text-slate-400 block">Graham Number</span>
            <span className="text-base font-bold text-emerald-400">
              {currency}{fundamentals.grahamNumber}
            </span>
            <span className="text-[10px] text-slate-500 block">√(22.5 × EPS × BVPS)</span>
          </div>
          <div>
            <span className="text-[11px] text-slate-400 block">Margin of Safety</span>
            <span className={`text-base font-bold ${isUndervalued ? 'text-emerald-300' : 'text-rose-400'}`}>
              {fundamentals.grahamMarginOfSafetyPercent > 0 ? '+' : ''}
              {fundamentals.grahamMarginOfSafetyPercent}%
            </span>
            <span className="text-[10px] text-slate-500 block">Target: ≥ +20%</span>
          </div>
        </div>

        {/* Specific Metrics */}
        <div className="grid grid-cols-3 gap-2 text-center text-xs">
          <div className="bg-slate-950/60 p-2 rounded-lg border border-slate-800">
            <span className="text-[10px] text-slate-400 block">Book Value</span>
            <span className="font-bold font-mono text-slate-200">{currency}{fundamentals.bookValuePerShare}</span>
          </div>
          <div className="bg-slate-950/60 p-2 rounded-lg border border-slate-800">
            <span className="text-[10px] text-slate-400 block">Current Ratio</span>
            <span className="font-bold font-mono text-slate-200">{fundamentals.currentRatio}x</span>
          </div>
          <div className="bg-slate-950/60 p-2 rounded-lg border border-slate-800">
            <span className="text-[10px] text-slate-400 block">Debt / Equity</span>
            <span className="font-bold font-mono text-slate-200">{fundamentals.debtToEquity}x</span>
          </div>
        </div>

        {/* Tests Passed / Failed */}
        <div className="space-y-1.5 text-xs">
          <span className="text-[11px] font-bold text-slate-400 uppercase tracking-wider block">Rule Verification</span>
          {fundamentals.grahamPassedTests.map((test, idx) => (
            <div key={idx} className="flex items-start gap-1.5 text-emerald-300 text-[11px]">
              <CheckCircle2 className="w-3.5 h-3.5 shrink-0 mt-0.5 text-emerald-400" />
              <span>{test}</span>
            </div>
          ))}
          {fundamentals.grahamFailedTests.map((test, idx) => (
            <div key={idx} className="flex items-start gap-1.5 text-slate-400 text-[11px]">
              <XCircle className="w-3.5 h-3.5 shrink-0 mt-0.5 text-rose-400" />
              <span>{test}</span>
            </div>
          ))}
        </div>

        {/* Quote */}
        <blockquote className="text-[11px] text-slate-400 italic border-l-2 border-emerald-500/40 pl-2.5">
          "An investment operation is one which, upon thorough analysis, promises safety of principal and an adequate return."
        </blockquote>
      </div>
    );
  }

  if (type === 'buffett') {
    const isUndervalued = fundamentals.buffettMarginOfSafetyPercent > 0;
    return (
      <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 space-y-4 hover:border-slate-700 transition">
        {/* Header */}
        <div className="flex items-center justify-between border-b border-slate-800 pb-3">
          <div className="flex items-center gap-2.5">
            <div className="w-8 h-8 rounded-lg bg-blue-500/15 border border-blue-500/30 flex items-center justify-center text-blue-400">
              <Award className="w-4 h-4" />
            </div>
            <div>
              <h3 className="font-bold text-white text-sm">Warren Buffett</h3>
              <p className="text-[11px] text-slate-400">The Oracle of Omaha</p>
            </div>
          </div>
          <div className="text-right">
            <div className="text-xl font-black font-mono text-blue-400">
              {fundamentals.buffettScore}
              <span className="text-xs text-slate-500 font-normal">/100</span>
            </div>
            <span className="text-[10px] uppercase font-bold text-slate-400">Buffett Score</span>
          </div>
        </div>

        {/* Core Formula & Fair Value Metric */}
        <div className="grid grid-cols-2 gap-2.5 bg-slate-950 p-3 rounded-xl border border-slate-800/80 font-mono text-xs">
          <div>
            <span className="text-[11px] text-slate-400 block">Owner Earnings DCF</span>
            <span className="text-base font-bold text-blue-400">
              {currency}{fundamentals.dcfFairValue}
            </span>
            <span className="text-[10px] text-slate-500 block">Cash Flow Present Value</span>
          </div>
          <div>
            <span className="text-[11px] text-slate-400 block">Economic Moat</span>
            <span className="text-base font-bold text-emerald-300">
              {fundamentals.moatStrength}
            </span>
            <span className="text-[10px] text-slate-500 block">Pricing Power Guard</span>
          </div>
        </div>

        {/* Specific Metrics */}
        <div className="grid grid-cols-3 gap-2 text-center text-xs">
          <div className="bg-slate-950/60 p-2 rounded-lg border border-slate-800">
            <span className="text-[10px] text-slate-400 block">Return on Equity</span>
            <span className="font-bold font-mono text-slate-200">{fundamentals.roe}%</span>
          </div>
          <div className="bg-slate-950/60 p-2 rounded-lg border border-slate-800">
            <span className="text-[10px] text-slate-400 block">Operating Margin</span>
            <span className="font-bold font-mono text-slate-200">{fundamentals.operatingMargin}%</span>
          </div>
          <div className="bg-slate-950/60 p-2 rounded-lg border border-slate-800">
            <span className="text-[10px] text-slate-400 block">DCF MoS</span>
            <span className={`font-bold font-mono ${isUndervalued ? 'text-emerald-400' : 'text-rose-400'}`}>
              {fundamentals.buffettMarginOfSafetyPercent > 0 ? '+' : ''}{fundamentals.buffettMarginOfSafetyPercent}%
            </span>
          </div>
        </div>

        {/* Tests Passed / Failed */}
        <div className="space-y-1.5 text-xs">
          <span className="text-[11px] font-bold text-slate-400 uppercase tracking-wider block">Moat &amp; Compounding Tests</span>
          {fundamentals.buffettPassedTests.map((test, idx) => (
            <div key={idx} className="flex items-start gap-1.5 text-blue-300 text-[11px]">
              <CheckCircle2 className="w-3.5 h-3.5 shrink-0 mt-0.5 text-blue-400" />
              <span>{test}</span>
            </div>
          ))}
          {fundamentals.buffettFailedTests.map((test, idx) => (
            <div key={idx} className="flex items-start gap-1.5 text-slate-400 text-[11px]">
              <XCircle className="w-3.5 h-3.5 shrink-0 mt-0.5 text-rose-400" />
              <span>{test}</span>
            </div>
          ))}
        </div>

        {/* Quote */}
        <blockquote className="text-[11px] text-slate-400 italic border-l-2 border-blue-500/40 pl-2.5">
          "It's far better to buy a wonderful company at a fair price than a fair company at a wonderful price."
        </blockquote>
      </div>
    );
  }

  // Peter Lynch
  const isLynchBargain = fundamentals.pegRatio <= 1.0;
  return (
    <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 space-y-4 hover:border-slate-700 transition">
      {/* Header */}
      <div className="flex items-center justify-between border-b border-slate-800 pb-3">
        <div className="flex items-center gap-2.5">
          <div className="w-8 h-8 rounded-lg bg-amber-500/15 border border-amber-500/30 flex items-center justify-center text-amber-400">
            <Zap className="w-4 h-4" />
          </div>
          <div>
            <h3 className="font-bold text-white text-sm">Peter Lynch</h3>
            <p className="text-[11px] text-slate-400">Fidelity Magellan Legend</p>
          </div>
        </div>
        <div className="text-right">
          <div className="text-xl font-black font-mono text-amber-400">
            {fundamentals.lynchScore}
            <span className="text-xs text-slate-500 font-normal">/100</span>
          </div>
          <span className="text-[10px] uppercase font-bold text-slate-400">Lynch Score</span>
        </div>
      </div>

      {/* Core Formula & Fair Value Metric */}
      <div className="grid grid-cols-2 gap-2.5 bg-slate-950 p-3 rounded-xl border border-slate-800/80 font-mono text-xs">
        <div>
          <span className="text-[11px] text-slate-400 block">PEG Ratio</span>
          <span className={`text-base font-bold ${isLynchBargain ? 'text-emerald-400' : 'text-amber-300'}`}>
            {fundamentals.pegRatio}x
          </span>
          <span className="text-[10px] text-slate-500 block">Target: &lt; 1.00 Golden</span>
        </div>
        <div>
          <span className="text-[11px] text-slate-400 block">Lynch Category</span>
          <span className="text-base font-bold text-amber-300">
            {fundamentals.lynchCategory}
          </span>
          <span className="text-[10px] text-slate-500 block">Profile Classification</span>
        </div>
      </div>

      {/* Specific Metrics */}
      <div className="grid grid-cols-3 gap-2 text-center text-xs">
        <div className="bg-slate-950/60 p-2 rounded-lg border border-slate-800">
          <span className="text-[10px] text-slate-400 block">Earnings Growth</span>
          <span className="font-bold font-mono text-slate-200">+{fundamentals.earningsGrowthRate}%</span>
        </div>
        <div className="bg-slate-950/60 p-2 rounded-lg border border-slate-800">
          <span className="text-[10px] text-slate-400 block">Lynch Fair Value</span>
          <span className="font-bold font-mono text-slate-200">{currency}{fundamentals.lynchFairValue}</span>
        </div>
        <div className="bg-slate-950/60 p-2 rounded-lg border border-slate-800">
          <span className="text-[10px] text-slate-400 block">Cash / Debt</span>
          <span className="font-bold font-mono text-slate-200">{fundamentals.cashToDebtRatio}x</span>
        </div>
      </div>

      {/* Tests Passed / Failed */}
      <div className="space-y-1.5 text-xs">
        <span className="text-[11px] font-bold text-slate-400 uppercase tracking-wider block">Lynch Growth Verification</span>
        {fundamentals.lynchPassedTests.map((test, idx) => (
          <div key={idx} className="flex items-start gap-1.5 text-amber-300 text-[11px]">
            <CheckCircle2 className="w-3.5 h-3.5 shrink-0 mt-0.5 text-amber-400" />
            <span>{test}</span>
          </div>
        ))}
        {fundamentals.lynchFailedTests.map((test, idx) => (
          <div key={idx} className="flex items-start gap-1.5 text-slate-400 text-[11px]">
            <XCircle className="w-3.5 h-3.5 shrink-0 mt-0.5 text-rose-400" />
            <span>{test}</span>
          </div>
        ))}
      </div>

      {/* Quote */}
      <blockquote className="text-[11px] text-slate-400 italic border-l-2 border-amber-500/40 pl-2.5">
        "The P/E ratio of any company that's fairly priced will equal its growth rate. If the P/E is less than the growth rate, you may have found yourself a bargain."
      </blockquote>
    </div>
  );
};
