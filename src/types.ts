export type MarketRegion = 'US' | 'IN' | 'GLOBAL';

export interface StockQuote {
  symbol: string;
  name: string;
  exchange: string;
  currency: string;
  price: number;
  change: number;
  changePercent: number;
  open: number;
  high: number;
  low: number;
  previousClose: number;
  volume: number;
  avgVolume?: number;
  marketCap?: number;
  pe?: number;
  forwardPe?: number;
  eps?: number;
  pb?: number;
  dividendYield?: number;
  beta?: number;
  fiftyTwoWeekHigh?: number;
  fiftyTwoWeekLow?: number;
  updatedAt: string;
}

export interface StockFundamentals {
  symbol: string;
  // Graham Metrics
  bookValuePerShare: number;
  currentRatio: number;
  debtToEquity: number;
  netCurrentAssetValuePerShare?: number; // NCAV / Net-Net
  grahamNumber: number;
  grahamMarginOfSafetyPercent: number;
  grahamPassedTests: string[];
  grahamFailedTests: string[];
  grahamScore: number; // 0 - 100

  // Buffett Metrics
  roe: number; // Return on Equity (%)
  roic?: number; // Return on Invested Capital (%)
  operatingMargin: number; // (%)
  netMargin: number; // (%)
  freeCashFlow?: number;
  ownerEarnings?: number;
  dcfFairValue: number;
  buffettMarginOfSafetyPercent: number;
  moatStrength: 'Wide Moat' | 'Narrow Moat' | 'No Moat';
  buffettPassedTests: string[];
  buffettFailedTests: string[];
  buffettScore: number; // 0 - 100

  // Peter Lynch Metrics
  pegRatio: number;
  earningsGrowthRate: number; // (%)
  lynchCategory: 'Fast Grower' | 'Stalwart' | 'Slow Grower' | 'Cyclical' | 'Turnaround' | 'Asset Play';
  lynchFairValue: number;
  cashToDebtRatio: number;
  lynchPassedTests: string[];
  lynchFailedTests: string[];
  lynchScore: number; // 0 - 100

  // Unified Synthesis
  compositeScore: number; // 0 - 100
  overallSignal: 'STRONG_BUY' | 'BUY' | 'HOLD' | 'WAIT_TRIM' | 'SELL' | 'AVOID';
  synthesisThesis: string;
  intrinsicValueRange: {
    conservative: number; // Graham / low DCF
    base: number; // Blend
    optimistic: number; // Growth DCF
  };
}

export interface HistoricalPricePoint {
  date: string;
  price: number;
  volume?: number;
}

export interface PortfolioPosition {
  id: string;
  symbol: string;
  name: string;
  exchange: string;
  currency: string;
  shares: number;
  avgCost: number;
  currentPrice: number;
  investedValue: number;
  currentValue: number;
  unrealizedPnL: number;
  unrealizedPnLPercent: number;
  grahamScore: number;
  buffettScore: number;
  lynchScore: number;
  compositeScore: number;
  signal: 'STRONG_BUY' | 'BUY' | 'HOLD' | 'WAIT_TRIM' | 'SELL' | 'AVOID';
  dateAdded: string;
}

export interface WatchlistItem {
  id: string;
  symbol: string;
  name: string;
  exchange: string;
  currency: string;
  currentPrice: number;
  targetBuyPrice: number;
  marginOfSafetyTargetPercent: number;
  compositeScore: number;
  signal: 'STRONG_BUY' | 'BUY' | 'HOLD' | 'WAIT_TRIM' | 'SELL' | 'AVOID';
  thesisNotes: string;
  dateAdded: string;
}

export interface PortfolioSummary {
  totalInvested: number;
  totalCurrentValue: number;
  totalUnrealizedPnL: number;
  totalUnrealizedPnLPercent: number;
  portfolioGrahamScore: number;
  portfolioBuffettScore: number;
  portfolioLynchScore: number;
  portfolioCompositeScore: number;
  currency: string;
}
