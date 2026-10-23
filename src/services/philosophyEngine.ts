import { StockFundamentals, StockQuote } from '../types';

export interface PhilosophyRawMetrics {
  price: number;
  eps: number;
  bookValuePerShare: number;
  pe: number;
  roe: number;
  operatingMargin: number;
  netMargin: number;
  debtToEquity: number;
  currentRatio: number;
  earningsGrowthRate: number;
  freeCashFlowPerShare?: number;
  cashPerShare?: number;
  marketCap?: number;
}

export function evaluateStockPhilosophy(
  quote: StockQuote,
  rawMetrics?: Partial<PhilosophyRawMetrics>
): StockFundamentals {
  const price = Math.max(0.01, quote.price || 1);
  const pe = rawMetrics?.pe ?? (quote.pe && quote.pe > 0 ? quote.pe : (quote.eps && quote.eps > 0 ? price / quote.eps : 18.0));
  const eps = rawMetrics?.eps ?? (quote.eps ?? (pe > 0 ? price / pe : price / 18.0));
  const bookValue = rawMetrics?.bookValuePerShare ?? (quote.pb && quote.pb > 0 ? price / quote.pb : price / 2.5);
  const roe = rawMetrics?.roe ?? (bookValue > 0 && eps > 0 ? Math.min(65, (eps / bookValue) * 100) : 16.5);
  const debtToEquity = rawMetrics?.debtToEquity ?? (quote.beta ? Math.max(0.05, Math.min(1.8, quote.beta * 0.45)) : 0.40);
  const currentRatio = rawMetrics?.currentRatio ?? 1.85;
  const operatingMargin = rawMetrics?.operatingMargin ?? Math.min(45, Math.max(5, roe * 0.85));
  const netMargin = rawMetrics?.netMargin ?? Math.min(35, Math.max(4, operatingMargin * 0.72));
  const earningsGrowthRate = rawMetrics?.earningsGrowthRate ?? Math.max(3, Math.min(35, (roe * 0.75) + 2.0));
  const cashPerShare = rawMetrics?.cashPerShare ?? (bookValue * 0.35);

  // ==========================================
  // 1. BENJAMIN GRAHAM PHILOSOPHY EVALUATION
  // ==========================================
  // Graham Number = sqrt(22.5 * EPS * BVPS)
  let grahamNumber = 0;
  if (eps > 0 && bookValue > 0) {
    grahamNumber = Math.sqrt(22.5 * eps * bookValue);
  } else if (eps > 0) {
    grahamNumber = eps * 15; // Graham classic earnings multiple alone
  } else {
    grahamNumber = price * 0.5; // Distressed
  }
  grahamNumber = Math.round(grahamNumber * 100) / 100;

  const grahamMarginOfSafetyPercent = Math.round(((grahamNumber - price) / grahamNumber) * 1000) / 10;

  const grahamPassedTests: string[] = [];
  const grahamFailedTests: string[] = [];

  // Graham Rule 1: Margin of Safety >= 20%
  if (grahamMarginOfSafetyPercent >= 20) {
    grahamPassedTests.push(`Margin of Safety (+${grahamMarginOfSafetyPercent}%) exceeds Graham's 20% minimum safety cushion`);
  } else if (grahamMarginOfSafetyPercent > 0) {
    grahamFailedTests.push(`Margin of Safety (+${grahamMarginOfSafetyPercent}%) is modest, below Graham's 20% target threshold`);
  } else {
    grahamFailedTests.push(`Negative Margin of Safety (${grahamMarginOfSafetyPercent}%): Trading at a premium to Graham Fair Value (${grahamNumber})`);
  }

  // Graham Rule 2: P/E * P/B <= 22.5
  const pbRatio = bookValue > 0 ? price / bookValue : 2.5;
  const grahamProduct = pe * pbRatio;
  if (grahamProduct <= 22.5) {
    grahamPassedTests.push(`Graham Valuation Rule Met: P/E (${pe.toFixed(1)}x) × P/B (${pbRatio.toFixed(1)}x) = ${grahamProduct.toFixed(1)} (<= 22.5)`);
  } else {
    grahamFailedTests.push(`Graham Valuation Exceeded: P/E × P/B = ${grahamProduct.toFixed(1)} (exceeds conservative 22.5 ceiling)`);
  }

  // Graham Rule 3: Debt to Equity < 0.60
  if (debtToEquity <= 0.60) {
    grahamPassedTests.push(`Conservative Capital Structure: Debt-to-Equity is ${debtToEquity.toFixed(2)}x (well below 0.60 safety cap)`);
  } else {
    grahamFailedTests.push(`High Financial Leverage: Debt-to-Equity is ${debtToEquity.toFixed(2)}x (exceeds Graham 0.60 cap)`);
  }

  // Graham Rule 4: Liquidity / Current Ratio >= 1.5
  if (currentRatio >= 1.5) {
    grahamPassedTests.push(`Working Capital Solvency: Current Ratio is ${currentRatio.toFixed(2)}x (>= 1.50 healthy buffer)`);
  } else {
    grahamFailedTests.push(`Liquidity Concern: Current Ratio is ${currentRatio.toFixed(2)}x (< 1.50)`);
  }

  // Graham Rule 5: Positive Earnings Stability
  if (eps > 0) {
    grahamPassedTests.push(`Earnings Stability: Company generates positive net income ($${eps.toFixed(2)} EPS)`);
  } else {
    grahamFailedTests.push(`Zero/Negative Earnings: Graham strictly avoids unproven loss-making companies`);
  }

  // Calculate Graham Score (0-100)
  let grahamScore = 40;
  if (grahamMarginOfSafetyPercent >= 25) grahamScore += 25;
  else if (grahamMarginOfSafetyPercent >= 10) grahamScore += 15;
  else if (grahamMarginOfSafetyPercent >= 0) grahamScore += 5;
  else grahamScore -= 15;

  if (grahamProduct <= 18) grahamScore += 15;
  else if (grahamProduct <= 22.5) grahamScore += 10;

  if (debtToEquity <= 0.40) grahamScore += 12;
  else if (debtToEquity <= 0.70) grahamScore += 6;

  if (currentRatio >= 1.8) grahamScore += 8;
  grahamScore = Math.max(10, Math.min(100, Math.round(grahamScore)));

  // ==========================================
  // 2. WARREN BUFFETT PHILOSOPHY EVALUATION
  // ==========================================
  // Buffett DCF Intrinsic Fair Value
  // Discounted Cash Flow based on Owner Earnings growing at conservative rates
  const discountRate = 0.095; // 9.5% hurdle rate / cost of capital
  const terminalGrowth = 0.03; // 3% long-term GDP growth
  const fcfPerShare = rawMetrics?.freeCashFlowPerShare ?? Math.max(0.1, eps * 0.95);

  let presentValue = 0;
  let currentProjectedFCF = fcfPerShare;
  const growthHorizonYears = 5;

  for (let yr = 1; yr <= growthHorizonYears; yr++) {
    // Stage 1 conservative growth tapering
    const yrGrowth = Math.max(0.04, (earningsGrowthRate / 100) * Math.pow(0.92, yr - 1));
    currentProjectedFCF *= (1 + yrGrowth);
    presentValue += currentProjectedFCF / Math.pow(1 + discountRate, yr);
  }

  // Terminal Value (Gordon Growth Model)
  const terminalValue = (currentProjectedFCF * (1 + terminalGrowth)) / (discountRate - terminalGrowth);
  const presentTerminalValue = terminalValue / Math.pow(1 + discountRate, growthHorizonYears);
  const dcfFairValue = Math.round((presentValue + presentTerminalValue) * 100) / 100;

  const buffettMarginOfSafetyPercent = Math.round(((dcfFairValue - price) / dcfFairValue) * 1000) / 10;

  const buffettPassedTests: string[] = [];
  const buffettFailedTests: string[] = [];

  // Buffett Moat & ROE Test
  let moatStrength: 'Wide Moat' | 'Narrow Moat' | 'No Moat' = 'Narrow Moat';
  if (roe >= 20 && operatingMargin >= 18) {
    moatStrength = 'Wide Moat';
    buffettPassedTests.push(`Wide Economic Moat: Stellar Return on Equity (${roe.toFixed(1)}%) & Operating Margin (${operatingMargin.toFixed(1)}%) show pricing power`);
  } else if (roe >= 14 && operatingMargin >= 12) {
    moatStrength = 'Narrow Moat';
    buffettPassedTests.push(`Narrow Economic Moat: Healthy Return on Equity (${roe.toFixed(1)}%) above 14% benchmark`);
  } else {
    moatStrength = 'No Moat';
    buffettFailedTests.push(`Weak Moat: Return on Equity (${roe.toFixed(1)}%) is below Buffett's 15% threshold for compounding machines`);
  }

  // Buffett Rule 2: Pricing vs DCF Fair Value
  if (buffettMarginOfSafetyPercent >= 15) {
    buffettPassedTests.push(`Wonderful Business at Wonderful Price: Trading at +${buffettMarginOfSafetyPercent}% discount to Owner-Earnings DCF Fair Value ($${dcfFairValue})`);
  } else if (buffettMarginOfSafetyPercent >= -5) {
    buffettPassedTests.push(`Wonderful Business at Fair Price: Trading near DCF Intrinsic Value ($${dcfFairValue}, MoS: ${buffettMarginOfSafetyPercent}%)`);
  } else {
    buffettFailedTests.push(`Overpriced Durable Compounder: Trading ${Math.abs(buffettMarginOfSafetyPercent)}% above conservative DCF Fair Value ($${dcfFairValue})`);
  }

  // Buffett Rule 3: Operating Profit Margins
  if (operatingMargin >= 15) {
    buffettPassedTests.push(`High Operating Profitability: Operating Margin of ${operatingMargin.toFixed(1)}% protects earnings during inflationary periods`);
  } else {
    buffettFailedTests.push(`Thin Operating Margins: ${operatingMargin.toFixed(1)}% margin leaves little margin for economic shocks`);
  }

  // Buffett Rule 4: Capital Allocation & Low Debt
  if (debtToEquity < 0.5) {
    buffettPassedTests.push(`Conservative Financing: Negligible debt allows management to buy back shares or invest opportunistically`);
  } else {
    buffettFailedTests.push(`Debt Burden: Debt-to-Equity is ${debtToEquity.toFixed(2)}x, reducing capital flexibility`);
  }

  // Calculate Buffett Score (0-100)
  let buffettScore = 35;
  if (moatStrength === 'Wide Moat') buffettScore += 25;
  else if (moatStrength === 'Narrow Moat') buffettScore += 15;

  if (roe >= 22) buffettScore += 15;
  else if (roe >= 15) buffettScore += 10;

  if (buffettMarginOfSafetyPercent >= 20) buffettScore += 15;
  else if (buffettMarginOfSafetyPercent >= 0) buffettScore += 10;
  else if (buffettMarginOfSafetyPercent < -25) buffettScore -= 10;

  if (operatingMargin >= 20) buffettScore += 10;
  buffettScore = Math.max(10, Math.min(100, Math.round(buffettScore)));

  // ==========================================
  // 3. PETER LYNCH PHILOSOPHY EVALUATION
  // ==========================================
  // PEG Ratio = P/E / Earnings Growth Rate (%)
  const safeGrowthRate = Math.max(1, earningsGrowthRate);
  const pegRatio = Math.round((pe / safeGrowthRate) * 100) / 100;

  // Peter Lynch Fair Value = P/E equal to Growth Rate -> Fair P/E = Growth -> Fair Price = EPS * Growth
  const lynchFairValue = Math.round(Math.max(1, eps * safeGrowthRate) * 100) / 100;

  // Lynch Category Detection
  let lynchCategory: 'Fast Grower' | 'Stalwart' | 'Slow Grower' | 'Cyclical' | 'Turnaround' | 'Asset Play' = 'Stalwart';
  const mcap = quote.marketCap || 50000000000;

  if (cashPerShare > price * 0.40) {
    lynchCategory = 'Asset Play';
  } else if (eps <= 0 || pe > 65) {
    lynchCategory = 'Turnaround';
  } else if (earningsGrowthRate >= 20 && mcap < 150000000000) {
    lynchCategory = 'Fast Grower';
  } else if (earningsGrowthRate >= 8 && mcap >= 25000000000) {
    lynchCategory = 'Stalwart';
  } else if (earningsGrowthRate < 8 && (quote.dividendYield || 0) >= 2.5) {
    lynchCategory = 'Slow Grower';
  } else {
    lynchCategory = 'Cyclical';
  }

  const lynchPassedTests: string[] = [];
  const lynchFailedTests: string[] = [];

  // Lynch Rule 1: PEG Ratio Test
  if (pegRatio < 1.0) {
    lynchPassedTests.push(`Peter Lynch Golden Bargain: PEG Ratio is ${pegRatio.toFixed(2)} (< 1.0 signifies remarkable growth at a discount)`);
  } else if (pegRatio <= 1.5) {
    lynchPassedTests.push(`Reasonable Valuation: PEG Ratio is ${pegRatio.toFixed(2)} (within 1.0 - 1.5 acceptable range)`);
  } else {
    lynchFailedTests.push(`Expensive for its Growth: PEG Ratio is ${pegRatio.toFixed(2)} (well above Peter Lynch's 1.0 - 1.2 sweet spot)`);
  }

  // Lynch Rule 2: Category Match
  lynchPassedTests.push(`Lynch Classification: Identified as a "${lynchCategory}" (${safeGrowthRate.toFixed(1)}% earnings trajectory)`);

  // Lynch Rule 3: Cash to Debt Solvency
  const cashToDebtRatio = debtToEquity > 0 ? (cashPerShare / (debtToEquity * bookValue)) : 2.5;
  if (cashToDebtRatio >= 1.0) {
    lynchPassedTests.push(`Clean Balance Sheet: Substantial cash reserves cushion against market downturns`);
  } else {
    lynchFailedTests.push(`Leverage Check: High debt load reduces Peter Lynch risk-adjusted appeal`);
  }

  // Calculate Lynch Score (0-100)
  let lynchScore = 40;
  if (pegRatio < 0.8) lynchScore += 30;
  else if (pegRatio < 1.0) lynchScore += 22;
  else if (pegRatio <= 1.4) lynchScore += 12;
  else if (pegRatio > 2.0) lynchScore -= 15;

  if (lynchCategory === 'Fast Grower' && pegRatio < 1.3) lynchScore += 18;
  if (lynchCategory === 'Stalwart' && pegRatio < 1.2) lynchScore += 14;
  if (earningsGrowthRate >= 12 && earningsGrowthRate <= 30) lynchScore += 10;
  lynchScore = Math.max(10, Math.min(100, Math.round(lynchScore)));

  // ==========================================
  // 4. UNIFIED 3-PHILOSOPHY SYNTHESIS MATRIX
  // ==========================================
  // Weighted Composite Score
  // 35% Graham (Value & Safety) + 40% Buffett (Quality & Moat) + 25% Lynch (Growth & PEG)
  const compositeScore = Math.round(
    (grahamScore * 0.35) +
    (buffettScore * 0.40) +
    (lynchScore * 0.25)
  );

  let overallSignal: 'STRONG_BUY' | 'BUY' | 'HOLD' | 'WAIT_TRIM' | 'SELL' | 'AVOID' = 'HOLD';

  if (compositeScore >= 78 && grahamMarginOfSafetyPercent >= 10 && pegRatio <= 1.4) {
    overallSignal = 'STRONG_BUY';
  } else if (compositeScore >= 66 && (grahamMarginOfSafetyPercent >= 0 || pegRatio <= 1.6)) {
    overallSignal = 'BUY';
  } else if (compositeScore >= 50) {
    overallSignal = 'HOLD';
  } else if (compositeScore >= 40) {
    overallSignal = 'WAIT_TRIM';
  } else if (compositeScore >= 25) {
    overallSignal = 'SELL';
  } else {
    overallSignal = 'AVOID';
  }

  // Intrinsic Value Range
  const conservativeFair = Math.round(Math.min(grahamNumber, dcfFairValue) * 100) / 100;
  const baseFair = Math.round(((grahamNumber * 0.4) + (dcfFairValue * 0.4) + (lynchFairValue * 0.2)) * 100) / 100;
  const optimisticFair = Math.round(Math.max(dcfFairValue, lynchFairValue) * 100) / 100;

  // Synthesis Thesis
  let synthesisThesis = '';
  if (overallSignal === 'STRONG_BUY' || overallSignal === 'BUY') {
    synthesisThesis = `${quote.symbol} satisfies the triple-legend criteria: Graham's Margin of Safety (+${grahamMarginOfSafetyPercent}%), Buffett's ${moatStrength} (${roe.toFixed(1)}% ROE), and Lynch's favorable PEG of ${pegRatio.toFixed(2)}x. Intrinsic fair value is appraised at $${baseFair} (Cons. $${conservativeFair} – Opt. $${optimisticFair}).`;
  } else if (overallSignal === 'HOLD') {
    synthesisThesis = `${quote.symbol} is a solid enterprise (${moatStrength}, ${roe.toFixed(1)}% ROE) trading reasonably close to fair value ($${baseFair}). Maintain existing long-term positions while waiting for a wider Margin of Safety before accumulating more.`;
  } else {
    synthesisThesis = `${quote.symbol} trades at a rich valuation (P/E ${pe.toFixed(1)}x, PEG ${pegRatio.toFixed(2)}x) with insufficient Graham Margin of Safety (${grahamMarginOfSafetyPercent}%). Buffett and Lynch counsel patience until price falls closer to conservative fair value ($${conservativeFair}).`;
  }

  return {
    symbol: quote.symbol,
    bookValuePerShare: Math.round(bookValue * 100) / 100,
    currentRatio: Math.round(currentRatio * 100) / 100,
    debtToEquity: Math.round(debtToEquity * 100) / 100,
    grahamNumber,
    grahamMarginOfSafetyPercent,
    grahamPassedTests,
    grahamFailedTests,
    grahamScore,
    roe: Math.round(roe * 10) / 10,
    operatingMargin: Math.round(operatingMargin * 10) / 10,
    netMargin: Math.round(netMargin * 10) / 10,
    dcfFairValue,
    buffettMarginOfSafetyPercent,
    moatStrength,
    buffettPassedTests,
    buffettFailedTests,
    buffettScore,
    pegRatio,
    earningsGrowthRate: Math.round(earningsGrowthRate * 10) / 10,
    lynchCategory,
    lynchFairValue,
    cashToDebtRatio: Math.round(cashToDebtRatio * 100) / 100,
    lynchPassedTests,
    lynchFailedTests,
    lynchScore,
    compositeScore,
    overallSignal,
    synthesisThesis,
    intrinsicValueRange: {
      conservative: conservativeFair,
      base: baseFair,
      optimistic: optimisticFair,
    },
  };
}
