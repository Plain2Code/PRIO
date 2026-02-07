/**
 * Central Zustand store — single source of truth for the entire frontend.
 *
 * Fed by:
 *   - WebSocket events (real-time: trades, status, equity, alerts)
 *   - REST API calls (initial load, paginated history, config CRUD)
 *
 * Components subscribe to slices of this store — no local polling needed.
 */

import { create } from 'zustand';
import { api } from '../api/client';

// ── Types ──────────────────────────────────────────

export interface AccountData {
  balance: number;
  equity: number;
  margin_used: number;
  unrealized_pnl: number;
  [key: string]: unknown;
}

export interface Position {
  instrument: string;
  units: number;
  side: string;
  average_price: number;
  unrealized_pnl: number;
  stop_loss?: number;
  take_profit?: number;
  [key: string]: unknown;
}

export interface Trade {
  instrument: string;
  pnl: number | null;
  status: string;
  closed_at: string;
  metadata?: string;
  direction?: string;
  [key: string]: unknown;
}

export interface PairDiagnostic {
  instrument: string;
  regime: string | null;
  regime_confidence: number;
  adx: number | null;
  signal_direction: string | null;
  signal_strength: number | null;
  confidence: number | null;
  rsi: number | null;
  verdict: string;
  detail: string | null;
  timestamp: string;
}

export interface Performance {
  total_trades: number;
  win_count: number;
  loss_count: number;
  total_pnl: number;
  win_rate: number;
  profit_factor: number | null;
  expectancy: number;
  sharpe_ratio: number;
  sortino_ratio: number;
  max_drawdown_pct: number;
  avg_win: number;
  avg_loss: number;
}

export interface RecoveryData {
  phase: number;
  phase_label: string;
  enabled: boolean;
  current_drawdown_pct: number;
  max_drawdown_pct: number;
  hard_stop_drawdown_pct: number;
  position_scale: number;
  [key: string]: unknown;
}

export interface ActivityItem {
  type: string;
  message: string;
  timestamp: string;
}

export interface SignalSummary {
  open_positions: number;
  max_positions: number;
  recovery_phase: number;
  is_trading: boolean;
  drawdown_pct: number;
  margin_pct: number;
  balance: number;
  equity: number;
}

// ── Store ──────────────────────────────────────────

interface AppStore {
  // Connection
  wsConnected: boolean;
  setWsConnected: (v: boolean) => void;

  // Trading state
  isTrading: boolean;
  tradingMode: string;
  activePairs: string[];

  // Account
  account: AccountData | null;

  // Positions
  positions: Position[];

  // Diagnostics (signal matrix — from bot.diagnostics)
  diagnostics: Record<string, PairDiagnostic>;
  signalSummary: SignalSummary | null;

  // Recovery
  recovery: RecoveryData | null;

  // Performance
  performance: Performance | null;

  // Trades
  trades: Trade[];
  tradesTotal: number;

  // Equity curve
  equityCurve: Array<{ timestamp: string; equity: number }>;

  // Drawdown
  drawdownSeries: Array<{ timestamp: string; drawdown_pct: number }>;

  // Activity feed
  activity: ActivityItem[];

  // Activity feed helper
  addActivity: (type: string, message: string, timestamp: string) => void;

  // Alerts
  alerts: string[];
  addAlert: (msg: string) => void;

  // ── WebSocket handler ──────────────────────────
  handleWSMessage: (msg: { type: string; data?: Record<string, unknown> }) => void;

  // ── API actions ────────────────────────────────
  fetchStatus: () => Promise<void>;
  fetchAccount: () => Promise<void>;
  fetchPositions: () => Promise<void>;
  fetchSignals: () => Promise<void>;
  fetchRecovery: () => Promise<void>;
  fetchPerformance: () => Promise<void>;
  fetchTrades: (limit?: number, offset?: number) => Promise<void>;
  fetchEquityCurve: () => Promise<void>;
  fetchDrawdown: () => Promise<void>;

  // Trading actions
  startTrading: () => Promise<void>;
  stopTrading: () => Promise<void>;
  killSwitch: () => Promise<{ positions_closed: unknown[]; errors: string[] | null }>;
}

export const useStore = create<AppStore>((set, get) => ({
  // ── Initial state ──────────────────────────────

  wsConnected: false,
  setWsConnected: (v) => set({ wsConnected: v }),

  isTrading: false,
  tradingMode: 'demo',
  activePairs: [],

  account: null,
  positions: [],
  diagnostics: {},
  signalSummary: null,
  recovery: null,
  performance: null,
  trades: [],
  tradesTotal: 0,
  equityCurve: [],
  drawdownSeries: [],
  activity: [],
  alerts: [],

  addAlert: (msg) =>
    set((s) => ({ alerts: [msg, ...s.alerts.slice(0, 9)] })),

  // ── WebSocket message handler ──────────────────

  handleWSMessage: (msg) => {
    const { type, data } = msg;
    const d = (data ?? {}) as Record<string, unknown>;
    const ts = (d.timestamp as string) || new Date().toISOString();

    switch (type) {
      case 'status':
        set({
          isTrading: (d.is_trading as boolean) ?? get().isTrading,
          tradingMode: (d.mode as string) ?? get().tradingMode,
        });
        get().addActivity('status', `Bot ${d.is_trading ? 'started' : 'stopped'}`, ts);
        break;

      case 'trade':
        get().addActivity('trade', `${d.instrument} ${d.direction ?? ''} ${d.units ?? ''}u`, ts);
        // Refresh positions and account after a trade
        get().fetchPositions();
        get().fetchAccount();
        break;

      case 'equity':
        set({
          account: {
            ...(get().account || { margin_used: 0, unrealized_pnl: 0 }),
            balance: (d.balance as number) ?? 0,
            equity: (d.equity as number) ?? 0,
          } as AccountData,
        });
        break;

      case 'regime':
        get().addActivity('regime', `${d.instrument}: ${d.regime} (${((d.confidence as number) * 100).toFixed(0)}%)`, ts);
        break;

      case 'alert':
        get().addAlert((d.message as string) || JSON.stringify(d));
        get().addActivity('alert', (d.message as string) || 'Alert', ts);
        break;

      default:
        break;
    }
  },

  // Helper to add activity item
  addActivity: (type: string, message: string, timestamp: string) =>
    set((s) => ({
      activity: [{ type, message, timestamp }, ...s.activity.slice(0, 49)],
    })),

  // ── API actions ────────────────────────────────

  fetchStatus: async () => {
    try {
      const data = await api.getTradingStatus();
      set({
        isTrading: data.is_trading,
        tradingMode: data.mode,
        activePairs: data.active_pairs,
      });
    } catch {
      /* ignore */
    }
  },

  fetchAccount: async () => {
    try {
      const data = await api.getAccount();
      set({ account: data });
    } catch {
      /* ignore */
    }
  },

  fetchPositions: async () => {
    try {
      const data = await api.getPositions();
      set({ positions: data.positions || [] });
    } catch {
      /* ignore */
    }
  },

  fetchSignals: async () => {
    try {
      const data = await api.getSignals();
      set({
        diagnostics: data.pairs || {},
        signalSummary: data.summary || null,
      });
    } catch {
      /* ignore */
    }
  },

  fetchRecovery: async () => {
    try {
      const data = await api.getRecovery();
      set({ recovery: data });
    } catch {
      /* ignore */
    }
  },

  fetchPerformance: async () => {
    try {
      const data = await api.getPerformance();
      set({ performance: data });
    } catch {
      /* ignore */
    }
  },

  fetchTrades: async (limit = 50, offset = 0) => {
    try {
      const data = await api.getTrades(limit, offset);
      set({ trades: data.trades || [], tradesTotal: data.total || 0 });
    } catch {
      /* ignore */
    }
  },

  fetchEquityCurve: async () => {
    try {
      const data = await api.getEquityCurve();
      set({ equityCurve: data.equity_curve || [] });
    } catch {
      /* ignore */
    }
  },

  fetchDrawdown: async () => {
    try {
      const data = await api.getDrawdown();
      set({ drawdownSeries: data.drawdown || [] });
    } catch {
      /* ignore */
    }
  },

  // ── Trading actions ────────────────────────────

  startTrading: async () => {
    const data = await api.startTrading();
    set({ isTrading: true, tradingMode: data.mode, activePairs: data.pairs });
  },

  stopTrading: async () => {
    await api.stopTrading();
    set({ isTrading: false });
  },

  killSwitch: async () => {
    const data = await api.killSwitch();
    set({ isTrading: false, positions: [] });
    return data;
  },
}));
