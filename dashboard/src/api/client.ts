// Dev: proxy to localhost:8000. Production: same-origin (empty string).
const BASE_URL = import.meta.env.VITE_API_URL ?? (import.meta.env.DEV ? 'http://localhost:8000' : '');

async function fetchJSON(url: string, options?: RequestInit) {
  const res = await fetch(`${BASE_URL}${url}`, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || res.statusText);
  }
  return res.json();
}

export const api = {
  // Trading
  startTrading: () => fetchJSON('/api/trading/start', { method: 'POST' }),
  stopTrading: () => fetchJSON('/api/trading/stop', { method: 'POST' }),
  killSwitch: () => fetchJSON('/api/trading/kill-switch', { method: 'POST' }),
  setMode: (mode: string) =>
    fetchJSON('/api/trading/mode', { method: 'POST', body: JSON.stringify({ mode }) }),
  getTradingStatus: () => fetchJSON('/api/trading/status'),

  // Config
  getCredentials: () => fetchJSON('/api/config/credentials'),
  setCapitalComCredentials: (data: { api_key: string; identifier: string; password: string; environment: string }) =>
    fetchJSON('/api/config/credentials/capitalcom', { method: 'POST', body: JSON.stringify(data) }),
  setTelegramCredentials: (data: { bot_token: string; chat_id: string }) =>
    fetchJSON('/api/config/credentials/telegram', { method: 'POST', body: JSON.stringify(data) }),
  getPairs: () => fetchJSON('/api/config/pairs'),
  updatePairs: (pairs: string[]) =>
    fetchJSON('/api/config/pairs', { method: 'POST', body: JSON.stringify({ pairs }) }),
  togglePair: (pair: string, enabled: boolean) =>
    fetchJSON('/api/config/pairs', { method: 'PATCH', body: JSON.stringify({ pair, enabled }) }),
  getRisk: () => fetchJSON('/api/config/risk'),
  updateRisk: (data: Record<string, unknown>) =>
    fetchJSON('/api/config/risk', { method: 'POST', body: JSON.stringify(data) }),
  getConfig: () => fetchJSON('/api/config/'),

  // Dashboard
  getAccount: () => fetchJSON('/api/dashboard/account'),
  getPositions: () => fetchJSON('/api/dashboard/positions'),
  getEquityCurve: () => fetchJSON('/api/dashboard/equity-curve'),
  getDrawdown: () => fetchJSON('/api/dashboard/drawdown'),
  getTrades: (limit = 50, offset = 0) =>
    fetchJSON(`/api/dashboard/trades?limit=${limit}&offset=${offset}`),
  getPerformance: () => fetchJSON('/api/dashboard/performance'),
  getSignals: () => fetchJSON('/api/dashboard/signals'),
  getRecovery: () => fetchJSON('/api/dashboard/recovery'),
  getCalendar: () => fetchJSON('/api/dashboard/calendar'),

  // System
  getSystemStatus: () => fetchJSON('/api/system/status'),
  getLogs: (limit = 100, level = 'INFO') =>
    fetchJSON(`/api/system/logs?limit=${limit}&level=${level}`),
  getLatency: () => fetchJSON('/api/system/latency'),
};
