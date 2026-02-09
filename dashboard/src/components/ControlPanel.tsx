import { useState, useEffect } from 'react';
import { api } from '../api/client';

const PAIR_THEMES: Record<string, string> = {
  EUR_USD: 'Fed vs ECB',
  GBP_JPY: 'Risk Appetite',
  AUD_NZD: 'Oceanic Spread',
  USD_CAD: 'Oil Proxy',
  EUR_GBP: 'EU Relative Value',
  NZD_JPY: 'Carry Trade',
  AUD_CAD: 'Commodity Cross',
};

export function ControlPanel() {
  const [isTrading, setIsTrading] = useState(false);
  const [mode, setMode] = useState('paper');
  const [brokerConnected, setBrokerConnected] = useState(false);
  const [uptime, setUptime] = useState<number | null>(null);
  const [pairs, setPairs] = useState<{ pair: string; enabled: boolean }[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const fetchStatus = async () => {
    try {
      const status = await api.getTradingStatus();
      setIsTrading(status.is_trading);
      setMode(status.mode);
      setBrokerConnected(status.broker_connected);
      setUptime(status.uptime_seconds);
    } catch {}
  };

  const fetchPairs = async () => {
    try {
      const res = await api.getPairs();
      setPairs(res.pairs || []);
    } catch {}
  };

  useEffect(() => {
    fetchStatus();
    fetchPairs();
    const interval = setInterval(fetchStatus, 5000);
    return () => clearInterval(interval);
  }, []);

  const handleStartStop = async () => {
    setLoading(true);
    setError('');
    try {
      if (isTrading) {
        await api.stopTrading();
      } else {
        await api.startTrading();
      }
      await fetchStatus();
    } catch (e: any) {
      setError(e.message);
    }
    setLoading(false);
  };

  const handleModeSwitch = async () => {
    if (isTrading) return;
    setError('');
    try {
      const newMode = mode === 'paper' ? 'live' : 'paper';
      await api.setMode(newMode);
      setMode(newMode);
    } catch (e: any) {
      setError(e.message);
    }
  };

  const handlePairToggle = async (pair: string, currentEnabled: boolean) => {
    if (isTrading) return;
    try {
      await api.togglePair(pair, !currentEnabled);
      await fetchPairs();
    } catch (e: any) {
      setError(e.message);
    }
  };

  const formatUptime = (s: number) => {
    const h = Math.floor(s / 3600);
    const m = Math.floor((s % 3600) / 60);
    const sec = Math.floor(s % 60);
    return `${h}h ${m}m ${sec}s`;
  };

  const enabledCount = pairs.filter(p => p.enabled).length;

  return (
    <div className="card">
      <h3>Control Panel</h3>
      {error && <div className="error-msg">{error}</div>}

      <div className="control-row">
        <button
          className={`btn ${isTrading ? 'btn-danger' : 'btn-success'}`}
          onClick={handleStartStop}
          disabled={loading}
        >
          {loading ? '...' : isTrading ? 'Stop Trading' : 'Start Trading'}
        </button>
        <button
          className={`btn ${mode === 'live' ? 'btn-danger' : 'btn-secondary'}`}
          onClick={handleModeSwitch}
          disabled={isTrading}
          title={isTrading ? 'Stop trading to change mode' : ''}
        >
          Mode: {mode.toUpperCase()}
        </button>
      </div>

      <div className="control-meta">
        <span>Broker: <span className={brokerConnected ? 'green' : 'red'}>{brokerConnected ? 'Connected' : 'Disconnected'}</span></span>
        {uptime !== null && <span>Uptime: {formatUptime(uptime)}</span>}
      </div>

      <h4>Trading Pairs <span style={{ color: '#888', fontWeight: 400, fontSize: '0.85em' }}>{enabledCount}/{pairs.length} active</span></h4>
      <div className="pairs-list">
        {pairs.map(p => (
          <div
            key={p.pair}
            className={`pair-row ${p.enabled ? 'active' : ''}`}
            onClick={() => handlePairToggle(p.pair, p.enabled)}
            style={{ cursor: isTrading ? 'not-allowed' : 'pointer', opacity: isTrading ? 0.5 : 1 }}
          >
            <div className="pair-toggle">
              <div className={`toggle-switch ${p.enabled ? 'on' : ''}`}>
                <div className="toggle-knob" />
              </div>
            </div>
            <span className="pair-name">{p.pair.replace('_', '/')}</span>
            <span className="pair-theme">{PAIR_THEMES[p.pair] || ''}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
