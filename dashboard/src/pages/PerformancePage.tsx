import { useEffect } from 'react';
import { useStore } from '../stores/store';
import { EquityCurve } from '../components/EquityCurve';
import { DrawdownChart } from '../components/DrawdownChart';
import { MLStatus } from '../components/MLStatus';

export function PerformancePage() {
  const fetchPerformance = useStore((s) => s.fetchPerformance);
  const fetchEquityCurve = useStore((s) => s.fetchEquityCurve);
  const fetchDrawdown = useStore((s) => s.fetchDrawdown);
  const fetchTrades = useStore((s) => s.fetchTrades);
  const performance = useStore((s) => s.performance);
  const trades = useStore((s) => s.trades);

  useEffect(() => {
    fetchPerformance();
    fetchEquityCurve();
    fetchDrawdown();
    fetchTrades();
  }, [fetchPerformance, fetchEquityCurve, fetchDrawdown, fetchTrades]);

  return (
    <>
      {/* Performance Metrics */}
      {performance && (
        <div className="card">
          <h3>Performance Metrics</h3>
          <div className="metrics-grid">
            <div className="metric">
              <span className="metric-label">Sharpe</span>
              <span className="metric-value">{performance.sharpe_ratio?.toFixed(2) ?? 'N/A'}</span>
            </div>
            <div className="metric">
              <span className="metric-label">Sortino</span>
              <span className="metric-value">{performance.sortino_ratio?.toFixed(2) ?? 'N/A'}</span>
            </div>
            <div className="metric">
              <span className="metric-label">Profit Factor</span>
              <span className="metric-value">{performance.profit_factor?.toFixed(2) ?? 'N/A'}</span>
            </div>
            <div className="metric">
              <span className="metric-label">Win Rate</span>
              <span className="metric-value">
                {performance.win_rate != null ? `${(performance.win_rate * 100).toFixed(1)}%` : 'N/A'}
              </span>
            </div>
            <div className="metric">
              <span className="metric-label">Max Drawdown</span>
              <span className="metric-value">{performance.max_drawdown_pct?.toFixed(2) ?? 'N/A'}%</span>
            </div>
            <div className="metric">
              <span className="metric-label">Expectancy</span>
              <span className="metric-value">{performance.expectancy?.toFixed(2) ?? 'N/A'}</span>
            </div>
            <div className="metric">
              <span className="metric-label">Total P&L</span>
              <span className={`metric-value ${(performance.total_pnl ?? 0) >= 0 ? 'positive' : 'negative'}`}>
                {performance.total_pnl?.toFixed(2) ?? '0.00'}
              </span>
            </div>
            <div className="metric">
              <span className="metric-label">Trades</span>
              <span className="metric-value">{performance.total_trades ?? 0}</span>
            </div>
          </div>
        </div>
      )}

      {/* Charts side by side */}
      <div className="grid-2-charts">
        <EquityCurve />
        <DrawdownChart />
      </div>

      {/* Recent Trades */}
      {trades.length > 0 && (
        <div className="card">
          <h3>Recent Trades</h3>
          <div className="table-wrapper">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Instrument</th>
                  <th className="text-right">P&L</th>
                  <th>Close Reason</th>
                  <th>Closed At</th>
                </tr>
              </thead>
              <tbody>
                {trades.slice(0, 20).map((t, i) => (
                  <tr key={i}>
                    <td>{t.instrument?.replace('_', '/')}</td>
                    <td className={`text-right ${(t.pnl ?? 0) >= 0 ? 'green' : 'red'}`} style={{ fontWeight: 600 }}>
                      {t.pnl != null ? `${t.pnl >= 0 ? '+' : ''}${t.pnl.toFixed(2)}` : 'N/A'}
                    </td>
                    <td>{t.metadata || '-'}</td>
                    <td style={{ color: '#999' }}>{t.closed_at ? new Date(t.closed_at).toLocaleString('en-GB') : '-'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* ML Status */}
      <MLStatus />
    </>
  );
}
