import { useState, useEffect } from 'react';
import { api } from '../api/client';

export function SystemStatus() {
  const [system, setSystem] = useState<any>(null);
  const [latency, setLatency] = useState<any>(null);
  const [recovery, setRecovery] = useState<any>(null);

  useEffect(() => {
    const fetch = async () => {
      try {
        const [sys, lat, rec] = await Promise.all([
          api.getSystemStatus(),
          api.getLatency(),
          api.getRecovery().catch(() => null),
        ]);
        setSystem(sys);
        setLatency(lat);
        setRecovery(rec);
      } catch {}
    };
    fetch();
    const interval = setInterval(fetch, 10000);
    return () => clearInterval(interval);
  }, []);

  if (!system) return <div className="card"><h3>System Status</h3><p className="empty">Loading...</p></div>;

  const formatUptime = (s: number) => {
    const h = Math.floor(s / 3600);
    const m = Math.floor((s % 3600) / 60);
    return `${h}h ${m}m`;
  };

  return (
    <div className="card">
      <h3>System Status</h3>

      <div className="status-grid">
        <div className="status-item">
          <span className="status-label">Health</span>
          <span className={`badge ${system.status === 'healthy' ? 'badge-green' : 'badge-red'}`}>
            {system.status}
          </span>
        </div>
        <div className="status-item">
          <span className="status-label">Uptime</span>
          <span>{formatUptime(system.uptime_seconds)}</span>
        </div>
        <div className="status-item">
          <span className="status-label">CPU</span>
          <span>{system.cpu_percent != null ? `${system.cpu_percent}%` : 'N/A'}</span>
        </div>
        <div className="status-item">
          <span className="status-label">Memory</span>
          <span>{system.memory_usage ? `${system.memory_usage.rss_mb} MB` : 'N/A'}</span>
        </div>
        <div className="status-item">
          <span className="status-label">Broker</span>
          <span className={system.broker_connected ? 'green' : 'red'}>
            {system.broker_connected ? 'Connected' : 'Disconnected'}
          </span>
        </div>
        <div className="status-item">
          <span className="status-label">Exec Engine</span>
          <span className={system.execution_engine?.running ? 'green' : 'red'}>
            {system.execution_engine?.running ? 'Running' : 'Stopped'}
          </span>
        </div>
        {latency?.latency && (
          <div className="status-item">
            <span className="status-label">Latency (avg)</span>
            <span>{latency.latency.avg_ms?.toFixed(1) || '0'} ms</span>
          </div>
        )}
      </div>

      {recovery && recovery.enabled && (
        <>
          <h4>Drawdown Recovery</h4>
          <div className="status-grid">
            <div className="status-item">
              <span className="status-label">Phase</span>
              <span style={{
                fontWeight: 600,
                color: recovery.phase === 0 ? '#00d4aa'
                  : recovery.phase === 1 ? '#ffa502'
                  : recovery.phase === 2 ? '#ffa502'
                  : '#ff4757',
              }}>
                {recovery.phase_label}
              </span>
            </div>
            <div className="status-item">
              <span className="status-label">Max DD</span>
              <span>{recovery.max_drawdown_pct}%</span>
            </div>
            <div className="status-item">
              <span className="status-label">Hard Stop</span>
              <span>{recovery.hard_stop_drawdown_pct}%</span>
            </div>
            {recovery.phase === 1 && recovery.cooloff_remaining_hours != null && (
              <div className="status-item">
                <span className="status-label">Cooloff</span>
                <span className="orange">{recovery.cooloff_remaining_hours}h remaining</span>
              </div>
            )}
            {recovery.phase === 2 && (
              <>
                <div className="status-item">
                  <span className="status-label">Position Scale</span>
                  <span className="orange">{(recovery.position_scale * 100).toFixed(0)}%</span>
                </div>
                <div className="status-item">
                  <span className="status-label">Recovery</span>
                  <span className="orange">
                    {recovery.recovery_profitable_trades}/{recovery.recovery_target_trades} trades ({recovery.recovery_progress_pct}%)
                  </span>
                </div>
              </>
            )}
          </div>
        </>
      )}
    </div>
  );
}
