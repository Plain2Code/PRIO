import { useState, useEffect } from 'react';
import { api } from '../api/client';

interface LogEntry {
  event?: string;
  message?: string;
  level?: string;
  timestamp?: string;
  instrument?: string;
  confidence?: number;
  direction?: string;
  reason?: string;
  granularity?: string;
  [key: string]: unknown;
}

const LEVEL_COLORS: Record<string, string> = {
  INFO: '#4a6cf7',
  WARNING: '#ffa502',
  ERROR: '#ff4757',
  DEBUG: '#666',
};

function formatTime(ts?: string): string {
  if (!ts) return '';
  try {
    return new Date(ts).toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
  } catch {
    return '';
  }
}

function describeEvent(entry: LogEntry): string {
  const event = entry.event || entry.message || '';
  if (event.includes('regime_ranging_skip'))
    return `${entry.instrument?.replace('_', '/')} ranging — skipped`;
  if (event.includes('signal_generated'))
    return `Signal: ${entry.instrument?.replace('_', '/')} ${entry.direction} (${((entry.confidence ?? 0) * 100).toFixed(0)}%)`;
  if (event.includes('trade_rejected'))
    return `Trade rejected: ${entry.instrument?.replace('_', '/')} — ${entry.reason}`;
  if (event.includes('trade_executed'))
    return `Trade executed: ${entry.instrument?.replace('_', '/')} ${entry.direction}`;
  if (event.includes('candles_fetched'))
    return `${entry.instrument?.replace('_', '/')} ${entry.granularity} data loaded`;
  if (event.includes('new_candle_detected'))
    return `New candle: ${entry.instrument?.replace('_', '/')}`;
  if (event.includes('exit_signal'))
    return `Exit signal: ${entry.instrument?.replace('_', '/')}`;
  if (event.includes('trading_started'))
    return 'Trading started';
  if (event.includes('trading_stopped'))
    return 'Trading stopped';
  return event.replace(/_/g, ' ');
}

export function ActivityFeed() {
  const [logs, setLogs] = useState<LogEntry[]>([]);

  useEffect(() => {
    const fetch = async () => {
      try {
        const data = await api.getLogs(30, 'INFO');
        setLogs(data.logs || []);
      } catch {}
    };
    fetch();
    const interval = setInterval(fetch, 5000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="card">
      <h3>Activity Feed</h3>
      <div className="feed-scroll">
        {logs.length === 0 ? (
          <span className="empty">No activity</span>
        ) : (
          logs.slice(0, 20).map((entry, i) => {
            const level = (entry.level || 'INFO').toUpperCase();
            return (
              <div key={i} className="feed-entry">
                <span className="feed-time">{formatTime(entry.timestamp)}</span>
                <span className="feed-level" style={{ color: LEVEL_COLORS[level] || '#666' }}>
                  {level === 'WARNING' ? 'WARN' : level === 'ERROR' ? 'ERR' : ''}
                </span>
                <span className="feed-message">{describeEvent(entry)}</span>
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}
