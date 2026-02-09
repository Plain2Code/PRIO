import { useState, useEffect } from 'react';
import { api } from '../api/client';

interface CalendarEvent {
  title: string;
  country: string;
  datetime_utc: string;
  impact: string;
  forecast: string;
  previous: string;
  affected_pairs: string[];
}

interface CalendarData {
  events: CalendarEvent[];
  status: {
    enabled: boolean;
    event_count: number;
    last_fetch: string | null;
    fetch_error: string | null;
    blackout_hours: number;
    impact_threshold: string;
  };
}

const IMPACT_COLORS: Record<string, { color: string; bg: string }> = {
  High:   { color: '#ff4757', bg: '#3a111122' },
  Medium: { color: '#ffa502', bg: '#3a2a0022' },
  Low:    { color: '#888',    bg: 'transparent' },
};

const CURRENCY_FLAGS: Record<string, string> = {
  USD: '\ud83c\uddfa\ud83c\uddf8',
  EUR: '\ud83c\uddea\ud83c\uddfa',
  GBP: '\ud83c\uddec\ud83c\udde7',
  JPY: '\ud83c\uddef\ud83c\uddf5',
  AUD: '\ud83c\udde6\ud83c\uddfa',
  NZD: '\ud83c\uddf3\ud83c\uddff',
  CAD: '\ud83c\udde8\ud83c\udde6',
  CHF: '\ud83c\udde8\ud83c\udded',
};

function timeUntil(dtStr: string): string {
  const now = Date.now();
  const target = new Date(dtStr).getTime();
  const diff = target - now;

  if (diff < 0) return 'passed';

  const hours = Math.floor(diff / 3600000);
  const mins = Math.floor((diff % 3600000) / 60000);

  if (hours > 24) return `${Math.floor(hours / 24)}d ${hours % 24}h`;
  if (hours > 0) return `${hours}h ${mins}m`;
  return `${mins}m`;
}

function formatTime(dtStr: string): string {
  const d = new Date(dtStr);
  return d.toLocaleTimeString('de-DE', { hour: '2-digit', minute: '2-digit', timeZone: 'UTC' }) + ' UTC';
}

function formatDate(dtStr: string): string {
  const d = new Date(dtStr);
  return d.toLocaleDateString('de-DE', { weekday: 'short', day: 'numeric', month: 'short', timeZone: 'UTC' });
}

export function EconomicCalendar() {
  const [data, setData] = useState<CalendarData | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const res = await api.getCalendar();
        setData(res);
      } catch {}
    };
    fetchData();
    const interval = setInterval(fetchData, 60000);
    return () => clearInterval(interval);
  }, []);

  if (!data || !data.status?.enabled) return null;

  const events = data.events || [];
  const highEvents = events.filter(e => e.impact === 'High');
  const otherEvents = events.filter(e => e.impact !== 'High');

  return (
    <div className="card">
      <div className="card-header">
        <h3 style={{ marginBottom: 0 }}>Economic Calendar</h3>
        <div className="signal-summary">
          <span>{events.length} events (next 48h)</span>
          <span style={{ color: '#ff4757', fontWeight: 600 }}>
            {highEvents.length} High Impact
          </span>
          {data.status.fetch_error && (
            <span style={{ color: '#ff4757' }}>Fetch error</span>
          )}
        </div>
      </div>

      {events.length === 0 ? (
        <span className="empty">No upcoming events</span>
      ) : (
        <div className="table-wrapper">
          <table className="data-table">
            <thead>
              <tr>
                <th>Time</th>
                <th className="text-center">In</th>
                <th>Currency</th>
                <th>Event</th>
                <th className="text-center">Impact</th>
                <th className="text-right">Forecast</th>
                <th className="text-right">Previous</th>
                <th>Pairs</th>
              </tr>
            </thead>
            <tbody>
              {[...highEvents, ...otherEvents].map((e, i) => {
                const impact = IMPACT_COLORS[e.impact] || IMPACT_COLORS.Low;
                const countdown = timeUntil(e.datetime_utc);
                const isPassed = countdown === 'passed';
                const isImminent = !isPassed && !countdown.includes('d') && !countdown.includes('h');

                return (
                  <tr key={i} style={{
                    background: impact.bg,
                    opacity: isPassed ? 0.4 : 1,
                  }}>
                    <td>
                      <div style={{ fontSize: '0.85em' }}>{formatDate(e.datetime_utc)}</div>
                      <div style={{ fontWeight: 600 }}>{formatTime(e.datetime_utc)}</div>
                    </td>
                    <td className="text-center" style={{
                      color: isImminent ? '#ff4757' : isPassed ? '#555' : '#ffa502',
                      fontWeight: 600,
                      fontSize: '0.85em',
                    }}>
                      {countdown}
                    </td>
                    <td>
                      <span style={{ marginRight: 4 }}>{CURRENCY_FLAGS[e.country] || ''}</span>
                      {e.country}
                    </td>
                    <td style={{ maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                      {e.title}
                    </td>
                    <td className="text-center">
                      <span
                        className="verdict-badge"
                        style={{ color: impact.color, background: impact.bg, border: `1px solid ${impact.color}33` }}
                      >
                        {e.impact.toUpperCase()}
                      </span>
                    </td>
                    <td className="text-right" style={{ color: '#aaa' }}>
                      {e.forecast || '--'}
                    </td>
                    <td className="text-right" style={{ color: '#888' }}>
                      {e.previous || '--'}
                    </td>
                    <td>
                      {e.affected_pairs.length > 0 ? (
                        <span style={{ color: '#888', fontSize: '0.85em' }}>
                          {e.affected_pairs.map(p => p.replace('_', '/')).join(', ')}
                        </span>
                      ) : (
                        <span style={{ color: '#444' }}>--</span>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
