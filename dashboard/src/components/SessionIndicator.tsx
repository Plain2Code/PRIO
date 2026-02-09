import { useState, useEffect } from 'react';

interface Session {
  name: string;
  start: number;
  end: number;
  color: string;
}

const SESSIONS: Session[] = [
  { name: 'Sydney',   start: 21, end: 6,  color: '#9b59b6' },
  { name: 'Tokyo',    start: 0,  end: 9,  color: '#e74c3c' },
  { name: 'London',   start: 7,  end: 16, color: '#3498db' },
  { name: 'New York', start: 12, end: 21, color: '#2ecc71' },
];

function isInSession(hour: number, s: Session): boolean {
  if (s.start < s.end) return hour >= s.start && hour < s.end;
  return hour >= s.start || hour < s.end;
}

function isWeekend(now: Date): boolean {
  const day = now.getUTCDay();
  if (day === 6) return true;
  if (day === 0 && now.getUTCHours() < 21) return true;
  if (day === 5 && now.getUTCHours() >= 21) return true;
  return false;
}

export function SessionIndicator() {
  const [now, setNow] = useState(new Date());

  useEffect(() => {
    const interval = setInterval(() => setNow(new Date()), 10000);
    return () => clearInterval(interval);
  }, []);

  const hour = now.getUTCHours();
  const minute = now.getUTCMinutes();
  const weekend = isWeekend(now);

  return (
    <div className="card">
      <h3>Trading Session</h3>
      <div className="session-layout">
        <div className="session-header">
          <span className="session-time">
            {String(hour).padStart(2, '0')}:{String(minute).padStart(2, '0')} UTC
          </span>
          <span className={`badge ${weekend ? 'badge-red' : 'badge-green'}`}>
            {weekend ? 'MARKET CLOSED' : 'MARKET OPEN'}
          </span>
        </div>
        <div className="session-note">
          Trading{' '}
          <span className="green" style={{ fontWeight: 600 }}>
            {weekend ? 'resumes when market opens' : 'per-pair session filter active'}
          </span>
        </div>
        <div className="session-badges">
          {SESSIONS.map(s => {
            const active = !weekend && isInSession(hour, s);
            return (
              <span
                key={s.name}
                className="session-badge"
                style={{
                  background: active ? s.color + '22' : '#1a1a2e',
                  color: active ? s.color : '#444',
                  border: `1px solid ${active ? s.color + '66' : '#2a2a4a'}`,
                }}
              >
                {s.name}
              </span>
            );
          })}
        </div>
        <div className="session-bar">
          {SESSIONS.map(s => {
            const duration = s.start < s.end ? s.end - s.start : (24 - s.start + s.end);
            return (
              <div
                key={s.name}
                className="session-bar-segment"
                style={{
                  left: `${(s.start / 24) * 100}%`,
                  width: `${(duration / 24) * 100}%`,
                  background: s.color + '33',
                  borderLeft: `1px solid ${s.color}66`,
                }}
              />
            );
          })}
          <div className="session-bar-marker" style={{ left: `${((hour + minute / 60) / 24) * 100}%` }} />
        </div>
        <div className="session-hours">
          <span>00:00</span><span>06:00</span><span>12:00</span><span>18:00</span><span>24:00</span>
        </div>
      </div>
    </div>
  );
}
