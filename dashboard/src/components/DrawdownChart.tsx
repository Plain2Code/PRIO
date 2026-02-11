import { useState, useEffect } from 'react';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { api } from '../api/client';

export function DrawdownChart() {
  const [data, setData] = useState<{timestamp: string; drawdown_pct: number}[]>([]);

  useEffect(() => {
    const fetch = async () => {
      try {
        const res = await api.getDrawdown();
        setData(res.drawdown || []);
      } catch {}
    };
    fetch();
    const interval = setInterval(fetch, 30000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="card">
      <h3>Drawdown</h3>
      {data.length === 0 ? (
        <p className="empty">No data yet</p>
      ) : (
        <ResponsiveContainer width="100%" height={280}>
          <AreaChart data={data}>
            <CartesianGrid strokeDasharray="3 3" stroke="var(--border-primary)" />
            <XAxis dataKey="timestamp" tick={{ fontSize: 11, fill: '#888' }} tickFormatter={(v) => new Date(v).toLocaleDateString()} />
            <YAxis tick={{ fontSize: 11, fill: '#888' }} unit="%" />
            <Tooltip
              contentStyle={{ background: 'var(--bg-card)', border: '1px solid var(--border-primary)', color: 'var(--text-primary)' }}
              labelFormatter={(v) => new Date(v).toLocaleString()}
              formatter={(value: number | undefined) => [`${(value ?? 0).toFixed(2)}%`, 'Drawdown']}
            />
            <Area type="monotone" dataKey="drawdown_pct" stroke="var(--color-danger)" fill="var(--color-danger)" fillOpacity={0.3} />
          </AreaChart>
        </ResponsiveContainer>
      )}
    </div>
  );
}
