import { useState, useEffect } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { api } from '../api/client';

export function EquityCurve() {
  const [data, setData] = useState<{timestamp: string; equity: number}[]>([]);

  useEffect(() => {
    const fetch = async () => {
      try {
        const res = await api.getEquityCurve();
        setData(res.equity_curve || []);
      } catch {}
    };
    fetch();
    const interval = setInterval(fetch, 30000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="card">
      <h3>Equity Curve</h3>
      {data.length === 0 ? (
        <p className="empty">No data yet</p>
      ) : (
        <ResponsiveContainer width="100%" height={280}>
          <LineChart data={data}>
            <CartesianGrid strokeDasharray="3 3" stroke="var(--border-primary)" />
            <XAxis dataKey="timestamp" tick={{ fontSize: 11, fill: '#888' }} tickFormatter={(v) => new Date(v).toLocaleDateString()} />
            <YAxis tick={{ fontSize: 11, fill: '#888' }} />
            <Tooltip
              contentStyle={{ background: 'var(--bg-card)', border: '1px solid var(--border-primary)', color: 'var(--text-primary)' }}
              labelFormatter={(v) => new Date(v).toLocaleString()}
              formatter={(value: number | undefined) => [`$${(value ?? 0).toFixed(2)}`, 'Equity']}
            />
            <Line type="monotone" dataKey="equity" stroke="var(--color-success)" strokeWidth={2} dot={false} />
          </LineChart>
        </ResponsiveContainer>
      )}
    </div>
  );
}
