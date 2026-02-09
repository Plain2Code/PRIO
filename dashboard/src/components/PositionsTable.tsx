import { useState, useEffect } from 'react';
import { api } from '../api/client';

export function PositionsTable() {
  const [positions, setPositions] = useState<any[]>([]);

  useEffect(() => {
    const fetch = async () => {
      try {
        const res = await api.getPositions();
        setPositions(res.positions || []);
      } catch {}
    };
    fetch();
    const interval = setInterval(fetch, 10000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="card">
      <h3>Open Positions ({positions.length})</h3>
      {positions.length === 0 ? (
        <p className="empty">No open positions</p>
      ) : (
        <div className="table-wrapper">
          <table className="data-table">
            <thead>
              <tr>
                <th>Instrument</th>
                <th>Side</th>
                <th>Units</th>
                <th>Avg Price</th>
                <th className="text-right">Unrealized P&amp;L</th>
              </tr>
            </thead>
            <tbody>
              {positions.map((p: any, i: number) => {
                const units = Number(p.units || 0);
                const side = p.side?.toUpperCase() || (units >= 0 ? 'LONG' : 'SHORT');
                const pnl = Number(p.unrealized_pnl || 0);
                return (
                  <tr key={i}>
                    <td>{p.instrument?.replace('_', '/')}</td>
                    <td className={side === 'LONG' ? 'green' : 'red'}>{side}</td>
                    <td>{Math.abs(units).toLocaleString()}</td>
                    <td>{p.average_price || '-'}</td>
                    <td className={`text-right ${pnl >= 0 ? 'green' : 'red'}`} style={{ fontWeight: 600 }}>
                      {pnl >= 0 ? '+' : ''}{pnl.toFixed(2)}
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
