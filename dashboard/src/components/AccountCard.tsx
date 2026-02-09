import { useState, useEffect } from 'react';
import { api } from '../api/client';

interface AccountData {
  balance: number;
  equity: number;
  margin_used: number;
  margin_available: number;
  unrealized_pnl: number;
  currency: string;
}

export function AccountCard() {
  const [account, setAccount] = useState<AccountData | null>(null);
  const [error, setError] = useState(false);

  useEffect(() => {
    const fetch = async () => {
      try {
        const data = await api.getAccount();
        setAccount(data);
        setError(false);
      } catch {
        setError(true);
      }
    };
    fetch();
    const interval = setInterval(fetch, 10000);
    return () => clearInterval(interval);
  }, []);

  const fmt = (n: number | undefined) =>
    n != null ? n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : '--';

  const pnl = account?.unrealized_pnl ?? 0;

  return (
    <div className="card">
      <h3>Account</h3>
      {error || !account ? (
        <span className="empty">Broker not connected</span>
      ) : (
        <div className="account-grid">
          <div className="status-item">
            <span className="status-label">Balance</span>
            <span className="account-value">
              {fmt(account.balance)} <span className="account-currency">{account.currency || 'EUR'}</span>
            </span>
          </div>
          <div className="status-item">
            <span className="status-label">Equity</span>
            <span className="account-value">{fmt(account.equity)}</span>
          </div>
          <div className="status-item">
            <span className="status-label">Unrealized P&L</span>
            <span className={`account-value-sm ${pnl >= 0 ? 'green' : 'red'}`}>
              {pnl >= 0 ? '+' : ''}{fmt(account.unrealized_pnl)}
            </span>
          </div>
          <div className="status-item">
            <span className="status-label">Margin Used</span>
            <span className="account-value-sm">{fmt(account.margin_used)}</span>
          </div>
        </div>
      )}
    </div>
  );
}
