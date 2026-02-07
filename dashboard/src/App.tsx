import { useEffect, useState } from 'react';
import { useStore } from './stores/store';
import { useWebSocket } from './hooks/useWebSocket';
import { DashboardPage } from './pages/DashboardPage';
import { PerformancePage } from './pages/PerformancePage';
import { SettingsPage } from './pages/SettingsPage';
import './App.css';

type View = 'dashboard' | 'performance' | 'settings';

const NAV_LABELS: Record<View, string> = {
  dashboard: 'Dashboard',
  performance: 'Performance',
  settings: 'Settings',
};

function App() {
  const [view, setView] = useState<View>('dashboard');
  const { lastMessage, isConnected } = useWebSocket();

  const alerts = useStore((s) => s.alerts);
  const setWsConnected = useStore((s) => s.setWsConnected);
  const handleWSMessage = useStore((s) => s.handleWSMessage);
  const fetchStatus = useStore((s) => s.fetchStatus);

  useEffect(() => {
    setWsConnected(isConnected);
  }, [isConnected, setWsConnected]);

  useEffect(() => {
    if (lastMessage) {
      handleWSMessage(lastMessage as { type: string; data?: Record<string, unknown> });
    }
  }, [lastMessage, handleWSMessage]);

  useEffect(() => {
    fetchStatus();
  }, [fetchStatus]);

  return (
    <div className="app">
      <header className="header">
        <div className="header-left">
          <h1 className="logo">PRIO</h1>
          <span className="subtitle">Forex Trading Bot</span>
        </div>
        <nav className="nav">
          {(Object.keys(NAV_LABELS) as View[]).map((v) => (
            <button
              key={v}
              className={`nav-btn ${view === v ? 'active' : ''}`}
              onClick={() => setView(v)}
            >
              {NAV_LABELS[v]}
            </button>
          ))}
        </nav>
        <div className="header-right">
          <span className={`ws-status ${isConnected ? 'connected' : ''}`}>
            {isConnected ? 'Live' : 'Offline'}
          </span>
        </div>
      </header>

      {alerts.length > 0 && (
        <div className="alerts">
          {alerts.slice(0, 5).map((a, i) => (
            <div key={i} className="alert">{a}</div>
          ))}
        </div>
      )}

      <main className="dashboard">
        {view === 'dashboard' && <DashboardPage />}
        {view === 'performance' && <PerformancePage />}
        {view === 'settings' && <SettingsPage />}
      </main>
    </div>
  );
}

export default App;
