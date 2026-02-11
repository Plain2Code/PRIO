import { useState, useEffect } from 'react';
import { api } from '../api/client';

type Tab = 'capitalcom' | 'telegram' | 'risk';

export function SettingsPanel() {
  const [tab, setTab] = useState<Tab>('capitalcom');
  const [creds, setCreds] = useState<any>(null);
  const [message, setMessage] = useState('');
  const [error, setError] = useState('');

  const [apiKey, setApiKey] = useState('');
  const [identifier, setIdentifier] = useState('');
  const [password, setPassword] = useState('');
  const [capitalcomEnv, setCapitalcomEnv] = useState('demo');

  const [botToken, setBotToken] = useState('');
  const [chatId, setChatId] = useState('');

  const [risk, setRisk] = useState<Record<string, any>>({});

  useEffect(() => {
    const fetch = async () => {
      try {
        const [credRes, riskRes] = await Promise.all([api.getCredentials(), api.getRisk()]);
        setCreds(credRes);
        setCapitalcomEnv(credRes.capitalcom_env || 'demo');
        setRisk(riskRes);
      } catch {}
    };
    fetch();
  }, []);

  const saveCapitalCom = async () => {
    setError('');
    setMessage('');
    if (!apiKey || !identifier || !password) {
      setError('API Key, Email/Identifier, and Password are required');
      return;
    }
    try {
      await api.setCapitalComCredentials({ api_key: apiKey, identifier, password, environment: capitalcomEnv });
      setMessage('Capital.com credentials saved successfully');
      setApiKey('');
      setIdentifier('');
      setPassword('');
      const res = await api.getCredentials();
      setCreds(res);
    } catch (e: any) {
      setError(e.message);
    }
  };

  const saveTelegram = async () => {
    setError('');
    setMessage('');
    if (!botToken || !chatId) {
      setError('Bot Token and Chat ID are required');
      return;
    }
    try {
      await api.setTelegramCredentials({ bot_token: botToken, chat_id: chatId });
      setMessage('Telegram credentials saved successfully');
      setBotToken('');
      setChatId('');
      const res = await api.getCredentials();
      setCreds(res);
    } catch (e: any) {
      setError(e.message);
    }
  };

  const saveRisk = async () => {
    setError('');
    setMessage('');
    try {
      const payload: Record<string, any> = {};
      for (const [key, value] of Object.entries(risk)) {
        if (typeof value === 'number' || typeof value === 'string') {
          payload[key] = value;
        }
      }
      await api.updateRisk(payload);
      setMessage('Risk parameters saved successfully');
    } catch (e: any) {
      setError(e.message);
    }
  };

  const updateRiskField = (key: string, value: string) => {
    const num = parseFloat(value);
    setRisk(prev => ({ ...prev, [key]: isNaN(num) ? value : num }));
  };

  const riskFields = [
    { key: 'max_position_size_pct', label: 'Max Position Size %', type: 'number' },
    { key: 'max_open_positions', label: 'Max Open Positions', type: 'number' },
    { key: 'max_correlated_positions', label: 'Max Correlated Positions', type: 'number' },
    { key: 'correlation_threshold', label: 'Correlation Threshold', type: 'number' },
    { key: 'default_leverage', label: 'Default Leverage', type: 'number' },
    { key: 'position_sizing_method', label: 'Sizing Method', type: 'text' },
    { key: 'fixed_position_pct', label: 'Fixed Position %', type: 'number' },
    { key: 'max_spread_pips', label: 'Max Spread (pips)', type: 'number' },
  ];

  return (
    <div className="card">
      <h3>Settings</h3>

      <div className="tabs">
        <button className={`tab ${tab === 'capitalcom' ? 'active' : ''}`} onClick={() => { setTab('capitalcom'); setMessage(''); setError(''); }}>Capital.com</button>
        <button className={`tab ${tab === 'telegram' ? 'active' : ''}`} onClick={() => { setTab('telegram'); setMessage(''); setError(''); }}>Telegram</button>
        <button className={`tab ${tab === 'risk' ? 'active' : ''}`} onClick={() => { setTab('risk'); setMessage(''); setError(''); }}>Risk</button>
      </div>

      {error && <div className="error-msg">{error}</div>}
      {message && <div className="success-msg">{message}</div>}

      {tab === 'capitalcom' && (
        <div className="form-section">
          {creds && (
            <div className="cred-status">
              <span>API Key: {creds.has_capitalcom_key ? <span className="green">Set</span> : <span className="red">Not set</span>}</span>
              <span>Identifier: {creds.has_capitalcom_identifier ? <span className="green">Set</span> : <span className="red">Not set</span>}</span>
              <span>Password: {creds.has_capitalcom_password ? <span className="green">Set</span> : <span className="red">Not set</span>}</span>
              <span>Environment: {creds.capitalcom_env}</span>
            </div>
          )}
          <div className="form-group">
            <label>API Key</label>
            <input type="password" value={apiKey} onChange={e => setApiKey(e.target.value)} placeholder="Enter Capital.com API Key" />
          </div>
          <div className="form-group">
            <label>Email / Identifier</label>
            <input type="text" value={identifier} onChange={e => setIdentifier(e.target.value)} placeholder="Enter your Capital.com email" />
          </div>
          <div className="form-group">
            <label>Password</label>
            <input type="password" value={password} onChange={e => setPassword(e.target.value)} placeholder="Enter your Capital.com password" />
          </div>
          <div className="form-group">
            <label>Environment</label>
            <select value={capitalcomEnv} onChange={e => setCapitalcomEnv(e.target.value)}>
              <option value="demo">Demo</option>
              <option value="live">Live</option>
            </select>
          </div>
          <button className="btn btn-primary" onClick={saveCapitalCom}>Save Capital.com Credentials</button>
        </div>
      )}

      {tab === 'telegram' && (
        <div className="form-section">
          {creds && (
            <div className="cred-status">
              <span>Bot Token: {creds.has_telegram_token ? <span className="green">Set</span> : <span className="red">Not set</span>}</span>
              <span>Chat ID: {creds.has_telegram_chat_id ? <span className="green">Set</span> : <span className="red">Not set</span>}</span>
            </div>
          )}
          <div className="form-group">
            <label>Bot Token</label>
            <input type="password" value={botToken} onChange={e => setBotToken(e.target.value)} placeholder="Enter Telegram Bot Token" />
          </div>
          <div className="form-group">
            <label>Chat ID</label>
            <input type="text" value={chatId} onChange={e => setChatId(e.target.value)} placeholder="Enter Chat ID" />
          </div>
          <button className="btn btn-primary" onClick={saveTelegram}>Save Telegram Credentials</button>
        </div>
      )}

      {tab === 'risk' && (
        <div className="form-section">
          <div className="risk-grid">
            {riskFields.map(f => (
              <div className="form-group" key={f.key}>
                <label>{f.label}</label>
                <input
                  type={f.type}
                  value={risk[f.key] ?? ''}
                  onChange={e => updateRiskField(f.key, e.target.value)}
                  step={f.type === 'number' ? '0.1' : undefined}
                />
              </div>
            ))}
          </div>
          <button className="btn btn-primary" onClick={saveRisk}>Save Risk Parameters</button>
        </div>
      )}
    </div>
  );
}
