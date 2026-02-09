import { useState } from 'react';
import { api } from '../api/client';

export function KillSwitch() {
  const [confirming, setConfirming] = useState(false);
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<string | null>(null);

  const handleKill = async () => {
    if (!confirming) {
      setConfirming(true);
      setTimeout(() => setConfirming(false), 5000);
      return;
    }
    setLoading(true);
    setResult(null);
    try {
      const res = await api.killSwitch();
      const closed = res.positions_closed?.length || 0;
      setResult(`Kill switch activated. ${closed} position(s) closed.`);
    } catch (e: any) {
      setResult(`Error: ${e.message}`);
    }
    setLoading(false);
    setConfirming(false);
  };

  return (
    <div className="card kill-switch-card">
      <h3>Kill Switch</h3>
      <p className="kill-description">Closes all positions and stops trading.</p>
      <button
        className={`btn-kill ${confirming ? 'btn-kill-confirm' : ''}`}
        onClick={handleKill}
        disabled={loading}
      >
        {loading ? 'Executing...' : confirming ? 'CONFIRM KILL' : 'KILL SWITCH'}
      </button>
      {result && <div className="kill-result">{result}</div>}
    </div>
  );
}
