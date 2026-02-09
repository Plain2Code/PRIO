import { useState, useEffect } from 'react';
import { api } from '../api/client';

interface PairRow {
  regime: string | null;
  regime_conf: number;
  regime_pass: boolean;
  regime_scale: number;
  adx: number | null;
  adx_threshold: number;
  adx_pass: boolean;
  hurst: number | null;
  hurst_threshold: number;
  hurst_pass: boolean;
  htf_direction: string | null;
  htf_pass: boolean;
  direction: string | null;
  signal_strength: number | null;
  signal_threshold: number;
  signal_pass: boolean;
  confidence: number | null;
  conf_threshold: number;
  conf_pass: boolean;
  rsi: number | null;
  rsi_score: number | null;
  rsi_pass: boolean;
  news_blackout: boolean;
  news_boost: boolean;
  news_events: string[] | null;
  has_position: boolean;
  verdict: string;
  block_reason: string | null;
  loop_ts: string | null;
}

interface SignalData {
  pairs: Record<string, PairRow>;
  summary: {
    open_positions: number;
    max_positions: number;
    recovery_phase: number;
    is_trading: boolean;
    drawdown_pct: number;
    margin_pct: number;
    balance: number;
    equity: number;
  };
  error?: string;
}

const PASS = '#00d4aa';
const FAIL = '#ff4757';
const NONE = '#555';

const VERDICTS: Record<string, { label: string; color: string; bg: string }> = {
  READY:               { label: 'READY',      color: PASS,    bg: '#0d332033' },
  SIGNAL_OK:           { label: 'SIGNAL OK',  color: PASS,    bg: '#0d332022' },
  EXECUTED:            { label: 'EXECUTED',    color: PASS,    bg: '#0d332033' },
  POSITION_OPEN:       { label: 'OPEN',       color: '#5b9bd5', bg: '#1a2a3a33' },
  NO_SIGNAL:           { label: 'NO SIGNAL',  color: '#888',  bg: 'transparent' },
  DAILY_LOSS:          { label: 'DAY LIMIT',  color: '#ffa502', bg: '#3a2a0022' },
  NEWS_BLOCK:          { label: 'NEWS',       color: '#ffa502', bg: '#3a2a0022' },
  RISK_BLOCK:          { label: 'RISK BLOCK', color: FAIL,    bg: '#3a111122' },
  SIZE_ZERO:           { label: 'SIZE=0',     color: '#ffa502', bg: '#3a2a0022' },
  EXEC_FAILED:         { label: 'EXEC FAIL',  color: FAIL,    bg: '#3a111122' },
  MARGIN_INSUFFICIENT: { label: 'MARGIN',     color: '#ffa502', bg: '#3a2a0022' },
  ERROR:               { label: 'ERROR',      color: FAIL,    bg: '#3a111122' },
  OUTSIDE_SESSION:     { label: 'SESSION',    color: '#888',  bg: 'transparent' },
  NO_DATA:             { label: '--',         color: '#444',  bg: 'transparent' },
};

function cellColor(pass: boolean, val: unknown): string {
  if (val === null || val === undefined) return NONE;
  return pass ? PASS : FAIL;
}

function formatDir(dir: string | null): string {
  if (!dir || dir === 'flat') return '--';
  return dir.toUpperCase();
}

function dirColor(dir: string | null): string {
  if (!dir || dir === 'flat') return NONE;
  return dir === 'long' ? PASS : FAIL;
}

function verdictDetail(r: PairRow): string | null {
  if (r.block_reason) return r.block_reason;
  if (r.verdict === 'NO_SIGNAL') {
    if (!r.adx_pass && r.adx !== null) return `ADX ${r.adx} < ${r.adx_threshold}`;
    if (!r.hurst_pass && r.hurst !== null) return `Hurst ${r.hurst.toFixed(2)} < ${r.hurst_threshold}`;
    if (!r.htf_pass) return 'HTF flat';
    if (!r.signal_pass && r.signal_strength !== null) return `Signal ${Math.abs(r.signal_strength).toFixed(2)} < ${r.signal_threshold}`;
  }
  return null;
}

export function SignalMatrix() {
  const [data, setData] = useState<SignalData | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const res = await api.getSignals();
        setData(res);
      } catch {}
    };
    fetchData();
    const interval = setInterval(fetchData, 30000);
    return () => clearInterval(interval);
  }, []);

  const pairs = data ? Object.entries(data.pairs) : [];
  const summary = data?.summary;

  return (
    <div className="card">
      <div className="card-header">
        <h3 style={{ marginBottom: 0 }}>Signal Matrix</h3>
        {summary && (
          <div className="signal-summary">
            <span style={{ color: summary.is_trading ? PASS : FAIL, fontWeight: 600 }}>
              {summary.is_trading ? 'ACTIVE' : 'STOPPED'}
            </span>
            <span>Pos {summary.open_positions}/{summary.max_positions}</span>
            <span style={{ color: summary.drawdown_pct >= 10 ? FAIL : summary.drawdown_pct >= 5 ? '#ffa502' : undefined }}>
              DD {summary.drawdown_pct}%
            </span>
            <span>Margin {summary.margin_pct}%</span>
            {summary.recovery_phase > 0 && (
              <span className="orange">Recovery Phase {summary.recovery_phase}</span>
            )}
          </div>
        )}
      </div>

      {data?.error ? (
        <span className="empty">{data.error}</span>
      ) : pairs.length === 0 ? (
        <span className="empty">No data</span>
      ) : (
        <div className="table-wrapper">
          <table className="signal-table">
            <thead>
              <tr>
                <th>Pair</th>
                <th className="text-center">Regime</th>
                <th className="text-right">ADX</th>
                <th className="text-right">Hurst</th>
                <th className="text-center">HTF</th>
                <th className="text-right">Signal</th>
                <th className="text-right">Conf</th>
                <th className="text-right">RSI</th>
                <th className="text-center">News</th>
                <th className="text-center">Status</th>
              </tr>
            </thead>
            <tbody>
              {pairs.map(([pair, r]) => {
                const v = VERDICTS[r.verdict] || VERDICTS.NO_DATA;
                const detail = verdictDetail(r);
                return (
                  <tr key={pair} style={{ background: v.bg }}>
                    <td className="signal-pair">{pair.replace('_', '/')}</td>

                    <td className="text-center">
                      {r.regime && r.regime !== 'unknown' ? (() => {
                        const color = r.regime === 'trending' ? PASS
                          : r.regime === 'building' ? '#ffa502'
                          : FAIL;
                        const bg = r.regime === 'trending' ? '#0d332033'
                          : r.regime === 'building' ? '#3a2a0022'
                          : '#3a111122';
                        return (
                          <span
                            className="regime-badge"
                            style={{
                              background: bg,
                              color,
                              border: `1px solid ${color}44`,
                            }}
                            title={`ADX: ${r.adx ?? '?'} | Scale: ${(r.regime_scale ?? 1).toFixed(2)}x`}
                          >
                            {r.regime} {(r.regime_scale ?? 1).toFixed(1)}x
                          </span>
                        );
                      })() : (
                        <span style={{ color: NONE }}>—</span>
                      )}
                    </td>

                    <td className="text-right signal-cell" style={{ color: cellColor(r.adx_pass, r.adx) }}>
                      {r.adx !== null ? r.adx : '--'}
                      <span className="signal-threshold"> /{r.adx_threshold}</span>
                    </td>

                    <td className="text-right signal-cell" style={{ color: r.hurst !== null ? cellColor(r.hurst_pass, r.hurst) : NONE }}>
                      {r.hurst !== null ? r.hurst.toFixed(2) : '--'}
                      {r.hurst_threshold > 0 && <span className="signal-threshold"> /{r.hurst_threshold}</span>}
                    </td>

                    <td className="text-center signal-cell" style={{ color: r.htf_direction ? cellColor(r.htf_pass, r.htf_direction) : NONE, textTransform: 'uppercase' }}>
                      {formatDir(r.htf_direction)}
                    </td>

                    <td className="text-right signal-cell">
                      {r.signal_strength !== null ? (
                        <>
                          <span style={{ color: cellColor(r.signal_pass, r.signal_strength) }}>
                            {r.signal_strength >= 0.005 ? '+' : ''}{r.signal_strength.toFixed(2)}
                          </span>
                          <span className="signal-threshold"> /{r.signal_threshold}</span>
                        </>
                      ) : (
                        <span style={{ color: NONE }}>--</span>
                      )}
                      {r.direction && r.direction !== 'flat' && (
                        <span className="signal-dir-arrow" style={{ color: dirColor(r.direction) }}>
                          {r.direction === 'long' ? '▲' : '▼'}
                        </span>
                      )}
                    </td>

                    <td className="text-right signal-cell" style={{ color: cellColor(r.conf_pass, r.confidence) }}>
                      {r.confidence !== null ? `${r.confidence}%` : '--'}
                      <span className="signal-threshold"> /{r.conf_threshold}%</span>
                    </td>

                    <td className="text-right signal-cell" style={{ color: r.rsi !== null ? cellColor(r.rsi_pass, r.rsi) : NONE }}>
                      {r.rsi !== null ? (
                        <>
                          {r.rsi}
                          {r.rsi_score !== null && <span className="signal-threshold"> ({r.rsi_score})</span>}
                        </>
                      ) : '--'}
                    </td>

                    <td className="text-center">
                      {r.news_blackout ? (
                        <span
                          className="verdict-badge"
                          style={{ color: '#ffa502', background: '#3a2a0022', border: '1px solid #ffa50233' }}
                          title={r.news_events?.join(', ') || 'News blackout'}
                        >
                          BLOCK
                        </span>
                      ) : r.news_boost ? (
                        <span
                          className="verdict-badge"
                          style={{ color: '#00d4aa', background: '#0d332033', border: '1px solid #00d4aa33' }}
                          title="Post-event boost active (+25% sizing)"
                        >
                          BOOST
                        </span>
                      ) : r.news_events && r.news_events.length > 0 ? (
                        <span style={{ color: '#888', fontSize: '0.85em' }} title={r.news_events.join(', ')}>
                          {r.news_events.length} evt
                        </span>
                      ) : (
                        <span style={{ color: '#444' }}>--</span>
                      )}
                    </td>

                    <td className="text-center" title={r.block_reason || ''}>
                      <span
                        className="verdict-badge"
                        style={{ color: v.color, background: v.bg, border: `1px solid ${v.color}33` }}
                      >
                        {v.label}
                      </span>
                      {detail && <div className="verdict-detail">{detail}</div>}
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
