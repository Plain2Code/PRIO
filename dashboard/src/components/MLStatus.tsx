export function MLStatus() {
  return (
    <div className="card">
      <h3>Regime Detection</h3>
      <p style={{ color: '#999', margin: '8px 0' }}>
        Regime derived from ADX — no ML models required.
      </p>
      <table className="data-table">
        <thead>
          <tr>
            <th>ADX Range</th>
            <th>Regime</th>
            <th>Scale</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td style={{ color: '#ff4757' }}>&lt; 15</td>
            <td style={{ color: '#ff4757' }}>weak</td>
            <td>0.5x</td>
          </tr>
          <tr>
            <td style={{ color: '#ffa502' }}>15 – 25</td>
            <td style={{ color: '#ffa502' }}>building</td>
            <td>0.75x</td>
          </tr>
          <tr>
            <td style={{ color: '#00d4aa' }}>&ge; 25</td>
            <td style={{ color: '#00d4aa' }}>trending</td>
            <td>1.0x</td>
          </tr>
        </tbody>
      </table>
    </div>
  );
}
