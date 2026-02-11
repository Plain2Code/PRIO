import { useEffect } from 'react';
import { useStore } from '../stores/store';
import { ControlPanel } from '../components/ControlPanel';
import { AccountCard } from '../components/AccountCard';
import { KillSwitch } from '../components/KillSwitch';
import { SessionIndicator } from '../components/SessionIndicator';
import { SignalMatrix } from '../components/SignalMatrix';
import { EconomicCalendar } from '../components/EconomicCalendar';
import { PositionsTable } from '../components/PositionsTable';
import { ActivityFeed } from '../components/ActivityFeed';

export function DashboardPage() {
  const fetchStatus = useStore((s) => s.fetchStatus);
  const fetchAccount = useStore((s) => s.fetchAccount);
  const fetchPositions = useStore((s) => s.fetchPositions);
  const fetchRecovery = useStore((s) => s.fetchRecovery);
  const isTrading = useStore((s) => s.isTrading);

  useEffect(() => {
    fetchStatus();
    fetchRecovery();
    if (isTrading) {
      fetchAccount();
      fetchPositions();
    }
    const interval = setInterval(() => {
      fetchStatus();
      if (isTrading) {
        fetchAccount();
        fetchPositions();
        fetchRecovery();
      }
    }, 15000);
    return () => clearInterval(interval);
  }, [isTrading, fetchStatus, fetchAccount, fetchPositions, fetchRecovery]);

  return (
    <>
      {/* Row 1: Controls + Account + Session/Kill */}
      <div className="grid-3">
        <ControlPanel />
        <AccountCard />
        <div style={{ display: 'flex', flexDirection: 'column', gap: 'var(--space-md)' }}>
          <SessionIndicator />
          <KillSwitch />
        </div>
      </div>

      {/* Row 2: Signal Matrix + Economic Calendar */}
      <SignalMatrix />
      <EconomicCalendar />

      {/* Row 3: Positions + Activity */}
      <div className="grid-2">
        <PositionsTable />
        <ActivityFeed />
      </div>
    </>
  );
}
