import { SettingsPanel } from '../components/SettingsPanel';
import { SystemStatus } from '../components/SystemStatus';

export function SettingsPage() {
  return (
    <div className="grid-settings">
      <SettingsPanel />
      <SystemStatus />
    </div>
  );
}
