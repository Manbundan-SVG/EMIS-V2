import type { AlertEventRow } from "@/lib/queries/alerts";

function severityClass(severity: string): string {
  if (severity === "high") return "severity-high";
  if (severity === "medium") return "severity-medium";
  return "severity-info";
}

export function AlertsPanel({ alerts }: { alerts: AlertEventRow[] }) {
  return (
    <div className="section card">
      <h2 className="section-title">Alerts</h2>
      {alerts.length === 0 ? (
        <p className="muted">No alerts yet.</p>
      ) : (
        <table className="table">
          <thead>
            <tr>
              <th>Severity</th><th>Title</th><th>Message</th><th>Time</th>
            </tr>
          </thead>
          <tbody>
            {alerts.map((alert) => (
              <tr key={alert.id}>
                <td className={severityClass(alert.severity)}>
                  {alert.severity.toUpperCase()}
                </td>
                <td>{alert.title}</td>
                <td className="muted">{alert.message}</td>
                <td className="muted">{new Date(alert.created_at).toLocaleString()}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
