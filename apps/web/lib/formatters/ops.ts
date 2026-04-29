const STAGE_ORDER = [
  "load_inputs",
  "build_features",
  "build_signals",
  "build_composite",
  "persist_outputs",
  "emit_alerts",
] as const;

export function formatDurationMs(value?: number | null): string {
  if (value === null || value === undefined) return "-";
  if (value < 1000) return `${value} ms`;
  return `${(value / 1000).toFixed(2)} s`;
}

export function formatTimestamp(value?: string | null): string {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "-";
  return date.toLocaleString();
}

export function formatNullable(value: unknown): string {
  if (value === null || value === undefined || value === "") return "-";
  return String(value);
}

export function orderStages<T extends { stage_name?: string | null }>(rows: T[]): T[] {
  return [...rows].sort((a, b) => {
    const aIndex = STAGE_ORDER.indexOf((a.stage_name ?? "") as (typeof STAGE_ORDER)[number]);
    const bIndex = STAGE_ORDER.indexOf((b.stage_name ?? "") as (typeof STAGE_ORDER)[number]);
    return (aIndex === -1 ? 999 : aIndex) - (bIndex === -1 ? 999 : bIndex);
  });
}
