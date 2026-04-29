"use client";

import { useOpsReplay } from "@/hooks/use-ops-replay";

interface Props {
  runId: string;
  onQueued?: () => void | Promise<void>;
}

export function ReplayActionButton({ runId, onQueued }: Props) {
  const { triggerReplay, isSubmitting, error } = useOpsReplay();

  async function handleClick() {
    await triggerReplay(runId);
    await onQueued?.();
  }

  return (
    <div className="panel-stack">
      <button
        type="button"
        className="btn btn-sm"
        onClick={() => void handleClick()}
        disabled={isSubmitting}
      >
        {isSubmitting ? "Queueing replay..." : "Replay run"}
      </button>
      {error ? <p className="muted">{error}</p> : null}
    </div>
  );
}
