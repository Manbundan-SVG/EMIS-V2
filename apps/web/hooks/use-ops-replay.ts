"use client";

import { useState } from "react";

export function useOpsReplay() {
  const [isSubmitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function triggerReplay(runId: string) {
    setSubmitting(true);
    setError(null);
    try {
      const response = await fetch(`/api/replays/${runId}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ requestedBy: "ops-dashboard" }),
      });
      const payload = await response.json();
      if (!response.ok || !payload.ok) {
        throw new Error(payload.error || payload.reason || "Replay failed");
      }
      return payload;
    } catch (err) {
      const message = err instanceof Error ? err.message : "Replay failed";
      setError(message);
      throw err;
    } finally {
      setSubmitting(false);
    }
  }

  return { triggerReplay, isSubmitting, error };
}
