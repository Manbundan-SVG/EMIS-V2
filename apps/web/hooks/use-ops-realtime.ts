"use client";

import { useEffect } from "react";

export function useOpsRealtime(args: {
  onDeadLetter?: () => void;
  onWorkerChange?: () => void;
  onQueueChange?: () => void;
  onAlertChange?: () => void;
  pollIntervalMs?: number;
}) {
  useEffect(() => {
    const interval = setInterval(() => {
      args.onDeadLetter?.();
      args.onWorkerChange?.();
      args.onQueueChange?.();
      args.onAlertChange?.();
    }, args.pollIntervalMs ?? 15_000);
    return () => clearInterval(interval);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [args.pollIntervalMs]);
}
