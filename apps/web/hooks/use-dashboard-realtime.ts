"use client";

import { useEffect } from "react";
import { createBrowserSupabaseClient } from "@/lib/supabase";

export function useDashboardRealtime(args: {
  workspaceId: string | null;
  onJobChange?: () => void;
  onCompositeChange?: () => void;
  onAlert?: () => void;
}) {
  useEffect(() => {
    if (!args.workspaceId) return;
    const supabase = createBrowserSupabaseClient();
    const channel = supabase
      .channel(`workspace:${args.workspaceId}:dashboard`)
      .on("postgres_changes", { event: "*", schema: "public", table: "job_runs", filter: `workspace_id=eq.${args.workspaceId}` }, () => args.onJobChange?.())
      .on("postgres_changes", { event: "*", schema: "public", table: "composite_scores", filter: `workspace_id=eq.${args.workspaceId}` }, () => args.onCompositeChange?.())
      .on("postgres_changes", { event: "INSERT", schema: "public", table: "alert_events", filter: `workspace_id=eq.${args.workspaceId}` }, () => args.onAlert?.())
      .subscribe();
    return () => { void supabase.removeChannel(channel); };
  }, [args.workspaceId, args.onJobChange, args.onCompositeChange, args.onAlert]);
}
