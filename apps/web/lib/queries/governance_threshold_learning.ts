import type {
  GovernanceThresholdAutopromotionSummaryRow,
  GovernanceThresholdReviewSummaryRow,
} from "@/lib/queries/metrics";

export async function getGovernanceThresholdLearningReview(workspace = "demo"): Promise<{
  reviewSummary: GovernanceThresholdReviewSummaryRow[];
  autopromotionSummary: GovernanceThresholdAutopromotionSummaryRow[];
}> {
  const res = await fetch(`/api/metrics/governance-threshold-learning-review?workspace=${workspace}`, {
    cache: "no-store",
  });
  const data = await res.json() as {
    ok: boolean;
    reviewSummary: GovernanceThresholdReviewSummaryRow[];
    autopromotionSummary: GovernanceThresholdAutopromotionSummaryRow[];
    error?: string;
  };
  if (!data.ok) {
    throw new Error(data.error ?? "failed to load threshold learning review");
  }
  return {
    reviewSummary: data.reviewSummary ?? [],
    autopromotionSummary: data.autopromotionSummary ?? [],
  };
}
