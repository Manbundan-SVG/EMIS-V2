"use client";

import { useCallback, useEffect, useState } from "react";
import { AlertPolicyPanel } from "@/components/alert-policy-panel";
import { AnomalyClusterPanel } from "@/components/anomaly-cluster-panel";
import { AttributionPanel } from "@/components/attribution-panel";
import { DeadLetterPanel } from "@/components/dead-letter-panel";
import { DriftPanel } from "@/components/drift-panel";
import { ExplanationPanel } from "@/components/explanation-panel";
import { FailureForensicsPanel } from "@/components/failure-forensics-panel";
import { GovernancePanel } from "@/components/governance-panel";
import { GovernanceAlertsPanel } from "@/components/governance-alerts-panel";
import { GovernanceCasePanel } from "@/components/governance-case-panel";
import { GovernanceDegradationPanel } from "@/components/governance-degradation-panel";
import { GovernanceLifecyclePanel } from "@/components/governance-lifecycle-panel";
import { GovernanceRoutingPanel } from "@/components/governance-routing-panel";
import { GovernanceRoutingEffectivenessPanel } from "@/components/governance-routing-effectiveness-panel";
import { GovernanceRoutingRecommendationsPanel } from "@/components/governance-routing-recommendations-panel";
import { GovernanceRoutingRecommendationReviewPanel } from "@/components/governance-routing-recommendation-review-panel";
import { GovernanceRoutingAutopromotionsPanel } from "@/components/governance-routing-autopromotions-panel";
import { GovernanceRoutingQualityPanel } from "@/components/governance-routing-quality-panel";
import { GovernanceIncidentAnalyticsPanel } from "@/components/governance-incident-analytics-panel";
import { GovernanceManagerOverviewPanel } from "@/components/governance-manager-overview-panel";
import { GovernanceRoutingOptimizationPanel } from "@/components/governance-routing-optimization-panel";
import { GovernanceRoutingPolicyReviewPanel } from "@/components/governance-routing-policy-review-panel";
import { GovernanceRoutingPolicyAutopromotionsPanel } from "@/components/governance-routing-policy-autopromotions-panel";
import { GovernanceRoutingPolicyRollbacksPanel } from "@/components/governance-routing-policy-rollbacks-panel";
import { GovernanceRoutingPolicyRollbackImpactPanel } from "@/components/governance-routing-policy-rollback-impact-panel";
import { GovernancePolicyOptimizationPanel } from "@/components/governance-policy-optimization-panel";
import { GovernancePolicyReviewPanel } from "@/components/governance-policy-review-panel";
import { GovernancePolicyAutopromotionsPanel } from "@/components/governance-policy-autopromotions-panel";
import { MultiAssetFoundationPanel } from "@/components/multi-asset-foundation-panel";
import { DependencyContextPanel } from "@/components/dependency-context-panel";
import { CrossAssetSignalsPanel } from "@/components/cross-asset-signals-panel";
import { CrossAssetExplainabilityPanel } from "@/components/cross-asset-explainability-panel";
import { CrossAssetAttributionPanel } from "@/components/cross-asset-attribution-panel";
import { DependencyPriorityWeightingPanel } from "@/components/dependency-priority-weighting-panel";
import { RegimeAwareCrossAssetPanel } from "@/components/regime-aware-cross-asset-panel";
import { CrossAssetReplayValidationPanel } from "@/components/cross-asset-replay-validation-panel";
import { CrossAssetTimingPanel } from "@/components/cross-asset-timing-panel";
import { CrossAssetTimingAttributionPanel } from "@/components/cross-asset-timing-attribution-panel";
import { CrossAssetTimingCompositePanel } from "@/components/cross-asset-timing-composite-panel";
import { CrossAssetTimingReplayValidationPanel } from "@/components/cross-asset-timing-replay-validation-panel";
import { CrossAssetTransitionDiagnosticsPanel } from "@/components/cross-asset-transition-diagnostics-panel";
import { CrossAssetTransitionAttributionPanel } from "@/components/cross-asset-transition-attribution-panel";
import { CrossAssetTransitionCompositePanel } from "@/components/cross-asset-transition-composite-panel";
import { CrossAssetTransitionReplayValidationPanel } from "@/components/cross-asset-transition-replay-validation-panel";
import { CrossAssetPatternPanel } from "@/components/cross-asset-pattern-panel";
import { CrossAssetArchetypeAttributionPanel } from "@/components/cross-asset-archetype-attribution-panel";
import { CrossAssetArchetypeCompositePanel } from "@/components/cross-asset-archetype-composite-panel";
import { CrossAssetArchetypeReplayValidationPanel } from "@/components/cross-asset-archetype-replay-validation-panel";
import { CrossAssetPatternClusterPanel } from "@/components/cross-asset-pattern-cluster-panel";
import { CrossAssetClusterAttributionPanel } from "@/components/cross-asset-cluster-attribution-panel";
import { CrossAssetClusterCompositePanel } from "@/components/cross-asset-cluster-composite-panel";
import { CrossAssetClusterReplayValidationPanel } from "@/components/cross-asset-cluster-replay-validation-panel";
import { CrossAssetPersistencePanel } from "@/components/cross-asset-persistence-panel";
import { CrossAssetPersistenceAttributionPanel } from "@/components/cross-asset-persistence-attribution-panel";
import { CrossAssetPersistenceCompositePanel } from "@/components/cross-asset-persistence-composite-panel";
import { CrossAssetPersistenceReplayValidationPanel } from "@/components/cross-asset-persistence-replay-validation-panel";
import { CrossAssetSignalDecayPanel } from "@/components/cross-asset-signal-decay-panel";
import { CrossAssetDecayAttributionPanel } from "@/components/cross-asset-decay-attribution-panel";
import { CrossAssetDecayCompositePanel } from "@/components/cross-asset-decay-composite-panel";
import { CrossAssetDecayReplayValidationPanel } from "@/components/cross-asset-decay-replay-validation-panel";
import { CrossAssetLayerConflictPanel } from "@/components/cross-asset-layer-conflict-panel";
import { CrossAssetConflictAttributionPanel } from "@/components/cross-asset-conflict-attribution-panel";
import { GovernanceOperatorPerformancePanel } from "@/components/governance-operator-performance-panel";
import { GovernanceThresholdLearningPanel } from "@/components/governance-threshold-learning-panel";
import { GovernanceThresholdLearningReviewPanel } from "@/components/governance-threshold-learning-review-panel";
import { GovernanceWorkloadPanel } from "@/components/governance-workload-panel";
import { GovernanceEscalationPanel } from "@/components/governance-escalation-panel";
import { IncidentTimelinePanel } from "@/components/incident-timeline-panel";
import { PriorRunDiffPanel } from "@/components/prior-run-diff-panel";
import { RegimeThresholdPanel } from "@/components/regime-threshold-panel";
import { RegimeTransitionPanel } from "@/components/regime-transition-panel";
import { ReplayDeltaPanel } from "@/components/replay-delta-panel";
import { ScopePanel } from "@/components/scope-panel";
import { WorkerHealthPanel } from "@/components/worker-health-panel";
import { OpsMetricsCards } from "@/components/ops-metrics-cards";
import { RunInspectionPanel } from "@/components/run-inspection-panel";
import { SlaPanel } from "@/components/sla-panel";
import { StabilityPanel } from "@/components/stability-panel";
import { VersionGovernancePanel } from "@/components/version-governance-panel";
import { useOpsRealtime } from "@/hooks/use-ops-realtime";
import type { DeadLetterRow } from "@/lib/queries/dead_letters";
import type { AlertPolicyRuleRow, QueueGovernanceRuleRow } from "@/lib/queries/governance";
import type { JobRun } from "@/lib/queries/jobs";
import type { ActiveRegimeThresholdRow, GovernanceAlertEventRow, GovernanceAlertStateRow, GovernanceAnomalyClusterRow, GovernanceCaseAgingSummaryRow, GovernanceCaseMixSummaryRow, GovernanceCaseSlaSummaryRow, GovernanceCaseSummaryRow, GovernanceChronicWatchlistSummaryRow, GovernanceDegradationStateRow, GovernanceEscalationEffectivenessAnalyticsRow, GovernanceEscalationEventRow, GovernanceEscalationSummaryRow, GovernanceIncidentAnalyticsSnapshotRow, GovernanceIncidentAnalyticsSummaryRow, GovernanceLifecycleRow, GovernanceManagerOverviewSummaryRow, GovernanceOperatingRiskSummaryRow, GovernanceOperatorCaseMetricsRow, GovernanceOperatorEffectivenessRow, GovernanceOperatorPerformanceSummaryRow, GovernanceOperatorPressureRow, GovernanceOperatorTeamComparisonSummaryRow, GovernancePerformanceSnapshotRow, GovernancePromotionHealthOverviewRow, GovernancePromotionRollbackRiskSummaryRow, GovernanceRecurrenceBurdenRow, GovernanceReassignmentPressureRow, GovernanceRecoveryEventRow, GovernanceReviewPriorityRow, GovernanceRootCauseTrendRow, GovernanceRoutingContextFitRow, GovernanceRoutingDecisionRow, GovernanceRoutingFeatureEffectivenessRow, GovernanceRoutingOptimizationSnapshotRow, GovernanceRoutingPolicyApplicationRow, GovernanceRoutingPolicyAutopromotionEligibilityRow, GovernanceRoutingPolicyAutopromotionPolicyRow, GovernanceRoutingPolicyAutopromotionRollbackCandidateRow, GovernanceRoutingPolicyAutopromotionSummaryRow, GovernanceRoutingPolicyOpportunityRow, GovernanceRoutingPolicyPendingRollbackRow, GovernanceRoutingPolicyPromotionSummaryRow, GovernanceRoutingPolicyReviewSummaryRow, GovernanceRoutingPolicyRollbackExecutionSummaryRow, GovernanceRoutingPolicyRollbackReviewSummaryRow, GovernanceRoutingPolicyRollbackImpactRow, GovernanceRoutingPolicyRollbackEffectivenessSummaryRow, GovernanceRoutingPolicyRollbackPendingEvaluationRow, GovernancePolicyOptimizationSnapshotRow, GovernancePolicyFeatureEffectivenessRow, GovernancePolicyContextFitRow, GovernancePolicyOpportunityRow, GovernancePolicyReviewSummaryRow, GovernancePolicyPromotionSummaryRow, GovernancePolicyPendingPromotionRow, GovernancePolicyAutopromotionSummaryRow, GovernancePolicyAutopromotionEligibilityRow, GovernancePolicyAutopromotionRollbackCandidateRow, MultiAssetSyncHealthRow, NormalizedMultiAssetMarketStateRow, MultiAssetFamilyStateSummaryRow, WatchlistContextSnapshotRow, WatchlistDependencyCoverageSummaryRow, WatchlistDependencyContextDetailRow, WatchlistDependencyFamilyStateRow, CrossAssetSignalSummaryRow, CrossAssetDependencyHealthRow, RunCrossAssetContextSummaryRow, CrossAssetExplanationSummaryRow, CrossAssetFamilyExplanationSummaryRow, RunCrossAssetExplanationBridgeRow, CrossAssetAttributionSummaryRow, CrossAssetFamilyAttributionSummaryRow, RunCompositeIntegrationSummaryRow, CrossAssetFamilyWeightedAttributionSummaryRow, CrossAssetSymbolWeightedAttributionSummaryRow, RunCrossAssetWeightedIntegrationSummaryRow, CrossAssetFamilyRegimeAttributionSummaryRow, CrossAssetSymbolRegimeAttributionSummaryRow, RunCrossAssetRegimeIntegrationSummaryRow, CrossAssetReplayValidationSummaryRow, CrossAssetFamilyReplayStabilitySummaryRow, CrossAssetReplayStabilityAggregateRow, CrossAssetLeadLagPairSummaryRow, CrossAssetFamilyTimingSummaryRow, RunCrossAssetTimingSummaryRow, CrossAssetFamilyTimingAttributionSummaryRow, CrossAssetSymbolTimingAttributionSummaryRow, RunCrossAssetTimingAttributionSummaryRow, CrossAssetTimingCompositeSummaryRow, CrossAssetFamilyTimingCompositeSummaryRow, RunCrossAssetFinalIntegrationSummaryRow, CrossAssetTimingReplayValidationSummaryRow, CrossAssetFamilyTimingReplayStabilitySummaryRow, CrossAssetTimingReplayStabilityAggregateRow, CrossAssetFamilyTransitionStateSummaryRow, CrossAssetFamilyTransitionEventSummaryRow, CrossAssetFamilySequenceSummaryRow, RunCrossAssetTransitionDiagnosticsSummaryRow, CrossAssetFamilyTransitionAttributionSummaryRow, CrossAssetSymbolTransitionAttributionSummaryRow, RunCrossAssetTransitionAttributionSummaryRow, CrossAssetTransitionCompositeSummaryRow, CrossAssetFamilyTransitionCompositeSummaryRow, RunCrossAssetSequencingIntegrationSummaryRow, CrossAssetTransitionReplayValidationSummaryRow, CrossAssetFamilyTransitionReplayStabilitySummaryRow, CrossAssetTransitionReplayStabilityAggregateRow, CrossAssetFamilyArchetypeSummaryRow, CrossAssetRunArchetypeSummaryRow, CrossAssetRegimeArchetypeSummaryRow, RunCrossAssetPatternSummaryRow, CrossAssetFamilyArchetypeAttributionSummaryRow, CrossAssetSymbolArchetypeAttributionSummaryRow, RunCrossAssetArchetypeAttributionSummaryRow, CrossAssetArchetypeCompositeSummaryRow, CrossAssetFamilyArchetypeCompositeSummaryRow, RunCrossAssetArchetypeIntegrationSummaryRow, CrossAssetArchetypeReplayValidationSummaryRow, CrossAssetFamilyArchetypeReplayStabilitySummaryRow, CrossAssetArchetypeReplayStabilityAggregateRow, CrossAssetArchetypeClusterSummaryRow, CrossAssetArchetypeRegimeRotationSummaryRow, CrossAssetPatternDriftEventSummaryRow, RunCrossAssetPatternClusterSummaryRow, CrossAssetFamilyClusterAttributionSummaryRow, CrossAssetSymbolClusterAttributionSummaryRow, RunCrossAssetClusterAttributionSummaryRow, CrossAssetClusterCompositeSummaryRow, CrossAssetFamilyClusterCompositeSummaryRow, RunCrossAssetClusterIntegrationSummaryRow, CrossAssetClusterReplayValidationSummaryRow, CrossAssetFamilyClusterReplayStabilitySummaryRow, CrossAssetClusterReplayStabilityAggregateRow, CrossAssetStatePersistenceSummaryRow, CrossAssetRegimeMemorySummaryRow, CrossAssetPersistenceTransitionEventSummaryRow, RunCrossAssetPersistenceSummaryRow, CrossAssetFamilyPersistenceAttributionSummaryRow, CrossAssetSymbolPersistenceAttributionSummaryRow, RunCrossAssetPersistenceAttributionSummaryRow, CrossAssetPersistenceCompositeSummaryRow, CrossAssetFamilyPersistenceCompositeSummaryRow, RunCrossAssetPersistenceIntegrationSummaryRow, CrossAssetPersistenceReplayValidationSummaryRow, CrossAssetFamilyPersistenceReplayStabilitySummaryRow, CrossAssetPersistenceReplayStabilityAggregateRow, CrossAssetSignalDecaySummaryRow, CrossAssetFamilySignalDecaySummaryRow, CrossAssetStaleMemoryEventSummaryRow, RunCrossAssetSignalDecaySummaryRow, CrossAssetFamilyDecayAttributionSummaryRow, CrossAssetSymbolDecayAttributionSummaryRow, RunCrossAssetDecayAttributionSummaryRow, CrossAssetDecayCompositeSummaryRow, CrossAssetFamilyDecayCompositeSummaryRow, RunCrossAssetDecayIntegrationSummaryRow, CrossAssetDecayReplayValidationSummaryRow, CrossAssetFamilyDecayReplayStabilitySummaryRow, CrossAssetDecayReplayStabilityAggregateRow, CrossAssetLayerAgreementSummaryRow, CrossAssetFamilyLayerAgreementSummaryRow, CrossAssetLayerConflictEventSummaryRow, RunCrossAssetLayerConflictSummaryRow, CrossAssetFamilyConflictAttributionSummaryRow, CrossAssetSymbolConflictAttributionSummaryRow, RunCrossAssetConflictAttributionSummaryRow, GovernanceRoutingPromotionImpactSummaryRow, GovernanceRoutingQualityRow, GovernanceRoutingRecommendationInputRow, GovernanceRoutingRecommendationRow, GovernanceStaleCaseSummaryRow, GovernanceTeamCaseMetricsRow, GovernanceTeamEffectivenessRow, GovernanceTeamPerformanceSummaryRow, GovernanceTeamPressureRow, GovernanceThresholdApplicationRow, GovernanceThresholdAutopromotionSummaryRow, GovernanceThresholdLearningRecommendationRow, GovernanceThresholdPerformanceSummaryRow, GovernanceThresholdPromotionImpactSummaryRow, GovernanceThresholdReviewSummaryRow, GovernanceTrendWindowRow, MacroSyncHealthRow, QueueDepthRow, QueueGovernanceStateRow, QueueRuntimeRow, RegimeThresholdOverrideRow, RegimeThresholdProfileRow, StabilitySummaryRow, VersionGovernanceRow, WatchlistAnomalySummaryRow, WatchlistSlaRow, WorkerHeartbeatRow } from "@/lib/queries/metrics";
import type { GovernanceIncidentDetail } from "@emis-types/ops";
import type {
  RunAttributionRow,
  RunDriftRow,
  RunExplanationRow,
  RunInputSnapshotRow,
  RunInspectionRow,
  RunPriorComparisonRow,
  RunScopeInspectionRow,
  ReplayDeltaRow,
  RegimeTransitionRow,
  RunStageTimingRow,
} from "@/lib/queries/runs";

export default function OpsDashboardShell({ workspaceSlug = "demo" }: { workspaceSlug?: string }) {
  const [deadLetters, setDeadLetters] = useState<DeadLetterRow[]>([]);
  const [workers, setWorkers] = useState<WorkerHeartbeatRow[]>([]);
  const [depth, setDepth] = useState<QueueDepthRow[]>([]);
  const [runtime, setRuntime] = useState<QueueRuntimeRow[]>([]);
  const [jobs, setJobs] = useState<JobRun[]>([]);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [runInspection, setRunInspection] = useState<RunInspectionRow | null>(null);
  const [runAttribution, setRunAttribution] = useState<RunAttributionRow | null>(null);
  const [runDrift, setRunDrift] = useState<RunDriftRow | null>(null);
  const [runRegimeTransition, setRunRegimeTransition] = useState<RegimeTransitionRow | null>(null);
  const [runReplayDelta, setRunReplayDelta] = useState<ReplayDeltaRow | null>(null);
  const [runExplanation, setRunExplanation] = useState<RunExplanationRow | null>(null);
  const [runStageTimings, setRunStageTimings] = useState<RunStageTimingRow[]>([]);
  const [runInputSnapshot, setRunInputSnapshot] = useState<RunInputSnapshotRow | null>(null);
  const [runScope, setRunScope] = useState<RunScopeInspectionRow | null>(null);
  const [runComparison, setRunComparison] = useState<RunPriorComparisonRow | null>(null);
  const [slaRows, setSlaRows] = useState<WatchlistSlaRow[]>([]);
  const [stabilityRows, setStabilityRows] = useState<StabilitySummaryRow[]>([]);
  const [versionGovernanceRows, setVersionGovernanceRows] = useState<VersionGovernanceRow[]>([]);
  const [governanceAlertEvents, setGovernanceAlertEvents] = useState<GovernanceAlertEventRow[]>([]);
  const [governanceAlertState, setGovernanceAlertState] = useState<GovernanceAlertStateRow[]>([]);
  const [anomalyClusters, setAnomalyClusters] = useState<GovernanceAnomalyClusterRow[]>([]);
  const [anomalySummary, setAnomalySummary] = useState<WatchlistAnomalySummaryRow[]>([]);
  const [governanceDegradationActive, setGovernanceDegradationActive] = useState<GovernanceDegradationStateRow[]>([]);
  const [governanceDegradationResolved, setGovernanceDegradationResolved] = useState<GovernanceDegradationStateRow[]>([]);
  const [governanceRecoveries, setGovernanceRecoveries] = useState<GovernanceRecoveryEventRow[]>([]);
  const [governanceLifecycleActive, setGovernanceLifecycleActive] = useState<GovernanceLifecycleRow[]>([]);
  const [governanceLifecycleAcknowledged, setGovernanceLifecycleAcknowledged] = useState<GovernanceLifecycleRow[]>([]);
  const [governanceLifecycleResolved, setGovernanceLifecycleResolved] = useState<GovernanceLifecycleRow[]>([]);
  const [governanceCasesActive, setGovernanceCasesActive] = useState<GovernanceCaseSummaryRow[]>([]);
  const [governanceCasesRecent, setGovernanceCasesRecent] = useState<GovernanceCaseSummaryRow[]>([]);
  const [routingDecisions, setRoutingDecisions] = useState<GovernanceRoutingDecisionRow[]>([]);
  const [routingOperatorMetrics, setRoutingOperatorMetrics] = useState<GovernanceOperatorCaseMetricsRow[]>([]);
  const [routingTeamMetrics, setRoutingTeamMetrics] = useState<GovernanceTeamCaseMetricsRow[]>([]);
  const [routingQuality, setRoutingQuality] = useState<GovernanceRoutingQualityRow[]>([]);
  const [reassignmentPressure, setReassignmentPressure] = useState<GovernanceReassignmentPressureRow[]>([]);
  const [routingEffectivenessOperators, setRoutingEffectivenessOperators] = useState<GovernanceOperatorEffectivenessRow[]>([]);
  const [routingEffectivenessTeams, setRoutingEffectivenessTeams] = useState<GovernanceTeamEffectivenessRow[]>([]);
  const [routingRecommendationInputs, setRoutingRecommendationInputs] = useState<GovernanceRoutingRecommendationInputRow[]>([]);
  const [routingRecommendations, setRoutingRecommendations] = useState<GovernanceRoutingRecommendationRow[]>([]);
  const [incidentAnalyticsSummary, setIncidentAnalyticsSummary] = useState<GovernanceIncidentAnalyticsSummaryRow | null>(null);
  const [incidentAnalyticsRootCauseTrends, setIncidentAnalyticsRootCauseTrends] = useState<GovernanceRootCauseTrendRow[]>([]);
  const [incidentAnalyticsRecurrenceBurden, setIncidentAnalyticsRecurrenceBurden] = useState<GovernanceRecurrenceBurdenRow[]>([]);
  const [incidentAnalyticsEscalationEffectiveness, setIncidentAnalyticsEscalationEffectiveness] = useState<GovernanceEscalationEffectivenessAnalyticsRow | null>(null);
  const [incidentAnalyticsSnapshots, setIncidentAnalyticsSnapshots] = useState<GovernanceIncidentAnalyticsSnapshotRow[]>([]);
  const [thresholdPromotionImpact, setThresholdPromotionImpact] = useState<GovernanceThresholdPromotionImpactSummaryRow[]>([]);
  const [routingPromotionImpact, setRoutingPromotionImpact] = useState<GovernanceRoutingPromotionImpactSummaryRow[]>([]);
  const [promotionRollbackRisk, setPromotionRollbackRisk] = useState<GovernancePromotionRollbackRiskSummaryRow[]>([]);
  const [managerOverview, setManagerOverview] = useState<GovernanceManagerOverviewSummaryRow[]>([]);
  const [managerChronicWatchlists, setManagerChronicWatchlists] = useState<GovernanceChronicWatchlistSummaryRow[]>([]);
  const [managerOperatorTeamComparison, setManagerOperatorTeamComparison] = useState<GovernanceOperatorTeamComparisonSummaryRow[]>([]);
  const [managerPromotionHealth, setManagerPromotionHealth] = useState<GovernancePromotionHealthOverviewRow[]>([]);
  const [managerOperatingRisk, setManagerOperatingRisk] = useState<GovernanceOperatingRiskSummaryRow[]>([]);
  const [managerReviewPriorities, setManagerReviewPriorities] = useState<GovernanceReviewPriorityRow[]>([]);
  const [managerTrendWindows, setManagerTrendWindows] = useState<GovernanceTrendWindowRow[]>([]);
  const [routingOptimizationSnapshot, setRoutingOptimizationSnapshot] = useState<GovernanceRoutingOptimizationSnapshotRow | null>(null);
  const [routingFeatureEffectiveness, setRoutingFeatureEffectiveness] = useState<GovernanceRoutingFeatureEffectivenessRow[]>([]);
  const [routingContextFit, setRoutingContextFit] = useState<GovernanceRoutingContextFitRow[]>([]);
  const [routingPolicyOpportunities, setRoutingPolicyOpportunities] = useState<GovernanceRoutingPolicyOpportunityRow[]>([]);
  const [routingPolicyReviewSummary, setRoutingPolicyReviewSummary] = useState<GovernanceRoutingPolicyReviewSummaryRow[]>([]);
  const [routingPolicyPromotionSummary, setRoutingPolicyPromotionSummary] = useState<GovernanceRoutingPolicyPromotionSummaryRow[]>([]);
  const [routingPolicyApplications, setRoutingPolicyApplications] = useState<GovernanceRoutingPolicyApplicationRow[]>([]);
  const [autopromotion_policies, setAutopromotionPolicies] = useState<GovernanceRoutingPolicyAutopromotionPolicyRow[]>([]);
  const [autopromotion_summary, setAutopromotionSummary] = useState<GovernanceRoutingPolicyAutopromotionSummaryRow[]>([]);
  const [autopromotion_eligibility, setAutopromotionEligibility] = useState<GovernanceRoutingPolicyAutopromotionEligibilityRow[]>([]);
  const [autopromotion_rollbacks, setAutopromotionRollbacks] = useState<GovernanceRoutingPolicyAutopromotionRollbackCandidateRow[]>([]);
  const [rollback_pending, setRollbackPending] = useState<GovernanceRoutingPolicyPendingRollbackRow[]>([]);
  const [rollback_review_summary, setRollbackReviewSummary] = useState<GovernanceRoutingPolicyRollbackReviewSummaryRow[]>([]);
  const [rollback_execution_summary, setRollbackExecutionSummary] = useState<GovernanceRoutingPolicyRollbackExecutionSummaryRow[]>([]);
  const [rollback_impact_rows, setRollbackImpactRows] = useState<GovernanceRoutingPolicyRollbackImpactRow[]>([]);
  const [rollback_effectiveness, setRollbackEffectiveness] = useState<GovernanceRoutingPolicyRollbackEffectivenessSummaryRow | null>(null);
  const [rollback_pending_evals, setRollbackPendingEvals] = useState<GovernanceRoutingPolicyRollbackPendingEvaluationRow[]>([]);
  const [policyOptimizationSnapshot, setPolicyOptimizationSnapshot] = useState<GovernancePolicyOptimizationSnapshotRow | null>(null);
  const [policyFeatureEffectiveness, setPolicyFeatureEffectiveness] = useState<GovernancePolicyFeatureEffectivenessRow[]>([]);
  const [policyContextFit, setPolicyContextFit] = useState<GovernancePolicyContextFitRow[]>([]);
  const [policyOpportunities, setPolicyOpportunities] = useState<GovernancePolicyOpportunityRow[]>([]);
  const [policyReviewSummary, setPolicyReviewSummary] = useState<GovernancePolicyReviewSummaryRow[]>([]);
  const [policyPromotionSummary, setPolicyPromotionSummary] = useState<GovernancePolicyPromotionSummaryRow[]>([]);
  const [policyPendingPromotions, setPolicyPendingPromotions] = useState<GovernancePolicyPendingPromotionRow[]>([]);
  const [policyAutopromotionSummary, setPolicyAutopromotionSummary] = useState<GovernancePolicyAutopromotionSummaryRow[]>([]);
  const [policyAutopromotionEligibility, setPolicyAutopromotionEligibility] = useState<GovernancePolicyAutopromotionEligibilityRow[]>([]);
  const [policyAutopromotionRollbacks, setPolicyAutopromotionRollbacks] = useState<GovernancePolicyAutopromotionRollbackCandidateRow[]>([]);
  const [multiAssetSyncHealth, setMultiAssetSyncHealth] = useState<MultiAssetSyncHealthRow[]>([]);
  const [multiAssetMarketState, setMultiAssetMarketState] = useState<NormalizedMultiAssetMarketStateRow[]>([]);
  const [multiAssetFamilySummary, setMultiAssetFamilySummary] = useState<MultiAssetFamilyStateSummaryRow[]>([]);
  const [dependencyContexts, setDependencyContexts] = useState<WatchlistContextSnapshotRow[]>([]);
  const [dependencyCoverage, setDependencyCoverage] = useState<WatchlistDependencyCoverageSummaryRow[]>([]);
  const [dependencyDetail, setDependencyDetail] = useState<WatchlistDependencyContextDetailRow[]>([]);
  const [dependencyFamily, setDependencyFamily] = useState<WatchlistDependencyFamilyStateRow[]>([]);
  const [crossAssetSignals, setCrossAssetSignals] = useState<CrossAssetSignalSummaryRow[]>([]);
  const [crossAssetDependencyHealth, setCrossAssetDependencyHealth] = useState<CrossAssetDependencyHealthRow[]>([]);
  const [crossAssetRunSummary, setCrossAssetRunSummary] = useState<RunCrossAssetContextSummaryRow[]>([]);
  const [crossAssetExplanation, setCrossAssetExplanation] = useState<CrossAssetExplanationSummaryRow[]>([]);
  const [crossAssetFamilyExplanation, setCrossAssetFamilyExplanation] = useState<CrossAssetFamilyExplanationSummaryRow[]>([]);
  const [crossAssetRunBridge, setCrossAssetRunBridge] = useState<RunCrossAssetExplanationBridgeRow[]>([]);
  const [crossAssetAttribution, setCrossAssetAttribution] = useState<CrossAssetAttributionSummaryRow[]>([]);
  const [crossAssetFamilyAttribution, setCrossAssetFamilyAttribution] = useState<CrossAssetFamilyAttributionSummaryRow[]>([]);
  const [crossAssetRunIntegration, setCrossAssetRunIntegration] = useState<RunCompositeIntegrationSummaryRow[]>([]);
  const [weightedFamilyAttribution, setWeightedFamilyAttribution] = useState<CrossAssetFamilyWeightedAttributionSummaryRow[]>([]);
  const [weightedSymbolAttribution, setWeightedSymbolAttribution] = useState<CrossAssetSymbolWeightedAttributionSummaryRow[]>([]);
  const [weightedRunIntegration, setWeightedRunIntegration] = useState<RunCrossAssetWeightedIntegrationSummaryRow[]>([]);
  const [regimeFamilyAttribution, setRegimeFamilyAttribution] = useState<CrossAssetFamilyRegimeAttributionSummaryRow[]>([]);
  const [regimeSymbolAttribution, setRegimeSymbolAttribution] = useState<CrossAssetSymbolRegimeAttributionSummaryRow[]>([]);
  const [regimeRunIntegration, setRegimeRunIntegration] = useState<RunCrossAssetRegimeIntegrationSummaryRow[]>([]);
  const [replayValidation, setReplayValidation] = useState<CrossAssetReplayValidationSummaryRow[]>([]);
  const [replayFamilyStability, setReplayFamilyStability] = useState<CrossAssetFamilyReplayStabilitySummaryRow[]>([]);
  const [replayStabilityAggregate, setReplayStabilityAggregate] = useState<CrossAssetReplayStabilityAggregateRow | null>(null);
  const [timingPair, setTimingPair] = useState<CrossAssetLeadLagPairSummaryRow[]>([]);
  const [timingFamily, setTimingFamily] = useState<CrossAssetFamilyTimingSummaryRow[]>([]);
  const [timingRun, setTimingRun] = useState<RunCrossAssetTimingSummaryRow[]>([]);
  const [timingFamilyAttribution, setTimingFamilyAttribution] = useState<CrossAssetFamilyTimingAttributionSummaryRow[]>([]);
  const [timingSymbolAttribution, setTimingSymbolAttribution] = useState<CrossAssetSymbolTimingAttributionSummaryRow[]>([]);
  const [timingRunAttribution, setTimingRunAttribution] = useState<RunCrossAssetTimingAttributionSummaryRow[]>([]);
  const [timingComposite, setTimingComposite] = useState<CrossAssetTimingCompositeSummaryRow[]>([]);
  const [timingFamilyComposite, setTimingFamilyComposite] = useState<CrossAssetFamilyTimingCompositeSummaryRow[]>([]);
  const [finalIntegrationSummary, setFinalIntegrationSummary] = useState<RunCrossAssetFinalIntegrationSummaryRow[]>([]);
  const [timingReplayValidation, setTimingReplayValidation] = useState<CrossAssetTimingReplayValidationSummaryRow[]>([]);
  const [timingReplayFamilyStability, setTimingReplayFamilyStability] = useState<CrossAssetFamilyTimingReplayStabilitySummaryRow[]>([]);
  const [timingReplayStabilityAggregate, setTimingReplayStabilityAggregate] = useState<CrossAssetTimingReplayStabilityAggregateRow | null>(null);
  const [transitionStateSummary, setTransitionStateSummary] = useState<CrossAssetFamilyTransitionStateSummaryRow[]>([]);
  const [transitionEventSummary, setTransitionEventSummary] = useState<CrossAssetFamilyTransitionEventSummaryRow[]>([]);
  const [sequenceSummary, setSequenceSummary] = useState<CrossAssetFamilySequenceSummaryRow[]>([]);
  const [runTransitionSummary, setRunTransitionSummary] = useState<RunCrossAssetTransitionDiagnosticsSummaryRow[]>([]);
  const [familyTransitionAttribution, setFamilyTransitionAttribution] = useState<CrossAssetFamilyTransitionAttributionSummaryRow[]>([]);
  const [symbolTransitionAttribution, setSymbolTransitionAttribution] = useState<CrossAssetSymbolTransitionAttributionSummaryRow[]>([]);
  const [runTransitionAttribution, setRunTransitionAttribution] = useState<RunCrossAssetTransitionAttributionSummaryRow[]>([]);
  const [transitionComposite, setTransitionComposite] = useState<CrossAssetTransitionCompositeSummaryRow[]>([]);
  const [familyTransitionComposite, setFamilyTransitionComposite] = useState<CrossAssetFamilyTransitionCompositeSummaryRow[]>([]);
  const [sequencingIntegration, setSequencingIntegration] = useState<RunCrossAssetSequencingIntegrationSummaryRow[]>([]);
  const [transitionReplayValidation, setTransitionReplayValidation] = useState<CrossAssetTransitionReplayValidationSummaryRow[]>([]);
  const [familyTransitionReplayStability, setFamilyTransitionReplayStability] = useState<CrossAssetFamilyTransitionReplayStabilitySummaryRow[]>([]);
  const [transitionReplayStabilityAggregate, setTransitionReplayStabilityAggregate] = useState<CrossAssetTransitionReplayStabilityAggregateRow | null>(null);
  const [familyArchetypeSummary, setFamilyArchetypeSummary] = useState<CrossAssetFamilyArchetypeSummaryRow[]>([]);
  const [runArchetypeSummary, setRunArchetypeSummary] = useState<CrossAssetRunArchetypeSummaryRow[]>([]);
  const [regimeArchetypeSummary, setRegimeArchetypeSummary] = useState<CrossAssetRegimeArchetypeSummaryRow[]>([]);
  const [runPatternSummary, setRunPatternSummary] = useState<RunCrossAssetPatternSummaryRow[]>([]);
  const [familyArchetypeAttribution, setFamilyArchetypeAttribution] = useState<CrossAssetFamilyArchetypeAttributionSummaryRow[]>([]);
  const [symbolArchetypeAttribution, setSymbolArchetypeAttribution] = useState<CrossAssetSymbolArchetypeAttributionSummaryRow[]>([]);
  const [runArchetypeAttribution, setRunArchetypeAttribution] = useState<RunCrossAssetArchetypeAttributionSummaryRow[]>([]);
  const [archetypeComposite, setArchetypeComposite] = useState<CrossAssetArchetypeCompositeSummaryRow[]>([]);
  const [familyArchetypeComposite, setFamilyArchetypeComposite] = useState<CrossAssetFamilyArchetypeCompositeSummaryRow[]>([]);
  const [archetypeIntegration, setArchetypeIntegration] = useState<RunCrossAssetArchetypeIntegrationSummaryRow[]>([]);
  const [archetypeReplayValidation, setArchetypeReplayValidation] = useState<CrossAssetArchetypeReplayValidationSummaryRow[]>([]);
  const [familyArchetypeReplayStability, setFamilyArchetypeReplayStability] = useState<CrossAssetFamilyArchetypeReplayStabilitySummaryRow[]>([]);
  const [archetypeReplayStabilityAggregate, setArchetypeReplayStabilityAggregate] = useState<CrossAssetArchetypeReplayStabilityAggregateRow | null>(null);
  const [patternClusterSummary, setPatternClusterSummary] = useState<CrossAssetArchetypeClusterSummaryRow[]>([]);
  const [patternRegimeRotation, setPatternRegimeRotation] = useState<CrossAssetArchetypeRegimeRotationSummaryRow[]>([]);
  const [patternDriftEvents, setPatternDriftEvents] = useState<CrossAssetPatternDriftEventSummaryRow[]>([]);
  const [runPatternCluster, setRunPatternCluster] = useState<RunCrossAssetPatternClusterSummaryRow[]>([]);
  const [familyClusterAttribution, setFamilyClusterAttribution] = useState<CrossAssetFamilyClusterAttributionSummaryRow[]>([]);
  const [symbolClusterAttribution, setSymbolClusterAttribution] = useState<CrossAssetSymbolClusterAttributionSummaryRow[]>([]);
  const [runClusterAttribution, setRunClusterAttribution] = useState<RunCrossAssetClusterAttributionSummaryRow[]>([]);
  const [clusterComposite, setClusterComposite] = useState<CrossAssetClusterCompositeSummaryRow[]>([]);
  const [familyClusterComposite, setFamilyClusterComposite] = useState<CrossAssetFamilyClusterCompositeSummaryRow[]>([]);
  const [clusterIntegration, setClusterIntegration] = useState<RunCrossAssetClusterIntegrationSummaryRow[]>([]);
  const [clusterReplayValidation, setClusterReplayValidation] = useState<CrossAssetClusterReplayValidationSummaryRow[]>([]);
  const [familyClusterReplayStability, setFamilyClusterReplayStability] = useState<CrossAssetFamilyClusterReplayStabilitySummaryRow[]>([]);
  const [clusterReplayStabilityAggregate, setClusterReplayStabilityAggregate] = useState<CrossAssetClusterReplayStabilityAggregateRow | null>(null);
  const [statePersistenceSummary, setStatePersistenceSummary] = useState<CrossAssetStatePersistenceSummaryRow[]>([]);
  const [regimeMemorySummary, setRegimeMemorySummary] = useState<CrossAssetRegimeMemorySummaryRow[]>([]);
  const [persistenceEventSummary, setPersistenceEventSummary] = useState<CrossAssetPersistenceTransitionEventSummaryRow[]>([]);
  const [runPersistenceSummary, setRunPersistenceSummary] = useState<RunCrossAssetPersistenceSummaryRow[]>([]);
  const [familyPersistenceAttribution, setFamilyPersistenceAttribution] = useState<CrossAssetFamilyPersistenceAttributionSummaryRow[]>([]);
  const [symbolPersistenceAttribution, setSymbolPersistenceAttribution] = useState<CrossAssetSymbolPersistenceAttributionSummaryRow[]>([]);
  const [runPersistenceAttribution, setRunPersistenceAttribution] = useState<RunCrossAssetPersistenceAttributionSummaryRow[]>([]);
  const [persistenceComposite, setPersistenceComposite] = useState<CrossAssetPersistenceCompositeSummaryRow[]>([]);
  const [familyPersistenceComposite, setFamilyPersistenceComposite] = useState<CrossAssetFamilyPersistenceCompositeSummaryRow[]>([]);
  const [finalPersistenceIntegrationSummary, setFinalPersistenceIntegrationSummary] = useState<RunCrossAssetPersistenceIntegrationSummaryRow[]>([]);
  const [persistenceReplayValidation, setPersistenceReplayValidation] = useState<CrossAssetPersistenceReplayValidationSummaryRow[]>([]);
  const [familyPersistenceReplayStability, setFamilyPersistenceReplayStability] = useState<CrossAssetFamilyPersistenceReplayStabilitySummaryRow[]>([]);
  const [persistenceReplayStabilityAggregate, setPersistenceReplayStabilityAggregate] = useState<CrossAssetPersistenceReplayStabilityAggregateRow | null>(null);
  const [signalDecaySummary, setSignalDecaySummary] = useState<CrossAssetSignalDecaySummaryRow[]>([]);
  const [familySignalDecaySummary, setFamilySignalDecaySummary] = useState<CrossAssetFamilySignalDecaySummaryRow[]>([]);
  const [staleMemoryEventSummary, setStaleMemoryEventSummary] = useState<CrossAssetStaleMemoryEventSummaryRow[]>([]);
  const [runSignalDecaySummary, setRunSignalDecaySummary] = useState<RunCrossAssetSignalDecaySummaryRow[]>([]);
  const [familyDecayAttributionSummary, setFamilyDecayAttributionSummary] = useState<CrossAssetFamilyDecayAttributionSummaryRow[]>([]);
  const [symbolDecayAttributionSummary, setSymbolDecayAttributionSummary] = useState<CrossAssetSymbolDecayAttributionSummaryRow[]>([]);
  const [runDecayAttributionSummary, setRunDecayAttributionSummary] = useState<RunCrossAssetDecayAttributionSummaryRow[]>([]);
  const [decayCompositeSummary, setDecayCompositeSummary] = useState<CrossAssetDecayCompositeSummaryRow[]>([]);
  const [familyDecayCompositeSummary, setFamilyDecayCompositeSummary] = useState<CrossAssetFamilyDecayCompositeSummaryRow[]>([]);
  const [finalDecayIntegrationSummary, setFinalDecayIntegrationSummary] = useState<RunCrossAssetDecayIntegrationSummaryRow[]>([]);
  const [decayReplayValidationSummary, setDecayReplayValidationSummary] = useState<CrossAssetDecayReplayValidationSummaryRow[]>([]);
  const [familyDecayReplayStabilitySummary, setFamilyDecayReplayStabilitySummary] = useState<CrossAssetFamilyDecayReplayStabilitySummaryRow[]>([]);
  const [decayReplayStabilityAggregate, setDecayReplayStabilityAggregate] = useState<CrossAssetDecayReplayStabilityAggregateRow | null>(null);
  const [layerAgreementSummary, setLayerAgreementSummary] = useState<CrossAssetLayerAgreementSummaryRow[]>([]);
  const [familyLayerAgreementSummary, setFamilyLayerAgreementSummary] = useState<CrossAssetFamilyLayerAgreementSummaryRow[]>([]);
  const [layerConflictEventSummary, setLayerConflictEventSummary] = useState<CrossAssetLayerConflictEventSummaryRow[]>([]);
  const [runLayerConflictSummary, setRunLayerConflictSummary] = useState<RunCrossAssetLayerConflictSummaryRow[]>([]);
  const [familyConflictAttributionSummary, setFamilyConflictAttributionSummary] = useState<CrossAssetFamilyConflictAttributionSummaryRow[]>([]);
  const [symbolConflictAttributionSummary, setSymbolConflictAttributionSummary] = useState<CrossAssetSymbolConflictAttributionSummaryRow[]>([]);
  const [runConflictAttributionSummary, setRunConflictAttributionSummary] = useState<RunCrossAssetConflictAttributionSummaryRow[]>([]);
  const [operatorPerformanceSummary, setOperatorPerformanceSummary] = useState<GovernanceOperatorPerformanceSummaryRow[]>([]);
  const [teamPerformanceSummary, setTeamPerformanceSummary] = useState<GovernanceTeamPerformanceSummaryRow[]>([]);
  const [operatorCaseMix, setOperatorCaseMix] = useState<GovernanceCaseMixSummaryRow[]>([]);
  const [teamCaseMix, setTeamCaseMix] = useState<GovernanceCaseMixSummaryRow[]>([]);
  const [performanceSnapshots, setPerformanceSnapshots] = useState<GovernancePerformanceSnapshotRow[]>([]);
  const [thresholdLearningPerformance, setThresholdLearningPerformance] = useState<GovernanceThresholdPerformanceSummaryRow[]>([]);
  const [thresholdLearningRecommendations, setThresholdLearningRecommendations] = useState<GovernanceThresholdLearningRecommendationRow[]>([]);
  const [thresholdReviewSummary, setThresholdReviewSummary] = useState<GovernanceThresholdReviewSummaryRow[]>([]);
  const [thresholdAutopromotionSummary, setThresholdAutopromotionSummary] = useState<GovernanceThresholdAutopromotionSummaryRow[]>([]);
  const [workloadAging, setWorkloadAging] = useState<GovernanceCaseAgingSummaryRow[]>([]);
  const [workloadStale, setWorkloadStale] = useState<GovernanceStaleCaseSummaryRow[]>([]);
  const [workloadOperatorPressure, setWorkloadOperatorPressure] = useState<GovernanceOperatorPressureRow[]>([]);
  const [workloadTeamPressure, setWorkloadTeamPressure] = useState<GovernanceTeamPressureRow[]>([]);
  const [workloadSla, setWorkloadSla] = useState<GovernanceCaseSlaSummaryRow[]>([]);
  const [escalationActive, setEscalationActive] = useState<GovernanceEscalationSummaryRow[]>([]);
  const [escalationEvents, setEscalationEvents] = useState<GovernanceEscalationEventRow[]>([]);
  const [escalationCandidates, setEscalationCandidates] = useState<GovernanceStaleCaseSummaryRow[]>([]);
  const [selectedCaseId, setSelectedCaseId] = useState<string | null>(null);
  const [selectedIncident, setSelectedIncident] = useState<GovernanceIncidentDetail | null>(null);
  const [thresholdProfiles, setThresholdProfiles] = useState<RegimeThresholdProfileRow[]>([]);
  const [thresholdOverrides, setThresholdOverrides] = useState<RegimeThresholdOverrideRow[]>([]);
  const [activeThresholds, setActiveThresholds] = useState<ActiveRegimeThresholdRow[]>([]);
  const [thresholdApplications, setThresholdApplications] = useState<GovernanceThresholdApplicationRow[]>([]);
  const [macroSyncHealth, setMacroSyncHealth] = useState<MacroSyncHealthRow[]>([]);
  const [governanceState, setGovernanceState] = useState<QueueGovernanceStateRow[]>([]);
  const [governanceRules, setGovernanceRules] = useState<QueueGovernanceRuleRow[]>([]);
  const [alertPolicies, setAlertPolicies] = useState<AlertPolicyRuleRow[]>([]);
  const [loading, setLoading] = useState(true);

  const loadDeadLetters = useCallback(async () => {
    const res = await fetch(`/api/dead-letter?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as { ok: boolean; rows: DeadLetterRow[] };
    if (data.ok) setDeadLetters(data.rows);
  }, [workspaceSlug]);

  const loadWorkers = useCallback(async () => {
    const res = await fetch(`/api/metrics/workers?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as { ok: boolean; rows: WorkerHeartbeatRow[] };
    if (data.ok) setWorkers(data.rows);
  }, [workspaceSlug]);

  const loadMetrics = useCallback(async () => {
    const res = await fetch(`/api/metrics/queue?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as { ok: boolean; depth: QueueDepthRow[]; runtime: QueueRuntimeRow[] };
    if (data.ok) {
      setDepth(data.depth ?? []);
      setRuntime(data.runtime ?? []);
    }
  }, [workspaceSlug]);

  const loadJobs = useCallback(async () => {
    const res = await fetch(`/api/jobs?workspaceSlug=${workspaceSlug}&limit=12`, { cache: "no-store" });
    const data = await res.json() as { ok: boolean; jobs: JobRun[] };
    if (!data.ok) return;

    setJobs(data.jobs ?? []);
    setSelectedRunId((current) => {
      if (!data.jobs || data.jobs.length === 0) return null;
      if (current && data.jobs.some((job) => job.id === current)) return current;
      return data.jobs[0].id;
    });
  }, [workspaceSlug]);

  const loadRunInspection = useCallback(async (runId: string) => {
    const res = await fetch(`/api/runs/${runId}`, { cache: "no-store" });
    const data = await res.json() as { ok: boolean; run: RunInspectionRow | null };
    if (data.ok) {
      setRunInspection(data.run ?? null);
    }
  }, []);

  const loadRunExplanation = useCallback(async (runId: string) => {
    const res = await fetch(`/api/runs/${runId}/explanation`, { cache: "no-store" });
    const data = await res.json() as { ok: boolean; explanation: RunExplanationRow | null };
    if (data.ok) {
      setRunExplanation(data.explanation ?? null);
    }
  }, []);

  const loadRunAttribution = useCallback(async (runId: string) => {
    const res = await fetch(`/api/runs/${runId}/attribution`, { cache: "no-store" });
    const data = await res.json() as { ok: boolean; attribution: RunAttributionRow | null };
    if (data.ok) {
      setRunAttribution(data.attribution ?? null);
    }
  }, []);

  const loadRunDrift = useCallback(async (runId: string) => {
    const res = await fetch(`/api/runs/${runId}/drift`, { cache: "no-store" });
    const data = await res.json() as { ok: boolean; drift: RunDriftRow | null };
    if (data.ok) {
      setRunDrift(data.drift ?? null);
    }
  }, []);

  const loadRunReplayDelta = useCallback(async (runId: string) => {
    const res = await fetch(`/api/runs/${runId}/replay-delta`, { cache: "no-store" });
    const data = await res.json() as { ok: boolean; replayDelta: ReplayDeltaRow | null };
    if (data.ok) {
      setRunReplayDelta(data.replayDelta ?? null);
    }
  }, []);

  const loadRunRegimeTransition = useCallback(async (runId: string) => {
    const res = await fetch(`/api/runs/${runId}/regime-transition`, { cache: "no-store" });
    const data = await res.json() as { ok: boolean; regimeTransition: RegimeTransitionRow | null };
    if (data.ok) {
      setRunRegimeTransition(data.regimeTransition ?? null);
    }
  }, []);

  const loadRunForensics = useCallback(async (runId: string) => {
    const res = await fetch(`/api/runs/${runId}/forensics`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      stageTimings: RunStageTimingRow[];
      inputSnapshot: RunInputSnapshotRow | null;
    };
    if (data.ok) {
      setRunStageTimings(data.stageTimings ?? []);
      setRunInputSnapshot(data.inputSnapshot ?? null);
    }
  }, []);

  const loadRunComparison = useCallback(async (runId: string) => {
    const res = await fetch(`/api/runs/${runId}/comparison`, { cache: "no-store" });
    const data = await res.json() as { ok: boolean; comparison: RunPriorComparisonRow | null };
    if (data.ok) {
      setRunComparison(data.comparison ?? null);
    }
  }, []);

  const loadRunScope = useCallback(async (runId: string) => {
    const res = await fetch(`/api/runs/${runId}/scope`, { cache: "no-store" });
    const data = await res.json() as { ok: boolean; scope: RunScopeInspectionRow | null };
    if (data.ok) {
      setRunScope(data.scope ?? null);
    }
  }, []);

  const loadSla = useCallback(async () => {
    const res = await fetch(`/api/metrics/sla?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as { ok: boolean; rows: WatchlistSlaRow[] };
    if (data.ok) {
      setSlaRows(data.rows ?? []);
    }
  }, [workspaceSlug]);

  const loadStability = useCallback(async () => {
    const res = await fetch(`/api/metrics/stability?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as { ok: boolean; rows: StabilitySummaryRow[] };
    if (data.ok) {
      setStabilityRows(data.rows ?? []);
    }
  }, [workspaceSlug]);

  const loadVersionGovernance = useCallback(async () => {
    const res = await fetch(`/api/metrics/version-governance?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as { ok: boolean; rows: VersionGovernanceRow[] };
    if (data.ok) {
      setVersionGovernanceRows(data.rows ?? []);
    }
  }, [workspaceSlug]);

  const loadGovernance = useCallback(async () => {
    const res = await fetch(`/api/queue/governance?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      state: QueueGovernanceStateRow[];
      rules: QueueGovernanceRuleRow[];
      alertPolicies: AlertPolicyRuleRow[];
    };
    if (data.ok) {
      setGovernanceState(data.state ?? []);
      setGovernanceRules(data.rules ?? []);
      setAlertPolicies(data.alertPolicies ?? []);
    }
  }, [workspaceSlug]);

  const loadGovernanceAlerts = useCallback(async () => {
    const res = await fetch(`/api/metrics/governance-alerts?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      events: GovernanceAlertEventRow[];
      state: GovernanceAlertStateRow[];
    };
    if (data.ok) {
      setGovernanceAlertEvents(data.events ?? []);
      setGovernanceAlertState(data.state ?? []);
    }
  }, [workspaceSlug]);

  const loadAnomalyClusters = useCallback(async () => {
    const res = await fetch(`/api/metrics/anomaly-clusters?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      clusters: GovernanceAnomalyClusterRow[];
      summary: WatchlistAnomalySummaryRow[];
    };
    if (data.ok) {
      setAnomalyClusters(data.clusters ?? []);
      setAnomalySummary(data.summary ?? []);
    }
  }, [workspaceSlug]);

  const loadGovernanceDegradation = useCallback(async () => {
    const res = await fetch(`/api/metrics/governance-degradation?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      activeStates: GovernanceDegradationStateRow[];
      resolvedStates: GovernanceDegradationStateRow[];
      recoveries: GovernanceRecoveryEventRow[];
    };
    if (data.ok) {
      setGovernanceDegradationActive(data.activeStates ?? []);
      setGovernanceDegradationResolved(data.resolvedStates ?? []);
      setGovernanceRecoveries(data.recoveries ?? []);
    }
  }, [workspaceSlug]);

  const loadGovernanceLifecycle = useCallback(async () => {
    const res = await fetch(`/api/metrics/governance-lifecycle?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      activeStates: GovernanceLifecycleRow[];
      acknowledgedStates: GovernanceLifecycleRow[];
      resolvedStates: GovernanceLifecycleRow[];
      recoveries: GovernanceRecoveryEventRow[];
    };
    if (data.ok) {
      setGovernanceLifecycleActive(data.activeStates ?? []);
      setGovernanceLifecycleAcknowledged(data.acknowledgedStates ?? []);
      setGovernanceLifecycleResolved(data.resolvedStates ?? []);
    }
  }, [workspaceSlug]);

  const loadGovernanceCases = useCallback(async () => {
    const res = await fetch(`/api/governance/cases?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      activeCases: GovernanceCaseSummaryRow[];
      recentCases: GovernanceCaseSummaryRow[];
    };
    if (data.ok) {
      setGovernanceCasesActive(data.activeCases ?? []);
      setGovernanceCasesRecent(data.recentCases ?? []);
      const ordered = [...(data.activeCases ?? []), ...(data.recentCases ?? [])];
      setSelectedCaseId((current) => {
        if (ordered.length === 0) return null;
        if (current && ordered.some((row) => row.id === current)) return current;
        return ordered[0].id;
      });
    }
  }, [workspaceSlug]);

  const loadGovernanceRouting = useCallback(async () => {
    const res = await fetch(`/api/metrics/governance-routing?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      routingDecisions: GovernanceRoutingDecisionRow[];
      operatorMetrics: GovernanceOperatorCaseMetricsRow[];
      teamMetrics: GovernanceTeamCaseMetricsRow[];
    };
    if (data.ok) {
      setRoutingDecisions(data.routingDecisions ?? []);
      setRoutingOperatorMetrics(data.operatorMetrics ?? []);
      setRoutingTeamMetrics(data.teamMetrics ?? []);
    }
  }, [workspaceSlug]);

  const loadGovernanceWorkload = useCallback(async () => {
    const res = await fetch(`/api/metrics/governance-workload?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      aging: GovernanceCaseAgingSummaryRow[];
      stale: GovernanceStaleCaseSummaryRow[];
      operatorPressure: GovernanceOperatorPressureRow[];
      teamPressure: GovernanceTeamPressureRow[];
      sla: GovernanceCaseSlaSummaryRow[];
    };
    if (data.ok) {
      setWorkloadAging(data.aging ?? []);
      setWorkloadStale(data.stale ?? []);
      setWorkloadOperatorPressure(data.operatorPressure ?? []);
      setWorkloadTeamPressure(data.teamPressure ?? []);
      setWorkloadSla(data.sla ?? []);
    }
  }, [workspaceSlug]);

  const loadGovernanceRoutingQuality = useCallback(async () => {
    const res = await fetch(`/api/metrics/governance-routing-quality?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      routingQuality: GovernanceRoutingQualityRow[];
      reassignmentPressure: GovernanceReassignmentPressureRow[];
    };
    if (data.ok) {
      setRoutingQuality(data.routingQuality ?? []);
      setReassignmentPressure(data.reassignmentPressure ?? []);
    }
  }, [workspaceSlug]);

  const loadGovernanceRoutingEffectiveness = useCallback(async () => {
    const res = await fetch(`/api/metrics/governance-routing-effectiveness?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      operators: GovernanceOperatorEffectivenessRow[];
      teams: GovernanceTeamEffectivenessRow[];
      recommendationInputs: GovernanceRoutingRecommendationInputRow[];
    };
    if (data.ok) {
      setRoutingEffectivenessOperators(data.operators ?? []);
      setRoutingEffectivenessTeams(data.teams ?? []);
      setRoutingRecommendationInputs(data.recommendationInputs ?? []);
    }
  }, [workspaceSlug]);

  const loadGovernanceRoutingRecommendations = useCallback(async () => {
    const res = await fetch(`/api/governance/routing-recommendations?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      recommendations: GovernanceRoutingRecommendationRow[];
    };
    if (data.ok) {
      setRoutingRecommendations(data.recommendations ?? []);
    }
  }, [workspaceSlug]);

  const loadGovernanceIncidentAnalytics = useCallback(async () => {
    const res = await fetch(`/api/metrics/governance-incident-analytics?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      summary: GovernanceIncidentAnalyticsSummaryRow | null;
      rootCauseTrends: GovernanceRootCauseTrendRow[];
      recurrenceBurden: GovernanceRecurrenceBurdenRow[];
      escalationEffectiveness: GovernanceEscalationEffectivenessAnalyticsRow | null;
      snapshots: GovernanceIncidentAnalyticsSnapshotRow[];
      thresholdPromotionImpact: GovernanceThresholdPromotionImpactSummaryRow[];
      routingPromotionImpact: GovernanceRoutingPromotionImpactSummaryRow[];
      rollbackRisk: GovernancePromotionRollbackRiskSummaryRow[];
    };
    if (data.ok) {
      setIncidentAnalyticsSummary(data.summary ?? null);
      setIncidentAnalyticsRootCauseTrends(data.rootCauseTrends ?? []);
      setIncidentAnalyticsRecurrenceBurden(data.recurrenceBurden ?? []);
      setIncidentAnalyticsEscalationEffectiveness(data.escalationEffectiveness ?? null);
      setIncidentAnalyticsSnapshots(data.snapshots ?? []);
      setThresholdPromotionImpact(data.thresholdPromotionImpact ?? []);
      setRoutingPromotionImpact(data.routingPromotionImpact ?? []);
      setPromotionRollbackRisk(data.rollbackRisk ?? []);
    }
  }, [workspaceSlug]);

  const loadGovernanceOperatorPerformance = useCallback(async () => {
    const res = await fetch(`/api/metrics/governance-operator-performance?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      operatorSummary: GovernanceOperatorPerformanceSummaryRow[];
      teamSummary: GovernanceTeamPerformanceSummaryRow[];
      operatorCaseMix: GovernanceCaseMixSummaryRow[];
      teamCaseMix: GovernanceCaseMixSummaryRow[];
      snapshots: GovernancePerformanceSnapshotRow[];
    };
    if (data.ok) {
      setOperatorPerformanceSummary(data.operatorSummary ?? []);
      setTeamPerformanceSummary(data.teamSummary ?? []);
      setOperatorCaseMix(data.operatorCaseMix ?? []);
      setTeamCaseMix(data.teamCaseMix ?? []);
      setPerformanceSnapshots(data.snapshots ?? []);
    }
  }, [workspaceSlug]);

  const loadGovernanceManagerOverview = useCallback(async () => {
    const res = await fetch(`/api/metrics/governance-manager-overview?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      managerOverview: GovernanceManagerOverviewSummaryRow[];
      chronicWatchlists: GovernanceChronicWatchlistSummaryRow[];
      operatorTeamComparison: GovernanceOperatorTeamComparisonSummaryRow[];
      promotionHealth: GovernancePromotionHealthOverviewRow[];
      operatingRisk: GovernanceOperatingRiskSummaryRow[];
      reviewPriorities: GovernanceReviewPriorityRow[];
      trendWindows: GovernanceTrendWindowRow[];
    };
    if (data.ok) {
      setManagerOverview(data.managerOverview ?? []);
      setManagerChronicWatchlists(data.chronicWatchlists ?? []);
      setManagerOperatorTeamComparison(data.operatorTeamComparison ?? []);
      setManagerPromotionHealth(data.promotionHealth ?? []);
      setManagerOperatingRisk(data.operatingRisk ?? []);
      setManagerReviewPriorities(data.reviewPriorities ?? []);
      setManagerTrendWindows(data.trendWindows ?? []);
    }
  }, [workspaceSlug]);

  const loadGovernanceThresholdLearning = useCallback(async () => {
    const res = await fetch(`/api/metrics/governance-threshold-learning?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      performance: GovernanceThresholdPerformanceSummaryRow[];
      recommendations: GovernanceThresholdLearningRecommendationRow[];
    };
    if (data.ok) {
      setThresholdLearningPerformance(data.performance ?? []);
      setThresholdLearningRecommendations(data.recommendations ?? []);
    }
  }, [workspaceSlug]);

  const loadGovernanceThresholdLearningReview = useCallback(async () => {
    const res = await fetch(`/api/metrics/governance-threshold-learning-review?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      reviewSummary: GovernanceThresholdReviewSummaryRow[];
      autopromotionSummary: GovernanceThresholdAutopromotionSummaryRow[];
    };
    if (data.ok) {
      setThresholdReviewSummary(data.reviewSummary ?? []);
      setThresholdAutopromotionSummary(data.autopromotionSummary ?? []);
    }
  }, [workspaceSlug]);

  const loadGovernanceEscalations = useCallback(async () => {
    const res = await fetch(`/api/metrics/governance-escalations?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      activeEscalations: GovernanceEscalationSummaryRow[];
      recentEvents: GovernanceEscalationEventRow[];
      candidateCases: GovernanceStaleCaseSummaryRow[];
    };
    if (data.ok) {
      setEscalationActive(data.activeEscalations ?? []);
      setEscalationEvents(data.recentEvents ?? []);
      setEscalationCandidates(data.candidateCases ?? []);
    }
  }, [workspaceSlug]);

  const loadIncidentDetail = useCallback(async (caseId: string) => {
    const res = await fetch(`/api/governance/cases/${caseId}`, { cache: "no-store" });
    const data = await res.json() as { ok: boolean; incident: GovernanceIncidentDetail | null };
    if (data.ok) {
      setSelectedIncident(data.incident ?? null);
    } else {
      setSelectedIncident(null);
    }
  }, []);

  const loadRegimeThresholds = useCallback(async () => {
    const res = await fetch(`/api/metrics/regime-thresholds?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      profiles: RegimeThresholdProfileRow[];
      overrides: RegimeThresholdOverrideRow[];
      active: ActiveRegimeThresholdRow[];
      applications: GovernanceThresholdApplicationRow[];
      macroSyncHealth: MacroSyncHealthRow[];
    };
    if (data.ok) {
      setThresholdProfiles(data.profiles ?? []);
      setThresholdOverrides(data.overrides ?? []);
      setActiveThresholds(data.active ?? []);
      setThresholdApplications(data.applications ?? []);
      setMacroSyncHealth(data.macroSyncHealth ?? []);
    }
  }, [workspaceSlug]);

  const loadGovernanceRoutingOptimization = useCallback(async () => {
    const res = await fetch(`/api/metrics/governance-routing-optimization?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      snapshot: GovernanceRoutingOptimizationSnapshotRow | null;
      featureEffectiveness: GovernanceRoutingFeatureEffectivenessRow[];
      contextFit: GovernanceRoutingContextFitRow[];
      policyOpportunities: GovernanceRoutingPolicyOpportunityRow[];
    };
    if (data.ok) {
      setRoutingOptimizationSnapshot(data.snapshot ?? null);
      setRoutingFeatureEffectiveness(data.featureEffectiveness ?? []);
      setRoutingContextFit(data.contextFit ?? []);
      setRoutingPolicyOpportunities(data.policyOpportunities ?? []);
    }
  }, [workspaceSlug]);

  const loadGovernanceRoutingPolicyReview = useCallback(async () => {
    const res = await fetch(`/api/governance/routing-policy-reviews?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      reviewSummary: GovernanceRoutingPolicyReviewSummaryRow[];
    };
    if (data.ok) {
      setRoutingPolicyReviewSummary(data.reviewSummary ?? []);
    }
  }, [workspaceSlug]);

  const loadGovernanceRoutingPolicyPromotion = useCallback(async () => {
    const res = await fetch(`/api/governance/routing-policy-promotions?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      promotionSummary: GovernanceRoutingPolicyPromotionSummaryRow[];
      applications: GovernanceRoutingPolicyApplicationRow[];
    };
    if (data.ok) {
      setRoutingPolicyPromotionSummary(data.promotionSummary ?? []);
      setRoutingPolicyApplications(data.applications ?? []);
    }
  }, [workspaceSlug]);

  const loadGovernanceRoutingPolicyAutopromotion = useCallback(async () => {
    const res = await fetch(`/api/governance/routing-policy-autopromotions?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      policies: GovernanceRoutingPolicyAutopromotionPolicyRow[];
      summary: GovernanceRoutingPolicyAutopromotionSummaryRow[];
      eligibility: GovernanceRoutingPolicyAutopromotionEligibilityRow[];
      rollbackCandidates: GovernanceRoutingPolicyAutopromotionRollbackCandidateRow[];
    };
    if (data.ok) {
      setAutopromotionPolicies(data.policies ?? []);
      setAutopromotionSummary(data.summary ?? []);
      setAutopromotionEligibility(data.eligibility ?? []);
      setAutopromotionRollbacks(data.rollbackCandidates ?? []);
    }
  }, [workspaceSlug]);

  const loadGovernanceRoutingPolicyRollbacks = useCallback(async () => {
    const res = await fetch(`/api/governance/routing-policy-rollbacks?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      pendingRollbacks: GovernanceRoutingPolicyPendingRollbackRow[];
      reviewSummary: GovernanceRoutingPolicyRollbackReviewSummaryRow[];
      executionSummary: GovernanceRoutingPolicyRollbackExecutionSummaryRow[];
    };
    if (data.ok) {
      setRollbackPending(data.pendingRollbacks ?? []);
      setRollbackReviewSummary(data.reviewSummary ?? []);
      setRollbackExecutionSummary(data.executionSummary ?? []);
    }
  }, [workspaceSlug]);

  const loadGovernanceRoutingPolicyRollbackImpact = useCallback(async () => {
    const res = await fetch(`/api/metrics/governance-routing-policy-rollback-impact?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      impactRows: GovernanceRoutingPolicyRollbackImpactRow[];
      effectivenessSummary: GovernanceRoutingPolicyRollbackEffectivenessSummaryRow | null;
      pendingEvaluations: GovernanceRoutingPolicyRollbackPendingEvaluationRow[];
    };
    if (data.ok) {
      setRollbackImpactRows(data.impactRows ?? []);
      setRollbackEffectiveness(data.effectivenessSummary ?? null);
      setRollbackPendingEvals(data.pendingEvaluations ?? []);
    }
  }, [workspaceSlug]);

  const loadGovernancePolicyOptimization = useCallback(async () => {
    const res = await fetch(`/api/metrics/governance-policy-optimization?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      snapshot: GovernancePolicyOptimizationSnapshotRow | null;
      featureEffectiveness: GovernancePolicyFeatureEffectivenessRow[];
      contextFit: GovernancePolicyContextFitRow[];
      policyOpportunities: GovernancePolicyOpportunityRow[];
    };
    if (data.ok) {
      setPolicyOptimizationSnapshot(data.snapshot ?? null);
      setPolicyFeatureEffectiveness(data.featureEffectiveness ?? []);
      setPolicyContextFit(data.contextFit ?? []);
      setPolicyOpportunities(data.policyOpportunities ?? []);
    }
  }, [workspaceSlug]);

  const loadGovernancePolicyReviews = useCallback(async () => {
    const res = await fetch(`/api/governance/policy-reviews?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      reviewSummary: GovernancePolicyReviewSummaryRow[];
    };
    if (data.ok) {
      setPolicyReviewSummary(data.reviewSummary ?? []);
    }
  }, [workspaceSlug]);

  const loadGovernancePolicyPromotions = useCallback(async () => {
    const res = await fetch(`/api/governance/policy-promotions?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      promotionSummary: GovernancePolicyPromotionSummaryRow[];
      pendingPromotions: GovernancePolicyPendingPromotionRow[];
    };
    if (data.ok) {
      setPolicyPromotionSummary(data.promotionSummary ?? []);
      setPolicyPendingPromotions(data.pendingPromotions ?? []);
    }
  }, [workspaceSlug]);

  const loadGovernancePolicyAutopromotions = useCallback(async () => {
    const res = await fetch(`/api/governance/policy-autopromotions?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      autopromotionSummary: GovernancePolicyAutopromotionSummaryRow[];
      eligibility: GovernancePolicyAutopromotionEligibilityRow[];
      rollbackCandidates: GovernancePolicyAutopromotionRollbackCandidateRow[];
    };
    if (data.ok) {
      setPolicyAutopromotionSummary(data.autopromotionSummary ?? []);
      setPolicyAutopromotionEligibility(data.eligibility ?? []);
      setPolicyAutopromotionRollbacks(data.rollbackCandidates ?? []);
    }
  }, [workspaceSlug]);

  const loadMultiAssetFoundation = useCallback(async () => {
    const res = await fetch(`/api/metrics/multi-asset-foundation?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      syncHealth: MultiAssetSyncHealthRow[];
      marketStateSample: NormalizedMultiAssetMarketStateRow[];
      familySummary: MultiAssetFamilyStateSummaryRow[];
    };
    if (data.ok) {
      setMultiAssetSyncHealth(data.syncHealth ?? []);
      setMultiAssetMarketState(data.marketStateSample ?? []);
      setMultiAssetFamilySummary(data.familySummary ?? []);
    }
  }, [workspaceSlug]);

  const loadDependencyContext = useCallback(async () => {
    const res = await fetch(`/api/metrics/dependency-context?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      latestContexts: WatchlistContextSnapshotRow[];
      coverageSummary: WatchlistDependencyCoverageSummaryRow[];
      contextDetail: WatchlistDependencyContextDetailRow[];
      familyState: WatchlistDependencyFamilyStateRow[];
    };
    if (data.ok) {
      setDependencyContexts(data.latestContexts ?? []);
      setDependencyCoverage(data.coverageSummary ?? []);
      setDependencyDetail(data.contextDetail ?? []);
      setDependencyFamily(data.familyState ?? []);
    }
  }, [workspaceSlug]);

  const loadCrossAssetSignals = useCallback(async () => {
    const res = await fetch(`/api/metrics/cross-asset-signals?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      signalSummary: CrossAssetSignalSummaryRow[];
      dependencyHealth: CrossAssetDependencyHealthRow[];
      runContextSummary: RunCrossAssetContextSummaryRow[];
    };
    if (data.ok) {
      setCrossAssetSignals(data.signalSummary ?? []);
      setCrossAssetDependencyHealth(data.dependencyHealth ?? []);
      setCrossAssetRunSummary(data.runContextSummary ?? []);
    }
  }, [workspaceSlug]);

  const loadCrossAssetExplainability = useCallback(async () => {
    const res = await fetch(`/api/metrics/cross-asset-explainability?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      explanationSummary: CrossAssetExplanationSummaryRow[];
      familySummary: CrossAssetFamilyExplanationSummaryRow[];
      runBridgeSummary: RunCrossAssetExplanationBridgeRow[];
    };
    if (data.ok) {
      setCrossAssetExplanation(data.explanationSummary ?? []);
      setCrossAssetFamilyExplanation(data.familySummary ?? []);
      setCrossAssetRunBridge(data.runBridgeSummary ?? []);
    }
  }, [workspaceSlug]);

  const loadCrossAssetAttribution = useCallback(async () => {
    const res = await fetch(`/api/metrics/cross-asset-attribution?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      attributionSummary: CrossAssetAttributionSummaryRow[];
      familyAttributionSummary: CrossAssetFamilyAttributionSummaryRow[];
      runIntegrationSummary: RunCompositeIntegrationSummaryRow[];
    };
    if (data.ok) {
      setCrossAssetAttribution(data.attributionSummary ?? []);
      setCrossAssetFamilyAttribution(data.familyAttributionSummary ?? []);
      setCrossAssetRunIntegration(data.runIntegrationSummary ?? []);
    }
  }, [workspaceSlug]);

  const loadDependencyPriorityWeighting = useCallback(async () => {
    const res = await fetch(`/api/metrics/dependency-priority-weighting?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      familyWeightedSummary: CrossAssetFamilyWeightedAttributionSummaryRow[];
      symbolWeightedSummary: CrossAssetSymbolWeightedAttributionSummaryRow[];
      runWeightedSummary: RunCrossAssetWeightedIntegrationSummaryRow[];
    };
    if (data.ok) {
      setWeightedFamilyAttribution(data.familyWeightedSummary ?? []);
      setWeightedSymbolAttribution(data.symbolWeightedSummary ?? []);
      setWeightedRunIntegration(data.runWeightedSummary ?? []);
    }
  }, [workspaceSlug]);

  const loadRegimeAwareCrossAsset = useCallback(async () => {
    const res = await fetch(`/api/metrics/regime-aware-cross-asset?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      familyRegimeSummary: CrossAssetFamilyRegimeAttributionSummaryRow[];
      symbolRegimeSummary: CrossAssetSymbolRegimeAttributionSummaryRow[];
      runRegimeSummary: RunCrossAssetRegimeIntegrationSummaryRow[];
    };
    if (data.ok) {
      setRegimeFamilyAttribution(data.familyRegimeSummary ?? []);
      setRegimeSymbolAttribution(data.symbolRegimeSummary ?? []);
      setRegimeRunIntegration(data.runRegimeSummary ?? []);
    }
  }, [workspaceSlug]);

  const loadCrossAssetReplayValidation = useCallback(async () => {
    const res = await fetch(`/api/metrics/cross-asset-replay-validation?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      replayValidationSummary: CrossAssetReplayValidationSummaryRow[];
      familyReplayStabilitySummary: CrossAssetFamilyReplayStabilitySummaryRow[];
      replayStabilityAggregate: CrossAssetReplayStabilityAggregateRow | null;
    };
    if (data.ok) {
      setReplayValidation(data.replayValidationSummary ?? []);
      setReplayFamilyStability(data.familyReplayStabilitySummary ?? []);
      setReplayStabilityAggregate(data.replayStabilityAggregate ?? null);
    }
  }, [workspaceSlug]);

  const loadCrossAssetTiming = useCallback(async () => {
    const res = await fetch(`/api/metrics/cross-asset-timing?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      pairSummary: CrossAssetLeadLagPairSummaryRow[];
      familyTimingSummary: CrossAssetFamilyTimingSummaryRow[];
      runTimingSummary: RunCrossAssetTimingSummaryRow[];
    };
    if (data.ok) {
      setTimingPair(data.pairSummary ?? []);
      setTimingFamily(data.familyTimingSummary ?? []);
      setTimingRun(data.runTimingSummary ?? []);
    }
  }, [workspaceSlug]);

  const loadCrossAssetTimingAttribution = useCallback(async () => {
    const res = await fetch(`/api/metrics/cross-asset-timing-attribution?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      familyTimingAttributionSummary: CrossAssetFamilyTimingAttributionSummaryRow[];
      symbolTimingAttributionSummary: CrossAssetSymbolTimingAttributionSummaryRow[];
      runTimingAttributionSummary: RunCrossAssetTimingAttributionSummaryRow[];
    };
    if (data.ok) {
      setTimingFamilyAttribution(data.familyTimingAttributionSummary ?? []);
      setTimingSymbolAttribution(data.symbolTimingAttributionSummary ?? []);
      setTimingRunAttribution(data.runTimingAttributionSummary ?? []);
    }
  }, [workspaceSlug]);

  const loadCrossAssetTimingComposite = useCallback(async () => {
    const res = await fetch(`/api/metrics/cross-asset-timing-composite?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      timingCompositeSummary: CrossAssetTimingCompositeSummaryRow[];
      familyTimingCompositeSummary: CrossAssetFamilyTimingCompositeSummaryRow[];
      finalIntegrationSummary: RunCrossAssetFinalIntegrationSummaryRow[];
    };
    if (data.ok) {
      setTimingComposite(data.timingCompositeSummary ?? []);
      setTimingFamilyComposite(data.familyTimingCompositeSummary ?? []);
      setFinalIntegrationSummary(data.finalIntegrationSummary ?? []);
    }
  }, [workspaceSlug]);

  const loadCrossAssetTimingReplayValidation = useCallback(async () => {
    const res = await fetch(`/api/metrics/cross-asset-timing-replay-validation?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      timingReplayValidationSummary: CrossAssetTimingReplayValidationSummaryRow[];
      familyTimingReplayStabilitySummary: CrossAssetFamilyTimingReplayStabilitySummaryRow[];
      timingReplayStabilityAggregate: CrossAssetTimingReplayStabilityAggregateRow | null;
    };
    if (data.ok) {
      setTimingReplayValidation(data.timingReplayValidationSummary ?? []);
      setTimingReplayFamilyStability(data.familyTimingReplayStabilitySummary ?? []);
      setTimingReplayStabilityAggregate(data.timingReplayStabilityAggregate ?? null);
    }
  }, [workspaceSlug]);

  const loadCrossAssetTransitionDiagnostics = useCallback(async () => {
    const res = await fetch(`/api/metrics/cross-asset-transition-diagnostics?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      transitionStateSummary: CrossAssetFamilyTransitionStateSummaryRow[];
      transitionEventSummary: CrossAssetFamilyTransitionEventSummaryRow[];
      sequenceSummary: CrossAssetFamilySequenceSummaryRow[];
      runTransitionSummary: RunCrossAssetTransitionDiagnosticsSummaryRow[];
    };
    if (data.ok) {
      setTransitionStateSummary(data.transitionStateSummary ?? []);
      setTransitionEventSummary(data.transitionEventSummary ?? []);
      setSequenceSummary(data.sequenceSummary ?? []);
      setRunTransitionSummary(data.runTransitionSummary ?? []);
    }
  }, [workspaceSlug]);

  const loadCrossAssetTransitionAttribution = useCallback(async () => {
    const res = await fetch(`/api/metrics/cross-asset-transition-attribution?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      familyTransitionAttributionSummary: CrossAssetFamilyTransitionAttributionSummaryRow[];
      symbolTransitionAttributionSummary: CrossAssetSymbolTransitionAttributionSummaryRow[];
      runTransitionAttributionSummary: RunCrossAssetTransitionAttributionSummaryRow[];
    };
    if (data.ok) {
      setFamilyTransitionAttribution(data.familyTransitionAttributionSummary ?? []);
      setSymbolTransitionAttribution(data.symbolTransitionAttributionSummary ?? []);
      setRunTransitionAttribution(data.runTransitionAttributionSummary ?? []);
    }
  }, [workspaceSlug]);

  const loadCrossAssetTransitionComposite = useCallback(async () => {
    const res = await fetch(`/api/metrics/cross-asset-transition-composite?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      transitionCompositeSummary: CrossAssetTransitionCompositeSummaryRow[];
      familyTransitionCompositeSummary: CrossAssetFamilyTransitionCompositeSummaryRow[];
      finalSequencingIntegrationSummary: RunCrossAssetSequencingIntegrationSummaryRow[];
    };
    if (data.ok) {
      setTransitionComposite(data.transitionCompositeSummary ?? []);
      setFamilyTransitionComposite(data.familyTransitionCompositeSummary ?? []);
      setSequencingIntegration(data.finalSequencingIntegrationSummary ?? []);
    }
  }, [workspaceSlug]);

  const loadCrossAssetTransitionReplayValidation = useCallback(async () => {
    const res = await fetch(`/api/metrics/cross-asset-transition-replay-validation?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      transitionReplayValidationSummary: CrossAssetTransitionReplayValidationSummaryRow[];
      familyTransitionReplayStabilitySummary: CrossAssetFamilyTransitionReplayStabilitySummaryRow[];
      transitionReplayStabilityAggregate: CrossAssetTransitionReplayStabilityAggregateRow | null;
    };
    if (data.ok) {
      setTransitionReplayValidation(data.transitionReplayValidationSummary ?? []);
      setFamilyTransitionReplayStability(data.familyTransitionReplayStabilitySummary ?? []);
      setTransitionReplayStabilityAggregate(data.transitionReplayStabilityAggregate ?? null);
    }
  }, [workspaceSlug]);

  const loadCrossAssetPatterns = useCallback(async () => {
    const res = await fetch(`/api/metrics/cross-asset-patterns?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      familyArchetypeSummary: CrossAssetFamilyArchetypeSummaryRow[];
      runArchetypeSummary: CrossAssetRunArchetypeSummaryRow[];
      regimeArchetypeSummary: CrossAssetRegimeArchetypeSummaryRow[];
      runPatternSummary: RunCrossAssetPatternSummaryRow[];
    };
    if (data.ok) {
      setFamilyArchetypeSummary(data.familyArchetypeSummary ?? []);
      setRunArchetypeSummary(data.runArchetypeSummary ?? []);
      setRegimeArchetypeSummary(data.regimeArchetypeSummary ?? []);
      setRunPatternSummary(data.runPatternSummary ?? []);
    }
  }, [workspaceSlug]);

  const loadCrossAssetArchetypeAttribution = useCallback(async () => {
    const res = await fetch(`/api/metrics/cross-asset-archetype-attribution?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      familyArchetypeAttributionSummary: CrossAssetFamilyArchetypeAttributionSummaryRow[];
      symbolArchetypeAttributionSummary: CrossAssetSymbolArchetypeAttributionSummaryRow[];
      runArchetypeAttributionSummary: RunCrossAssetArchetypeAttributionSummaryRow[];
    };
    if (data.ok) {
      setFamilyArchetypeAttribution(data.familyArchetypeAttributionSummary ?? []);
      setSymbolArchetypeAttribution(data.symbolArchetypeAttributionSummary ?? []);
      setRunArchetypeAttribution(data.runArchetypeAttributionSummary ?? []);
    }
  }, [workspaceSlug]);

  const loadCrossAssetArchetypeComposite = useCallback(async () => {
    const res = await fetch(`/api/metrics/cross-asset-archetype-composite?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      archetypeCompositeSummary: CrossAssetArchetypeCompositeSummaryRow[];
      familyArchetypeCompositeSummary: CrossAssetFamilyArchetypeCompositeSummaryRow[];
      finalArchetypeIntegrationSummary: RunCrossAssetArchetypeIntegrationSummaryRow[];
    };
    if (data.ok) {
      setArchetypeComposite(data.archetypeCompositeSummary ?? []);
      setFamilyArchetypeComposite(data.familyArchetypeCompositeSummary ?? []);
      setArchetypeIntegration(data.finalArchetypeIntegrationSummary ?? []);
    }
  }, [workspaceSlug]);

  const loadCrossAssetArchetypeReplayValidation = useCallback(async () => {
    const res = await fetch(`/api/metrics/cross-asset-archetype-replay-validation?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      archetypeReplayValidationSummary: CrossAssetArchetypeReplayValidationSummaryRow[];
      familyArchetypeReplayStabilitySummary: CrossAssetFamilyArchetypeReplayStabilitySummaryRow[];
      archetypeReplayStabilityAggregate: CrossAssetArchetypeReplayStabilityAggregateRow | null;
    };
    if (data.ok) {
      setArchetypeReplayValidation(data.archetypeReplayValidationSummary ?? []);
      setFamilyArchetypeReplayStability(data.familyArchetypeReplayStabilitySummary ?? []);
      setArchetypeReplayStabilityAggregate(data.archetypeReplayStabilityAggregate ?? null);
    }
  }, [workspaceSlug]);

  const loadCrossAssetPatternClusters = useCallback(async () => {
    const res = await fetch(`/api/metrics/cross-asset-pattern-clusters?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      clusterSummary: CrossAssetArchetypeClusterSummaryRow[];
      regimeRotationSummary: CrossAssetArchetypeRegimeRotationSummaryRow[];
      driftEventSummary: CrossAssetPatternDriftEventSummaryRow[];
      runPatternClusterSummary: RunCrossAssetPatternClusterSummaryRow[];
    };
    if (data.ok) {
      setPatternClusterSummary(data.clusterSummary ?? []);
      setPatternRegimeRotation(data.regimeRotationSummary ?? []);
      setPatternDriftEvents(data.driftEventSummary ?? []);
      setRunPatternCluster(data.runPatternClusterSummary ?? []);
    }
  }, [workspaceSlug]);

  const loadCrossAssetClusterAttribution = useCallback(async () => {
    const res = await fetch(`/api/metrics/cross-asset-cluster-attribution?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      familyClusterAttributionSummary: CrossAssetFamilyClusterAttributionSummaryRow[];
      symbolClusterAttributionSummary: CrossAssetSymbolClusterAttributionSummaryRow[];
      runClusterAttributionSummary: RunCrossAssetClusterAttributionSummaryRow[];
    };
    if (data.ok) {
      setFamilyClusterAttribution(data.familyClusterAttributionSummary ?? []);
      setSymbolClusterAttribution(data.symbolClusterAttributionSummary ?? []);
      setRunClusterAttribution(data.runClusterAttributionSummary ?? []);
    }
  }, [workspaceSlug]);

  const loadCrossAssetClusterComposite = useCallback(async () => {
    const res = await fetch(`/api/metrics/cross-asset-cluster-composite?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      clusterCompositeSummary: CrossAssetClusterCompositeSummaryRow[];
      familyClusterCompositeSummary: CrossAssetFamilyClusterCompositeSummaryRow[];
      finalClusterIntegrationSummary: RunCrossAssetClusterIntegrationSummaryRow[];
    };
    if (data.ok) {
      setClusterComposite(data.clusterCompositeSummary ?? []);
      setFamilyClusterComposite(data.familyClusterCompositeSummary ?? []);
      setClusterIntegration(data.finalClusterIntegrationSummary ?? []);
    }
  }, [workspaceSlug]);

  const loadCrossAssetClusterReplayValidation = useCallback(async () => {
    const res = await fetch(`/api/metrics/cross-asset-cluster-replay-validation?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      clusterReplayValidationSummary: CrossAssetClusterReplayValidationSummaryRow[];
      familyClusterReplayStabilitySummary: CrossAssetFamilyClusterReplayStabilitySummaryRow[];
      clusterReplayStabilityAggregate: CrossAssetClusterReplayStabilityAggregateRow | null;
    };
    if (data.ok) {
      setClusterReplayValidation(data.clusterReplayValidationSummary ?? []);
      setFamilyClusterReplayStability(data.familyClusterReplayStabilitySummary ?? []);
      setClusterReplayStabilityAggregate(data.clusterReplayStabilityAggregate ?? null);
    }
  }, [workspaceSlug]);

  const loadCrossAssetPersistence = useCallback(async () => {
    const res = await fetch(`/api/metrics/cross-asset-persistence?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      statePersistenceSummary: CrossAssetStatePersistenceSummaryRow[];
      regimeMemorySummary: CrossAssetRegimeMemorySummaryRow[];
      persistenceEventSummary: CrossAssetPersistenceTransitionEventSummaryRow[];
      runPersistenceSummary: RunCrossAssetPersistenceSummaryRow[];
    };
    if (data.ok) {
      setStatePersistenceSummary(data.statePersistenceSummary ?? []);
      setRegimeMemorySummary(data.regimeMemorySummary ?? []);
      setPersistenceEventSummary(data.persistenceEventSummary ?? []);
      setRunPersistenceSummary(data.runPersistenceSummary ?? []);
    }
  }, [workspaceSlug]);

  const loadCrossAssetPersistenceAttribution = useCallback(async () => {
    const res = await fetch(`/api/metrics/cross-asset-persistence-attribution?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      familyPersistenceAttributionSummary: CrossAssetFamilyPersistenceAttributionSummaryRow[];
      symbolPersistenceAttributionSummary: CrossAssetSymbolPersistenceAttributionSummaryRow[];
      runPersistenceAttributionSummary: RunCrossAssetPersistenceAttributionSummaryRow[];
    };
    if (data.ok) {
      setFamilyPersistenceAttribution(data.familyPersistenceAttributionSummary ?? []);
      setSymbolPersistenceAttribution(data.symbolPersistenceAttributionSummary ?? []);
      setRunPersistenceAttribution(data.runPersistenceAttributionSummary ?? []);
    }
  }, [workspaceSlug]);

  const loadCrossAssetPersistenceComposite = useCallback(async () => {
    const res = await fetch(`/api/metrics/cross-asset-persistence-composite?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      persistenceCompositeSummary: CrossAssetPersistenceCompositeSummaryRow[];
      familyPersistenceCompositeSummary: CrossAssetFamilyPersistenceCompositeSummaryRow[];
      finalPersistenceIntegrationSummary: RunCrossAssetPersistenceIntegrationSummaryRow[];
    };
    if (data.ok) {
      setPersistenceComposite(data.persistenceCompositeSummary ?? []);
      setFamilyPersistenceComposite(data.familyPersistenceCompositeSummary ?? []);
      setFinalPersistenceIntegrationSummary(data.finalPersistenceIntegrationSummary ?? []);
    }
  }, [workspaceSlug]);

  const loadCrossAssetPersistenceReplayValidation = useCallback(async () => {
    const res = await fetch(`/api/metrics/cross-asset-persistence-replay-validation?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      persistenceReplayValidationSummary: CrossAssetPersistenceReplayValidationSummaryRow[];
      familyPersistenceReplayStabilitySummary: CrossAssetFamilyPersistenceReplayStabilitySummaryRow[];
      persistenceReplayStabilityAggregate: CrossAssetPersistenceReplayStabilityAggregateRow | null;
    };
    if (data.ok) {
      setPersistenceReplayValidation(data.persistenceReplayValidationSummary ?? []);
      setFamilyPersistenceReplayStability(data.familyPersistenceReplayStabilitySummary ?? []);
      setPersistenceReplayStabilityAggregate(data.persistenceReplayStabilityAggregate ?? null);
    }
  }, [workspaceSlug]);

  const loadCrossAssetSignalDecay = useCallback(async () => {
    const res = await fetch(`/api/metrics/cross-asset-signal-decay?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      signalDecaySummary: CrossAssetSignalDecaySummaryRow[];
      familySignalDecaySummary: CrossAssetFamilySignalDecaySummaryRow[];
      staleMemoryEventSummary: CrossAssetStaleMemoryEventSummaryRow[];
      runSignalDecaySummary: RunCrossAssetSignalDecaySummaryRow[];
    };
    if (data.ok) {
      setSignalDecaySummary(data.signalDecaySummary ?? []);
      setFamilySignalDecaySummary(data.familySignalDecaySummary ?? []);
      setStaleMemoryEventSummary(data.staleMemoryEventSummary ?? []);
      setRunSignalDecaySummary(data.runSignalDecaySummary ?? []);
    }
  }, [workspaceSlug]);

  const loadCrossAssetDecayAttribution = useCallback(async () => {
    const res = await fetch(`/api/metrics/cross-asset-decay-attribution?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      familyDecayAttributionSummary: CrossAssetFamilyDecayAttributionSummaryRow[];
      symbolDecayAttributionSummary: CrossAssetSymbolDecayAttributionSummaryRow[];
      runDecayAttributionSummary: RunCrossAssetDecayAttributionSummaryRow[];
    };
    if (data.ok) {
      setFamilyDecayAttributionSummary(data.familyDecayAttributionSummary ?? []);
      setSymbolDecayAttributionSummary(data.symbolDecayAttributionSummary ?? []);
      setRunDecayAttributionSummary(data.runDecayAttributionSummary ?? []);
    }
  }, [workspaceSlug]);

  const loadCrossAssetDecayComposite = useCallback(async () => {
    const res = await fetch(`/api/metrics/cross-asset-decay-composite?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      decayCompositeSummary: CrossAssetDecayCompositeSummaryRow[];
      familyDecayCompositeSummary: CrossAssetFamilyDecayCompositeSummaryRow[];
      finalDecayIntegrationSummary: RunCrossAssetDecayIntegrationSummaryRow[];
    };
    if (data.ok) {
      setDecayCompositeSummary(data.decayCompositeSummary ?? []);
      setFamilyDecayCompositeSummary(data.familyDecayCompositeSummary ?? []);
      setFinalDecayIntegrationSummary(data.finalDecayIntegrationSummary ?? []);
    }
  }, [workspaceSlug]);

  const loadCrossAssetDecayReplayValidation = useCallback(async () => {
    const res = await fetch(`/api/metrics/cross-asset-decay-replay-validation?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      decayReplayValidationSummary: CrossAssetDecayReplayValidationSummaryRow[];
      familyDecayReplayStabilitySummary: CrossAssetFamilyDecayReplayStabilitySummaryRow[];
      decayReplayStabilityAggregate: CrossAssetDecayReplayStabilityAggregateRow | null;
    };
    if (data.ok) {
      setDecayReplayValidationSummary(data.decayReplayValidationSummary ?? []);
      setFamilyDecayReplayStabilitySummary(data.familyDecayReplayStabilitySummary ?? []);
      setDecayReplayStabilityAggregate(data.decayReplayStabilityAggregate ?? null);
    }
  }, [workspaceSlug]);

  const loadCrossAssetLayerConflict = useCallback(async () => {
    const res = await fetch(`/api/metrics/cross-asset-layer-conflict?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      layerAgreementSummary: CrossAssetLayerAgreementSummaryRow[];
      familyLayerAgreementSummary: CrossAssetFamilyLayerAgreementSummaryRow[];
      layerConflictEventSummary: CrossAssetLayerConflictEventSummaryRow[];
      runLayerConflictSummary: RunCrossAssetLayerConflictSummaryRow[];
    };
    if (data.ok) {
      setLayerAgreementSummary(data.layerAgreementSummary ?? []);
      setFamilyLayerAgreementSummary(data.familyLayerAgreementSummary ?? []);
      setLayerConflictEventSummary(data.layerConflictEventSummary ?? []);
      setRunLayerConflictSummary(data.runLayerConflictSummary ?? []);
    }
  }, [workspaceSlug]);

  const loadCrossAssetConflictAttribution = useCallback(async () => {
    const res = await fetch(`/api/metrics/cross-asset-conflict-attribution?workspace=${workspaceSlug}`, { cache: "no-store" });
    const data = await res.json() as {
      ok: boolean;
      familyConflictAttributionSummary: CrossAssetFamilyConflictAttributionSummaryRow[];
      symbolConflictAttributionSummary: CrossAssetSymbolConflictAttributionSummaryRow[];
      runConflictAttributionSummary: RunCrossAssetConflictAttributionSummaryRow[];
    };
    if (data.ok) {
      setFamilyConflictAttributionSummary(data.familyConflictAttributionSummary ?? []);
      setSymbolConflictAttributionSummary(data.symbolConflictAttributionSummary ?? []);
      setRunConflictAttributionSummary(data.runConflictAttributionSummary ?? []);
    }
  }, [workspaceSlug]);

  const loadAll = useCallback(async () => {
    setLoading(true);
    await Promise.all([
      loadDeadLetters(),
      loadWorkers(),
      loadMetrics(),
      loadJobs(),
      loadSla(),
      loadStability(),
      loadVersionGovernance(),
      loadGovernanceAlerts(),
      loadAnomalyClusters(),
      loadGovernanceDegradation(),
      loadGovernanceLifecycle(),
      loadGovernanceCases(),
        loadGovernanceRouting(),
        loadGovernanceRoutingQuality(),
        loadGovernanceRoutingEffectiveness(),
        loadGovernanceRoutingRecommendations(),
        loadGovernanceIncidentAnalytics(),
        loadGovernanceOperatorPerformance(),
        loadGovernanceManagerOverview(),
        loadGovernanceRoutingOptimization(),
        loadGovernanceRoutingPolicyReview(),
        loadGovernanceRoutingPolicyPromotion(),
        loadGovernanceRoutingPolicyAutopromotion(),
        loadGovernanceRoutingPolicyRollbacks(),
        loadGovernanceRoutingPolicyRollbackImpact(),
        loadGovernancePolicyOptimization(),
        loadGovernancePolicyReviews(),
        loadGovernancePolicyPromotions(),
        loadGovernancePolicyAutopromotions(),
        loadMultiAssetFoundation(),
        loadDependencyContext(),
        loadCrossAssetSignals(),
        loadCrossAssetExplainability(),
        loadCrossAssetAttribution(),
        loadDependencyPriorityWeighting(),
        loadRegimeAwareCrossAsset(),
        loadCrossAssetReplayValidation(),
        loadCrossAssetTiming(),
        loadCrossAssetTimingAttribution(),
        loadCrossAssetTimingComposite(),
        loadCrossAssetTimingReplayValidation(),
        loadCrossAssetTransitionDiagnostics(), loadCrossAssetTransitionAttribution(), loadCrossAssetTransitionComposite(), loadCrossAssetTransitionReplayValidation(), loadCrossAssetPatterns(), loadCrossAssetArchetypeAttribution(), loadCrossAssetArchetypeComposite(), loadCrossAssetArchetypeReplayValidation(), loadCrossAssetPatternClusters(), loadCrossAssetClusterAttribution(), loadCrossAssetClusterComposite(), loadCrossAssetClusterReplayValidation(), loadCrossAssetPersistence(), loadCrossAssetPersistenceAttribution(), loadCrossAssetPersistenceComposite(), loadCrossAssetPersistenceReplayValidation(), loadCrossAssetSignalDecay(), loadCrossAssetDecayAttribution(), loadCrossAssetDecayComposite(), loadCrossAssetDecayReplayValidation(), loadCrossAssetLayerConflict(), loadCrossAssetConflictAttribution(),
        loadGovernanceThresholdLearning(),
      loadGovernanceThresholdLearningReview(),
      loadGovernanceWorkload(),
      loadGovernanceEscalations(),
      loadRegimeThresholds(),
      loadGovernance(),
    ]);
    setLoading(false);
  }, [loadDeadLetters, loadWorkers, loadMetrics, loadJobs, loadSla, loadStability, loadVersionGovernance, loadGovernanceAlerts, loadAnomalyClusters, loadGovernanceDegradation, loadGovernanceLifecycle, loadGovernanceCases, loadGovernanceRouting, loadGovernanceRoutingQuality, loadGovernanceRoutingEffectiveness, loadGovernanceRoutingRecommendations, loadGovernanceIncidentAnalytics, loadGovernanceOperatorPerformance, loadGovernanceManagerOverview, loadGovernanceRoutingOptimization, loadGovernanceRoutingPolicyReview, loadGovernanceRoutingPolicyPromotion, loadGovernanceRoutingPolicyAutopromotion, loadGovernanceRoutingPolicyRollbacks, loadGovernanceRoutingPolicyRollbackImpact, loadGovernancePolicyOptimization, loadGovernancePolicyReviews, loadGovernancePolicyPromotions, loadGovernancePolicyAutopromotions, loadMultiAssetFoundation, loadDependencyContext, loadCrossAssetSignals, loadCrossAssetExplainability, loadCrossAssetAttribution, loadDependencyPriorityWeighting, loadRegimeAwareCrossAsset, loadCrossAssetReplayValidation, loadCrossAssetTiming, loadCrossAssetTimingAttribution, loadCrossAssetTimingComposite, loadCrossAssetTimingReplayValidation, loadCrossAssetTransitionDiagnostics, loadCrossAssetTransitionAttribution, loadCrossAssetTransitionComposite, loadCrossAssetTransitionReplayValidation, loadCrossAssetPatterns, loadCrossAssetArchetypeAttribution, loadCrossAssetArchetypeComposite, loadCrossAssetArchetypeReplayValidation, loadCrossAssetPatternClusters, loadCrossAssetClusterAttribution, loadCrossAssetClusterComposite, loadCrossAssetClusterReplayValidation, loadCrossAssetPersistence, loadCrossAssetPersistenceAttribution, loadCrossAssetPersistenceComposite, loadCrossAssetPersistenceReplayValidation, loadCrossAssetSignalDecay, loadCrossAssetDecayAttribution, loadCrossAssetDecayComposite, loadCrossAssetDecayReplayValidation, loadCrossAssetLayerConflict, loadCrossAssetConflictAttribution, loadGovernanceThresholdLearning, loadGovernanceThresholdLearningReview, loadGovernanceWorkload, loadGovernanceEscalations, loadRegimeThresholds, loadGovernance]);

  useEffect(() => {
    void loadAll();
  }, [loadAll]);

  useEffect(() => {
    if (!selectedRunId) {
      setRunInspection(null);
      setRunAttribution(null);
      setRunDrift(null);
      setRunRegimeTransition(null);
      setRunReplayDelta(null);
      setRunExplanation(null);
      setRunStageTimings([]);
      setRunInputSnapshot(null);
      setRunScope(null);
      setRunComparison(null);
      return;
    }
    void Promise.all([
      loadRunInspection(selectedRunId),
      loadRunAttribution(selectedRunId),
      loadRunDrift(selectedRunId),
      loadRunRegimeTransition(selectedRunId),
      loadRunReplayDelta(selectedRunId),
      loadRunExplanation(selectedRunId),
      loadRunForensics(selectedRunId),
      loadRunScope(selectedRunId),
      loadRunComparison(selectedRunId),
    ]);
  }, [loadRunAttribution, loadRunComparison, loadRunDrift, loadRunExplanation, loadRunForensics, loadRunInspection, loadRunRegimeTransition, loadRunReplayDelta, loadRunScope, selectedRunId]);

  useEffect(() => {
    if (!selectedCaseId) {
      setSelectedIncident(null);
      return;
    }
    void loadIncidentDetail(selectedCaseId);
  }, [loadIncidentDetail, selectedCaseId]);

  useOpsRealtime({
    onDeadLetter: () => {
      void loadDeadLetters();
      void loadMetrics();
    },
    onWorkerChange: () => {
      void loadWorkers();
    },
    onQueueChange: () => {
        void Promise.all([loadMetrics(), loadJobs(), loadSla(), loadStability(), loadVersionGovernance(), loadGovernanceAlerts(), loadAnomalyClusters(), loadGovernanceDegradation(), loadGovernanceLifecycle(), loadGovernanceCases(), loadGovernanceRouting(), loadGovernanceRoutingQuality(), loadGovernanceRoutingEffectiveness(), loadGovernanceRoutingRecommendations(), loadGovernanceIncidentAnalytics(), loadGovernanceOperatorPerformance(), loadGovernanceManagerOverview(), loadGovernanceRoutingOptimization(), loadGovernanceRoutingPolicyReview(), loadGovernanceRoutingPolicyPromotion(), loadGovernanceRoutingPolicyAutopromotion(), loadGovernanceRoutingPolicyRollbacks(), loadGovernanceRoutingPolicyRollbackImpact(), loadGovernancePolicyOptimization(), loadGovernancePolicyReviews(), loadGovernancePolicyPromotions(), loadGovernancePolicyAutopromotions(), loadMultiAssetFoundation(), loadDependencyContext(), loadCrossAssetSignals(), loadCrossAssetExplainability(), loadCrossAssetAttribution(), loadDependencyPriorityWeighting(), loadRegimeAwareCrossAsset(), loadCrossAssetReplayValidation(), loadCrossAssetTiming(), loadCrossAssetTimingAttribution(), loadCrossAssetTimingComposite(), loadCrossAssetTimingReplayValidation(), loadCrossAssetTransitionDiagnostics(), loadCrossAssetTransitionAttribution(), loadCrossAssetTransitionComposite(), loadCrossAssetTransitionReplayValidation(), loadCrossAssetPatterns(), loadCrossAssetArchetypeAttribution(), loadCrossAssetArchetypeComposite(), loadCrossAssetArchetypeReplayValidation(), loadCrossAssetPatternClusters(), loadCrossAssetClusterAttribution(), loadCrossAssetClusterComposite(), loadCrossAssetClusterReplayValidation(), loadCrossAssetPersistence(), loadCrossAssetPersistenceAttribution(), loadCrossAssetPersistenceComposite(), loadCrossAssetPersistenceReplayValidation(), loadCrossAssetSignalDecay(), loadCrossAssetDecayAttribution(), loadCrossAssetDecayComposite(), loadCrossAssetDecayReplayValidation(), loadCrossAssetLayerConflict(), loadCrossAssetConflictAttribution(), loadGovernanceThresholdLearning(), loadGovernanceThresholdLearningReview(), loadGovernanceWorkload(), loadGovernanceEscalations(), loadRegimeThresholds(), loadGovernance()]);
      if (selectedRunId) void Promise.all([
        loadRunInspection(selectedRunId),
        loadRunAttribution(selectedRunId),
        loadRunDrift(selectedRunId),
        loadRunRegimeTransition(selectedRunId),
        loadRunReplayDelta(selectedRunId),
        loadRunExplanation(selectedRunId),
        loadRunForensics(selectedRunId),
        loadRunScope(selectedRunId),
        loadRunComparison(selectedRunId),
      ]);
      if (selectedCaseId) void loadIncidentDetail(selectedCaseId);
    },
    onAlertChange: () => {
        void Promise.all([loadGovernanceAlerts(), loadAnomalyClusters(), loadGovernanceDegradation(), loadGovernanceLifecycle(), loadGovernanceCases(), loadGovernanceRouting(), loadGovernanceRoutingQuality(), loadGovernanceRoutingEffectiveness(), loadGovernanceRoutingRecommendations(), loadGovernanceIncidentAnalytics(), loadGovernanceOperatorPerformance(), loadGovernanceManagerOverview(), loadGovernanceRoutingOptimization(), loadGovernanceRoutingPolicyReview(), loadGovernanceRoutingPolicyPromotion(), loadGovernanceRoutingPolicyAutopromotion(), loadGovernanceRoutingPolicyRollbacks(), loadGovernanceRoutingPolicyRollbackImpact(), loadGovernancePolicyOptimization(), loadGovernancePolicyReviews(), loadGovernancePolicyPromotions(), loadGovernancePolicyAutopromotions(), loadMultiAssetFoundation(), loadDependencyContext(), loadCrossAssetSignals(), loadCrossAssetExplainability(), loadCrossAssetAttribution(), loadDependencyPriorityWeighting(), loadRegimeAwareCrossAsset(), loadCrossAssetReplayValidation(), loadCrossAssetTiming(), loadCrossAssetTimingAttribution(), loadCrossAssetTimingComposite(), loadCrossAssetTimingReplayValidation(), loadCrossAssetTransitionDiagnostics(), loadCrossAssetTransitionAttribution(), loadCrossAssetTransitionComposite(), loadCrossAssetTransitionReplayValidation(), loadCrossAssetPatterns(), loadCrossAssetArchetypeAttribution(), loadCrossAssetArchetypeComposite(), loadCrossAssetArchetypeReplayValidation(), loadCrossAssetPatternClusters(), loadCrossAssetClusterAttribution(), loadCrossAssetClusterComposite(), loadCrossAssetClusterReplayValidation(), loadCrossAssetPersistence(), loadCrossAssetPersistenceAttribution(), loadCrossAssetPersistenceComposite(), loadCrossAssetPersistenceReplayValidation(), loadCrossAssetSignalDecay(), loadCrossAssetDecayAttribution(), loadCrossAssetDecayComposite(), loadCrossAssetDecayReplayValidation(), loadCrossAssetLayerConflict(), loadCrossAssetConflictAttribution(), loadGovernanceThresholdLearning(), loadGovernanceThresholdLearningReview(), loadGovernanceWorkload(), loadGovernanceEscalations(), loadRegimeThresholds()]);
      if (selectedRunId) void Promise.all([
        loadRunInspection(selectedRunId),
        loadRunAttribution(selectedRunId),
        loadRunDrift(selectedRunId),
        loadRunRegimeTransition(selectedRunId),
        loadRunReplayDelta(selectedRunId),
        loadRunExplanation(selectedRunId),
        loadRunForensics(selectedRunId),
        loadRunScope(selectedRunId),
        loadRunComparison(selectedRunId),
      ]);
      if (selectedCaseId) void loadIncidentDetail(selectedCaseId);
    },
  });

  async function handleRequeue(id: number, reset: boolean) {
    const res = await fetch("/api/dead-letter", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ deadLetterId: id, resetRetryCount: reset }),
    });
    const data = await res.json() as { ok: boolean };
    if (data.ok) {
      await Promise.all([loadDeadLetters(), loadMetrics()]);
    }
  }

  async function handleReplayQueued() {
    await Promise.all([loadJobs(), loadMetrics(), loadSla(), loadGovernance()]);
  }

  return (
    <main className="container">
      <div className="header">
        <h1>Ops Dashboard</h1>
        <button type="button" className="btn btn-sm" onClick={() => void loadAll()}>
          Refresh
        </button>
      </div>

      <section className="section">
        <OpsMetricsCards depth={depth} runtime={runtime} loading={loading} />
      </section>

      <section className="section ops-phase23-grid">
        <RunInspectionPanel
          jobs={jobs}
          loading={loading}
          selectedRunId={selectedRunId}
          onSelectRun={setSelectedRunId}
          run={runInspection}
          onReplayQueued={handleReplayQueued}
        />
        <SlaPanel items={slaRows} loading={loading} />
      </section>

      <section className="section">
        <StabilityPanel rows={stabilityRows} loading={loading} />
      </section>

      <section className="section">
        <VersionGovernancePanel rows={versionGovernanceRows} loading={loading} />
      </section>

      <section className="section">
        <GovernanceAlertsPanel events={governanceAlertEvents} state={governanceAlertState} loading={loading} />
      </section>

      <section className="section">
        <RegimeThresholdPanel
          profiles={thresholdProfiles}
          overrides={thresholdOverrides}
          active={activeThresholds}
          applications={thresholdApplications}
          macroSyncHealth={macroSyncHealth}
          loading={loading}
        />
      </section>

      <GovernanceThresholdLearningPanel
        performance={thresholdLearningPerformance}
        recommendations={thresholdLearningRecommendations}
        loading={loading}
      />

      <GovernanceThresholdLearningReviewPanel
        reviewSummary={thresholdReviewSummary}
        autopromotionSummary={thresholdAutopromotionSummary}
        loading={loading}
      />

      <section className="section">
        <AnomalyClusterPanel clusters={anomalyClusters} summary={anomalySummary} loading={loading} />
      </section>

      <section className="section">
        <GovernanceDegradationPanel
          activeStates={governanceDegradationActive}
          resolvedStates={governanceDegradationResolved}
          recoveries={governanceRecoveries}
          loading={loading}
        />
      </section>

      <GovernanceLifecyclePanel
        activeStates={governanceLifecycleActive}
        acknowledgedStates={governanceLifecycleAcknowledged}
        resolvedStates={governanceLifecycleResolved}
        recoveries={governanceRecoveries}
        loading={loading}
      />

      <GovernanceCasePanel
        activeCases={governanceCasesActive}
        recentCases={governanceCasesRecent}
        selectedCaseId={selectedCaseId}
        onSelectCase={setSelectedCaseId}
        loading={loading}
      />

      <GovernanceRoutingPanel
        routingDecisions={routingDecisions}
        operatorMetrics={routingOperatorMetrics}
        teamMetrics={routingTeamMetrics}
        loading={loading}
      />

      <GovernanceRoutingQualityPanel
        routingQuality={routingQuality}
        reassignmentPressure={reassignmentPressure}
        loading={loading}
      />

      <GovernanceRoutingEffectivenessPanel
        operators={routingEffectivenessOperators}
        teams={routingEffectivenessTeams}
        recommendationInputs={routingRecommendationInputs}
        loading={loading}
      />

      <GovernanceRoutingRecommendationsPanel
        recommendations={routingRecommendations}
        loading={loading}
      />

      <GovernanceRoutingRecommendationReviewPanel
        recommendations={routingRecommendations}
        loading={loading}
        onChanged={loadGovernanceRoutingRecommendations}
      />

      <GovernanceRoutingAutopromotionsPanel workspace={workspaceSlug} />

      <GovernanceRoutingOptimizationPanel
        snapshot={routingOptimizationSnapshot}
        featureEffectiveness={routingFeatureEffectiveness}
        contextFit={routingContextFit}
        policyOpportunities={routingPolicyOpportunities}
        loading={loading}
      />

      <GovernanceRoutingPolicyReviewPanel
        workspace={workspaceSlug}
        policyOpportunities={routingPolicyOpportunities}
        reviewSummary={routingPolicyReviewSummary}
        promotionSummary={routingPolicyPromotionSummary}
        applications={routingPolicyApplications}
        loading={loading}
        onChanged={() => Promise.all([loadGovernanceRoutingPolicyReview(), loadGovernanceRoutingPolicyPromotion()])}
      />

      <GovernanceRoutingPolicyAutopromotionsPanel
        workspace={workspaceSlug}
        policies={autopromotion_policies}
        summary={autopromotion_summary}
        eligibility={autopromotion_eligibility}
        rollbackCandidates={autopromotion_rollbacks}
        loading={loading}
        onChanged={() => void loadGovernanceRoutingPolicyAutopromotion()}
      />

      <GovernanceRoutingPolicyRollbacksPanel
        workspace={workspaceSlug}
        pendingRollbacks={rollback_pending}
        reviewSummary={rollback_review_summary}
        executionSummary={rollback_execution_summary}
        loading={loading}
        onChanged={() => Promise.all([loadGovernanceRoutingPolicyRollbacks(), loadGovernanceRoutingPolicyAutopromotion()])}
      />

      <GovernanceRoutingPolicyRollbackImpactPanel
        impactRows={rollback_impact_rows}
        effectivenessSummary={rollback_effectiveness}
        pendingEvaluations={rollback_pending_evals}
        loading={loading}
      />

      <GovernancePolicyOptimizationPanel
        snapshot={policyOptimizationSnapshot}
        featureEffectiveness={policyFeatureEffectiveness}
        contextFit={policyContextFit}
        policyOpportunities={policyOpportunities}
        loading={loading}
      />

      <GovernancePolicyReviewPanel
        workspace={workspaceSlug}
        reviewSummary={policyReviewSummary}
        promotionSummary={policyPromotionSummary}
        pendingPromotions={policyPendingPromotions}
        opportunities={policyOpportunities}
        loading={loading}
        onChanged={() => void Promise.all([loadGovernancePolicyReviews(), loadGovernancePolicyPromotions(), loadGovernancePolicyOptimization()])}
      />

      <GovernancePolicyAutopromotionsPanel
        workspace={workspaceSlug}
        autopromotionSummary={policyAutopromotionSummary}
        eligibility={policyAutopromotionEligibility}
        rollbackCandidates={policyAutopromotionRollbacks}
        loading={loading}
        onChanged={() => void loadGovernancePolicyAutopromotions()}
      />

      <MultiAssetFoundationPanel
        syncHealth={multiAssetSyncHealth}
        marketStateSample={multiAssetMarketState}
        familySummary={multiAssetFamilySummary}
        loading={loading}
      />

      <DependencyContextPanel
        latestContexts={dependencyContexts}
        coverageSummary={dependencyCoverage}
        contextDetail={dependencyDetail}
        familyState={dependencyFamily}
        loading={loading}
      />

      <CrossAssetSignalsPanel
        signalSummary={crossAssetSignals}
        dependencyHealth={crossAssetDependencyHealth}
        runContextSummary={crossAssetRunSummary}
        loading={loading}
      />

      <CrossAssetExplainabilityPanel
        explanationSummary={crossAssetExplanation}
        familySummary={crossAssetFamilyExplanation}
        runBridgeSummary={crossAssetRunBridge}
        loading={loading}
      />

      <CrossAssetAttributionPanel
        attributionSummary={crossAssetAttribution}
        familyAttributionSummary={crossAssetFamilyAttribution}
        runIntegrationSummary={crossAssetRunIntegration}
        loading={loading}
      />

      <DependencyPriorityWeightingPanel
        familyWeightedSummary={weightedFamilyAttribution}
        symbolWeightedSummary={weightedSymbolAttribution}
        runWeightedSummary={weightedRunIntegration}
        loading={loading}
      />

      <RegimeAwareCrossAssetPanel
        familyRegimeSummary={regimeFamilyAttribution}
        symbolRegimeSummary={regimeSymbolAttribution}
        runRegimeSummary={regimeRunIntegration}
        loading={loading}
      />

      <CrossAssetReplayValidationPanel
        replayValidationSummary={replayValidation}
        familyReplayStabilitySummary={replayFamilyStability}
        replayStabilityAggregate={replayStabilityAggregate}
        loading={loading}
      />

      <CrossAssetTimingPanel
        pairSummary={timingPair}
        familyTimingSummary={timingFamily}
        runTimingSummary={timingRun}
        loading={loading}
      />

      <CrossAssetTimingAttributionPanel
        familyTimingAttributionSummary={timingFamilyAttribution}
        symbolTimingAttributionSummary={timingSymbolAttribution}
        runTimingAttributionSummary={timingRunAttribution}
        loading={loading}
      />

      <CrossAssetTimingCompositePanel
        timingCompositeSummary={timingComposite}
        familyTimingCompositeSummary={timingFamilyComposite}
        finalIntegrationSummary={finalIntegrationSummary}
        loading={loading}
      />

      <CrossAssetTimingReplayValidationPanel
        timingReplayValidationSummary={timingReplayValidation}
        familyTimingReplayStabilitySummary={timingReplayFamilyStability}
        timingReplayStabilityAggregate={timingReplayStabilityAggregate}
        loading={loading}
      />

      <CrossAssetTransitionDiagnosticsPanel
        transitionStateSummary={transitionStateSummary}
        transitionEventSummary={transitionEventSummary}
        sequenceSummary={sequenceSummary}
        runTransitionSummary={runTransitionSummary}
        loading={loading}
      />

      <CrossAssetTransitionAttributionPanel
        familyTransitionAttributionSummary={familyTransitionAttribution}
        symbolTransitionAttributionSummary={symbolTransitionAttribution}
        runTransitionAttributionSummary={runTransitionAttribution}
        loading={loading}
      />

      <CrossAssetTransitionCompositePanel
        transitionCompositeSummary={transitionComposite}
        familyTransitionCompositeSummary={familyTransitionComposite}
        finalSequencingIntegrationSummary={sequencingIntegration}
        loading={loading}
      />

      <CrossAssetTransitionReplayValidationPanel
        transitionReplayValidationSummary={transitionReplayValidation}
        familyTransitionReplayStabilitySummary={familyTransitionReplayStability}
        transitionReplayStabilityAggregate={transitionReplayStabilityAggregate}
        loading={loading}
      />

      <CrossAssetPatternPanel
        familyArchetypeSummary={familyArchetypeSummary}
        runArchetypeSummary={runArchetypeSummary}
        regimeArchetypeSummary={regimeArchetypeSummary}
        runPatternSummary={runPatternSummary}
        loading={loading}
      />

      <CrossAssetArchetypeAttributionPanel
        familyArchetypeAttributionSummary={familyArchetypeAttribution}
        symbolArchetypeAttributionSummary={symbolArchetypeAttribution}
        runArchetypeAttributionSummary={runArchetypeAttribution}
        loading={loading}
      />

      <CrossAssetArchetypeCompositePanel
        archetypeCompositeSummary={archetypeComposite}
        familyArchetypeCompositeSummary={familyArchetypeComposite}
        finalArchetypeIntegrationSummary={archetypeIntegration}
        loading={loading}
      />

      <CrossAssetArchetypeReplayValidationPanel
        archetypeReplayValidationSummary={archetypeReplayValidation}
        familyArchetypeReplayStabilitySummary={familyArchetypeReplayStability}
        archetypeReplayStabilityAggregate={archetypeReplayStabilityAggregate}
        loading={loading}
      />

      <CrossAssetPatternClusterPanel
        clusterSummary={patternClusterSummary}
        regimeRotationSummary={patternRegimeRotation}
        driftEventSummary={patternDriftEvents}
        runPatternClusterSummary={runPatternCluster}
        loading={loading}
      />

      <CrossAssetClusterAttributionPanel
        familyClusterAttributionSummary={familyClusterAttribution}
        symbolClusterAttributionSummary={symbolClusterAttribution}
        runClusterAttributionSummary={runClusterAttribution}
        loading={loading}
      />

      <CrossAssetClusterCompositePanel
        clusterCompositeSummary={clusterComposite}
        familyClusterCompositeSummary={familyClusterComposite}
        finalClusterIntegrationSummary={clusterIntegration}
        loading={loading}
      />

      <CrossAssetClusterReplayValidationPanel
        clusterReplayValidationSummary={clusterReplayValidation}
        familyClusterReplayStabilitySummary={familyClusterReplayStability}
        clusterReplayStabilityAggregate={clusterReplayStabilityAggregate}
        loading={loading}
      />

      <CrossAssetPersistencePanel
        statePersistenceSummary={statePersistenceSummary}
        regimeMemorySummary={regimeMemorySummary}
        persistenceEventSummary={persistenceEventSummary}
        runPersistenceSummary={runPersistenceSummary}
        loading={loading}
      />

      <CrossAssetPersistenceAttributionPanel
        familyPersistenceAttributionSummary={familyPersistenceAttribution}
        symbolPersistenceAttributionSummary={symbolPersistenceAttribution}
        runPersistenceAttributionSummary={runPersistenceAttribution}
        loading={loading}
      />

      <CrossAssetPersistenceCompositePanel
        persistenceCompositeSummary={persistenceComposite}
        familyPersistenceCompositeSummary={familyPersistenceComposite}
        finalPersistenceIntegrationSummary={finalPersistenceIntegrationSummary}
        loading={loading}
      />

      <CrossAssetPersistenceReplayValidationPanel
        persistenceReplayValidationSummary={persistenceReplayValidation}
        familyPersistenceReplayStabilitySummary={familyPersistenceReplayStability}
        persistenceReplayStabilityAggregate={persistenceReplayStabilityAggregate}
        loading={loading}
      />

      <CrossAssetSignalDecayPanel
        signalDecaySummary={signalDecaySummary}
        familySignalDecaySummary={familySignalDecaySummary}
        staleMemoryEventSummary={staleMemoryEventSummary}
        runSignalDecaySummary={runSignalDecaySummary}
        loading={loading}
      />

      <CrossAssetDecayAttributionPanel
        familyDecayAttributionSummary={familyDecayAttributionSummary}
        symbolDecayAttributionSummary={symbolDecayAttributionSummary}
        runDecayAttributionSummary={runDecayAttributionSummary}
        loading={loading}
      />

      <CrossAssetDecayCompositePanel
        decayCompositeSummary={decayCompositeSummary}
        familyDecayCompositeSummary={familyDecayCompositeSummary}
        finalDecayIntegrationSummary={finalDecayIntegrationSummary}
        loading={loading}
      />

      <CrossAssetDecayReplayValidationPanel
        decayReplayValidationSummary={decayReplayValidationSummary}
        familyDecayReplayStabilitySummary={familyDecayReplayStabilitySummary}
        decayReplayStabilityAggregate={decayReplayStabilityAggregate}
        loading={loading}
      />

      <CrossAssetLayerConflictPanel
        layerAgreementSummary={layerAgreementSummary}
        familyLayerAgreementSummary={familyLayerAgreementSummary}
        layerConflictEventSummary={layerConflictEventSummary}
        runLayerConflictSummary={runLayerConflictSummary}
        loading={loading}
      />

      <CrossAssetConflictAttributionPanel
        familyConflictAttributionSummary={familyConflictAttributionSummary}
        symbolConflictAttributionSummary={symbolConflictAttributionSummary}
        runConflictAttributionSummary={runConflictAttributionSummary}
        loading={loading}
      />

      <GovernanceIncidentAnalyticsPanel
        summary={incidentAnalyticsSummary}
        rootCauseTrends={incidentAnalyticsRootCauseTrends}
        recurrenceBurden={incidentAnalyticsRecurrenceBurden}
        escalationEffectiveness={incidentAnalyticsEscalationEffectiveness}
        snapshots={incidentAnalyticsSnapshots}
        thresholdPromotionImpact={thresholdPromotionImpact}
        routingPromotionImpact={routingPromotionImpact}
        rollbackRisk={promotionRollbackRisk}
        loading={loading}
      />

        <GovernanceManagerOverviewPanel
          managerOverview={managerOverview}
          chronicWatchlists={managerChronicWatchlists}
          operatorTeamComparison={managerOperatorTeamComparison}
          promotionHealth={managerPromotionHealth}
          operatingRisk={managerOperatingRisk}
          reviewPriorities={managerReviewPriorities}
          trendWindows={managerTrendWindows}
          loading={loading}
        />

      <GovernanceOperatorPerformancePanel
        operatorSummary={operatorPerformanceSummary}
        teamSummary={teamPerformanceSummary}
        operatorCaseMix={operatorCaseMix}
        teamCaseMix={teamCaseMix}
        snapshots={performanceSnapshots}
        loading={loading}
      />


      {/* Note: trailing operator/team workload, escalation, incident timeline,
          regime threshold, scope and prior-run-diff panels live in the broader
          shell. The Phase 4.7A signal decay panel is wired in above. */}

    </main>
  );
}
