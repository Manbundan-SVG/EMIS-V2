// Phase Frontend V1.0 — Ops Intelligence Console
//
// Types for the executive summary and attribution-ladder views. These
// types describe the shape of the aggregated read-only data exposed to
// the V1 console; they intersect existing per-layer summary types but
// are scoped to the cards/ladders the V1 UI actually renders.

export interface OpsIntelligenceCompositeCard {
  preConflict: number | null;
  netContribution: number | null;
  postConflict: number | null;
}

export interface OpsIntelligenceConsensusCard {
  state: string | null;
  agreementScore: number | null;
}

export interface OpsIntelligenceConflictCard {
  score: number | null;
  dominantSource: string | null;
}

export interface OpsIntelligenceFreshnessCard {
  state: string | null;
  aggregateDecayScore: number | null;
  staleMemoryFlag: boolean | null;
  contradictionFlag: boolean | null;
}

export interface OpsIntelligencePersistenceCard {
  state: string | null;
}

export interface OpsIntelligenceReplayCard {
  validationCount: number | null;
  driftDetectedCount: number | null;
  contextMatchRate: number | null;
  conflictAttributionMatchRate: number | null;
  conflictCompositeMatchRate: number | null;
  latestValidatedAt: string | null;
}

export interface OpsIntelligenceDominantFamilyCard {
  raw: string | null;
  weighted: string | null;
  regime: string | null;
  timing: string | null;
  transition: string | null;
  archetype: string | null;
  cluster: string | null;
  persistence: string | null;
  decay: string | null;
  conflict: string | null;
}

export interface OpsIntelligenceLatestEventCard {
  type: string | null;
  family: string | null;
  ts: string | null;
}

export interface OpsIntelligenceCards {
  composite: OpsIntelligenceCompositeCard;
  layerConsensus: OpsIntelligenceConsensusCard;
  conflict: OpsIntelligenceConflictCard;
  freshness: OpsIntelligenceFreshnessCard;
  persistence: OpsIntelligencePersistenceCard;
  replay: OpsIntelligenceReplayCard;
  dominantFamily: OpsIntelligenceDominantFamilyCard;
  latestEvent: OpsIntelligenceLatestEventCard;
}

export interface OpsIntelligenceAttributionLadder {
  raw: number | null;
  weighted: number | null;
  regime: number | null;
  timing: number | null;
  transition: number | null;
  archetype: number | null;
  cluster: number | null;
  persistence: number | null;
  decay: number | null;
  conflict: number | null;
  // composite endpoints: composite_pre_conflict / composite_post_conflict
  compositePre: number | null;
  compositePost: number | null;
}

export interface OpsIntelligenceMetadataStamp {
  scoringVersion: string | null;
  integrationMode: string | null;
  sourceContributionLayer: string | null;
  sourceCompositeLayer: string | null;
}

export interface OpsIntelligence {
  workspaceId: string;
  workspaceSlug: string;
  watchlistId: string | null;
  latestRunId: string | null;
  latestRunCreatedAt: string | null;
  cards: OpsIntelligenceCards;
  attributionLadder: OpsIntelligenceAttributionLadder;
  metadata: OpsIntelligenceMetadataStamp;
}

export interface OpsIntelligenceResponse {
  ok: boolean;
  intelligence: OpsIntelligence | null;
  error?: string;
}
