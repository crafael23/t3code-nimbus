import type {
  OrchestrationLatestTurn,
  OrchestrationReadModel,
  OrchestrationSession,
  ProviderKind,
  ProviderSessionRuntimeStatus,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";

import type { ProviderRuntimeBindingWithMetadata } from "../Services/ProviderSessionDirectory.ts";

export type InactivityAnchorSource =
  | "latest_turn_completed_at"
  | "latest_turn_started_at"
  | "latest_turn_requested_at"
  | "session_last_seen_at";

export type DeadlineBasisSource = "anchor" | "recent_provider_send_turn";

export interface ReapScheduleEntry {
  readonly threadId: ThreadId;
  readonly provider: ProviderKind;
  readonly deadlineAtMs: number;
  readonly anchorAt: string;
  readonly anchorSource: InactivityAnchorSource;
  readonly deadlineBasisAt: string;
  readonly deadlineBasisSource: DeadlineBasisSource;
  readonly bindingStatus: ProviderSessionRuntimeStatus | undefined;
  readonly sessionStatus: OrchestrationSession["status"] | null;
  readonly activeTurnId: TurnId | null;
  readonly readModelThreadPresent: boolean;
  readonly sessionUpdatedAt: string | null;
  readonly lastSeenAt: string;
  readonly latestTurnId: TurnId | null;
  readonly latestTurnState: OrchestrationLatestTurn["state"] | null;
  readonly latestTurnRequestedAt: string | null;
  readonly latestTurnStartedAt: string | null;
  readonly latestTurnCompletedAt: string | null;
}

export interface InvalidAnchorEntry {
  readonly threadId: ThreadId;
  readonly provider: ProviderKind;
  readonly bindingStatus: ProviderSessionRuntimeStatus | undefined;
  readonly readModelThreadPresent: boolean;
  readonly sessionStatus: OrchestrationSession["status"] | null;
  readonly sessionUpdatedAt: string | null;
  readonly activeTurnId: TurnId | null;
  readonly lastSeenAt: string;
  readonly latestTurnId: TurnId | null;
  readonly latestTurnState: OrchestrationLatestTurn["state"] | null;
  readonly latestTurnRequestedAt: string | null;
  readonly latestTurnStartedAt: string | null;
  readonly latestTurnCompletedAt: string | null;
  readonly inactivityAnchorAt: string;
  readonly inactivityAnchorSource: InactivityAnchorSource;
}

export interface DeriveReapEntriesResult {
  readonly entries: ReadonlyArray<ReapScheduleEntry>;
  readonly skippedStopped: number;
  readonly skippedActiveTurn: number;
  readonly invalidAnchors: ReadonlyArray<InvalidAnchorEntry>;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function readRecentProviderSendTurnAt(runtimePayload: unknown): string | undefined {
  if (!isRecord(runtimePayload) || runtimePayload.lastRuntimeEvent !== "provider.sendTurn") {
    return undefined;
  }

  const lastRuntimeEventAt = runtimePayload.lastRuntimeEventAt;
  return typeof lastRuntimeEventAt === "string" && lastRuntimeEventAt.length > 0
    ? lastRuntimeEventAt
    : undefined;
}

function resolveInactivityAnchor(input: {
  readonly latestTurn: {
    readonly requestedAt: string;
    readonly startedAt: string | null;
    readonly completedAt: string | null;
  } | null;
  readonly lastSeenAt: string;
}): { readonly at: string; readonly source: InactivityAnchorSource } {
  const latestTurn = input.latestTurn;
  if (latestTurn?.completedAt !== null && latestTurn?.completedAt !== undefined) {
    return {
      at: latestTurn.completedAt,
      source: "latest_turn_completed_at",
    };
  }
  if (latestTurn?.startedAt !== null && latestTurn?.startedAt !== undefined) {
    return {
      at: latestTurn.startedAt,
      source: "latest_turn_started_at",
    };
  }
  if (latestTurn !== null && latestTurn !== undefined) {
    return {
      at: latestTurn.requestedAt,
      source: "latest_turn_requested_at",
    };
  }
  return {
    at: input.lastSeenAt,
    source: "session_last_seen_at",
  };
}

export function deriveReapEntries(input: {
  readonly bindings: ReadonlyArray<ProviderRuntimeBindingWithMetadata>;
  readonly readModel: OrchestrationReadModel;
  readonly inactivityThresholdMs: number;
}): DeriveReapEntriesResult {
  const threadsById = new Map(
    input.readModel.threads.map((thread) => [thread.id, thread] as const),
  );
  const entries: ReapScheduleEntry[] = [];
  const invalidAnchors: InvalidAnchorEntry[] = [];
  let skippedStopped = 0;
  let skippedActiveTurn = 0;

  for (const binding of input.bindings) {
    if (binding.status === "stopped") {
      skippedStopped += 1;
      continue;
    }

    const thread = threadsById.get(binding.threadId);
    const inactivityAnchor = resolveInactivityAnchor({
      latestTurn: thread?.latestTurn ?? null,
      lastSeenAt: binding.lastSeenAt,
    });
    const inactivityAnchorMs = Date.parse(inactivityAnchor.at);

    if (Number.isNaN(inactivityAnchorMs)) {
      invalidAnchors.push({
        threadId: binding.threadId,
        provider: binding.provider,
        bindingStatus: binding.status,
        readModelThreadPresent: thread !== undefined,
        sessionStatus: thread?.session?.status ?? null,
        sessionUpdatedAt: thread?.session?.updatedAt ?? null,
        activeTurnId: thread?.session?.activeTurnId ?? null,
        lastSeenAt: binding.lastSeenAt,
        latestTurnId: thread?.latestTurn?.turnId ?? null,
        latestTurnState: thread?.latestTurn?.state ?? null,
        latestTurnRequestedAt: thread?.latestTurn?.requestedAt ?? null,
        latestTurnStartedAt: thread?.latestTurn?.startedAt ?? null,
        latestTurnCompletedAt: thread?.latestTurn?.completedAt ?? null,
        inactivityAnchorAt: inactivityAnchor.at,
        inactivityAnchorSource: inactivityAnchor.source,
      });
      continue;
    }

    if (thread?.session?.activeTurnId !== null && thread?.session?.activeTurnId !== undefined) {
      skippedActiveTurn += 1;
      continue;
    }

    const recentSendTurnAt = readRecentProviderSendTurnAt(binding.runtimePayload);
    const recentSendTurnAtMs =
      recentSendTurnAt === undefined ? Number.NaN : Date.parse(recentSendTurnAt);
    let deadlineBasisAt = inactivityAnchor.at;
    let deadlineBasisSource: DeadlineBasisSource = "anchor";
    let deadlineBasisMs = inactivityAnchorMs;
    if (
      recentSendTurnAt !== undefined &&
      !Number.isNaN(recentSendTurnAtMs) &&
      recentSendTurnAtMs > inactivityAnchorMs
    ) {
      deadlineBasisAt = recentSendTurnAt;
      deadlineBasisSource = "recent_provider_send_turn";
      deadlineBasisMs = recentSendTurnAtMs;
    }

    entries.push({
      threadId: binding.threadId,
      provider: binding.provider,
      deadlineAtMs: deadlineBasisMs + input.inactivityThresholdMs,
      anchorAt: inactivityAnchor.at,
      anchorSource: inactivityAnchor.source,
      deadlineBasisAt,
      deadlineBasisSource,
      bindingStatus: binding.status,
      sessionStatus: thread?.session?.status ?? null,
      activeTurnId: thread?.session?.activeTurnId ?? null,
      readModelThreadPresent: thread !== undefined,
      sessionUpdatedAt: thread?.session?.updatedAt ?? null,
      lastSeenAt: binding.lastSeenAt,
      latestTurnId: thread?.latestTurn?.turnId ?? null,
      latestTurnState: thread?.latestTurn?.state ?? null,
      latestTurnRequestedAt: thread?.latestTurn?.requestedAt ?? null,
      latestTurnStartedAt: thread?.latestTurn?.startedAt ?? null,
      latestTurnCompletedAt: thread?.latestTurn?.completedAt ?? null,
    });
  }

  entries.sort(
    (left, right) =>
      left.deadlineAtMs - right.deadlineAtMs ||
      String(left.threadId).localeCompare(String(right.threadId)),
  );

  return {
    entries,
    skippedStopped,
    skippedActiveTurn,
    invalidAnchors,
  };
}
