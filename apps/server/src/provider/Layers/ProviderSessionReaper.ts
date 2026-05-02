import type { ProviderKind } from "@t3tools/contracts";
import {
  Cause,
  Clock,
  Context,
  Duration,
  Effect,
  Layer,
  Metric,
  Option,
  Queue,
  Scope,
  Stream,
  Tracer,
} from "effect";

import {
  increment,
  metricAttributes,
  providerSessionReaperDueCandidatesTotal,
  providerSessionReaperReapLag,
  providerSessionReaperReapedTotal,
  providerSessionReaperReconcileDuration,
  providerSessionReaperScheduleSize,
  providerSessionReaperSignalFeedRestartsTotal,
  providerSessionReaperWakeCoalescedTotal,
  providerSessionReaperWakeupsTotal,
} from "../../observability/Metrics.ts";
import { OrchestrationEngineService } from "../../orchestration/Services/OrchestrationEngine.ts";
import { ProviderSessionDirectory } from "../Services/ProviderSessionDirectory.ts";
import { ProviderSessionDirectoryEvents } from "../Services/ProviderSessionDirectoryEvents.ts";
import {
  DEFAULT_PROVIDER_SESSION_REAPER_FALLBACK_RECONCILE_INTERVAL_MS,
  DEFAULT_PROVIDER_SESSION_REAPER_INACTIVITY_THRESHOLD_MS,
  DEFAULT_PROVIDER_SESSION_REAPER_STOP_FAILURE_RETRY_INTERVAL_MS,
  ProviderSessionReaper,
  type ProviderSessionReaperShape,
} from "../Services/ProviderSessionReaper.ts";
import { ProviderService } from "../Services/ProviderService.ts";
import type { InvalidAnchorEntry, ReapScheduleEntry } from "./reaperDeadlines.ts";
import { deriveReapEntries } from "./reaperDeadlines.ts";

const REAPER_MODE = "deadline";
const SIGNAL_FEED_RESTART_DELAY_MS = 1_000;

export interface ProviderSessionReaperLiveOptions {
  readonly inactivityThresholdMs?: number;
  readonly fallbackReconcileIntervalMs?: number;
  readonly stopFailureRetryIntervalMs?: number;
}

type ReaperSignal =
  | { readonly type: "startup" }
  | { readonly type: "reconcile-all"; readonly reason: string }
  | { readonly type: "runtime-binding-changed"; readonly threadId: string }
  | { readonly type: "orchestration-thread-changed"; readonly threadId: string }
  | { readonly type: "thread-deleted"; readonly threadId: string };

interface ReconcileSnapshot {
  readonly bindingCount: number;
  readonly readModelThreadCount: number;
  readonly entries: ReadonlyArray<ReapScheduleEntry>;
  readonly skippedStopped: number;
  readonly skippedActiveTurn: number;
  readonly invalidAnchors: ReadonlyArray<InvalidAnchorEntry>;
  readonly reconciledAtMs: number;
}

interface CoalescedWake {
  readonly signal: (signal: ReaperSignal) => Effect.Effect<void>;
  readonly await: Effect.Effect<ReaperSignal>;
}

interface SchedulerIterationResult {
  readonly nextDeadlineAtMs: number | undefined;
  readonly observedStartupWake: boolean;
}

function buildReaperLogContext(input: {
  readonly now: number;
  readonly inactivityThresholdMs: number;
  readonly entry: ReapScheduleEntry;
}) {
  const inactivityAnchorMs = Date.parse(input.entry.anchorAt);
  const idleDurationMs = input.now - inactivityAnchorMs;

  return {
    threadId: input.entry.threadId,
    provider: input.entry.provider,
    bindingStatus: input.entry.bindingStatus,
    readModelThreadPresent: input.entry.readModelThreadPresent,
    sessionStatus: input.entry.sessionStatus,
    sessionUpdatedAt: input.entry.sessionUpdatedAt,
    activeTurnId: input.entry.activeTurnId,
    lastSeenAt: input.entry.lastSeenAt,
    latestTurnId: input.entry.latestTurnId,
    latestTurnState: input.entry.latestTurnState,
    latestTurnRequestedAt: input.entry.latestTurnRequestedAt,
    latestTurnStartedAt: input.entry.latestTurnStartedAt,
    latestTurnCompletedAt: input.entry.latestTurnCompletedAt,
    inactivityAnchorAt: input.entry.anchorAt,
    inactivityAnchorSource: input.entry.anchorSource,
    inactivityAnchorMs,
    deadlineBasisAt: input.entry.deadlineBasisAt,
    deadlineBasisSource: input.entry.deadlineBasisSource,
    deadlineAt: new Date(input.entry.deadlineAtMs).toISOString(),
    deadlineAtMs: input.entry.deadlineAtMs,
    inactivityThresholdMs: input.inactivityThresholdMs,
    idleDurationMs,
    remainingUntilReapMs: Math.max(0, input.entry.deadlineAtMs - input.now),
    reapLagMs: Math.max(0, input.now - input.entry.deadlineAtMs),
  };
}

function buildInvalidAnchorLogContext(input: {
  readonly now: number;
  readonly inactivityThresholdMs: number;
  readonly entry: InvalidAnchorEntry;
}) {
  return {
    threadId: input.entry.threadId,
    provider: input.entry.provider,
    bindingStatus: input.entry.bindingStatus,
    readModelThreadPresent: input.entry.readModelThreadPresent,
    sessionStatus: input.entry.sessionStatus,
    sessionUpdatedAt: input.entry.sessionUpdatedAt,
    activeTurnId: input.entry.activeTurnId,
    lastSeenAt: input.entry.lastSeenAt,
    latestTurnId: input.entry.latestTurnId,
    latestTurnState: input.entry.latestTurnState,
    latestTurnRequestedAt: input.entry.latestTurnRequestedAt,
    latestTurnStartedAt: input.entry.latestTurnStartedAt,
    latestTurnCompletedAt: input.entry.latestTurnCompletedAt,
    inactivityAnchorAt: input.entry.inactivityAnchorAt,
    inactivityAnchorSource: input.entry.inactivityAnchorSource,
    inactivityAnchorMs: null,
    reconciledAt: new Date(input.now).toISOString(),
    inactivityThresholdMs: input.inactivityThresholdMs,
    idleDurationMs: null,
    remainingUntilReapMs: null,
  };
}

function wakeReason(signal: ReaperSignal | "timeout"): string {
  if (signal === "timeout") {
    return "timeout";
  }
  if (signal.type === "startup") {
    return "startup";
  }
  if (signal.type === "reconcile-all" && signal.reason === "fallback-tick") {
    return "fallback";
  }
  return `signal:${signal.type}`;
}

function wakeLogContext(signal: ReaperSignal | "timeout"): Record<string, unknown> {
  if (signal === "timeout" || signal.type === "startup") {
    return {};
  }
  if (signal.type === "reconcile-all") {
    return {
      reconcileReason: signal.reason,
    };
  }
  return {
    threadId: signal.threadId,
  };
}

function providerBreakdown(
  entries: ReadonlyArray<ReapScheduleEntry>,
): Record<ProviderKind, number> {
  const counts: Record<ProviderKind, number> = {
    codex: 0,
    claudeAgent: 0,
    cursor: 0,
    opencode: 0,
  };
  for (const entry of entries) {
    counts[entry.provider] += 1;
  }
  return counts;
}

function earliestDeadline(...deadlines: ReadonlyArray<number | undefined>): number | undefined {
  let earliest: number | undefined = undefined;
  for (const deadline of deadlines) {
    if (deadline === undefined) {
      continue;
    }
    earliest = earliest === undefined ? deadline : Math.min(earliest, deadline);
  }
  return earliest;
}

const makeCoalescedWake = () =>
  Effect.gen(function* () {
    const queue = yield* Effect.acquireRelease(Queue.dropping<ReaperSignal>(1), Queue.shutdown);

    return {
      signal: (signal) =>
        Queue.offer(queue, signal).pipe(
          Effect.flatMap((enqueued) =>
            enqueued
              ? Effect.void
              : increment(providerSessionReaperWakeCoalescedTotal, { mode: REAPER_MODE }),
          ),
        ),
      await: Queue.take(queue),
    } satisfies CoalescedWake;
  });

const makeProviderSessionReaper = (options?: ProviderSessionReaperLiveOptions) =>
  Effect.gen(function* () {
    const providerService = yield* ProviderService;
    const directory = yield* ProviderSessionDirectory;
    const directoryEvents = yield* ProviderSessionDirectoryEvents;
    const orchestrationEngine = yield* OrchestrationEngineService;

    const inactivityThresholdMs = Math.max(
      1,
      options?.inactivityThresholdMs ?? DEFAULT_PROVIDER_SESSION_REAPER_INACTIVITY_THRESHOLD_MS,
    );
    const fallbackReconcileIntervalMs = Math.max(
      1,
      options?.fallbackReconcileIntervalMs ??
        DEFAULT_PROVIDER_SESSION_REAPER_FALLBACK_RECONCILE_INTERVAL_MS,
    );
    const stopFailureRetryIntervalMs = Math.max(
      1,
      options?.stopFailureRetryIntervalMs ??
        DEFAULT_PROVIDER_SESSION_REAPER_STOP_FAILURE_RETRY_INTERVAL_MS,
    );
    const mode = REAPER_MODE;

    const recordWake = (signal: ReaperSignal | "timeout", extra?: Record<string, unknown>) =>
      increment(providerSessionReaperWakeupsTotal, {
        mode,
        reason: wakeReason(signal),
      }).pipe(
        Effect.andThen(
          Effect.logDebug("provider.session.reaper.wake", {
            mode,
            reason: wakeReason(signal),
            ...wakeLogContext(signal),
            ...extra,
          }),
        ),
      );

    const reconcileAuthoritativeState = () =>
      Effect.gen(function* () {
        yield* Effect.annotateCurrentSpan({
          "provider.session_reaper.mode": mode,
          "provider.session_reaper.inactivity_threshold_ms": inactivityThresholdMs,
        });
        const startedAtMs = yield* Clock.currentTimeMillis;
        yield* Effect.logDebug("provider.session.reaper.reconcile-started", {
          mode,
          inactivityThresholdMs,
        });

        const readModel = yield* orchestrationEngine.getReadModel();
        const bindings = yield* directory.listBindings();
        const derived = deriveReapEntries({
          bindings,
          readModel,
          inactivityThresholdMs,
        });
        const reconciledAtMs = yield* Clock.currentTimeMillis;
        const reconcileDurationMs = Math.max(0, reconciledAtMs - startedAtMs);
        yield* Effect.annotateCurrentSpan({
          "provider.session_reaper.binding_count": bindings.length,
          "provider.session_reaper.read_model_thread_count": readModel.threads.length,
          "provider.session_reaper.schedule_size": derived.entries.length,
          "provider.session_reaper.skipped_stopped_count": derived.skippedStopped,
          "provider.session_reaper.skipped_active_turn_count": derived.skippedActiveTurn,
          "provider.session_reaper.invalid_anchor_count": derived.invalidAnchors.length,
          "provider.session_reaper.reconcile_duration_ms": reconcileDurationMs,
        });

        yield* Metric.update(
          Metric.withAttributes(providerSessionReaperReconcileDuration, metricAttributes({ mode })),
          Duration.millis(reconcileDurationMs),
        );

        for (const invalidAnchor of derived.invalidAnchors) {
          yield* Effect.logWarning(
            "provider.session.reaper.invalid-inactivity-anchor",
            buildInvalidAnchorLogContext({
              now: reconciledAtMs,
              inactivityThresholdMs,
              entry: invalidAnchor,
            }),
          );
        }

        yield* Effect.logDebug("provider.session.reaper.reconcile-completed", {
          mode,
          bindingCount: bindings.length,
          readModelThreadCount: readModel.threads.length,
          scheduleSize: derived.entries.length,
          skippedStoppedCount: derived.skippedStopped,
          skippedActiveTurnCount: derived.skippedActiveTurn,
          invalidAnchorCount: derived.invalidAnchors.length,
          reconcileDurationMs,
        });

        return {
          bindingCount: bindings.length,
          readModelThreadCount: readModel.threads.length,
          entries: derived.entries,
          skippedStopped: derived.skippedStopped,
          skippedActiveTurn: derived.skippedActiveTurn,
          invalidAnchors: derived.invalidAnchors,
          reconciledAtMs,
        } satisfies ReconcileSnapshot;
      }).pipe(
        Effect.withSpan("provider.session.reaper.reconcile", {
          attributes: {
            "provider.session_reaper.mode": mode,
          },
        }),
      );

    const stopDueEntries = (input: {
      readonly entries: ReadonlyArray<ReapScheduleEntry>;
      readonly nowMs: number;
    }) =>
      Effect.gen(function* () {
        yield* Effect.annotateCurrentSpan({
          "provider.session_reaper.mode": mode,
          "provider.session_reaper.due_count": input.entries.length,
        });
        yield* increment(
          providerSessionReaperDueCandidatesTotal,
          {
            mode,
          },
          input.entries.length,
        );

        let reapedCount = 0;
        let stopFailedCount = 0;

        for (const entry of input.entries) {
          const reaped = yield* Effect.gen(function* () {
            const logContext = buildReaperLogContext({
              now: input.nowMs,
              inactivityThresholdMs,
              entry,
            });

            yield* Effect.annotateCurrentSpan({
              "provider.session_reaper.mode": mode,
              "provider.thread_id": entry.threadId,
              "provider.kind": entry.provider,
              "provider.session_reaper.read_model_thread_present": entry.readModelThreadPresent,
              "provider.session_reaper.inactivity_anchor_at": entry.anchorAt,
              "provider.session_reaper.inactivity_anchor_source": entry.anchorSource,
              "provider.session_reaper.deadline_basis_at": entry.deadlineBasisAt,
              "provider.session_reaper.deadline_basis_source": entry.deadlineBasisSource,
              "provider.session_reaper.deadline_at_ms": entry.deadlineAtMs,
              "provider.session_reaper.reap_lag_ms": logContext.reapLagMs,
              "provider.session_reaper.last_seen_at": entry.lastSeenAt,
              ...(entry.bindingStatus !== undefined
                ? { "provider.session_reaper.binding_status": entry.bindingStatus }
                : {}),
              ...(entry.sessionStatus !== null
                ? { "provider.session_reaper.session_status": entry.sessionStatus }
                : {}),
              ...(entry.sessionUpdatedAt !== null
                ? { "provider.session_reaper.session_updated_at": entry.sessionUpdatedAt }
                : {}),
              ...(entry.activeTurnId !== null
                ? { "provider.session_reaper.active_turn_id": entry.activeTurnId }
                : {}),
              ...(entry.latestTurnId !== null
                ? { "provider.session_reaper.latest_turn_id": entry.latestTurnId }
                : {}),
              ...(entry.latestTurnState !== null
                ? { "provider.session_reaper.latest_turn_state": entry.latestTurnState }
                : {}),
              ...(entry.latestTurnRequestedAt !== null
                ? {
                    "provider.session_reaper.latest_turn_requested_at": entry.latestTurnRequestedAt,
                  }
                : {}),
              ...(entry.latestTurnStartedAt !== null
                ? { "provider.session_reaper.latest_turn_started_at": entry.latestTurnStartedAt }
                : {}),
              ...(entry.latestTurnCompletedAt !== null
                ? {
                    "provider.session_reaper.latest_turn_completed_at": entry.latestTurnCompletedAt,
                  }
                : {}),
            });

            yield* Metric.update(
              Metric.withAttributes(
                providerSessionReaperReapLag,
                metricAttributes({
                  mode,
                  provider: entry.provider,
                }),
              ),
              Duration.millis(Math.max(0, input.nowMs - entry.deadlineAtMs)),
            );

            yield* Effect.logDebug("provider.session.reaper.reap-candidate", {
              ...logContext,
              decision: "attempt_stop_session",
            });

            const reaped = yield* providerService
              .stopSession({
                threadId: entry.threadId,
              })
              .pipe(
                Effect.tap(() =>
                  Effect.logInfo("provider.session.reaped", {
                    ...logContext,
                    reason: "inactivity_threshold",
                  }).pipe(
                    Effect.andThen(
                      increment(providerSessionReaperReapedTotal, {
                        mode,
                        provider: entry.provider,
                      }),
                    ),
                  ),
                ),
                Effect.as(true),
                Effect.catchCause((cause) => {
                  if (Cause.hasInterruptsOnly(cause)) {
                    return Effect.failCause(cause);
                  }
                  stopFailedCount += 1;
                  return Effect.logWarning("provider.session.reaper.stop-failed", {
                    ...logContext,
                    reason: "inactivity_threshold",
                    cause,
                  }).pipe(Effect.as(false));
                }),
              );

            yield* Effect.annotateCurrentSpan({
              "provider.session_reaper.stop_outcome": reaped ? "reaped" : "failed",
            });
            return reaped;
          }).pipe(
            Effect.withSpan("provider.session.reaper.stop_session", {
              kind: "client",
              attributes: {
                "provider.session_reaper.mode": mode,
                "provider.thread_id": entry.threadId,
                "provider.kind": entry.provider,
                "provider.session_reaper.inactivity_anchor_at": entry.anchorAt,
                "provider.session_reaper.inactivity_anchor_source": entry.anchorSource,
                "provider.session_reaper.deadline_basis_at": entry.deadlineBasisAt,
                "provider.session_reaper.deadline_basis_source": entry.deadlineBasisSource,
                "provider.session_reaper.deadline_at_ms": entry.deadlineAtMs,
              },
            }),
          );

          if (reaped) {
            reapedCount += 1;
          }
        }

        yield* Effect.annotateCurrentSpan({
          "provider.session_reaper.reaped_count": reapedCount,
          "provider.session_reaper.stop_failed_count": stopFailedCount,
        });

        return {
          reapedCount,
          stopFailedCount,
        } as const;
      }).pipe(
        Effect.withSpan("provider.session.reaper.stop_due", {
          kind: "internal",
          attributes: {
            "provider.session_reaper.mode": mode,
          },
        }),
      );

    const runDeadlineScheduler = Effect.gen(function* () {
      const wake = yield* makeCoalescedWake();

      const signalWake = (signal: ReaperSignal) => wake.signal(signal);

      const forkFeed = (name: string, effect: () => Effect.Effect<void>) => {
        const run = (): Effect.Effect<void> =>
          Effect.suspend(effect).pipe(
            Effect.catchCause((cause) => {
              if (Cause.hasInterruptsOnly(cause)) {
                return Effect.failCause(cause);
              }
              return increment(providerSessionReaperSignalFeedRestartsTotal, {
                mode,
                feed: name,
              }).pipe(
                Effect.andThen(
                  Effect.logWarning("provider.session.reaper.signal-stream-failed", {
                    mode,
                    feed: name,
                    cause,
                  }),
                ),
                Effect.andThen(
                  signalWake({ type: "reconcile-all", reason: `feed-failed:${name}` }),
                ),
                Effect.andThen(Effect.sleep(Duration.millis(SIGNAL_FEED_RESTART_DELAY_MS))),
                Effect.flatMap(() => run()),
              );
            }),
          );

        return Effect.forkScoped(run());
      };

      yield* forkFeed("provider-session-directory-events", () =>
        Stream.runForEach(directoryEvents.changes, (change) =>
          signalWake({
            type: "runtime-binding-changed",
            threadId: change.threadId,
          }),
        ),
      );

      yield* forkFeed("orchestration-domain-events", () =>
        Stream.runForEach(orchestrationEngine.streamDomainEvents, (event) => {
          switch (event.type) {
            case "thread.deleted":
              return signalWake({
                type: "thread-deleted",
                threadId: event.payload.threadId,
              });
            case "thread.reverted":
            case "thread.session-set":
            case "thread.turn-diff-completed":
              return signalWake({
                type: "orchestration-thread-changed",
                threadId: event.payload.threadId,
              });
            default:
              return Effect.void;
          }
        }),
      );

      yield* Effect.forkScoped(
        signalWake({ type: "reconcile-all", reason: "fallback-tick" }).pipe(
          Effect.delay(Duration.millis(fallbackReconcileIntervalMs)),
          Effect.forever,
        ),
      );

      yield* signalWake({ type: "startup" });
      let nextDeadlineAtMs: number | undefined = undefined;
      let observedStartupWake = false;

      while (true) {
        const iterationResult: SchedulerIterationResult = yield* Effect.gen(function* () {
          yield* Effect.annotateCurrentSpan({
            "provider.session_reaper.mode": mode,
            ...(nextDeadlineAtMs !== undefined
              ? {
                  "provider.session_reaper.next_deadline_at_ms": nextDeadlineAtMs,
                  "provider.session_reaper.next_deadline_at": new Date(
                    nextDeadlineAtMs,
                  ).toISOString(),
                }
              : {}),
          });

          let wakeSignal: ReaperSignal | "timeout";
          if (nextDeadlineAtMs === undefined) {
            const signal = yield* wake.await;
            observedStartupWake ||= signal.type === "startup";
            wakeSignal = signal;
            yield* recordWake(signal);
          } else {
            const waitMs = Math.max(0, nextDeadlineAtMs - (yield* Clock.currentTimeMillis));
            const signal = yield* wake.await.pipe(Effect.timeoutOption(Duration.millis(waitMs)));
            if (Option.isSome(signal)) {
              observedStartupWake ||= signal.value.type === "startup";
              wakeSignal = signal.value;
              yield* recordWake(signal.value, {
                deadlineAt: new Date(nextDeadlineAtMs).toISOString(),
                waitMs,
              });
            } else {
              wakeSignal = "timeout";
              yield* recordWake("timeout", {
                deadlineAt: new Date(nextDeadlineAtMs).toISOString(),
                waitMs,
              });
            }
          }

          yield* Effect.annotateCurrentSpan({
            "provider.session_reaper.wake_reason": wakeReason(wakeSignal),
            ...(wakeSignal !== "timeout" && "threadId" in wakeSignal
              ? { "provider.thread_id": wakeSignal.threadId }
              : {}),
            ...(wakeSignal !== "timeout" && wakeSignal.type === "reconcile-all"
              ? { "provider.session_reaper.reconcile_reason": wakeSignal.reason }
              : {}),
          });

          const snapshotOption = yield* reconcileAuthoritativeState().pipe(
            Effect.map(Option.some),
            Effect.catchCause((cause) => {
              if (Cause.hasInterruptsOnly(cause)) {
                return Effect.failCause(cause);
              }
              return Effect.logWarning("provider.session.reaper.reconcile-failed", {
                mode,
                cause,
              }).pipe(Effect.as(Option.none<ReconcileSnapshot>()));
            }),
          );

          if (Option.isNone(snapshotOption)) {
            yield* Effect.annotateCurrentSpan({
              "provider.session_reaper.reconcile_outcome": "failed",
            });
            return {
              nextDeadlineAtMs: undefined,
              observedStartupWake,
            };
          }

          const snapshot = snapshotOption.value;
          const now = snapshot.reconciledAtMs;
          const dueEntries = snapshot.entries.filter((entry) => entry.deadlineAtMs <= now);
          const futureEntries = snapshot.entries.filter((entry) => entry.deadlineAtMs > now);

          yield* Metric.update(
            Metric.withAttributes(providerSessionReaperScheduleSize, metricAttributes({ mode })),
            futureEntries.length,
          );

          yield* Effect.annotateCurrentSpan({
            "provider.session_reaper.reconcile_outcome": "success",
            "provider.session_reaper.schedule_size": futureEntries.length,
            "provider.session_reaper.due_count": dueEntries.length,
            "provider.session_reaper.skipped_stopped_count": snapshot.skippedStopped,
            "provider.session_reaper.skipped_active_turn_count": snapshot.skippedActiveTurn,
            "provider.session_reaper.invalid_anchor_count": snapshot.invalidAnchors.length,
          });

          if (observedStartupWake && dueEntries.length > 0) {
            observedStartupWake = false;
            yield* Effect.logInfo("provider.session.reaper.overdue-on-startup", {
              mode,
              overdueCount: dueEntries.length,
              providers: providerBreakdown(dueEntries),
            });
          }

          if (dueEntries.length > 0) {
            const stopResult = yield* stopDueEntries({
              entries: dueEntries,
              nowMs: now,
            });
            yield* Effect.annotateCurrentSpan({
              "provider.session_reaper.reaped_count": stopResult.reapedCount,
              "provider.session_reaper.stop_failed_count": stopResult.stopFailedCount,
            });
            if (stopResult.stopFailedCount > 0) {
              const nextFutureDeadlineAtMs = futureEntries[0]?.deadlineAtMs;
              const retryDeadlineAtMs = now + stopFailureRetryIntervalMs;
              const nextDeadlineAtMs = earliestDeadline(retryDeadlineAtMs, nextFutureDeadlineAtMs);
              yield* Effect.logDebug("provider.session.reaper.stop-failure-retry-scheduled", {
                mode,
                stopFailedCount: stopResult.stopFailedCount,
                reapedCount: stopResult.reapedCount,
                stopFailureRetryIntervalMs,
                retryDeadlineAtMs,
                retryDeadlineAt: new Date(retryDeadlineAtMs).toISOString(),
                ...(nextFutureDeadlineAtMs !== undefined
                  ? {
                      nextFutureDeadlineAtMs,
                      nextFutureDeadlineAt: new Date(nextFutureDeadlineAtMs).toISOString(),
                    }
                  : {}),
                ...(nextDeadlineAtMs !== undefined
                  ? {
                      nextDeadlineAtMs,
                      nextDeadlineAt: new Date(nextDeadlineAtMs).toISOString(),
                      waitMs: Math.max(0, nextDeadlineAtMs - now),
                    }
                  : {}),
              });
              return {
                nextDeadlineAtMs,
                observedStartupWake: false,
              };
            }
            if (stopResult.reapedCount > 0 && futureEntries.length > 0) {
              yield* signalWake({ type: "reconcile-all", reason: "post-stop" });
            }
            return {
              nextDeadlineAtMs: undefined,
              observedStartupWake: false,
            };
          }

          const nextEntry = futureEntries[0];
          if (nextEntry === undefined) {
            return {
              nextDeadlineAtMs: undefined,
              observedStartupWake: false,
            };
          }

          yield* Effect.logDebug("provider.session.reaper.next-deadline-selected", {
            ...buildReaperLogContext({
              now,
              inactivityThresholdMs,
              entry: nextEntry,
            }),
            mode,
            waitMs: Math.max(0, nextEntry.deadlineAtMs - now),
          });

          return {
            nextDeadlineAtMs: nextEntry.deadlineAtMs,
            observedStartupWake: false,
          };
        }).pipe(
          Effect.withSpan("provider.session.reaper.iteration", {
            kind: "internal",
            root: true,
            attributes: {
              "provider.session_reaper.mode": mode,
            },
          }),
        );

        nextDeadlineAtMs = iterationResult.nextDeadlineAtMs;
        observedStartupWake = iterationResult.observedStartupWake;
      }
    });

    const start: ProviderSessionReaperShape["start"] = () =>
      Effect.gen(function* () {
        yield* Effect.forkScoped(
          runDeadlineScheduler.pipe(
            Effect.updateContext((context: Context.Context<Scope.Scope>) =>
              Context.omit(Tracer.ParentSpan)(context),
            ),
          ),
        );

        yield* Effect.logInfo("provider.session.reaper.scheduler-started", {
          mode,
          inactivityThresholdMs,
          fallbackReconcileIntervalMs,
          stopFailureRetryIntervalMs,
        });
      }).pipe(
        Effect.withSpan("provider.session.reaper.start", {
          kind: "internal",
          root: true,
          attributes: {
            "provider.session_reaper.mode": mode,
            "provider.session_reaper.inactivity_threshold_ms": inactivityThresholdMs,
            "provider.session_reaper.fallback_reconcile_interval_ms": fallbackReconcileIntervalMs,
            "provider.session_reaper.stop_failure_retry_interval_ms": stopFailureRetryIntervalMs,
          },
        }),
      );

    return {
      start,
    } satisfies ProviderSessionReaperShape;
  });

export const makeProviderSessionReaperLive = (options?: ProviderSessionReaperLiveOptions) =>
  Layer.effect(ProviderSessionReaper, makeProviderSessionReaper(options));
