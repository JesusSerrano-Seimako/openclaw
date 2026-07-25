import { runWithGatewayIndependentRootWorkContinuation } from "../process/gateway-work-admission.js";
import type { AcceptedSessionSpawn } from "./accepted-session-spawn.js";
import type { RequesterSettleWakeBatchState } from "./subagent-announce.requester-settle-wake.js";
import type {
  SubagentRegistryLifecycleParams,
  SubagentRegistryLifecycleShared,
} from "./subagent-registry-lifecycle-shared.js";
import { settleRequesterTurnAfterSessionSpawns } from "./subagent-registry-requester-yield.js";
import type { RequesterSettleWakeState, SubagentRunRecord } from "./subagent-registry.types.js";

export function createSubagentRegistryRequesterSettle(
  params: SubagentRegistryLifecycleParams,
  shared: SubagentRegistryLifecycleShared,
) {
  const pendingRequesterSettleWakeRearms = new Set<string>();
  const scheduledRequesterSettleWakeRuns = new Set<string>();
  const scheduledRequesterSettleWakeTimers = new Map<string, ReturnType<typeof setTimeout>>();
  const { buildSafeLifecycleErrorMeta, maskRunId, maskSessionKey } = shared;

  const transitionRequesterSettleWakeBatch = (
    runIds: readonly string[],
    state: RequesterSettleWakeBatchState,
  ) => {
    const entries = runIds
      .map((runId) => params.runs.get(runId))
      .filter(
        (entry): entry is SubagentRunRecord =>
          Boolean(entry?.requesterSettleWake) &&
          entry?.requesterSettleWake?.rearmGeneration === state.rearmGeneration,
      );
    const previousStates = entries.map((entry) => structuredClone(entry.requesterSettleWake));
    for (const entry of entries) {
      entry.requesterSettleWake = {
        ...state,
        ...(entry.requesterSettleWake?.retireAfterSettle === true
          ? { retireAfterSettle: true }
          : {}),
      };
    }
    try {
      params.persistOrThrow();
    } catch (error) {
      entries.forEach((entry, index) => {
        entry.requesterSettleWake = previousStates[index];
      });
      throw error;
    }
  };

  const completeRequesterSettleWakeBatch = (
    runIds: readonly string[],
    rearmGeneration?: number,
  ) => {
    const entries = runIds
      .map((runId) => [runId, params.runs.get(runId)] as const)
      .filter(
        (pair): pair is readonly [string, SubagentRunRecord] =>
          Boolean(pair[1]?.requesterSettleWake) &&
          pair[1]?.requesterSettleWake?.rearmGeneration === rearmGeneration,
      );
    const requesterSessionKeys = new Set(entries.map(([, entry]) => entry.requesterSessionKey));
    const previousStates = entries.map(([, entry]) => ({
      requesterSettleWake: structuredClone(entry.requesterSettleWake),
      retireAfterRequesterTurn: entry.retireAfterRequesterTurn,
    }));
    for (const [runId, entry] of entries) {
      if (entry.requesterTurnRunId) {
        entry.retireAfterRequesterTurn =
          entry.retireAfterRequesterTurn === true ||
          entry.requesterSettleWake?.retireAfterSettle === true
            ? true
            : undefined;
        entry.requesterSettleWake = undefined;
      } else if (entry.requesterSettleWake?.retireAfterSettle === true) {
        params.runs.delete(runId);
      } else {
        entry.requesterSettleWake = undefined;
      }
    }
    try {
      params.persistOrThrow();
    } catch (error) {
      entries.forEach(([runId, entry], index) => {
        const previous = previousStates[index];
        params.runs.set(runId, entry);
        entry.requesterSettleWake = previous?.requesterSettleWake;
        entry.retireAfterRequesterTurn = previous?.retireAfterRequesterTurn;
      });
      throw error;
    }
    for (const [runId, entry] of entries) {
      const retryTimer = scheduledRequesterSettleWakeTimers.get(runId);
      if (retryTimer) {
        clearTimeout(retryTimer);
        scheduledRequesterSettleWakeTimers.delete(runId);
      }
      if (entry.requesterSettleWake === undefined || !params.runs.has(runId)) {
        params.resumedRuns.delete(runId);
        params.clearPendingLifecycleError(runId);
      }
    }
    for (const [runId, entry] of params.runs) {
      if (entry.requesterSettleWake && requesterSessionKeys.has(entry.requesterSessionKey)) {
        scheduleRequesterSettleWake(runId, entry);
      }
    }
  };

  const markRequesterSettleWakePending = (
    entry: SubagentRunRecord,
    options?: { retireAfterSettle?: boolean },
  ) => {
    const existing = entry.requesterSettleWake;
    entry.requesterSettleWake = {
      status: existing?.status ?? "pending",
      attemptCount: existing?.attemptCount ?? 0,
      ...(existing?.replayCount !== undefined ? { replayCount: existing.replayCount } : {}),
      ...(existing?.nextAttemptAt !== undefined ? { nextAttemptAt: existing.nextAttemptAt } : {}),
      ...(existing?.batchRunIds ? { batchRunIds: [...existing.batchRunIds] } : {}),
      ...(existing?.requesterYieldBatch === true ? { requesterYieldBatch: true } : {}),
      ...(existing?.afterRequesterYield === true ? { afterRequesterYield: true } : {}),
      ...(existing?.rearmGeneration !== undefined
        ? { rearmGeneration: existing.rearmGeneration }
        : {}),
      ...(existing?.lastError !== undefined ? { lastError: existing.lastError } : {}),
      ...(existing?.retireAfterSettle === true || options?.retireAfterSettle === true
        ? { retireAfterSettle: true }
        : {}),
    } satisfies RequesterSettleWakeState;
  };

  const persistRequesterSettleWakePending = (
    entry: SubagentRunRecord,
    options?: { cleanupCompletedAt?: number; retireAfterSettle?: boolean },
  ) => {
    const previousCleanupCompletedAt = entry.cleanupCompletedAt;
    const previousWake = structuredClone(entry.requesterSettleWake);
    if (options?.cleanupCompletedAt !== undefined) {
      entry.cleanupCompletedAt = options.cleanupCompletedAt;
    }
    markRequesterSettleWakePending(entry, options);
    try {
      params.persistOrThrow();
    } catch (error) {
      entry.cleanupCompletedAt = previousCleanupCompletedAt;
      entry.requesterSettleWake = previousWake;
      throw error;
    }
  };

  // Once a child reaches a terminal settle, let the announce layer decide
  // whether its requester's batch has fully drained and, if so, wake the
  // registry-less top-level requester to synthesize. Settle bookkeeping never
  // blocks on the wake, but the wake must run as tracked root work: a live
  // cleanup parent reserves the root synchronously, so restart or suspend
  // cannot reach quiescence between scheduling and the wake's gateway turn.
  // Failures are logged only.
  function scheduleRequesterSettleWakeRetry(runId: string, entry: SubagentRunRecord): void {
    const nextAttemptAt = entry.requesterSettleWake?.nextAttemptAt;
    if (
      nextAttemptAt === undefined ||
      nextAttemptAt <= Date.now() ||
      scheduledRequesterSettleWakeTimers.has(runId)
    ) {
      return;
    }
    const timer = setTimeout(
      () => {
        scheduledRequesterSettleWakeTimers.delete(runId);
        const current = params.runs.get(runId);
        if (current === entry && current.requesterSettleWake) {
          scheduleRequesterSettleWake(runId, current);
        }
      },
      Math.max(0, nextAttemptAt - Date.now()),
    );
    timer.unref?.();
    scheduledRequesterSettleWakeTimers.set(runId, timer);
  }

  function scheduleRequesterSettleWake(runId: string, entry: SubagentRunRecord): void {
    const requesterSessionKey = entry.requesterSessionKey?.trim();
    if (
      entry.collect ||
      !requesterSessionKey ||
      scheduledRequesterSettleWakeRuns.has(runId) ||
      scheduledRequesterSettleWakeTimers.has(runId)
    ) {
      return;
    }
    if ((entry.requesterSettleWake?.nextAttemptAt ?? 0) > Date.now()) {
      scheduleRequesterSettleWakeRetry(runId, entry);
      return;
    }
    scheduledRequesterSettleWakeRuns.add(runId);
    void runWithGatewayIndependentRootWorkContinuation(() =>
      params.maybeWakeRequesterAfterAllChildrenSettled({
        requesterSessionKey,
        requesterOrigin: entry.requesterOrigin,
        settledEntry: entry,
        transitionBatch: transitionRequesterSettleWakeBatch,
        completeBatch: completeRequesterSettleWakeBatch,
      }),
    )
      .catch((error: unknown) => {
        params.warn("requester settle wake failed", {
          error: buildSafeLifecycleErrorMeta(error),
          runId: maskRunId(runId),
          requesterSessionKey: maskSessionKey(requesterSessionKey),
        });
      })
      .finally(() => {
        scheduledRequesterSettleWakeRuns.delete(runId);
        const wasRearmedWhileRunning = pendingRequesterSettleWakeRearms.delete(runId);
        const current = params.runs.get(runId);
        if (current === entry && current.requesterSettleWake) {
          if (wasRearmedWhileRunning) {
            // A requester yield can freeze a delivered batch while this run is
            // resolving its earlier no-wake decision. Admit that durable update now.
            scheduleRequesterSettleWake(runId, current);
          } else {
            scheduleRequesterSettleWakeRetry(runId, current);
          }
        }
      });
  }

  const clearScheduledRequesterSettleWakeTimers = () => {
    for (const timer of scheduledRequesterSettleWakeTimers.values()) {
      clearTimeout(timer);
    }
    scheduledRequesterSettleWakeTimers.clear();
    pendingRequesterSettleWakeRearms.clear();
  };

  return {
    clearScheduledRequesterSettleWakeTimers,
    markRequesterSettleWakePending,
    persistRequesterSettleWakePending,
    resumeRequesterSettleWake: scheduleRequesterSettleWake,
    scheduleRequesterSettleWake,
    settleRequesterTurnAfterSessionSpawns: (args: {
      requesterSessionKey: string;
      requesterTurnRunId: string;
      requesterYielded: boolean;
      acceptedSessionSpawns: readonly AcceptedSessionSpawn[];
    }) =>
      settleRequesterTurnAfterSessionSpawns({
        ...args,
        runs: params.runs,
        persistOrThrow: () => params.persistOrThrow(),
        schedule: (runId, entry) => {
          if (scheduledRequesterSettleWakeRuns.has(runId)) {
            pendingRequesterSettleWakeRearms.add(runId);
            return;
          }
          scheduleRequesterSettleWake(runId, entry);
        },
      }),
  };
}

export type SubagentRegistryRequesterSettle = ReturnType<
  typeof createSubagentRegistryRequesterSettle
>;
