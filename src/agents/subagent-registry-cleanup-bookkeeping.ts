import {
  isGatewayRestartDraining,
  runWithGatewayIndependentRootWorkAdmission,
} from "../process/gateway-work-admission.js";
import { defaultRuntime } from "../runtime.js";
import { retireSessionMcpRuntimeForSessionKey } from "./agent-bundle-mcp-tools.js";
import { removeInternalSessionEffectsSession } from "./internal-session-effects.js";
import {
  ensureCompletionState,
  ensureDeliveryState,
  getDeliveryLastError,
  isDeliverySuspended,
} from "./subagent-delivery-state.js";
import {
  SUBAGENT_ENDED_REASON_COMPLETE,
  SUBAGENT_ENDED_REASON_KILLED,
} from "./subagent-lifecycle-events.js";
import { resolveCleanupCompletionReason } from "./subagent-registry-cleanup.js";
import {
  ANNOUNCE_EXPIRY_MS,
  logAnnounceGiveUp,
  MIN_ANNOUNCE_RETRY_DELAY_MS,
  safeRemoveAttachmentsDir,
} from "./subagent-registry-helpers.js";
import type {
  SubagentRegistryLifecycleParams,
  SubagentRegistryLifecycleShared,
} from "./subagent-registry-lifecycle-shared.js";
import type { SubagentRegistryRequesterSettle } from "./subagent-registry-requester-settle.js";
import type { SubagentRunRecord } from "./subagent-registry.types.js";

export function createSubagentRegistryCleanupBookkeeping(
  params: SubagentRegistryLifecycleParams,
  shared: SubagentRegistryLifecycleShared,
  requesterSettle: SubagentRegistryRequesterSettle,
) {
  const scheduledResumeTimers = new Set<ReturnType<typeof setTimeout>>();
  const cleanupGenerations = new WeakMap<SubagentRunRecord, number>();
  const {
    buildSafeLifecycleErrorMeta,
    clearPendingFinalDelivery,
    emitCompletionEndedHookIfNeeded,
    markPendingFinalDelivery,
    maskRunId,
    maskSessionKey,
    newerGenerationOwnsSession,
    safeMarkRequiredCompletionDeliveryBlocked,
    safeSetSubagentTaskDeliveryStatus,
  } = shared;
  const {
    markRequesterSettleWakePending,
    persistRequesterSettleWakePending,
    scheduleRequesterSettleWake,
  } = requesterSettle;

  const scheduleResumeSubagentRun = (
    runId: string,
    entry: SubagentRunRecord,
    delayMs: number,
    cleanupGeneration?: number,
  ) => {
    const timer = setTimeout(() => {
      scheduledResumeTimers.delete(timer);
      void runWithGatewayIndependentRootWorkAdmission(async () => {
        if (params.runs.get(runId) !== entry) {
          return;
        }
        if (cleanupGeneration !== undefined) {
          if (!isCleanupAttemptCurrent(runId, entry, cleanupGeneration)) {
            return;
          }
          entry.cleanupHandled = false;
          params.persist();
        }
        params.resumedRuns.delete(runId);
        params.resumeSubagentRun(runId);
      }).catch((err: unknown) => {
        defaultRuntime.log(`[warn] subagent cleanup resume failed (${runId}): ${String(err)}`);
        const current = params.runs.get(runId);
        if (
          isGatewayRestartDraining() &&
          current === entry &&
          typeof current.cleanupCompletedAt !== "number"
        ) {
          scheduleResumeSubagentRun(
            runId,
            entry,
            Math.max(delayMs, MIN_ANNOUNCE_RETRY_DELAY_MS),
            cleanupGeneration,
          );
        }
      });
    }, delayMs);
    timer.unref?.();
    scheduledResumeTimers.add(timer);
  };

  const runDetachedCleanupAttempt = (args: {
    runId: string;
    entry: SubagentRunRecord;
    cleanupGeneration: number;
    run: () => Promise<void>;
  }) => {
    // Completion makes the task projection non-blocking before delivery and
    // cleanup finish. This independent lease bridges that handoff and owns the
    // full detached attempt, including its final durable registry write.
    void runWithGatewayIndependentRootWorkAdmission(async () => {
      try {
        await args.run();
      } catch (err) {
        defaultRuntime.log(
          `[warn] subagent cleanup finalize failed (${args.runId}): ${String(err)}`,
        );
        const current = params.runs.get(args.runId);
        if (
          !current ||
          current.cleanupCompletedAt ||
          !isCleanupAttemptCurrent(args.runId, args.entry, args.cleanupGeneration)
        ) {
          return;
        }
        current.cleanupHandled = false;
        params.resumedRuns.delete(args.runId);
        params.persist();
      }
    }).catch((err: unknown) => {
      defaultRuntime.log(
        `[warn] subagent cleanup admission failed (${args.runId}): ${String(err)}`,
      );
      if (isGatewayRestartDraining()) {
        scheduleResumeSubagentRun(
          args.runId,
          args.entry,
          MIN_ANNOUNCE_RETRY_DELAY_MS,
          args.cleanupGeneration,
        );
      }
    });
  };

  const suspendPendingFinalDelivery = (args: {
    runId: string;
    entry: SubagentRunRecord;
    reason: "retry-limit" | "expiry";
    error?: string;
  }) => {
    const previousEntry = structuredClone(args.entry);
    markPendingFinalDelivery({
      entry: args.entry,
      error: args.error ?? getDeliveryLastError(args.entry) ?? args.reason,
    });
    const now = Date.now();
    const delivery = ensureDeliveryState(args.entry);
    delivery.status = "suspended";
    delivery.suspendedAt ??= now;
    delivery.suspendedReason = args.reason;
    args.entry.cleanupHandled = false;
    args.entry.wakeOnDescendantSettle = undefined;
    const completion = ensureCompletionState(args.entry);
    completion.fallbackResultText = undefined;
    completion.fallbackCapturedAt = undefined;
    params.resumedRuns.delete(args.runId);
    safeSetSubagentTaskDeliveryStatus({
      entry: args.entry,
      deliveryStatus: "failed",
      deliveryError: getDeliveryLastError(args.entry) ?? args.reason,
    });
    safeMarkRequiredCompletionDeliveryBlocked({
      entry: args.entry,
      reason: getDeliveryLastError(args.entry) ?? args.reason,
    });
    logAnnounceGiveUp(args.entry, args.reason);
    markRequesterSettleWakePending(args.entry);
    try {
      params.persistOrThrow();
    } catch (error) {
      const mutableEntry = args.entry as unknown as Record<string, unknown>;
      for (const key of Object.keys(mutableEntry)) {
        delete mutableEntry[key];
      }
      Object.assign(args.entry, previousEntry);
      throw error;
    }
    // Suspension is terminal for automatic retries, so it settles this child
    // for requester-drain purposes even though cleanup stays incomplete.
    scheduleRequesterSettleWake(args.runId, args.entry);
  };

  const shouldSuspendPendingFinalDelivery = (entry: SubagentRunRecord) =>
    entry.expectsCompletionMessage === true &&
    entry.cleanup === "keep" &&
    entry.endedReason === SUBAGENT_ENDED_REASON_COMPLETE &&
    entry.outcome?.status === "ok";

  const finalizeResumedAnnounceGiveUp = async (giveUpParams: {
    runId: string;
    entry: SubagentRunRecord;
    reason: "retry-limit" | "expiry";
  }) => {
    if (shouldSuspendPendingFinalDelivery(giveUpParams.entry)) {
      suspendPendingFinalDelivery({
        runId: giveUpParams.runId,
        entry: giveUpParams.entry,
        reason: giveUpParams.reason,
        error: getDeliveryLastError(giveUpParams.entry),
      });
      return;
    }
    const deliveryError = getDeliveryLastError(giveUpParams.entry) ?? giveUpParams.reason;
    clearPendingFinalDelivery(giveUpParams.entry);
    const failedDelivery = ensureDeliveryState(giveUpParams.entry);
    failedDelivery.status = "failed";
    failedDelivery.lastError = deliveryError;
    safeSetSubagentTaskDeliveryStatus({
      entry: giveUpParams.entry,
      deliveryStatus: "failed",
      deliveryError,
    });
    safeMarkRequiredCompletionDeliveryBlocked({
      entry: giveUpParams.entry,
      reason: deliveryError,
    });
    giveUpParams.entry.wakeOnDescendantSettle = undefined;
    const completion = ensureCompletionState(giveUpParams.entry);
    completion.fallbackResultText = undefined;
    completion.fallbackCapturedAt = undefined;
    const shouldDeleteAttachments =
      giveUpParams.entry.cleanup === "delete" || !giveUpParams.entry.retainAttachmentsOnKeep;
    if (shouldDeleteAttachments) {
      await safeRemoveAttachmentsDir(giveUpParams.entry);
    }
    const completionReason = resolveCleanupCompletionReason(giveUpParams.entry);
    logAnnounceGiveUp(giveUpParams.entry, giveUpParams.reason);
    // Retry-limit / expiry give-up should not leave cleanup stuck behind the
    // best-effort ended hook. Mark the run cleaned first, then fire the hook.
    completeCleanupBookkeeping({
      runId: giveUpParams.runId,
      entry: giveUpParams.entry,
      cleanup: giveUpParams.entry.cleanup,
      completedAt: Date.now(),
    });
    await emitCompletionEndedHookIfNeeded(giveUpParams.entry, completionReason, () =>
      isEndedHookOwnerCurrent(giveUpParams.runId, giveUpParams.entry),
    );
  };

  const beginSubagentCleanup = (runId: string) => {
    const entry = params.runs.get(runId);
    if (!entry) {
      return false;
    }
    if (entry.cleanupCompletedAt || entry.cleanupHandled) {
      return false;
    }
    entry.cleanupHandled = true;
    cleanupGenerations.set(entry, (cleanupGenerations.get(entry) ?? 0) + 1);
    params.persist();
    return true;
  };

  const isCleanupAttemptCurrent = (
    runId: string,
    entry: SubagentRunRecord,
    generation: number,
  ): boolean =>
    params.runs.get(runId) === entry &&
    entry.cleanupHandled === true &&
    entry.pauseReason !== "sessions_yield" &&
    cleanupGenerations.get(entry) === generation &&
    !newerGenerationOwnsSession(entry);

  const retireSupersededCleanupIfNeeded = async (
    runId: string,
    entry: SubagentRunRecord,
    generation: number,
  ): Promise<boolean> => {
    if (
      params.runs.get(runId) !== entry ||
      cleanupGenerations.get(entry) !== generation ||
      !newerGenerationOwnsSession(entry)
    ) {
      return false;
    }
    // Cleanup can yield to attachment, mirror, or announce work. A successor
    // registered while it was suspended owns every session-scoped side effect.
    await params.retireSupersededRun(runId, entry);
    params.persist();
    return true;
  };

  const retireSupersededCleanupInBackground = (
    runId: string,
    entry: SubagentRunRecord,
    generation: number,
  ) => {
    // Delivery callbacks are synchronous and may arrive after their announce
    // attempt returns. Give the async retirement tail its own snapshot blocker.
    void runWithGatewayIndependentRootWorkAdmission(async () => {
      await retireSupersededCleanupIfNeeded(runId, entry, generation);
    }).catch((error: unknown) => {
      defaultRuntime.log(
        `[warn] subagent superseded cleanup retirement failed (${runId}): ${String(error)}`,
      );
    });
  };

  const isEndedHookOwnerCurrent = (runId: string, entry: SubagentRunRecord): boolean => {
    const current = params.runs.get(runId);
    return (current === undefined || current === entry) && !newerGenerationOwnsSession(entry);
  };

  const retryDeferredCompletedAnnounces = (excludeRunId?: string) => {
    const now = Date.now();
    for (const [runId, entry] of params.runs.entries()) {
      if (excludeRunId && runId === excludeRunId) {
        continue;
      }
      if (typeof entry.endedAt !== "number") {
        continue;
      }
      if (entry.cleanupCompletedAt || entry.cleanupHandled) {
        continue;
      }
      if (isDeliverySuspended(entry)) {
        continue;
      }
      if (params.suppressAnnounceForSteerRestart(entry)) {
        continue;
      }
      const endedAgo = now - (entry.endedAt ?? now);
      if (entry.expectsCompletionMessage !== true && endedAgo > ANNOUNCE_EXPIRY_MS) {
        if (!beginSubagentCleanup(runId)) {
          continue;
        }
        runDetachedCleanupAttempt({
          runId,
          entry,
          cleanupGeneration: cleanupGenerations.get(entry)!,
          run: async () => {
            await finalizeResumedAnnounceGiveUp({
              runId,
              entry,
              reason: "expiry",
            });
          },
        });
        continue;
      }
      params.resumedRuns.delete(runId);
      params.resumeSubagentRun(runId);
    }
  };

  const completeCleanupBookkeeping = (cleanupParams: {
    runId: string;
    entry: SubagentRunRecord;
    cleanup: "delete" | "keep";
    completedAt: number;
    preserveTranscript?: boolean;
    provisionalKill?: boolean;
    // Set by the suspended-delivery discard path: the settle wake already ran
    // when the delivery was suspended, so a discard hours later must not
    // re-evaluate the requester drain.
    skipRequesterSettleWake?: boolean;
  }) => {
    const runCleanupTail = (label: string, run: () => Promise<unknown>) => {
      // These best-effort tails can outlive the durable registry transition,
      // but they still mutate session-owned resources and must block snapshots.
      void runWithGatewayIndependentRootWorkAdmission(run).catch((error: unknown) => {
        defaultRuntime.log(
          `[warn] subagent ${label} failed (${cleanupParams.runId}): ${String(error)}`,
        );
      });
    };
    if (!cleanupParams.preserveTranscript) {
      runCleanupTail("session cleanup", async () => {
        await removeInternalSessionEffectsSession(cleanupParams.entry.execution?.transcriptTarget);
      });
    }
    if (cleanupParams.entry.spawnMode !== "session") {
      runCleanupTail("bundle MCP cleanup", async () => {
        await retireSessionMcpRuntimeForSessionKey({
          sessionKey: cleanupParams.entry.childSessionKey,
          reason: "subagent-run-cleanup",
          preserveActiveLeases: true,
          onError: (error, sessionId) => {
            params.warn("failed to retire subagent bundle MCP runtime", {
              error: buildSafeLifecycleErrorMeta(error),
              sessionId,
              runId: maskRunId(cleanupParams.runId),
              childSessionKey: maskSessionKey(cleanupParams.entry.childSessionKey),
            });
          },
        });
      });
    }
    if (cleanupParams.provisionalKill) {
      // The provider result or bounded kill reconciliation owns terminal settle.
      // Waking here could tell the requester to finalize while the child still runs.
      return;
    }
    if (cleanupParams.entry.collect) {
      // Delete-mode session cleanup already ran before this durable bookkeeping.
      // Preserve only the collector result tombstone for waits and group caps.
      if (cleanupParams.cleanup === "delete") {
        params.clearPendingLifecycleError(cleanupParams.runId);
        runCleanupTail("context-engine cleanup", async () => {
          await params.notifyContextEngineSubagentEnded({
            childSessionKey: cleanupParams.entry.childSessionKey,
            reason: "deleted",
            agentDir: cleanupParams.entry.agentDir,
            workspaceDir: cleanupParams.entry.workspaceDir,
          });
        });
      }
      cleanupParams.entry.cleanupCompletedAt = cleanupParams.completedAt;
      cleanupParams.entry.requesterSettleWake = undefined;
      params.persist();
      retryDeferredCompletedAnnounces(cleanupParams.runId);
      return;
    }
    if (cleanupParams.cleanup === "delete") {
      params.clearPendingLifecycleError(cleanupParams.runId);
      runCleanupTail("context-engine cleanup", async () => {
        await params.notifyContextEngineSubagentEnded({
          childSessionKey: cleanupParams.entry.childSessionKey,
          reason: "deleted",
          agentDir: cleanupParams.entry.agentDir,
          workspaceDir: cleanupParams.entry.workspaceDir,
        });
      });
      if (cleanupParams.skipRequesterSettleWake) {
        params.runs.delete(cleanupParams.runId);
        params.persist();
        retryDeferredCompletedAnnounces(cleanupParams.runId);
        return;
      }
      persistRequesterSettleWakePending(cleanupParams.entry, {
        cleanupCompletedAt: cleanupParams.completedAt,
        retireAfterSettle: true,
      });
      retryDeferredCompletedAnnounces(cleanupParams.runId);
      scheduleRequesterSettleWake(cleanupParams.runId, cleanupParams.entry);
      return;
    }
    runCleanupTail("context-engine cleanup", async () => {
      await params.notifyContextEngineSubagentEnded({
        childSessionKey: cleanupParams.entry.childSessionKey,
        reason: "completed",
        agentDir: cleanupParams.entry.agentDir,
        workspaceDir: cleanupParams.entry.workspaceDir,
      });
    });
    if (
      cleanupParams.entry.endedReason === SUBAGENT_ENDED_REASON_KILLED &&
      cleanupParams.entry.suppressAnnounceReason !== "killed"
    ) {
      // A reconciled killed row has served its tombstone purpose. Retire only
      // the registry record; keep-mode still preserves the child session.
      params.clearPendingLifecycleError(cleanupParams.runId);
      if (cleanupParams.skipRequesterSettleWake) {
        params.runs.delete(cleanupParams.runId);
        params.persist();
        retryDeferredCompletedAnnounces(cleanupParams.runId);
        return;
      }
      persistRequesterSettleWakePending(cleanupParams.entry, {
        cleanupCompletedAt: cleanupParams.completedAt,
        retireAfterSettle: true,
      });
      retryDeferredCompletedAnnounces(cleanupParams.runId);
      scheduleRequesterSettleWake(cleanupParams.runId, cleanupParams.entry);
      return;
    }
    if (!cleanupParams.skipRequesterSettleWake) {
      persistRequesterSettleWakePending(cleanupParams.entry, {
        cleanupCompletedAt: cleanupParams.completedAt,
      });
    } else {
      cleanupParams.entry.cleanupCompletedAt = cleanupParams.completedAt;
      params.persist();
    }
    retryDeferredCompletedAnnounces(cleanupParams.runId);
    if (!cleanupParams.skipRequesterSettleWake) {
      scheduleRequesterSettleWake(cleanupParams.runId, cleanupParams.entry);
    }
  };

  const retireRunModeBundleMcpRuntime = async (cleanupParams: {
    runId: string;
    entry: SubagentRunRecord;
    reason: string;
  }) => {
    if (cleanupParams.entry.spawnMode === "session") {
      return;
    }
    await retireSessionMcpRuntimeForSessionKey({
      sessionKey: cleanupParams.entry.childSessionKey,
      reason: cleanupParams.reason,
      preserveActiveLeases: true,
      onError: (error, sessionId) => {
        params.warn("failed to retire subagent bundle MCP runtime", {
          error: buildSafeLifecycleErrorMeta(error),
          sessionId,
          runId: maskRunId(cleanupParams.runId),
          childSessionKey: maskSessionKey(cleanupParams.entry.childSessionKey),
        });
      },
    });
  };

  const clearScheduledResumeTimers = () => {
    for (const timer of scheduledResumeTimers) {
      clearTimeout(timer);
    }
    scheduledResumeTimers.clear();
  };

  const invalidateCleanupAttempt = (entry: SubagentRunRecord) => {
    cleanupGenerations.set(entry, (cleanupGenerations.get(entry) ?? 0) + 1);
  };

  return {
    beginSubagentCleanup,
    clearScheduledResumeTimers,
    completeCleanupBookkeeping,
    finalizeResumedAnnounceGiveUp,
    getCleanupGeneration: (entry: SubagentRunRecord) => cleanupGenerations.get(entry),
    invalidateCleanupAttempt,
    isCleanupAttemptCurrent,
    isEndedHookOwnerCurrent,
    retireRunModeBundleMcpRuntime,
    retireSupersededCleanupIfNeeded,
    retireSupersededCleanupInBackground,
    runDetachedCleanupAttempt,
    scheduleResumeSubagentRun,
    shouldSuspendPendingFinalDelivery,
    suspendPendingFinalDelivery,
  };
}

export type SubagentRegistryCleanupBookkeeping = ReturnType<
  typeof createSubagentRegistryCleanupBookkeeping
>;
