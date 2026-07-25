import { defaultRuntime } from "../runtime.js";
import { normalizeDeliveryContext } from "../utils/delivery-context.shared.js";
import {
  ensureCompletionState,
  ensureDeliveryState,
  getDeliveryLastError,
} from "./subagent-delivery-state.js";
import type { SubagentRegistryCleanupBookkeeping } from "./subagent-registry-cleanup-bookkeeping.js";
import {
  resolveCleanupCompletionReason,
  resolveDeferredCleanupDecision,
} from "./subagent-registry-cleanup.js";
import {
  ANNOUNCE_COMPLETION_HARD_EXPIRY_MS,
  ANNOUNCE_EXPIRY_MS,
  logAnnounceGiveUp,
  MAX_ANNOUNCE_RETRY_COUNT,
  MIN_ANNOUNCE_RETRY_DELAY_MS,
  resolveAnnounceRetryDelayMs,
  safeRemoveAttachmentsDir,
} from "./subagent-registry-helpers.js";
import type {
  RunSubagentAnnounceFlow,
  SubagentRegistryLifecycleParams,
  SubagentRegistryLifecycleShared,
} from "./subagent-registry-lifecycle-shared.js";
import type { SubagentRunRecord } from "./subagent-registry.types.js";
import { deleteSubagentSessionForCleanup } from "./subagent-session-cleanup.js";

export function createSubagentRegistryAnnounceCleanup(
  params: SubagentRegistryLifecycleParams,
  shared: SubagentRegistryLifecycleShared,
  cleanupBookkeeping: SubagentRegistryCleanupBookkeeping,
) {
  const {
    buildSafeLifecycleErrorMeta,
    clearPendingFinalDelivery,
    emitCompletionEndedHookIfNeeded,
    formatAnnounceDeliveryError,
    hasPriorRequesterDeliveryMirror,
    loadPendingFinalDeliveryPayload,
    markPendingFinalDelivery,
    maskRunId,
    maskSessionKey,
    recordAnnounceDeliveryResult,
    safeMarkRequiredCompletionDeliveryBlocked,
    safeSetSubagentTaskDeliveryStatus,
  } = shared;
  const {
    beginSubagentCleanup,
    completeCleanupBookkeeping,
    getCleanupGeneration,
    isCleanupAttemptCurrent,
    isEndedHookOwnerCurrent,
    retireSupersededCleanupIfNeeded,
    retireSupersededCleanupInBackground,
    runDetachedCleanupAttempt,
    scheduleResumeSubagentRun,
    shouldSuspendPendingFinalDelivery,
    suspendPendingFinalDelivery,
  } = cleanupBookkeeping;

  const finalizeSubagentCleanup = async (
    runId: string,
    cleanup: "delete" | "keep",
    didAnnounce: boolean,
    cleanupGeneration: number,
    options?: {
      skipAnnounce?: boolean;
      skipDeliveryStatus?: boolean;
      skipRequesterDelivery?: boolean;
    },
  ) => {
    const entry = params.runs.get(runId);
    if (!entry) {
      return;
    }
    if (!isCleanupAttemptCurrent(runId, entry, cleanupGeneration)) {
      await retireSupersededCleanupIfNeeded(runId, entry, cleanupGeneration);
      return;
    }
    if (entry.expectsCompletionMessage === false || options?.skipRequesterDelivery) {
      clearPendingFinalDelivery(entry);
      if (options?.skipRequesterDelivery) {
        ensureDeliveryState(entry).status = "not_required";
        entry.suppressCompletionDelivery = undefined;
      }
      entry.wakeOnDescendantSettle = undefined;
      const shouldDeleteAttachments = cleanup === "delete" || !entry.retainAttachmentsOnKeep;
      if (shouldDeleteAttachments) {
        await safeRemoveAttachmentsDir(entry);
      }
      if (!isCleanupAttemptCurrent(runId, entry, cleanupGeneration)) {
        await retireSupersededCleanupIfNeeded(runId, entry, cleanupGeneration);
        return;
      }
      completeCleanupBookkeeping({
        runId,
        entry,
        cleanup,
        completedAt: Date.now(),
      });
      await emitCompletionEndedHookIfNeeded(entry, resolveCleanupCompletionReason(entry), () =>
        isEndedHookOwnerCurrent(runId, entry),
      );
      return;
    }
    if (didAnnounce) {
      const delivery = ensureDeliveryState(entry);
      const shouldCreditDelivery =
        !options?.skipAnnounce ||
        delivery.status === "delivered" ||
        typeof delivery.announcedAt === "number";
      if (shouldCreditDelivery) {
        const deliveredAt = delivery.deliveredAt ?? delivery.announcedAt ?? Date.now();
        delivery.status = "delivered";
        delivery.deliveredAt = deliveredAt;
        delivery.announcedAt = delivery.announcedAt ?? deliveredAt;
        if (!options?.skipAnnounce) {
          delivery.announcedAt = deliveredAt;
          params.persist();
        }
      }
      clearPendingFinalDelivery(entry);
      const finalDelivery = ensureDeliveryState(entry);
      if (shouldCreditDelivery) {
        finalDelivery.status = "delivered";
        finalDelivery.suspendedAt = undefined;
        finalDelivery.suspendedReason = undefined;
      }
      if (shouldCreditDelivery && !options?.skipDeliveryStatus) {
        safeSetSubagentTaskDeliveryStatus({
          entry,
          deliveryStatus: "delivered",
        });
      }
      finalDelivery.lastError = undefined;
      finalDelivery.lastDropReason = undefined;
      entry.wakeOnDescendantSettle = undefined;
      const completion = ensureCompletionState(entry);
      completion.fallbackResultText = undefined;
      completion.fallbackCapturedAt = undefined;
      const completionReason = resolveCleanupCompletionReason(entry);
      const shouldDeleteAttachments = cleanup === "delete" || !entry.retainAttachmentsOnKeep;
      if (shouldDeleteAttachments) {
        await safeRemoveAttachmentsDir(entry);
      }
      if (!isCleanupAttemptCurrent(runId, entry, cleanupGeneration)) {
        await retireSupersededCleanupIfNeeded(runId, entry, cleanupGeneration);
        return;
      }
      completeCleanupBookkeeping({
        runId,
        entry,
        cleanup,
        completedAt: Date.now(),
      });
      // Hook loading is best-effort; durable delivery and cleanup must already
      // be terminal before plugin code can fail or stall.
      await emitCompletionEndedHookIfNeeded(entry, completionReason, () =>
        isEndedHookOwnerCurrent(runId, entry),
      );
      return;
    }

    const now = Date.now();
    const deferredDecision = resolveDeferredCleanupDecision({
      entry,
      now,
      activeDescendantRuns: Math.max(0, params.countPendingDescendantRuns(entry.childSessionKey)),
      announceExpiryMs: ANNOUNCE_EXPIRY_MS,
      announceCompletionHardExpiryMs: ANNOUNCE_COMPLETION_HARD_EXPIRY_MS,
      maxAnnounceRetryCount: MAX_ANNOUNCE_RETRY_COUNT,
      deferDescendantDelayMs: MIN_ANNOUNCE_RETRY_DELAY_MS,
      resolveAnnounceRetryDelayMs,
    });

    if (deferredDecision.kind === "defer-descendants") {
      ensureDeliveryState(entry).lastAttemptAt = now;
      entry.wakeOnDescendantSettle = true;
      entry.cleanupHandled = false;
      params.resumedRuns.delete(runId);
      params.persist();
      scheduleResumeSubagentRun(runId, entry, deferredDecision.delayMs);
      return;
    }

    if (deferredDecision.kind === "give-up") {
      if (shouldSuspendPendingFinalDelivery(entry)) {
        suspendPendingFinalDelivery({
          runId,
          entry,
          reason: deferredDecision.reason,
          error: getDeliveryLastError(entry),
        });
        return;
      }
      const deliveryError = getDeliveryLastError(entry) ?? deferredDecision.reason;
      clearPendingFinalDelivery(entry);
      const failedDelivery = ensureDeliveryState(entry);
      failedDelivery.status = "failed";
      failedDelivery.lastError = deliveryError;
      if (deferredDecision.retryCount != null) {
        failedDelivery.attemptCount = deferredDecision.retryCount;
        failedDelivery.lastAttemptAt = now;
      }
      safeSetSubagentTaskDeliveryStatus({
        entry,
        deliveryStatus: "failed",
        deliveryError,
      });
      safeMarkRequiredCompletionDeliveryBlocked({
        entry,
        reason: deliveryError,
      });
      entry.wakeOnDescendantSettle = undefined;
      const completion = ensureCompletionState(entry);
      completion.fallbackResultText = undefined;
      completion.fallbackCapturedAt = undefined;
      const shouldDeleteAttachments = cleanup === "delete" || !entry.retainAttachmentsOnKeep;
      if (shouldDeleteAttachments) {
        await safeRemoveAttachmentsDir(entry);
      }
      if (!isCleanupAttemptCurrent(runId, entry, cleanupGeneration)) {
        await retireSupersededCleanupIfNeeded(runId, entry, cleanupGeneration);
        return;
      }
      const completionReason = resolveCleanupCompletionReason(entry);
      logAnnounceGiveUp(entry, deferredDecision.reason);
      // Giving up on announce delivery is terminal for cleanup even if the
      // best-effort hook is still resolving.
      completeCleanupBookkeeping({
        runId,
        entry,
        cleanup,
        completedAt: now,
      });
      await emitCompletionEndedHookIfNeeded(entry, completionReason, () =>
        isEndedHookOwnerCurrent(runId, entry),
      );
      return;
    }

    markPendingFinalDelivery({
      entry,
      error: didAnnounce ? undefined : "announce deferred or direct delivery failed",
    });
    entry.cleanupHandled = false;
    params.resumedRuns.delete(runId);
    params.persist();
    if (deferredDecision.resumeDelayMs == null) {
      return;
    }
    scheduleResumeSubagentRun(runId, entry, deferredDecision.resumeDelayMs);
  };

  const startSubagentAnnounceCleanupFlow = (runId: string, entry: SubagentRunRecord): boolean => {
    if (entry.killReconciliation) {
      // Restores and unrelated cleanup retries must not publish a provisional
      // kill. The sweeper re-enters here after durable reconciliation.
      return false;
    }
    const cleanup = entry.cleanup;
    if (typeof entry.delivery?.announcedAt === "number" || entry.delivery?.status === "delivered") {
      if (!beginSubagentCleanup(runId)) {
        return false;
      }
      const cleanupGeneration = getCleanupGeneration(entry)!;
      runDetachedCleanupAttempt({
        runId,
        entry,
        cleanupGeneration,
        run: async () => {
          await finalizeSubagentCleanup(runId, cleanup, true, cleanupGeneration, {
            skipAnnounce: true,
          });
        },
      });
      return true;
    }
    if (!beginSubagentCleanup(runId)) {
      return false;
    }
    const cleanupGeneration = getCleanupGeneration(entry)!;
    const skipRequesterDelivery = entry.suppressCompletionDelivery === true;
    if (entry.expectsCompletionMessage === false || skipRequesterDelivery) {
      runDetachedCleanupAttempt({
        runId,
        entry,
        cleanupGeneration,
        run: async () => {
          // This driver is detached. Yield once so synchronous successor
          // registration can invalidate it before sessions.delete is submitted.
          await Promise.resolve();
          if (!isCleanupAttemptCurrent(runId, entry, cleanupGeneration)) {
            await retireSupersededCleanupIfNeeded(runId, entry, cleanupGeneration);
            return;
          }
          if (cleanup === "delete") {
            // This durable boundary prevents a late yield from reviving a run
            // after deletion may already have reached the gateway.
            entry.deleteCleanupDispatchedAt ??= Date.now();
            params.persist();
            await deleteSubagentSessionForCleanup({
              callGateway: params.callGateway,
              childSessionKey: entry.childSessionKey,
              spawnMode: entry.spawnMode,
              onError: (error) =>
                params.warn("sessions.delete failed during subagent cleanup", {
                  error: buildSafeLifecycleErrorMeta(error),
                  runId: maskRunId(runId),
                  childSessionKey: maskSessionKey(entry.childSessionKey),
                }),
            });
          }
          if (!isCleanupAttemptCurrent(runId, entry, cleanupGeneration)) {
            await retireSupersededCleanupIfNeeded(runId, entry, cleanupGeneration);
            return;
          }
          await finalizeSubagentCleanup(runId, cleanup, true, cleanupGeneration, {
            skipAnnounce: true,
            skipDeliveryStatus: true,
            skipRequesterDelivery,
          });
        },
      });
      return true;
    }
    const pendingPayload = loadPendingFinalDeliveryPayload(entry);
    const requesterOrigin = normalizeDeliveryContext(pendingPayload.requesterOrigin);
    let latestDeliveryError = getDeliveryLastError(entry);
    const finalizeAnnounceCleanup = async (didAnnounce: boolean) => {
      if (!isCleanupAttemptCurrent(runId, entry, cleanupGeneration)) {
        await retireSupersededCleanupIfNeeded(runId, entry, cleanupGeneration);
        return;
      }
      const shouldCreditPriorDelivery =
        !didAnnounce && (await hasPriorRequesterDeliveryMirror(entry));
      if (!isCleanupAttemptCurrent(runId, entry, cleanupGeneration)) {
        await retireSupersededCleanupIfNeeded(runId, entry, cleanupGeneration);
        return;
      }
      if (shouldCreditPriorDelivery) {
        latestDeliveryError = undefined;
      }
      if (!didAnnounce && latestDeliveryError) {
        ensureDeliveryState(entry).lastError = latestDeliveryError;
      }
      await finalizeSubagentCleanup(
        runId,
        cleanup,
        didAnnounce || shouldCreditPriorDelivery,
        cleanupGeneration,
      );
    };

    const announceParams: Parameters<RunSubagentAnnounceFlow>[0] = {
      childSessionKey: pendingPayload.childSessionKey,
      childRunId: pendingPayload.childRunId,
      requesterSessionKey: pendingPayload.requesterSessionKey,
      requesterOrigin,
      requesterDisplayKey: pendingPayload.requesterDisplayKey,
      task: pendingPayload.task,
      timeoutMs: params.subagentAnnounceTimeoutMs,
      cleanup,
      roundOneReply: pendingPayload.frozenResultText ?? undefined,
      fallbackReply: pendingPayload.fallbackFrozenResultText ?? undefined,
      waitForCompletion: false,
      startedAt: pendingPayload.startedAt,
      endedAt: pendingPayload.endedAt,
      label: pendingPayload.label,
      outcome: pendingPayload.outcome,
      spawnMode: pendingPayload.spawnMode,
      expectsCompletionMessage: pendingPayload.expectsCompletionMessage,
      wakeOnDescendantSettle: pendingPayload.wakeOnDescendantSettle === true,
      onBeforeDeleteChildSession:
        cleanup === "delete"
          ? () => {
              if (!isCleanupAttemptCurrent(runId, entry, cleanupGeneration)) {
                return false;
              }
              // Announce owns delete submission; fence late yields at the
              // exact handoff instead of when cleanup merely starts.
              entry.deleteCleanupDispatchedAt ??= Date.now();
              params.persist();
              return true;
            }
          : undefined,
      onDeliveryResult: (delivery) => {
        if (!isCleanupAttemptCurrent(runId, entry, cleanupGeneration)) {
          retireSupersededCleanupInBackground(runId, entry, cleanupGeneration);
          return;
        }
        recordAnnounceDeliveryResult(entry, delivery);
        if (delivery.delivered) {
          const deliveryState = ensureDeliveryState(entry);
          if (deliveryState.lastError !== undefined) {
            deliveryState.lastError = undefined;
            params.persist();
          }
          latestDeliveryError = undefined;
          return;
        }
        if (delivery.path === "none") {
          ensureDeliveryState(entry).lastDropReason = "sink_unavailable";
        }
        latestDeliveryError = formatAnnounceDeliveryError(delivery);
        if (ensureDeliveryState(entry).lastError !== latestDeliveryError) {
          ensureDeliveryState(entry).lastError = latestDeliveryError;
          params.persist();
        }
      },
    };
    runDetachedCleanupAttempt({
      runId,
      entry,
      cleanupGeneration,
      run: async () => {
        let didAnnounce = false;
        try {
          didAnnounce = await params.runSubagentAnnounceFlow(announceParams);
        } catch (error) {
          defaultRuntime.log(
            `[warn] Subagent announce flow failed during cleanup for run ${runId}: ${String(error)}`,
          );
        }
        await finalizeAnnounceCleanup(didAnnounce);
      },
    });
    return true;
  };

  return {
    startSubagentAnnounceCleanupFlow,
  };
}
