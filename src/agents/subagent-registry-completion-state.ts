import type { DetachedTaskFindResult } from "../tasks/detached-task-runtime-contract.js";
import { isProvisionalSubagentKillTask } from "../tasks/task-cancellation-state.js";
import { type SubagentRunOutcome, withSubagentOutcomeTiming } from "./subagent-announce-output.js";
import { clearDeliveryState, ensureCompletionState } from "./subagent-delivery-state.js";
import {
  SUBAGENT_ENDED_REASON_COMPLETE,
  SUBAGENT_ENDED_REASON_ERROR,
  SUBAGENT_ENDED_REASON_KILLED,
  type SubagentLifecycleEndedReason,
} from "./subagent-lifecycle-events.js";
import type { SubagentRegistryCleanupBookkeeping } from "./subagent-registry-cleanup-bookkeeping.js";
import {
  resolveKilledSubagentTaskEndedAt,
  shouldUpdateRunOutcome,
} from "./subagent-registry-completion.js";
import type {
  SubagentRegistryLifecycleParams,
  SubagentRegistryLifecycleShared,
} from "./subagent-registry-lifecycle-shared.js";
import type { SubagentCompletionRequest, SubagentRunRecord } from "./subagent-registry.types.js";
import {
  resolveSubagentRunEffectiveEndedAt,
  resolveSubagentRunDeadlineMs,
} from "./subagent-run-timeout.js";
import { updateSwarmCollectorCompletion } from "./swarm-collector.js";
import { peekSwarmStructuredOutput } from "./tools/structured-output-tool.js";

export type PreparedSubagentRunCompletion = {
  entry: SubagentRunRecord;
  terminalGeneration: number;
  mutated: boolean;
  completionReason: SubagentLifecycleEndedReason;
  sessionSuperseded: boolean;
};

export function createSubagentRegistryCompletionState(
  params: SubagentRegistryLifecycleParams,
  shared: SubagentRegistryLifecycleShared,
  cleanupBookkeeping: SubagentRegistryCleanupBookkeeping,
) {
  const terminalCompletionLocks = new Map<string, Promise<void>>();
  const terminalGenerations = new WeakMap<SubagentRunRecord, number>();
  const {
    freezeRunResultAtCompletion,
    newerGenerationOwnsSession,
    refreshPendingFinalDeliveryPayload,
    safeFinalizeSubagentTaskRun,
  } = shared;

  function shouldPreservePublishedExplicitRunTimeout(args: { entry: SubagentRunRecord }): boolean {
    if (
      typeof args.entry.runTimeoutSeconds !== "number" ||
      !Number.isFinite(args.entry.runTimeoutSeconds) ||
      args.entry.runTimeoutSeconds <= 0 ||
      args.entry.outcome?.status !== "timeout" ||
      typeof args.entry.endedAt !== "number"
    ) {
      return false;
    }
    const deadlineMs = resolveSubagentRunDeadlineMs(args.entry);
    if (deadlineMs === undefined || args.entry.endedAt < deadlineMs) {
      return false;
    }
    if (
      args.entry.cleanupHandled ||
      typeof args.entry.cleanupCompletedAt === "number" ||
      typeof args.entry.endedHookEmittedAt === "number" ||
      args.entry.delivery?.status === "delivered" ||
      typeof args.entry.delivery?.announcedAt === "number"
    ) {
      return true;
    }
    return false;
  }

  function resolveExpiredExplicitRunDeadlineMs(args: {
    entry: SubagentRunRecord;
    nextEndedAt: number;
    observedStartedAt?: number;
  }): number | undefined {
    const effectiveEndedAt = resolveSubagentRunEffectiveEndedAt(
      args.entry,
      args.nextEndedAt,
      args.observedStartedAt,
    );
    return effectiveEndedAt < args.nextEndedAt ? effectiveEndedAt : undefined;
  }

  function isOlderEquivalentTerminalCallback(args: {
    entry: SubagentRunRecord;
    endedAt: number;
    outcome: SubagentRunOutcome;
    reason: SubagentLifecycleEndedReason;
  }): boolean {
    const current = args.entry.outcome;
    if (
      typeof args.entry.endedAt !== "number" ||
      args.endedAt >= args.entry.endedAt ||
      args.entry.endedReason !== args.reason ||
      current?.status !== args.outcome.status
    ) {
      return false;
    }
    return (
      current.status !== "error" ||
      args.outcome.status !== "error" ||
      current.error === args.outcome.error
    );
  }

  const acquireTerminalCompletionLock = async (runId: string): Promise<() => void> => {
    const previous = terminalCompletionLocks.get(runId) ?? Promise.resolve();
    let releaseLock = () => {};
    const current = new Promise<void>((resolve) => {
      releaseLock = resolve;
    });
    terminalCompletionLocks.set(runId, current);
    await previous;
    return () => {
      releaseLock();
      if (terminalCompletionLocks.get(runId) === current) {
        terminalCompletionLocks.delete(runId);
      }
    };
  };

  const isTerminalCallbackCurrent = (
    runId: string,
    entry: SubagentRunRecord,
    generation: number,
  ): boolean =>
    params.runs.get(runId) === entry &&
    entry.pauseReason !== "sessions_yield" &&
    terminalGenerations.get(entry) === generation;
  const prepareSubagentRunCompletion = async (completeParams: SubagentCompletionRequest) => {
    const releaseCompletionLock = await acquireTerminalCompletionLock(completeParams.runId);
    let entry: SubagentRunRecord | undefined;
    let terminalGeneration = 0;
    let mutated = false;
    let completionReason = completeParams.reason;
    let sessionSuperseded = false;
    let suppressTaskFinalization: boolean;
    let provisionalKillSnapshot: SubagentRunRecord | undefined;
    let postCaptureTaskResolution: DetachedTaskFindResult | undefined;
    let entrySnapshot: SubagentRunRecord | undefined;
    try {
      params.clearPendingLifecycleError(completeParams.runId);
      entry = params.runs.get(completeParams.runId);
      if (!entry) {
        return;
      }
      const currentEntry = entry;
      entrySnapshot = structuredClone(entry);
      const restoreEntrySnapshot = (snapshot?: SubagentRunRecord) => {
        if (!snapshot) {
          return;
        }
        const target = currentEntry as unknown as Record<string, unknown>;
        for (const key of Object.keys(target)) {
          delete target[key];
        }
        Object.assign(target, snapshot);
      };
      const recoveryRequested = completeParams.recoverInterrupted === true;
      if (!recoveryRequested && entry.terminalOwner === "interrupted-recovery") {
        // Restart recovery already persisted the terminal winner for this exact
        // run. Late provider/lifecycle callbacks cannot reopen that decision.
        return;
      }
      if (recoveryRequested) {
        const ownsInterruptedRecovery = entry.terminalOwner === "interrupted-recovery";
        // Mismatched partial terminal evidence is an existing winner and must
        // not be overwritten. Exact normalized evidence may be the same recovery
        // request deferred by restart admission, so drain it.
        const hasTerminalEvidence =
          typeof entry.endedAt === "number" ||
          entry.outcome !== undefined ||
          entry.endedReason !== undefined ||
          entry.execution?.status === "terminal";
        const expectedElapsedMs =
          typeof currentEntry.startedAt === "number" && typeof completeParams.endedAt === "number"
            ? Math.max(0, completeParams.endedAt - currentEntry.startedAt)
            : undefined;
        const outcomeMatchesInterruptedRecovery = (outcome: SubagentRunOutcome | undefined) =>
          completeParams.outcome.status === "error" &&
          outcome?.status === "error" &&
          outcome.error === completeParams.outcome.error &&
          (outcome.startedAt === undefined || outcome.startedAt === currentEntry.startedAt) &&
          (outcome.endedAt === undefined || outcome.endedAt === completeParams.endedAt) &&
          (outcome.elapsedMs === undefined || outcome.elapsedMs === expectedElapsedMs);
        const executionMatchesInterruptedRecovery =
          entry.execution?.status !== "terminal" ||
          (entry.execution.endedAt === completeParams.endedAt &&
            (entry.execution.startedAt === undefined ||
              entry.execution.startedAt === currentEntry.startedAt) &&
            outcomeMatchesInterruptedRecovery(entry.execution.outcome));
        const matchesRequestedInterruptedTerminal =
          typeof completeParams.endedAt === "number" &&
          entry.endedAt === completeParams.endedAt &&
          outcomeMatchesInterruptedRecovery(entry.outcome) &&
          entry.endedReason === SUBAGENT_ENDED_REASON_ERROR &&
          executionMatchesInterruptedRecovery;
        if (
          !ownsInterruptedRecovery &&
          (entry.killReconciliation !== undefined ||
            entry.endedReason === SUBAGENT_ENDED_REASON_KILLED ||
            entry.pauseReason === "sessions_yield" ||
            typeof entry.cleanupCompletedAt === "number" ||
            (hasTerminalEvidence && !matchesRequestedInterruptedTerminal))
        ) {
          return;
        }
        if (!ownsInterruptedRecovery) {
          const endedAt =
            typeof completeParams.endedAt === "number" ? completeParams.endedAt : Date.now();
          const outcome = withSubagentOutcomeTiming(
            { status: "error", error: completeParams.outcome.error },
            { startedAt: entry.startedAt, endedAt },
          );
          entry.endedAt = endedAt;
          entry.outcome = outcome;
          entry.endedReason = SUBAGENT_ENDED_REASON_ERROR;
          entry.pauseReason = undefined;
          entry.execution = {
            ...entry.execution,
            status: "terminal",
            startedAt: entry.startedAt,
            endedAt,
            outcome,
            interruptedAt: undefined,
            interruptionReason: undefined,
          };
          entry.completion = {
            ...ensureCompletionState(entry),
            resultText: null,
            capturedAt: endedAt,
          };
          entry.cleanupHandled = false;
          entry.terminalOwner = "interrupted-recovery";
          mutated = true;
          try {
            params.persistOrThrow();
          } catch (error) {
            restoreEntrySnapshot(entrySnapshot);
            throw error;
          }
          // Any later delivery-payload write rolls back to this durable owner,
          // never to the pre-recovery running row.
          entrySnapshot = structuredClone(entry);
          mutated = false;
        }
      }
      sessionSuperseded = newerGenerationOwnsSession(currentEntry);
      if (
        completeParams.reason === SUBAGENT_ENDED_REASON_KILLED &&
        entry.endedReason !== undefined &&
        entry.endedReason !== SUBAGENT_ENDED_REASON_KILLED &&
        entry.outcome !== undefined
      ) {
        // Any finalized provider outcome is canonical. A delayed abort listener
        // must not replace success, failure, or timeout with a killed marker.
        return;
      }
      let requestedEndedAt =
        typeof completeParams.endedAt === "number" ? completeParams.endedAt : Date.now();
      if (
        shouldPreservePublishedExplicitRunTimeout({
          entry,
        })
      ) {
        return;
      }
      const shouldDrainExistingTerminal =
        recoveryRequested ||
        isOlderEquivalentTerminalCallback({
          entry,
          endedAt: requestedEndedAt,
          outcome: completeParams.outcome,
          reason: completeParams.reason,
        });
      if (shouldDrainExistingTerminal) {
        // Preserve the newer canonical timing while allowing this duplicate
        // caller to rescue a stalled cleanup and delivery tail.
        requestedEndedAt = entry.endedAt!;
        completionReason = entry.endedReason ?? completeParams.reason;
      }
      let endedAt = requestedEndedAt;
      let completionOutcome =
        shouldDrainExistingTerminal && entry.outcome ? entry.outcome : completeParams.outcome;
      const liveStructuredOutput = entry.collect
        ? (entry.structuredOutput ??
          peekSwarmStructuredOutput(entry.runId) ??
          (entry.swarmRunId ? peekSwarmStructuredOutput(entry.swarmRunId) : undefined))
        : undefined;
      if (!entry.structuredOutput && liveStructuredOutput) {
        entry.structuredOutput = liveStructuredOutput;
        mutated = true;
      }
      if (
        liveStructuredOutput?.structured !== undefined &&
        completionOutcome.status === "error" &&
        completionOutcome.error === "completed"
      ) {
        // Tool-only collector turns use this runner sentinel after the result is
        // durably recorded. Normalize before every task/session/hook projection.
        completionOutcome = { status: "ok" };
        completionReason = SUBAGENT_ENDED_REASON_COMPLETE;
      }
      const observedStartedAt =
        !shouldDrainExistingTerminal &&
        typeof completeParams.startedAt === "number" &&
        Number.isFinite(completeParams.startedAt)
          ? completeParams.startedAt
          : undefined;
      const expiredDeadlineMs = recoveryRequested
        ? undefined
        : resolveExpiredExplicitRunDeadlineMs({
            entry,
            nextEndedAt: endedAt,
            observedStartedAt,
          });
      if (expiredDeadlineMs !== undefined) {
        endedAt = expiredDeadlineMs;
        completionOutcome = { status: "timeout" };
        completionReason = SUBAGENT_ENDED_REASON_COMPLETE;
      }
      if (
        completionReason !== SUBAGENT_ENDED_REASON_KILLED &&
        entry.endedReason === SUBAGENT_ENDED_REASON_KILLED &&
        entry.killReconciliation === undefined
      ) {
        // Only current-version provisional kills carry reconciliation state.
        // Legacy or already-stabilized killed rows are terminal cancellation.
        return;
      }
      const isSteerRestartKill =
        completeParams.reason === SUBAGENT_ENDED_REASON_KILLED &&
        entry.suppressAnnounceReason === "steer-restart";
      suppressTaskFinalization = isSteerRestartKill;
      if (completionReason === SUBAGENT_ENDED_REASON_KILLED && !isSteerRestartKill) {
        entry.suppressAnnounceReason = "killed";
        entry.killReconciliation ??= {
          killedAt: requestedEndedAt,
        };
        mutated = true;
      }

      if (
        completionReason !== SUBAGENT_ENDED_REASON_KILLED &&
        entry.endedReason === SUBAGENT_ENDED_REASON_KILLED &&
        entry.killReconciliation !== undefined
      ) {
        const killReconciliation = entry.killReconciliation;
        const taskResolution = params.resolveSubagentTask(entry);
        const stableTaskCancellation =
          taskResolution.lookup === "available" &&
          taskResolution.task?.status === "cancelled" &&
          !isProvisionalSubagentKillTask(taskResolution.task);
        const cancellationEndedAt = resolveKilledSubagentTaskEndedAt(entry);
        const completionPredatesCancellation =
          typeof cancellationEndedAt === "number" && endedAt < cancellationEndedAt;
        if (stableTaskCancellation && !completionPredatesCancellation) {
          // tasks.cancel promotes the provisional marker to durable operator
          // intent. Only an already-durable earlier completion may reopen it.
          return;
        }
        provisionalKillSnapshot = structuredClone(currentEntry);
        // The sweeper uses marker identity to reject a concurrently replaced
        // kill generation. A completion rollback must retain the same marker.
        provisionalKillSnapshot.killReconciliation = killReconciliation;
        // Completion capture yields. Stage the provider result off-registry so
        // an unrelated persistence write cannot publish a tentative winner.
        entry = structuredClone(currentEntry);
        entry.suppressCompletionDelivery =
          killReconciliation.suppressTaskDelivery === true ? true : undefined;
        entry.suppressAnnounceReason = undefined;
        entry.killReconciliation = undefined;
        entry.cleanupHandled = false;
        entry.cleanupCompletedAt = undefined;
        clearDeliveryState(entry);
        mutated = true;
      }

      if (observedStartedAt !== undefined && entry.startedAt !== observedStartedAt) {
        entry.startedAt = observedStartedAt;
        if (typeof entry.sessionStartedAt !== "number") {
          entry.sessionStartedAt = observedStartedAt;
        }
        mutated = true;
      }

      if (
        completionReason === SUBAGENT_ENDED_REASON_COMPLETE &&
        completionOutcome.status !== "error" &&
        provisionalKillSnapshot !== undefined
      ) {
        // A killed lifecycle may freeze an empty result before the canonical end
        // wins. Preserve any reply already captured by an earlier successful callback.
        const completion = ensureCompletionState(entry);
        const hasCapturedReply =
          typeof completion.resultText === "string" && completion.resultText.trim().length > 0;
        if (
          !hasCapturedReply &&
          (completion.resultText !== undefined || completion.capturedAt !== undefined)
        ) {
          completion.resultText = undefined;
          completion.capturedAt = undefined;
          mutated = true;
        }
      }
      if (entry.endedAt !== endedAt) {
        entry.endedAt = endedAt;
        entry.execution = {
          ...entry.execution,
          status: "terminal",
          startedAt: entry.startedAt,
          endedAt,
        };
        mutated = true;
      }
      const outcome =
        recoveryRequested && entry.outcome
          ? entry.outcome
          : withSubagentOutcomeTiming(completionOutcome, {
              startedAt: entry.startedAt,
              endedAt,
            });
      if (shouldUpdateRunOutcome(entry.outcome, outcome)) {
        entry.outcome = outcome;
        mutated = true;
      }
      if (
        entry.execution?.status !== "terminal" ||
        entry.execution.endedAt !== endedAt ||
        entry.execution.outcome !== outcome
      ) {
        entry.execution = {
          ...entry.execution,
          status: "terminal",
          startedAt: entry.startedAt,
          endedAt,
          outcome,
        };
        mutated = true;
      }
      if (entry.endedReason !== completionReason) {
        entry.endedReason = completionReason;
        mutated = true;
      }
      if (entry.pauseReason !== undefined) {
        entry.pauseReason = undefined;
        mutated = true;
      }

      if (completeParams.completionSnapshot) {
        const completion = ensureCompletionState(entry);
        if (
          completion.resultText !== completeParams.completionSnapshot.resultText ||
          completion.capturedAt !== completeParams.completionSnapshot.capturedAt
        ) {
          completion.resultText = completeParams.completionSnapshot.resultText;
          completion.capturedAt = completeParams.completionSnapshot.capturedAt;
          mutated = true;
        }
      }

      // A newer generation may share the session key. Its transcript/reply is
      // not evidence for this older run, so reconcile only the terminal task state.
      if (recoveryRequested || sessionSuperseded) {
        const completion = ensureCompletionState(entry);
        if (completion.resultText === undefined) {
          completion.resultText = null;
          completion.capturedAt = Date.now();
          mutated = true;
        }
      } else {
        const didFreezeResult = await freezeRunResultAtCompletion(entry, outcome);
        sessionSuperseded = newerGenerationOwnsSession(entry);
        if (sessionSuperseded) {
          const completion = ensureCompletionState(entry);
          completion.resultText = null;
          completion.capturedAt = Date.now();
          mutated = true;
        } else if (didFreezeResult) {
          mutated = true;
        }
      }
      if (updateSwarmCollectorCompletion(entry, params.getRuntimeConfig())) {
        mutated = true;
      }
      if (provisionalKillSnapshot) {
        // Keep the tombstone's superseded generation boundary through task
        // commit. Clearing it on the canonical registry row must not let a
        // late old-run result select a newer task sharing the session key.
        const taskResolution = params.resolveSubagentTask(provisionalKillSnapshot);
        postCaptureTaskResolution = taskResolution;
        const stableTaskCancellation =
          taskResolution.lookup === "available" &&
          taskResolution.task?.status === "cancelled" &&
          !isProvisionalSubagentKillTask(taskResolution.task);
        const cancellationEndedAt = resolveKilledSubagentTaskEndedAt(provisionalKillSnapshot);
        const completionPredatesCancellation =
          typeof cancellationEndedAt === "number" && endedAt < cancellationEndedAt;
        if (stableTaskCancellation && !completionPredatesCancellation) {
          // Cancellation can become durable while completion capture yields.
          // The provider transition is staged, so the live tombstone is intact.
          return;
        }
      }
      if (refreshPendingFinalDeliveryPayload(entry)) {
        mutated = true;
      }

      const opaqueTaskArbitration =
        provisionalKillSnapshot !== undefined &&
        postCaptureTaskResolution?.lookup === "unavailable";
      // A steer abort ends one agent run but continues the same detached task.
      // The successor must remain able to publish its eventual terminal state.
      if (provisionalKillSnapshot) {
        const finalizedTasks = safeFinalizeSubagentTaskRun({
          entry,
          outcome,
          taskResolution: postCaptureTaskResolution,
        });
        const taskWasAbsent =
          postCaptureTaskResolution?.lookup === "available" &&
          postCaptureTaskResolution.task === undefined;
        if ((!finalizedTasks || finalizedTasks.length === 0) && !taskWasAbsent) {
          if (opaqueTaskArbitration) {
            // The optional lookup cannot prove cancellation. Let the legacy
            // runtime's own finalizer decide whether provider completion won.
            return;
          }
          const latestTaskResolution = params.resolveSubagentTask(provisionalKillSnapshot);
          const latestTask = latestTaskResolution.task;
          const stableTaskCancellation =
            latestTask?.status === "cancelled" && !isProvisionalSubagentKillTask(latestTask);
          const cancellationEndedAt = resolveKilledSubagentTaskEndedAt(provisionalKillSnapshot);
          const completionPredatesCancellation =
            typeof cancellationEndedAt === "number" && endedAt < cancellationEndedAt;
          if (stableTaskCancellation && !completionPredatesCancellation) {
            return;
          }
          throw new Error("subagent task projection did not finalize");
        }

        // Task results do not auto-publish for subagents. Commit that durable,
        // idempotent projection first: after a crash the persisted kill marker
        // can replay it, while the inverse ordering could strand a provisional task.
        entry.browserCleanupDispatchedAt ??= currentEntry.browserCleanupDispatchedAt;
        if (currentEntry.killReconciliation?.suppressTaskDelivery === true) {
          entry.suppressCompletionDelivery = true;
        }
        const liveBeforeCommit = structuredClone(currentEntry);
        restoreEntrySnapshot(entry);
        entry = currentEntry;
        try {
          params.persistOrThrow();
        } catch (error) {
          restoreEntrySnapshot(liveBeforeCommit);
          throw error;
        }
        // A provider result supersedes provisional cleanup only after both
        // durable owners accept it. Rejected callbacks leave the kill tail live.
        cleanupBookkeeping.invalidateCleanupAttempt(entry);
      } else {
        try {
          if (mutated) {
            params.persistOrThrow();
          }
        } catch (error) {
          restoreEntrySnapshot(entrySnapshot);
          throw error;
        }
        if (!suppressTaskFinalization) {
          safeFinalizeSubagentTaskRun({ entry, outcome });
        }
      }
      terminalGeneration = (terminalGenerations.get(entry) ?? 0) + 1;
      terminalGenerations.set(entry, terminalGeneration);
    } finally {
      // Only the canonical state/capture transition is serialized. Cleanup
      // remains re-entrant so a stalled browser close cannot strand a duplicate callback.
      releaseCompletionLock();
    }

    if (!entry) {
      return;
    }
    return {
      entry,
      terminalGeneration,
      mutated,
      completionReason,
      sessionSuperseded,
    } satisfies PreparedSubagentRunCompletion;
  };

  return {
    isTerminalCallbackCurrent,
    prepareSubagentRunCompletion,
  };
}

export type SubagentRegistryCompletionState = ReturnType<
  typeof createSubagentRegistryCompletionState
>;
