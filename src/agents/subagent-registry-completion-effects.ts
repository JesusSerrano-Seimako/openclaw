import { runWithGatewayIndependentRootWorkAdmission } from "../process/gateway-work-admission.js";
import { emitSessionLifecycleEvent } from "../sessions/session-lifecycle-events.js";
import { recordSubagentTerminalState } from "../sessions/session-state-events.js";
import { createLazyImportLoader } from "../shared/lazy-promise.js";
import { SUBAGENT_ENDED_REASON_KILLED } from "./subagent-lifecycle-events.js";
import type { SubagentRegistryCleanupBookkeeping } from "./subagent-registry-cleanup-bookkeeping.js";
import type { SubagentRegistryCompletionState } from "./subagent-registry-completion-state.js";
import { persistSubagentSessionTiming } from "./subagent-registry-helpers.js";
import type {
  SubagentRegistryLifecycleParams,
  SubagentRegistryLifecycleShared,
} from "./subagent-registry-lifecycle-shared.js";
import type { SubagentCompletionRequest, SubagentRunRecord } from "./subagent-registry.types.js";
import { releaseSwarmRun } from "./swarm-scheduler.js";

type BrowserCleanupModule = Pick<
  typeof import("../browser-lifecycle-cleanup.js"),
  "cleanupBrowserSessionsForLifecycleEnd"
>;

const browserCleanupLoader = createLazyImportLoader<BrowserCleanupModule>(
  () => import("../browser-lifecycle-cleanup.js"),
);

async function loadCleanupBrowserSessionsForLifecycleEnd(): Promise<
  BrowserCleanupModule["cleanupBrowserSessionsForLifecycleEnd"]
> {
  return (await browserCleanupLoader.load()).cleanupBrowserSessionsForLifecycleEnd;
}

export function createSubagentRegistryCompletionEffects(
  params: SubagentRegistryLifecycleParams,
  shared: SubagentRegistryLifecycleShared,
  completionState: SubagentRegistryCompletionState,
  cleanupBookkeeping: SubagentRegistryCleanupBookkeeping,
  startSubagentAnnounceCleanupFlow: (runId: string, entry: SubagentRunRecord) => boolean,
) {
  // Presentation is transient, so dedupe only this record's competing terminal callbacks.
  // Persisted lifecycle truth stays limited to durable completion and delivery state.
  const progressEndedEntries = new WeakSet<SubagentRunRecord>();
  const { buildSafeLifecycleErrorMeta, maskRunId, maskSessionKey, newerGenerationOwnsSession } =
    shared;
  const { isTerminalCallbackCurrent, prepareSubagentRunCompletion } = completionState;
  const { retireRunModeBundleMcpRuntime } = cleanupBookkeeping;

  const completeSubagentRunAttempt = async (completeParams: SubagentCompletionRequest) => {
    const prepared = await prepareSubagentRunCompletion(completeParams);
    if (!prepared) {
      return;
    }
    const { entry, terminalGeneration, mutated, completionReason } = prepared;
    let { sessionSuperseded } = prepared;

    if (!entry) {
      return;
    }
    if (!isTerminalCallbackCurrent(completeParams.runId, entry, terminalGeneration)) {
      return;
    }
    const retireSupersededSession = async (currentEntry: SubagentRunRecord) => {
      if (completionReason !== SUBAGENT_ENDED_REASON_KILLED) {
        await params.retireSupersededRun(completeParams.runId, currentEntry);
        params.persist();
      }
    };
    sessionSuperseded = sessionSuperseded || newerGenerationOwnsSession(entry);
    if (sessionSuperseded) {
      // This callback belongs to an older run that shared the session key.
      // Update only its task projection; the newer generation owns all session effects.
      await retireSupersededSession(entry);
      return;
    }
    if (entry.collect) {
      releaseSwarmRun(entry.schedulerSlotId ?? entry.runId);
    }
    const isProvisionalKill = entry.killReconciliation !== undefined;
    // Record only the current, non-superseded callback with a committed outcome; the
    // run-terminal dedupe key is first-write-wins, so a provisional/stale status here
    // would permanently mislabel the signal-log terminal kind.
    if (!isProvisionalKill && entry.outcome?.status && entry.outcome.status !== "unknown") {
      recordSubagentTerminalState({
        childSessionKey: entry.childSessionKey,
        runId: entry.runId,
        requesterSessionKey: entry.requesterSessionKey,
        outcomeStatus: entry.outcome.status,
      });
    }

    if (!completeParams.suppressSessionEffects) {
      try {
        await persistSubagentSessionTiming(entry, {
          // Recheck while patchSessionEntry owns its write lock so this old
          // completion cannot commit after a synchronous ownership transfer.
          isCurrentGeneration: () =>
            isTerminalCallbackCurrent(completeParams.runId, entry, terminalGeneration) &&
            !newerGenerationOwnsSession(entry),
        });
      } catch (err) {
        params.warn("failed to persist subagent session timing", {
          err,
          runId: entry.runId,
          childSessionKey: entry.childSessionKey,
        });
      }
    }
    if (!isTerminalCallbackCurrent(completeParams.runId, entry, terminalGeneration)) {
      return;
    }
    if (newerGenerationOwnsSession(entry)) {
      await retireSupersededSession(entry);
      return;
    }

    const suppressedForSteerRestart = params.suppressAnnounceForSteerRestart(entry);
    if (mutated && !suppressedForSteerRestart && !completeParams.suppressSessionEffects) {
      emitSessionLifecycleEvent({
        sessionKey: entry.childSessionKey,
        reason: "subagent-status",
        parentSessionKey: entry.requesterSessionKey,
        label: entry.label,
      });
      // The enclosing steer/session-effects guard admits only the real terminal generation.
      if (!isProvisionalKill && !progressEndedEntries.has(entry)) {
        progressEndedEntries.add(entry);
        await params.emitSubagentProgressEndedForRun(entry);
        if (!isTerminalCallbackCurrent(completeParams.runId, entry, terminalGeneration)) {
          return;
        }
      }
    }
    const shouldEmitEndedHook =
      !suppressedForSteerRestart &&
      !isProvisionalKill &&
      !completeParams.suppressSessionEffects &&
      params.shouldEmitEndedHookForRun({
        entry,
        reason: completionReason,
      });
    const shouldDeferEndedHook =
      shouldEmitEndedHook &&
      completeParams.triggerCleanup &&
      entry.expectsCompletionMessage === true &&
      !suppressedForSteerRestart;
    if (!shouldDeferEndedHook && shouldEmitEndedHook) {
      await params.emitSubagentEndedHookForRun({
        entry,
        reason: completionReason,
        sendFarewell: completeParams.sendFarewell,
        accountId: completeParams.accountId,
        isCurrent: () =>
          isTerminalCallbackCurrent(completeParams.runId, entry, terminalGeneration) &&
          !newerGenerationOwnsSession(entry),
      });
      if (!isTerminalCallbackCurrent(completeParams.runId, entry, terminalGeneration)) {
        return;
      }
      if (newerGenerationOwnsSession(entry)) {
        await retireSupersededSession(entry);
        return;
      }
    }

    if (!completeParams.triggerCleanup || suppressedForSteerRestart) {
      return;
    }

    // registerSubagentRun fires both an in-process listener and a gateway
    // waitForSubagentCompletion RPC; both can reach this point for the same
    // runId in embedded mode. Dedupe only the browser driver tab-close IPC
    // with a sync check-then-set. The retire + announce tail below must still
    // run for every caller, so a slow or held first browser cleanup cannot
    // strand a duplicate caller's completion behind it.
    if (entry.browserCleanupDispatchedAt === undefined) {
      entry.browserCleanupDispatchedAt = Date.now();
      try {
        const cleanupBrowserSessions =
          params.cleanupBrowserSessionsForLifecycleEnd ??
          (await loadCleanupBrowserSessionsForLifecycleEnd());
        await cleanupBrowserSessions({
          sessionKeys: [entry.childSessionKey],
          onWarn: (msg) => params.warn(msg, { runId: entry.runId }),
        });
      } catch (error) {
        params.warn("failed to cleanup browser sessions for completed subagent", {
          error: buildSafeLifecycleErrorMeta(error),
          runId: maskRunId(completeParams.runId),
          childSessionKey: maskSessionKey(entry.childSessionKey),
        });
      }
      if (!isTerminalCallbackCurrent(completeParams.runId, entry, terminalGeneration)) {
        return;
      }
      if (newerGenerationOwnsSession(entry)) {
        await retireSupersededSession(entry);
        return;
      }
    }

    try {
      await retireRunModeBundleMcpRuntime({
        runId: completeParams.runId,
        entry,
        reason: "subagent-run-complete",
      });
    } catch (error) {
      params.warn("failed to retire subagent bundle MCP runtime after completion", {
        error: buildSafeLifecycleErrorMeta(error),
        runId: maskRunId(completeParams.runId),
        childSessionKey: maskSessionKey(entry.childSessionKey),
      });
    }
    if (!isTerminalCallbackCurrent(completeParams.runId, entry, terminalGeneration)) {
      return;
    }
    if (newerGenerationOwnsSession(entry)) {
      await retireSupersededSession(entry);
      return;
    }

    if (isProvisionalKill) {
      // Browser and MCP resources can close immediately, but completion delivery
      // waits for the provider result or the killed tombstone reconciliation.
      return;
    }

    startSubagentAnnounceCleanupFlow(completeParams.runId, entry);
  };

  const completeSubagentRun = async (completeParams: SubagentCompletionRequest) => {
    // Task finalization can make the run disappear from suspension blockers
    // before browser/MCP retirement and cleanup delivery hand off. Own this
    // entire transition as an independent root so that boundary stays atomic.
    // Callers can detach while retaining parent ALS, so nesting is intentional.
    await runWithGatewayIndependentRootWorkAdmission(async () => {
      await completeSubagentRunAttempt(completeParams);
    });
  };

  return {
    completeSubagentRun,
  };
}
