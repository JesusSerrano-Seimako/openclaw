/**
 * Subagent registry lifecycle transitions.
 *
 * Composes terminal arbitration, cleanup delivery, requester wakes, and attached-resource cleanup.
 */
import { createSubagentRegistryAnnounceCleanup } from "./subagent-registry-announce-cleanup.js";
import { createSubagentRegistryCleanupBookkeeping } from "./subagent-registry-cleanup-bookkeeping.js";
import { createSubagentRegistryCompletionEffects } from "./subagent-registry-completion-effects.js";
import { createSubagentRegistryCompletionState } from "./subagent-registry-completion-state.js";
import {
  createSubagentRegistryLifecycleShared,
  type SubagentRegistryLifecycleParams,
} from "./subagent-registry-lifecycle-shared.js";
import { createSubagentRegistryRequesterSettle } from "./subagent-registry-requester-settle.js";

export function createSubagentRegistryLifecycleController(params: SubagentRegistryLifecycleParams) {
  const shared = createSubagentRegistryLifecycleShared(params);
  const requesterSettle = createSubagentRegistryRequesterSettle(params, shared);
  const cleanupBookkeeping = createSubagentRegistryCleanupBookkeeping(
    params,
    shared,
    requesterSettle,
  );
  const announceCleanup = createSubagentRegistryAnnounceCleanup(params, shared, cleanupBookkeeping);
  const completionState = createSubagentRegistryCompletionState(params, shared, cleanupBookkeeping);
  const completionEffects = createSubagentRegistryCompletionEffects(
    params,
    shared,
    completionState,
    cleanupBookkeeping,
    announceCleanup.startSubagentAnnounceCleanupFlow,
  );

  const clearScheduledResumeTimers = () => {
    cleanupBookkeeping.clearScheduledResumeTimers();
    requesterSettle.clearScheduledRequesterSettleWakeTimers();
  };

  return {
    clearScheduledResumeTimers,
    completeCleanupBookkeeping: cleanupBookkeeping.completeCleanupBookkeeping,
    completeSubagentRun: completionEffects.completeSubagentRun,
    finalizeResumedAnnounceGiveUp: cleanupBookkeeping.finalizeResumedAnnounceGiveUp,
    refreshFrozenResultFromSession: shared.refreshFrozenResultFromSession,
    resumeRequesterSettleWake: requesterSettle.resumeRequesterSettleWake,
    settleRequesterTurnAfterSessionSpawns: requesterSettle.settleRequesterTurnAfterSessionSpawns,
    startSubagentAnnounceCleanupFlow: announceCleanup.startSubagentAnnounceCleanupFlow,
  };
}
