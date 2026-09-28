/**
 * Level completion evaluation: partial goal compare + free-form objectives.
 */
import type { ClusterState } from '../engine/types';
import { compareCluster, type GoalDiff } from '../engine/compare';
import type { Level } from '../levels';

export interface LevelEval {
  complete: boolean;
  diffs: GoalDiff[];
  /** Soft checks (e.g. any job DONE) that ignore id keys. */
  soft: GoalDiff[];
}

/**
 * Evaluate whether the cluster satisfies a level goal.
 * - Partial structural goals use compareCluster (subset).
 * - Empty goals (e.g. yarn-01) complete when the user has run any command
 *   that advanced tick (caller passes commandCount).
 */
export function evaluateLevel(
  state: ClusterState,
  level: Level,
  commandCount: number,
): LevelEval {
  const diffs: GoalDiff[] = [];
  const soft: GoalDiff[] = [];

  // Structural goal.
  const structural = { ...level.goalState };
  // Strip empty yarn.applications marker goals — handled below.
  if (structural.yarn && structural.yarn.applications && Object.keys(structural.yarn.applications).length === 0) {
    delete structural.yarn.applications;
    if (Object.keys(structural.yarn).length === 0) delete structural.yarn;
  }

  const structuralKeys = JSON.stringify(structural);
  if (structuralKeys !== '{}' && structuralKeys !== '{"hdfs":{"nodes":{}}}') {
    // Only run structural compare if goal is non-trivial.
    const hasRealGoal = hasSubstance(level.goalState);
    if (hasRealGoal) {
      const result = compareCluster(state, structural);
      diffs.push(...result.diffs);
    }
  }

  // Level-specific soft checks.
  if (level.id === 'yarn-01-nodes') {
    if (commandCount < 1) {
      soft.push({
        path: 'commands',
        expected: 1,
        actual: commandCount,
        message: 'Run at least one command (yarn node -list)',
      });
    }
  }

  if (level.id === 'yarn-03-app-lifecycle') {
    const killed = Object.values(state.yarn.applications).some((a) => a.state === 'KILLED');
    if (!killed) {
      soft.push({
        path: 'yarn.applications',
        expected: 'at least one KILLED',
        actual: Object.values(state.yarn.applications).map((a) => a.state).join(',') || '(none)',
        message: 'No application has been killed yet',
      });
    }
  }

  if (level.id === 'mr-02-wordcount' || level.id === 'mr-03-output' || level.id === 'e2e-01-pipeline' || level.id === 'e2e-02-fault') {
    const anyDone = Object.values(state.mapreduce.jobs).some((j) => j.state === 'DONE');
    if (!anyDone) {
      soft.push({
        path: 'mapreduce.jobs',
        expected: 'at least one DONE',
        actual: Object.values(state.mapreduce.jobs).map((j) => j.state).join(',') || '(none)',
        message: 'No MapReduce job has reached DONE',
      });
    }
  }

  if (level.id === 'rep-02-kill-dn') {
    // Block must still be fully replicated (3) after kill.
    const block = state.hdfs.blocks['blk_k0'];
    const need = state.hdfs.replication;
    if (!block || block.replicas.length < need) {
      soft.push({
        path: 'hdfs.blocks.blk_k0.replicas',
        expected: `>= ${need}`,
        actual: block?.replicas.length ?? 0,
        message: 'blk_k0 is under-replicated after the kill',
      });
    }
    if (state.hdfs.datanodes['dn2']?.alive !== false) {
      soft.push({
        path: 'hdfs.datanodes.dn2.alive',
        expected: false,
        actual: state.hdfs.datanodes['dn2']?.alive,
        message: 'dn2 is still alive',
      });
    }
  }

  const all = [...diffs, ...soft];
  return { complete: all.length === 0, diffs, soft };
}

function hasSubstance(goal: unknown): boolean {
  if (goal == null) return false;
  if (typeof goal !== 'object') return true;
  if (Array.isArray(goal)) return goal.length > 0;
  const obj = goal as Record<string, unknown>;
  for (const [k, v] of Object.entries(obj)) {
    if (k === 'tick') continue;
    if (v === undefined) return true; // absence constraint
    if (hasSubstance(v)) return true;
  }
  return false;
}
