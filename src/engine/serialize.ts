/**
 * Serialize / deserialize cluster state for undo, level import/export, share links.
 */
import type { ClusterState, PartialClusterState } from './types';
import { cloneState, createDefaultCluster, mergeState } from './cluster';

export function exportState(state: ClusterState): string {
  return JSON.stringify(state);
}

export function importState(json: string): ClusterState {
  const parsed = JSON.parse(json) as ClusterState;
  if (!parsed.hdfs || !parsed.yarn || !parsed.mapreduce) {
    throw new Error('Invalid cluster state JSON');
  }
  return parsed;
}

export function exportStatePretty(state: ClusterState): string {
  return JSON.stringify(state, null, 2);
}

export function snapshot(state: ClusterState): string {
  return exportState(cloneState(state));
}

export function restore(snapshotJson: string): ClusterState {
  return importState(snapshotJson);
}

/** Build a full state from defaults + partial (levels). */
export function materialize(partial?: PartialClusterState): ClusterState {
  return mergeState(createDefaultCluster(), partial);
}
