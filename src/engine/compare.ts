/**
 * Goal comparison for levels (order-independent where LGB tree-compare is).
 */
import type { ClusterState, PartialClusterState } from './types';

export interface GoalDiff {
  path: string;
  expected: unknown;
  actual: unknown;
  message: string;
}

export interface GoalCompareResult {
  ok: boolean;
  diffs: GoalDiff[];
}

function isPlainObject(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

function deepEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) return false;
    // Multiset compare for arrays of primitives (order-independent).
    if (a.every((x) => typeof x !== 'object' || x === null)) {
      const as = [...(a as (string | number | boolean | null)[])].map(String).sort();
      const bs = [...(b as (string | number | boolean | null)[])].map(String).sort();
      return as.every((x, i) => x === bs[i]);
    }
    return a.every((x, i) => deepEqual(x, b[i]));
  }
  if (isPlainObject(a) && isPlainObject(b)) {
    const ak = Object.keys(a);
    const bk = Object.keys(b);
    if (ak.length !== bk.length) return false;
    return ak.every((k) => deepEqual(a[k], b[k]));
  }
  return false;
}

/**
 * Compare `actual` cluster against a partial `goal`.
 * Keys absent from goal are don't-care.
 */
export function compareCluster(
  actual: ClusterState,
  goal: PartialClusterState,
): GoalCompareResult {
  const diffs: GoalDiff[] = [];

  if (goal.tick !== undefined && actual.tick < goal.tick) {
    diffs.push({
      path: 'tick',
      expected: `>= ${goal.tick}`,
      actual: actual.tick,
      message: `cluster tick is ${actual.tick}, expected at least ${goal.tick}`,
    });
  }

  if (goal.hdfs) {
    compareObject('hdfs', goal.hdfs as Record<string, unknown>, actual.hdfs as unknown as Record<string, unknown>, diffs);
  }
  if (goal.yarn) {
    compareObject('yarn', goal.yarn as Record<string, unknown>, actual.yarn as unknown as Record<string, unknown>, diffs);
  }
  if (goal.mapreduce) {
    compareObject(
      'mapreduce',
      goal.mapreduce as Record<string, unknown>,
      actual.mapreduce as unknown as Record<string, unknown>,
      diffs,
    );
  }

  return { ok: diffs.length === 0, diffs };
}

function compareObject(
  prefix: string,
  expected: Record<string, unknown>,
  actual: Record<string, unknown>,
  diffs: GoalDiff[],
): void {
  for (const [key, exp] of Object.entries(expected)) {
    const path = `${prefix}.${key}`;
    const act = actual[key];

    // Explicit undefined in a goal means "must not exist".
    if (exp === undefined) {
      if (act !== undefined) {
        diffs.push({
          path,
          expected: undefined,
          actual: act,
          message: `${path}: expected to be absent, but it exists`,
        });
      }
      continue;
    }

    if (isPlainObject(exp) && isPlainObject(act)) {
      // Subset match: only keys present in goal are checked.
      compareObject(path, exp, act, diffs);
      continue;
    }

    if (Array.isArray(exp) && Array.isArray(act)) {
      if (!deepEqual(exp, act)) {
        diffs.push({
          path,
          expected: exp,
          actual: act,
          message: `${path} mismatch`,
        });
      }
      continue;
    }

    if (isPlainObject(exp) && act === undefined) {
      diffs.push({
        path,
        expected: exp,
        actual: undefined,
        message: `${path} missing`,
      });
      continue;
    }

    if (!deepEqual(exp, act)) {
      diffs.push({
        path,
        expected: exp,
        actual: act,
        message: `${path}: expected ${JSON.stringify(exp)}, got ${JSON.stringify(act)}`,
      });
    }
  }
}

/** Common goal helpers for level authors. */
export function fileExists(path: string): PartialClusterState {
  return {
    hdfs: {
      nodes: {
        [path]: { path, sizeBytes: 0, blockIds: [], owner: '', createdTick: 0, isDirectory: false },
      } as never,
    },
  };
}

export function directoryExists(path: string): PartialClusterState {
  return {
    hdfs: {
      nodes: {
        [path]: { path, sizeBytes: 0, blockIds: [], owner: '', createdTick: 0, isDirectory: true },
      } as never,
    },
  };
}

export function jobIsDone(jobId: string): PartialClusterState {
  return {
    mapreduce: {
      jobs: {
        [jobId]: {
          id: jobId,
          name: '',
          appId: '',
          inputPath: '',
          outputPath: '',
          state: 'DONE',
          splits: 0,
          numMaps: 0,
          numReduces: 0,
          combiner: false,
          maps: [],
          reduces: [],
          outputFiles: [],
        },
      } as never,
    },
  };
}
