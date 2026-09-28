/**
 * Default cluster factory and deep-clone helpers.
 */
import type { ClusterState, PartialClusterState, DataNode, NodeManager } from './types';

const DEFAULT_BLOCK_SIZE = 64;
const DEFAULT_REPLICATION = 3;

export function createDefaultDataNode(id: string, host: string, capacityBlocks = 32): DataNode {
  return {
    id,
    host,
    capacityBlocks,
    alive: true,
    blocks: [],
  };
}

export function createDefaultNodeManager(
  id: string,
  host: string,
  memoryMb = 4096,
  vcores = 4,
): NodeManager {
  return {
    id,
    host,
    memoryMb,
    vcores,
    alive: true,
    containers: [],
  };
}

export function createDefaultCluster(): ClusterState {
  return {
    tick: 0,
    hdfs: {
      blockSizeBytes: DEFAULT_BLOCK_SIZE,
      replication: DEFAULT_REPLICATION,
      nodes: {
        '/': {
          path: '/',
          sizeBytes: 0,
          blockIds: [],
          owner: 'hdfs',
          createdTick: 0,
          isDirectory: true,
        },
      },
      blocks: {},
      datanodes: {
        dn1: createDefaultDataNode('dn1', 'node1'),
        dn2: createDefaultDataNode('dn2', 'node2'),
        dn3: createDefaultDataNode('dn3', 'node3'),
        dn4: createDefaultDataNode('dn4', 'node4'),
      },
      namenodeStarted: true,
      safemode: false,
      nextBlockNum: 0,
    },
    yarn: {
      nodeManagers: {
        nm1: createDefaultNodeManager('nm1', 'node1'),
        nm2: createDefaultNodeManager('nm2', 'node2'),
        nm3: createDefaultNodeManager('nm3', 'node3'),
      },
      applications: {},
      containers: {},
      nextAppNum: 1,
      nextContainerNum: 1,
    },
    mapreduce: {
      jobs: {},
      nextJobNum: 1,
      fileContents: {},
    },
  };
}

/** Deep clone via JSON (state is plain data). */
export function cloneState(state: ClusterState): ClusterState {
  return JSON.parse(JSON.stringify(state)) as ClusterState;
}

/**
 * Merge a partial state onto a base cluster (deep merge for records).
 * Arrays and primitives from `partial` win when present.
 */
export function mergeState(base: ClusterState, partial: PartialClusterState | undefined): ClusterState {
  if (!partial) return cloneState(base);
  const out = cloneState(base);

  if (partial.tick !== undefined) out.tick = partial.tick;

  if (partial.hdfs) {
    out.hdfs = { ...out.hdfs, ...partial.hdfs };
    if (partial.hdfs.nodes) out.hdfs.nodes = { ...out.hdfs.nodes };
    if (partial.hdfs.blocks) out.hdfs.blocks = { ...out.hdfs.blocks };
    if (partial.hdfs.datanodes) {
      // Replace only named datanodes; keep others.
      const merged = { ...out.hdfs.datanodes };
      for (const [id, dn] of Object.entries(partial.hdfs.datanodes)) {
        merged[id] = dn as DataNode;
      }
      out.hdfs.datanodes = merged;
    }
  }

  if (partial.yarn) {
    out.yarn = { ...out.yarn, ...partial.yarn };
    if (partial.yarn.nodeManagers) {
      const merged = { ...out.yarn.nodeManagers };
      for (const [id, nm] of Object.entries(partial.yarn.nodeManagers)) {
        merged[id] = nm as NodeManager;
      }
      out.yarn.nodeManagers = merged;
    }
    if (partial.yarn.applications) out.yarn.applications = { ...partial.yarn.applications };
    if (partial.yarn.containers) out.yarn.containers = { ...partial.yarn.containers };
  }

  if (partial.mapreduce) {
    out.mapreduce = { ...out.mapreduce, ...partial.mapreduce };
    if (partial.mapreduce.jobs) out.mapreduce.jobs = { ...partial.mapreduce.jobs };
    if (partial.mapreduce.fileContents) {
      out.mapreduce.fileContents = { ...partial.mapreduce.fileContents };
    }
  }

  return out;
}

export function normalizePath(path: string): string {
  if (!path.startsWith('/')) path = '/' + path;
  const parts = path.split('/').filter(Boolean);
  const stack: string[] = [];
  for (const p of parts) {
    if (p === '.') continue;
    if (p === '..') {
      stack.pop();
      continue;
    }
    stack.push(p);
  }
  return '/' + stack.join('/');
}

export function parentPath(path: string): string {
  const norm = normalizePath(path);
  if (norm === '/') return '/';
  const idx = norm.lastIndexOf('/');
  return idx <= 0 ? '/' : norm.slice(0, idx);
}

export function baseName(path: string): string {
  const norm = normalizePath(path);
  if (norm === '/') return '/';
  return norm.slice(norm.lastIndexOf('/') + 1);
}

export function joinPath(dir: string, name: string): string {
  const d = normalizePath(dir);
  if (d === '/') return normalizePath('/' + name);
  return normalizePath(d + '/' + name);
}
