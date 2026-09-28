/**
 * HDFS simulation: namespace ops, block placement, replication, fault tolerance.
 */
import type {
  Block,
  BlockId,
  ClusterState,
  CommandResult,
  DataNode,
  DataNodeId,
  HdfsFile,
  HdfsState,
  VisualEvent,
} from './types';
import { normalizePath, parentPath, joinPath } from './cluster';

function ok(lines: string[], events?: VisualEvent[], mutated = false): CommandResult {
  return { ok: true, lines, events, mutated };
}

function err(message: string): CommandResult {
  return { ok: false, lines: [message], error: true, mutated: false };
}

function liveNodes(hdfs: HdfsState): DataNode[] {
  return Object.values(hdfs.datanodes).filter((d) => d.alive);
}

function pickTargets(hdfs: HdfsState, exclude: DataNodeId[] = []): DataNode[] {
  // Prefer least-used live nodes not already holding a replica.
  const candidates = liveNodes(hdfs).filter((d) => !exclude.includes(d.id));
  candidates.sort((a, b) => a.blocks.length - b.blocks.length || a.id.localeCompare(b.id));
  const need = hdfs.replication;
  return candidates.slice(0, need);
}

export function mkdir(state: ClusterState, path: string, createParents = false): CommandResult {
  const hdfs = state.hdfs;
  if (!hdfs.namenodeStarted) return err('mkdir: NameNode is not running');
  if (hdfs.safemode) return err('mkdir: Cannot create directory in safemode');

  const p = normalizePath(path);
  if (hdfs.nodes[p]) return err(`mkdir: '${p}': File exists`);

  const parent = parentPath(p);
  if (!hdfs.nodes[parent]) {
    if (!createParents) {
      return err(`mkdir: '${parent}': No such file or directory`);
    }
    // Create parents top-down.
    const parts = p.split('/').filter(Boolean);
    let cur = '';
    for (let i = 0; i < parts.length; i++) {
      cur += '/' + parts[i];
      if (hdfs.nodes[cur]) continue;
      const par = parentPath(cur);
      const parNode = hdfs.nodes[par];
      if (!parNode?.isDirectory) {
        return err(`mkdir: '${par}': No such file or directory`);
      }
      hdfs.nodes[cur] = {
        path: cur,
        sizeBytes: 0,
        blockIds: [],
        owner: 'hdfs',
        createdTick: state.tick,
        isDirectory: true,
      };
    }
    state.tick += 1;
    return ok([`Created directory ${p}`], undefined, true);
  }

  const parentNode = hdfs.nodes[parent];
  if (!parentNode || !parentNode.isDirectory) {
    return err(`mkdir: '${parent}': Not a directory`);
  }

  hdfs.nodes[p] = {
    path: p,
    sizeBytes: 0,
    blockIds: [],
    owner: 'hdfs',
    createdTick: state.tick,
    isDirectory: true,
  };
  state.tick += 1;
  return ok([`Created directory ${p}`], undefined, true);
}

/**
 * Put a local file into HDFS. `sizeUnits` is abstract size (block-size units).
 * Optionally seed text contents for MapReduce wordcount.
 */
export function put(
  state: ClusterState,
  localName: string,
  hdfsPath: string,
  sizeUnits = 2,
  contents?: string,
): CommandResult {
  const hdfs = state.hdfs;
  if (!hdfs.namenodeStarted) return err('put: NameNode is not running');
  if (hdfs.safemode) return err('put: Cannot write in safemode');
  if (sizeUnits < 1) return err('put: invalid size');

  let target = normalizePath(hdfsPath);
  // If target is an existing directory, place file inside it.
  const asDir = hdfs.nodes[target];
  if (asDir?.isDirectory) {
    target = joinPath(target, localName);
  }

  if (hdfs.nodes[target] && !hdfs.nodes[target]!.isDirectory) {
    return err(`put: '${target}': File exists`);
  }
  if (hdfs.nodes[target]?.isDirectory) {
    return err(`put: '${target}': Is a directory`);
  }

  const parent = parentPath(target);
  if (!hdfs.nodes[parent]) {
    return err(`put: '${parent}': No such file or directory`);
  }
  if (!hdfs.nodes[parent]!.isDirectory) {
    return err(`put: '${parent}': Not a directory`);
  }

  const alive = liveNodes(hdfs);
  if (alive.length === 0) return err('put: No live DataNodes');

  // sizeUnits is the number of abstract blocks to place.
  const blockCount = sizeUnits;
  const blockIds: BlockId[] = [];
  const events: VisualEvent[] = [];
  const replicaMap: Record<BlockId, DataNodeId[]> = {};

  for (let i = 0; i < blockCount; i++) {
    const blockId = `blk_${hdfs.nextBlockNum++}`;
    const targets = pickTargets(hdfs);
    if (targets.length === 0) return err('put: No DataNodes with capacity');

    const replicas: DataNodeId[] = [];
    for (const dn of targets) {
      if (dn.blocks.length >= dn.capacityBlocks) continue;
      dn.blocks.push(blockId);
      replicas.push(dn.id);
    }
    if (replicas.length === 0) return err('put: DataNodes are full');

    hdfs.blocks[blockId] = {
      id: blockId,
      sizeBytes: hdfs.blockSizeBytes,
      replicas,
    };
    blockIds.push(blockId);
    replicaMap[blockId] = [...replicas];
  }

  const sizeBytes = blockCount * hdfs.blockSizeBytes;
  hdfs.nodes[target] = {
    path: target,
    sizeBytes,
    blockIds,
    owner: 'hdfs',
    createdTick: state.tick,
    isDirectory: false,
  };

  if (contents !== undefined) {
    state.mapreduce.fileContents[target] = contents;
  }

  state.tick += 1;
  events.push({
    kind: 'put-blocks',
    path: target,
    blockIds,
    replicas: replicaMap,
  });

  const under = blockIds.filter((id) => {
    const b = hdfs.blocks[id];
    return b && b.replicas.length < hdfs.replication;
  });

  const lines = [
    `put: ${localName} -> ${target}`,
    `  blocks: ${blockIds.length}, size: ${sizeBytes} bytes`,
    `  replication: ${hdfs.replication} (target)`,
  ];
  if (under.length) {
    lines.push(`  warning: ${under.length} block(s) under-replicated`);
  }
  for (const id of blockIds) {
    lines.push(`  ${id} -> [${hdfs.blocks[id]!.replicas.join(', ')}]`);
  }

  return ok(lines, events, true);
}

export function ls(state: ClusterState, path = '/'): CommandResult {
  const hdfs = state.hdfs;
  const p = normalizePath(path);
  const node = hdfs.nodes[p];
  if (!node) return err(`ls: '${p}': No such file or directory`);

  if (!node.isDirectory) {
    return ok([
      `${node.sizeBytes}\t${p}\tblocks=${node.blockIds.length}`,
    ]);
  }

  const children = Object.values(hdfs.nodes)
    .filter((n) => n.path !== p && parentPath(n.path) === p)
    .sort((a, b) => a.path.localeCompare(b.path));

  if (children.length === 0) {
    return ok([`(empty) ${p}`]);
  }

  const lines = children.map((c) => {
    if (c.isDirectory) {
      return `drwxr-xr\t-\t${c.owner}\t${c.path}/`;
    }
    return `-rw-r--r\t${c.sizeBytes}\t${c.owner}\t${c.path}`;
  });
  return ok(lines);
}

export function cat(state: ClusterState, path: string): CommandResult {
  const p = normalizePath(path);
  const node = state.hdfs.nodes[p];
  if (!node) return err(`cat: '${p}': No such file or directory`);
  if (node.isDirectory) return err(`cat: '${p}': Is a directory`);

  const contents = state.mapreduce.fileContents[p];
  if (contents !== undefined) {
    return ok(contents.split('\n'));
  }
  return ok([
    `[binary content of ${p}: ${node.sizeBytes} bytes, ${node.blockIds.length} block(s)]`,
  ]);
}

export function getFile(state: ClusterState, hdfsPath: string, localName?: string): CommandResult {
  const p = normalizePath(hdfsPath);
  const node = state.hdfs.nodes[p];
  if (!node) return err(`get: '${p}': No such file or directory`);
  if (node.isDirectory) return err(`get: '${p}': Is a directory`);

  const dest = localName ?? p.slice(p.lastIndexOf('/') + 1);
  return ok([
    `get: ${p} -> ./${dest}`,
    `  copied ${node.sizeBytes} bytes from ${node.blockIds.length} block(s)`,
  ]);
}

export function rm(state: ClusterState, path: string, recursive = false): CommandResult {
  const hdfs = state.hdfs;
  const p = normalizePath(path);
  const node = hdfs.nodes[p];
  if (!node) return err(`rm: '${p}': No such file or directory`);
  if (p === '/') return err(`rm: cannot remove root`);

  if (node.isDirectory) {
    const children = Object.values(hdfs.nodes).filter(
      (n) => n.path !== p && (n.path === p || n.path.startsWith(p + '/')),
    );
    if (children.length > 0 && !recursive) {
      return err(`rm: '${p}': Is a directory (use -r)`);
    }
    for (const c of children) {
      deleteFileBlocks(state, c.path);
    }
  }
  deleteFileBlocks(state, p);

  const lines = [`Deleted ${node.isDirectory ? 'directory' : 'file'} ${p}`];
  state.tick += 1;
  return ok(lines, undefined, true);
}

function deleteFileBlocks(state: ClusterState, path: string): void {
  const hdfs = state.hdfs;
  const node = hdfs.nodes[path];
  if (!node) return;
  for (const bid of node.blockIds) {
    const block = hdfs.blocks[bid];
    if (!block) continue;
    for (const dnId of block.replicas) {
      const dn = hdfs.datanodes[dnId];
      if (dn) {
        dn.blocks = dn.blocks.filter((b) => b !== bid);
      }
    }
    delete hdfs.blocks[bid];
  }
  delete hdfs.nodes[path];
  delete state.mapreduce.fileContents[path];
}

export function stat(state: ClusterState, path: string): CommandResult {
  const p = normalizePath(path);
  const node = state.hdfs.nodes[p];
  if (!node) return err(`stat: '${p}': No such file or directory`);
  return ok([
    `path: ${node.path}`,
    `type: ${node.isDirectory ? 'directory' : 'file'}`,
    `size: ${node.sizeBytes}`,
    `blocks: ${node.blockIds.length}`,
    `owner: ${node.owner}`,
    `createdTick: ${node.createdTick}`,
    ...(node.blockIds.length
      ? node.blockIds.map(
          (b) =>
            `  ${b}: replicas=[${(state.hdfs.blocks[b]?.replicas ?? []).join(', ')}]`,
        )
      : []),
  ]);
}

export function setReplication(
  state: ClusterState,
  rep: number,
  path: string,
  _wait = false,
): CommandResult {
  const hdfs = state.hdfs;
  if (rep < 1) return err(`setrep: invalid replication ${rep}`);
  const p = normalizePath(path);
  const node = hdfs.nodes[p];
  if (!node) return err(`setrep: '${p}': No such file or directory`);
  if (node.isDirectory) return err(`setrep: '${p}': Is a directory`);

  hdfs.replication = rep;
  const events: VisualEvent[] = [];
  const lines: string[] = [`Replication set to ${rep} for ${p}`];

  for (const bid of node.blockIds) {
    const block = hdfs.blocks[bid];
    if (!block) continue;
    const result = ensureReplication(hdfs, bid, rep);
    if (result.added.length) {
      events.push({ kind: 'replicate', blockId: bid, to: result.added });
      lines.push(`  ${bid}: added replicas on [${result.added.join(', ')}]`);
    }
    if (result.removed.length) {
      lines.push(`  ${bid}: removed replicas from [${result.removed.join(', ')}]`);
    }
    if (block.replicas.length < rep) {
      lines.push(`  ${bid}: still under-replicated (${block.replicas.length}/${rep})`);
    }
  }

  state.tick += 1;
  return ok(lines, events, true);
}

function ensureReplication(
  hdfs: HdfsState,
  blockId: BlockId,
  rep: number,
): { added: DataNodeId[]; removed: DataNodeId[] } {
  const block = hdfs.blocks[blockId];
  if (!block) return { added: [], removed: [] };

  const added: DataNodeId[] = [];
  const removed: DataNodeId[] = [];

  // Remove excess replicas (keep first `rep` alive hosts).
  if (block.replicas.length > rep) {
    const keep = block.replicas.slice(0, rep);
    const drop = block.replicas.slice(rep);
    for (const dnId of drop) {
      const dn = hdfs.datanodes[dnId];
      if (dn) dn.blocks = dn.blocks.filter((b) => b !== blockId);
      removed.push(dnId);
    }
    block.replicas = keep;
  }

  // Add missing replicas.
  if (block.replicas.length < rep) {
    const targets = pickTargets(hdfs, block.replicas);
    for (const dn of targets) {
      if (block.replicas.length >= rep) break;
      if (dn.blocks.includes(blockId)) continue;
      if (dn.blocks.length >= dn.capacityBlocks) continue;
      dn.blocks.push(blockId);
      block.replicas.push(dn.id);
      added.push(dn.id);
    }
  }

  return { added, removed };
}

export function killDataNode(state: ClusterState, id: DataNodeId): CommandResult {
  const dn = state.hdfs.datanodes[id];
  if (!dn) return err(`kill-datanode: unknown DataNode ${id}`);
  if (!dn.alive) return err(`kill-datanode: ${id} is already down`);

  dn.alive = false;
  const events: VisualEvent[] = [{ kind: 'node-down', nodeId: id }];
  const lines = [`DataNode ${id} (${dn.host}) is now down`, `  lost ${dn.blocks.length} replica(s)`];

  // Re-replicate blocks that fell below target.
  const affected = new Set(dn.blocks);
  for (const bid of affected) {
    const block = state.hdfs.blocks[bid];
    if (!block) continue;
    block.replicas = block.replicas.filter((r) => r !== id);
    const result = ensureReplication(state.hdfs, bid, state.hdfs.replication);
    if (result.added.length) {
      events.push({ kind: 'replicate', blockId: bid, to: result.added });
      lines.push(`  re-replicated ${bid} -> [${result.added.join(', ')}]`);
    }
    if (block.replicas.length < state.hdfs.replication) {
      lines.push(`  warning: ${bid} under-replicated (${block.replicas.length}/${state.hdfs.replication})`);
    }
  }
  dn.blocks = [];

  state.tick += 1;
  return ok(lines, events, true);
}

export function startDataNode(state: ClusterState, id: DataNodeId): CommandResult {
  const dn = state.hdfs.datanodes[id];
  if (!dn) return err(`start-datanode: unknown DataNode ${id}`);
  if (dn.alive) return err(`start-datanode: ${id} is already up`);
  dn.alive = true;
  state.tick += 1;
  return ok(
    [`DataNode ${id} (${dn.host}) is back online`],
    [{ kind: 'node-up', nodeId: id }],
    true,
  );
}

export function report(state: ClusterState): CommandResult {
  const hdfs = state.hdfs;
  const total = Object.values(hdfs.datanodes).length;
  const live = liveNodes(hdfs).length;
  const totalBlocks = Object.keys(hdfs.blocks).length;
  const under = Object.values(hdfs.blocks).filter(
    (b) => b.replicas.length < hdfs.replication,
  ).length;
  const files = Object.values(hdfs.nodes).filter((n) => !n.isDirectory).length;

  const lines = [
    'Configured Capacity: ' + total * 32 * hdfs.blockSizeBytes + ' bytes',
    `Present Capacity: ${live * 32 * hdfs.blockSizeBytes} bytes`,
    `DFS Remaining: ${(live * 32 - totalBlocks) * hdfs.blockSizeBytes} bytes`,
    `DFS Used: ${totalBlocks * hdfs.blockSizeBytes} bytes`,
    `Under-replicated blocks: ${under}`,
    `Files and directories: ${Object.keys(hdfs.nodes).length}`,
    `Files: ${files}`,
    `Blocks: ${totalBlocks}`,
    `Live datanodes (${live}):`,
    ...Object.values(hdfs.datanodes).map((dn) => {
      const status = dn.alive ? 'In Service' : 'Decommissioned';
      return `  ${dn.id} (${dn.host}): ${status}, blocks=${dn.blocks.length}, cap=${dn.capacityBlocks}`;
    }),
    `Dead datanodes (${total - live}):`,
    ...Object.values(hdfs.datanodes)
      .filter((d) => !d.alive)
      .map((dn) => `  ${dn.id} (${dn.host})`),
  ];
  return ok(lines);
}

export function safemode(state: ClusterState, action: 'enter' | 'leave'): CommandResult {
  state.hdfs.safemode = action === 'enter';
  state.tick += 1;
  return ok(
    [`Safemode is ${action === 'enter' ? 'ON' : 'OFF'}`],
    undefined,
    true,
  );
}

/** Place under-replicated blocks (used after startup / kills). */
export function rebalanceUnderReplicated(state: ClusterState): string[] {
  const lines: string[] = [];
  const events: VisualEvent[] = [];
  for (const block of Object.values(state.hdfs.blocks)) {
    if (block.replicas.length >= state.hdfs.replication) continue;
    const result = ensureReplication(state.hdfs, block.id, state.hdfs.replication);
    if (result.added.length) {
      events.push({ kind: 'replicate', blockId: block.id, to: result.added });
      lines.push(`re-replicated ${block.id} -> [${result.added.join(', ')}]`);
    }
  }
  return lines;
}

export type { Block, HdfsFile, HdfsState };
