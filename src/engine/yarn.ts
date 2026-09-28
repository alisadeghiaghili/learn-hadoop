/**
 * YARN simulation: NodeManagers, containers, application lifecycle.
 */
import type {
  AppId,
  ClusterState,
  CommandResult,
  Container,
  ContainerId,
  ContainerKind,
  NodeManager,
  NodeManagerId,
  VisualEvent,
  YarnApp,
} from './types';

function ok(lines: string[], events?: VisualEvent[], mutated = false): CommandResult {
  return { ok: true, lines, events, mutated };
}

function err(message: string): CommandResult {
  return { ok: false, lines: [message], error: true, mutated: false };
}

export function nodeList(state: ClusterState): CommandResult {
  const nms = Object.values(state.yarn.nodeManagers);
  const lines = [
    'Total Nodes: ' + nms.length,
    ...nms.map((nm) => {
      const status = nm.alive ? 'RUNNING' : 'LOST';
      return (
        `Node-Id\t${nm.id} (${nm.host})\tState=${status}` +
        `\tNM-Memory=${nm.memoryMb}MB used=${usedMemory(state, nm.id)}` +
        `\tVCores=${nm.vcores} used=${usedVcores(state, nm.id)}` +
        `\tContainers=${nm.containers.length}`
      );
    }),
  ];
  return ok(lines);
}

function usedMemory(state: ClusterState, nmId: NodeManagerId): number {
  const nm = state.yarn.nodeManagers[nmId];
  if (!nm) return 0;
  return nm.containers.reduce((s, cid) => s + (state.yarn.containers[cid]?.memoryMb ?? 0), 0);
}

function usedVcores(state: ClusterState, nmId: NodeManagerId): number {
  const nm = state.yarn.nodeManagers[nmId];
  if (!nm) return 0;
  return nm.containers.reduce((s, cid) => s + (state.yarn.containers[cid]?.vcores ?? 0), 0);
}

export function appList(state: ClusterState, allStates = false): CommandResult {
  const apps = Object.values(state.yarn.applications).filter((a) => {
    if (allStates) return true;
    return a.state === 'RUNNING' || a.state === 'ACCEPTED' || a.state === 'SUBMITTED' || a.state === 'NEW';
  });
  if (apps.length === 0) return ok(['Total applications: 0']);
  const lines = [
    `Total applications: ${apps.length}`,
    'Application-Id\tApplication-Name\tApplication-Type\tUser\tQueue\tState\tFinal-State\tProgress',
    ...apps.map(
      (a) =>
        `${a.id}\t${a.name}\tMAPREDUCE\thdfs\tdefault\t${a.state}\t` +
        `${a.state === 'FINISHED' ? 'SUCCEEDED' : a.state === 'FAILED' ? 'FAILED' : 'UNDEFINED'}\t` +
        `${(a.progress * 100).toFixed(1)}%`,
    ),
  ];
  return ok(lines);
}

export function appStatus(state: ClusterState, appId: AppId): CommandResult {
  const app = state.yarn.applications[appId];
  if (!app) return err(`application: unknown app ${appId}`);
  const job = app.jobId ? state.mapreduce.jobs[app.jobId] : undefined;
  return ok([
    `Application-Id: ${app.id}`,
    `Application-Name: ${app.name}`,
    `State: ${app.state}`,
    `Progress: ${(app.progress * 100).toFixed(1)}%`,
    ...(app.amContainerId ? [`AM-Container: ${app.amContainerId}`] : []),
    ...(app.diagnostics ? [`Diagnostics: ${app.diagnostics}`] : []),
    ...(job
      ? [
          `Job-Id: ${job.id}`,
          `Job-State: ${job.state}`,
          `maps: ${job.maps.filter((m) => m.state === 'DONE').length}/${job.numMaps}`,
          `reduces: ${job.reduces.filter((r) => r.state === 'DONE').length}/${job.numReduces}`,
        ]
      : []),
  ]);
}

export function appKill(state: ClusterState, appId: AppId): CommandResult {
  const app = state.yarn.applications[appId];
  if (!app) return err(`application: unknown app ${appId}`);
  if (app.state === 'FINISHED' || app.state === 'FAILED' || app.state === 'KILLED') {
    return err(`application: ${appId} already in ${app.state}`);
  }

  releaseAppContainers(state, appId);
  app.state = 'KILLED';
  app.diagnostics = 'Application killed by user';
  if (app.jobId) {
    const job = state.mapreduce.jobs[app.jobId];
    if (job && job.state !== 'DONE') job.state = 'KILLED';
  }
  state.tick += 1;
  return ok([`Killed application ${appId}`], undefined, true);
}

function releaseAppContainers(state: ClusterState, appId: AppId): void {
  for (const c of Object.values(state.yarn.containers)) {
    if (c.appId !== appId) continue;
    releaseContainer(state, c.id);
  }
}

export function releaseContainer(state: ClusterState, containerId: ContainerId): void {
  const c = state.yarn.containers[containerId];
  if (!c) return;
  const nm = state.yarn.nodeManagers[c.nodeId];
  if (nm) {
    nm.containers = nm.containers.filter((id) => id !== containerId);
  }
  delete state.yarn.containers[containerId];
}

export function allocateContainer(
  state: ClusterState,
  appId: AppId,
  kind: ContainerKind,
  memoryMb: number,
  vcores: number,
  preferredNode?: NodeManagerId,
): Container | null {
  const nms = Object.values(state.yarn.nodeManagers).filter((n) => n.alive);
  const ordered = preferredNode
    ? [...nms.filter((n) => n.id === preferredNode), ...nms.filter((n) => n.id !== preferredNode)]
    : nms;

  for (const nm of ordered) {
    const memUsed = usedMemory(state, nm.id);
    const coreUsed = usedVcores(state, nm.id);
    if (memUsed + memoryMb <= nm.memoryMb && coreUsed + vcores <= nm.vcores) {
      const id: ContainerId = `container_${state.yarn.nextContainerNum++}`;
      const container: Container = {
        id,
        appId,
        kind,
        memoryMb,
        vcores,
        nodeId: nm.id,
      };
      state.yarn.containers[id] = container;
      nm.containers.push(id);
      return container;
    }
  }
  return null;
}

export function createApp(
  state: ClusterState,
  name: string,
  jobId?: string,
): YarnApp {
  const id: AppId = `application_${String(state.yarn.nextAppNum++).padStart(4, '0')}_0001`;
  const app: YarnApp = {
    id,
    name,
    state: 'NEW',
    progress: 0,
    ...(jobId ? { jobId } : {}),
  };
  state.yarn.applications[id] = app;
  return app;
}

export function killNodeManager(state: ClusterState, id: NodeManagerId): CommandResult {
  const nm = state.yarn.nodeManagers[id];
  if (!nm) return err(`kill-nodemanager: unknown NodeManager ${id}`);
  if (!nm.alive) return err(`kill-nodemanager: ${id} is already down`);

  nm.alive = false;
  const events: VisualEvent[] = [{ kind: 'node-down', nodeId: id }];
  const lines = [`NodeManager ${id} (${nm.host}) is now down`];

  // Fail containers on this NM and possibly their apps.
  const affectedApps = new Set<AppId>();
  for (const cid of [...nm.containers]) {
    const c = state.yarn.containers[cid];
    if (c) {
      affectedApps.add(c.appId);
      if (c.kind === 'am') {
        const app = state.yarn.applications[c.appId];
        if (app && (app.state === 'RUNNING' || app.state === 'ACCEPTED')) {
          app.state = 'FAILED';
          app.diagnostics = `AM container ${cid} lost on ${id}`;
          if (app.jobId) {
            const job = state.mapreduce.jobs[app.jobId];
            if (job && job.state !== 'DONE') job.state = 'FAILED';
          }
          lines.push(`  application ${c.appId} FAILED (AM lost)`);
        }
      }
    }
    releaseContainer(state, cid);
  }
  nm.containers = [];
  lines.push(`  released ${affectedApps.size || 0} app container group(s)`);

  state.tick += 1;
  return ok(lines, events, true);
}

export function startNodeManager(state: ClusterState, id: NodeManagerId): CommandResult {
  const nm = state.yarn.nodeManagers[id];
  if (!nm) return err(`start-nodemanager: unknown NodeManager ${id}`);
  if (nm.alive) return err(`start-nodemanager: ${id} is already up`);
  nm.alive = true;
  state.tick += 1;
  return ok(
    [`NodeManager ${id} (${nm.host}) is back online`],
    [{ kind: 'node-up', nodeId: id }],
    true,
  );
}

export function findLiveNodeManager(state: ClusterState): NodeManager | null {
  return Object.values(state.yarn.nodeManagers).find((n) => n.alive) ?? null;
}

export type { NodeManager, YarnApp };
