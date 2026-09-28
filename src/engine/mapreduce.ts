/**
 * MapReduce simulation: wordcount job pipeline (map → shuffle → reduce).
 * Jobs run to completion in discrete ticks when `advanceJob` is called;
 * the command layer drives stages so visuals can animate between them.
 */
import type {
  ClusterState,
  CommandResult,
  JobId,
  MrJob,
  MrTask,
  VisualEvent,
} from './types';
import { normalizePath } from './cluster';
import { put } from './hdfs';
import { allocateContainer, createApp, releaseContainer } from './yarn';

const MAP_MEM = 512;
const REDUCE_MEM = 512;
const AM_MEM = 512;

function ok(lines: string[], events?: VisualEvent[], mutated = false): CommandResult {
  return { ok: true, lines, events, mutated };
}

function err(message: string): CommandResult {
  return { ok: false, lines: [message], error: true, mutated: false };
}

export function tokenize(text: string): string[] {
  return text
    .toLowerCase()
    .split(/[^a-z0-9']+/)
    .filter(Boolean);
}

export function wordCount(text: string): Record<string, number> {
  const counts: Record<string, number> = {};
  for (const w of tokenize(text)) {
    counts[w] = (counts[w] ?? 0) + 1;
  }
  return counts;
}

/**
 * Submit a wordcount MapReduce job.
 * Creates YARN app + containers and sets job to MAP stage with all map tasks RUNNING.
 */
export function submitWordCount(
  state: ClusterState,
  inputPath: string,
  outputPath: string,
  combiner = false,
): CommandResult {
  const hdfs = state.hdfs;
  const input = normalizePath(inputPath);
  const output = normalizePath(outputPath);

  if (!hdfs.nodes[input]) return err(`Job submission failed: input '${input}' not found`);
  if (hdfs.nodes[input]!.isDirectory) {
    // Allow directory of files: concatenate contents.
  }
  if (hdfs.nodes[output]) return err(`Job submission failed: output '${output}' already exists`);

  // Gather input text.
  let text = '';
  const inputNode = hdfs.nodes[input]!;
  if (inputNode.isDirectory) {
    const children = Object.values(hdfs.nodes).filter(
      (n) => !n.isDirectory && n.path.startsWith(input + '/') && !n.path.slice(input.length + 1).includes('/'),
    );
    if (children.length === 0) return err(`Job submission failed: input dir '${input}' is empty`);
    for (const c of children) {
      text += (state.mapreduce.fileContents[c.path] ?? '') + '\n';
    }
  } else {
    text = state.mapreduce.fileContents[input] ?? `[data of ${input}]`;
  }

  const aliveNM = Object.values(state.yarn.nodeManagers).filter((n) => n.alive);
  if (aliveNM.length === 0) return err('Job submission failed: no live NodeManagers');

  const jobNum = state.mapreduce.nextJobNum++;
  const jobId: JobId = `job_${String(jobNum).padStart(4, '0')}_0001`;
  const app = createApp(state, 'wordcount', jobId);

  // AM container required.
  const am = allocateContainer(state, app.id, 'am', AM_MEM, 1);
  if (!am) {
    app.state = 'FAILED';
    app.diagnostics = 'Could not allocate AM container';
    return err('Job submission failed: could not allocate AM container');
  }
  app.amContainerId = am.id;
  app.state = 'ACCEPTED';

  const splits = inputNode.isDirectory
    ? Math.max(
        1,
        Object.values(hdfs.nodes).filter(
          (n) => !n.isDirectory && n.path.startsWith(input + '/'),
        ).length,
      )
    : Math.max(1, inputNode.blockIds.length);

  const numReduces = 1;
  const maps: MrTask[] = [];
  const reduces: MrTask[] = [];

  for (let i = 0; i < splits; i++) {
    const c = allocateContainer(state, app.id, 'map', MAP_MEM, 1);
    if (!c) {
      failJob(state, jobId, appIdOf(state, jobId), 'Could not allocate map container');
      return err(`Job submission failed: could not allocate map container ${i}`);
    }
    maps.push({ index: i, state: 'RUNNING', containerId: c.id, attempt: 1 });
  }

  // Reduce containers allocated at reduce stage; pre-create task slots.
  for (let i = 0; i < numReduces; i++) {
    reduces.push({ index: i, state: 'PENDING', attempt: 1 });
  }

  const counts = wordCount(text);

  const job: MrJob = {
    id: jobId,
    name: 'wordcount',
    appId: app.id,
    inputPath: input,
    outputPath: output,
    state: 'MAP',
    splits,
    numMaps: splits,
    numReduces,
    combiner,
    maps,
    reduces,
    outputFiles: [],
    wordCounts: counts,
  };
  state.mapreduce.jobs[jobId] = job;
  app.state = 'RUNNING';
  app.progress = 0.1;

  state.tick += 1;

  const events: VisualEvent[] = [
    { kind: 'job-start', jobId },
    { kind: 'job-stage', jobId, stage: 'MAP' },
  ];

  return ok(
    [
      `Submitting job ${jobId} (wordcount)`,
      `  input: ${input}`,
      `  output: ${output}`,
      `  splits/maps: ${splits}`,
      `  reduces: ${numReduces}`,
      `  combiner: ${combiner ? 'on' : 'off'}`,
      `  application: ${app.id}`,
      `  AM: ${am.id}`,
      ...maps.map((m) => `  map task ${m.index} on ${m.containerId}`),
    ],
    events,
    true,
  );
}

function appIdOf(state: ClusterState, jobId: JobId): string {
  return state.mapreduce.jobs[jobId]?.appId ?? '';
}

function failJob(state: ClusterState, jobId: JobId, appId: string, why: string): void {
  const job = state.mapreduce.jobs[jobId];
  if (job) {
    job.state = 'FAILED';
    for (const t of [...job.maps, ...job.reduces]) {
      if (t.state === 'RUNNING') t.state = 'FAILED';
      if (t.containerId) releaseContainer(state, t.containerId);
    }
  }
  for (const c of Object.values(state.yarn.containers)) {
    if (c.appId === appId) releaseContainer(state, c.id);
  }
  const app = state.yarn.applications[appId];
  if (app) {
    app.state = 'FAILED';
    app.diagnostics = why;
    app.progress = 0;
  }
}

/**
 * Advance a job one stage:
 * MAP -> SHUFFLE -> REDUCE -> DONE
 * Returns events for animation. Mutates state.
 */
export function advanceJob(state: ClusterState, jobId: JobId): CommandResult {
  const job = state.mapreduce.jobs[jobId];
  if (!job) return err(`job: unknown ${jobId}`);
  const events: VisualEvent[] = [];

  if (job.state === 'MAP') {
    // Complete all map tasks.
    for (const t of job.maps) {
      t.state = 'DONE';
      if (t.containerId) {
        releaseContainer(state, t.containerId);
        t.containerId = undefined;
      }
    }
    job.state = 'SHUFFLE';
    const app = state.yarn.applications[job.appId];
    if (app) app.progress = 0.4;
    events.push({ kind: 'job-stage', jobId, stage: 'SHUFFLE' });
    events.push({ kind: 'map-progress', jobId, done: job.numMaps, total: job.numMaps });
    state.tick += 1;
    return ok([`job ${jobId}: map complete (${job.numMaps}/${job.numMaps})`, `job ${jobId}: shuffle starting`], events, true);
  }

  if (job.state === 'SHUFFLE') {
    job.state = 'REDUCE';
    // Allocate reduce containers.
    for (const t of job.reduces) {
      const c = allocateContainer(state, job.appId, 'reduce', REDUCE_MEM, 1);
      if (!c) {
        failJob(state, job.id, job.appId, 'Could not allocate reduce container');
        return err(`job ${jobId}: could not allocate reduce container`);
      }
      t.containerId = c.id;
      t.state = 'RUNNING';
    }
    const app = state.yarn.applications[job.appId];
    if (app) app.progress = 0.7;
    events.push({ kind: 'job-stage', jobId, stage: 'REDUCE' });
    state.tick += 1;
    return ok(
      [
        `job ${jobId}: shuffle complete`,
        `job ${jobId}: reduce starting`,
        ...job.reduces.map((r) => `  reduce task ${r.index} on ${r.containerId}`),
      ],
      events,
      true,
    );
  }

  if (job.state === 'REDUCE') {
    for (const t of job.reduces) {
      t.state = 'DONE';
      if (t.containerId) {
        releaseContainer(state, t.containerId);
        t.containerId = undefined;
      }
    }
    job.state = 'DONE';

    // Write output file to HDFS.
    const outName = '_r-00000';
    const outPath = normalizePath(job.outputPath + '/' + outName);
    const counts = job.wordCounts ?? {};
    const lines = Object.keys(counts)
      .sort()
      .map((w) => `${w}\t${counts[w]}`);
    const body = lines.join('\n');

    // Create output directory + file via put-like placement.
    const hdfs = state.hdfs;
    if (!hdfs.nodes[job.outputPath]) {
      hdfs.nodes[job.outputPath] = {
        path: job.outputPath,
        sizeBytes: 0,
        blockIds: [],
        owner: 'hdfs',
        createdTick: state.tick,
        isDirectory: true,
      };
    }

    const sizeUnits = Math.max(1, Math.ceil(body.length / 64));
    const putResult = put(state, outName, job.outputPath, sizeUnits, body);
    if (!putResult.ok) {
      failJob(state, job.id, job.appId, 'Could not write output');
      return err(`job ${jobId}: failed to write output — ${putResult.lines.join(' ')}`);
    }

    job.outputFiles = [outPath];
    state.mapreduce.fileContents[outPath] = body;

    const app = state.yarn.applications[job.appId];
    if (app) {
      app.state = 'FINISHED';
      app.progress = 1;
    }

    // Release AM.
    if (app?.amContainerId) {
      releaseContainer(state, app.amContainerId);
    }

    events.push({ kind: 'job-stage', jobId, stage: 'DONE' });
    events.push({ kind: 'reduce-progress', jobId, done: job.numReduces, total: job.numReduces });
    events.push({ kind: 'job-done', jobId });
    state.tick += 1;

    return ok(
      [
        `job ${jobId}: reduce complete`,
        `job ${jobId}: DONE`,
        `  output: ${outPath}`,
        `  unique words: ${Object.keys(counts).length}`,
      ],
      events,
      true,
    );
  }

  return err(`job ${jobId}: cannot advance from state ${job.state}`);
}

export function jobStatus(state: ClusterState, jobId: JobId): CommandResult {
  const job = state.mapreduce.jobs[jobId];
  if (!job) return err(`job: unknown ${jobId}`);
  const mapsDone = job.maps.filter((m) => m.state === 'DONE').length;
  const redDone = job.reduces.filter((r) => r.state === 'DONE').length;
  return ok([
    `Job-Id: ${job.id}`,
    `Job-Name: ${job.name}`,
    `State: ${job.state}`,
    `Input: ${job.inputPath}`,
    `Output: ${job.outputPath}`,
    `maps: ${mapsDone}/${job.numMaps}`,
    `reduces: ${redDone}/${job.numReduces}`,
    `combiner: ${job.combiner ? 'on' : 'off'}`,
    ...(job.outputFiles.length ? [`outputs: ${job.outputFiles.join(', ')}`] : []),
  ]);
}

export function jobList(state: ClusterState): CommandResult {
  const jobs = Object.values(state.mapreduce.jobs);
  if (!jobs.length) return ok(['Total jobs: 0']);
  return ok([
    `Total jobs: ${jobs.length}`,
    ...jobs.map((j) => `${j.id}\t${j.name}\t${j.state}\t${j.inputPath} -> ${j.outputPath}`),
  ]);
}

/**
 * If any map/reduce container was lost, mark those tasks failed.
 * Returns whether the job is still runnable (for future retry logic).
 */
export function onContainerLost(state: ClusterState, containerId: string): void {
  for (const job of Object.values(state.mapreduce.jobs)) {
    if (job.state === 'DONE' || job.state === 'FAILED' || job.state === 'KILLED') continue;
    for (const t of [...job.maps, ...job.reduces] as MrTask[]) {
      if (t.containerId === containerId && t.state === 'RUNNING') {
        t.state = 'FAILED';
        t.containerId = undefined;
      }
    }
  }
}

export type { MrJob };
