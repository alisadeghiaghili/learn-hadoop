/**
 * Command dispatch: routes parsed commands to engine operations.
 * App-level commands (levels, undo) are handled by the shell via hooks.
 */
import type { ClusterState, CommandResult, VisualEvent } from '../engine/types';
import * as hdfs from '../engine/hdfs';
import * as yarn from '../engine/yarn';
import * as mr from '../engine/mapreduce';
import type { ParsedCommand } from './parser';

export interface DispatchHooks {
  /** Called for app commands (levels, undo, reset, help, ...). */
  appCommand?: (cmd: ParsedCommand, state: ClusterState) => CommandResult | null;
}

function fail(message: string): CommandResult {
  return { ok: false, lines: [message], error: true, mutated: false };
}

function okHelp(lines: string[]): CommandResult {
  return { ok: true, lines, mutated: false };
}

const HELP = [
  'LearnHadoop commands',
  '',
  'HDFS',
  '  hdfs dfs -mkdir <path>',
  '  hdfs dfs -put <local> <hdfsPath> [sizeUnits] [text...]',
  '  hdfs dfs -get <hdfsPath> [local]',
  '  hdfs dfs -ls [path]',
  '  hdfs dfs -cat <path>',
  '  hdfs dfs -rm [-r] <path>',
  '  hdfs dfs -stat <path>',
  '  hdfs dfs -setrep [-w] <n> <path>',
  '  hdfs dfsadmin -report',
  '  hdfs dfsadmin -safemode enter|leave',
  '',
  'YARN',
  '  yarn node -list',
  '  yarn application -list [-appStates ALL]',
  '  yarn application -status <appId>',
  '  yarn application -kill <appId>',
  '',
  'MapReduce',
  '  yarn jar wordcount.jar <in> <out> [combiner]',
  '  mapreduce job -list',
  '  mapreduce job -status <jobId>',
  '  mapreduce job -advance <jobId>   # drive map/shuffle/reduce stages',
  '',
  'Simulation helpers',
  '  kill-datanode <id> | start-datanode <id>',
  '  kill-nodemanager <id> | start-nodemanager <id>',
  '',
  'App',
  '  help | levels | sandbox | level <id> | goal | undo | reset | clear',
  '  build level | import level | export state',
];

export function dispatch(
  state: ClusterState,
  cmd: ParsedCommand,
  hooks: DispatchHooks = {},
): CommandResult {
  const { program, args } = cmd;

  // App commands first.
  if (hooks.appCommand) {
    const appResult = hooks.appCommand(cmd, state);
    if (appResult) return appResult;
  }

  switch (program) {
    case 'help':
      return okHelp(HELP);

    case 'clear':
      return { ok: true, lines: [], mutated: false };

    case 'hdfs':
      return dispatchHdfs(state, args);
    case 'yarn':
      return dispatchYarn(state, args);
    case 'mapreduce':
      return dispatchMapReduce(state, args);

    case 'kill-datanode': {
      const id = args[0];
      if (!id) return fail('usage: kill-datanode <id>');
      return hdfs.killDataNode(state, id);
    }
    case 'start-datanode': {
      const id = args[0];
      if (!id) return fail('usage: start-datanode <id>');
      return hdfs.startDataNode(state, id);
    }
    case 'kill-nodemanager': {
      const id = args[0];
      if (!id) return fail('usage: kill-nodemanager <id>');
      return yarn.killNodeManager(state, id);
    }
    case 'start-nodemanager': {
      const id = args[0];
      if (!id) return fail('usage: start-nodemanager <id>');
      return yarn.startNodeManager(state, id);
    }

    default:
      return fail(`${program}: command not found. Type 'help'.`);
  }
}

function dispatchHdfs(state: ClusterState, args: string[]): CommandResult {
  // forms: dfs -mkdir /x   |   dfsadmin -report
  const surface = args[0];
  const rest = args.slice(1);
  if (surface === 'dfs') return dispatchDfs(state, rest);
  if (surface === 'dfsadmin') return dispatchDfsAdmin(state, rest);
  return fail(`hdfs: unknown subcommand '${surface ?? ''}'. Try 'hdfs dfs' or 'hdfs dfsadmin'.`);
}

function dispatchDfs(state: ClusterState, args: string[]): CommandResult {
  // Collect -cmd and operands. Also support `hdfs dfs -mkdir /p` style.
  if (!args.length) return fail('hdfs dfs: missing operation (e.g. -mkdir)');

  const op = args[0]!;
  let rest = args.slice(1);
  const flags = new Set<string>();
  rest = rest.filter((t) => {
    if (t === '-r' || t === '-R' || t === '-w' || t === '-f' || t === '-p') {
      flags.add(t);
      return false;
    }
    return true;
  });

  switch (op) {
    case '-mkdir':
    case 'mkdir': {
      const p = rest[0];
      if (!p) return fail('usage: hdfs dfs -mkdir [-p] <path>');
      return hdfs.mkdir(state, p, flags.has('-p'));
    }
    case '-put':
    case 'put':
    case '-copyFromLocal':
    case 'copyFromLocal': {
      const local = rest[0];
      const dest = rest[1];
      if (!local || !dest) return fail('usage: hdfs dfs -put <local> <hdfsPath> [sizeUnits] [text...]');
      const sizeUnits = rest[2] ? Number(rest[2]) : 2;
      const text = rest.length > 3 ? rest.slice(3).join(' ') : undefined;
      return hdfs.put(state, local, dest, Number.isFinite(sizeUnits) ? sizeUnits : 2, text);
    }
    case '-get':
    case 'get':
    case '-copyToLocal':
    case 'copyToLocal': {
      const src = rest[0];
      if (!src) return fail('usage: hdfs dfs -get <hdfsPath> [local]');
      return hdfs.getFile(state, src, rest[1]);
    }
    case '-ls':
    case 'ls':
      return hdfs.ls(state, rest[0] ?? '/');
    case '-cat':
    case 'cat': {
      const p = rest[0];
      if (!p) return fail('usage: hdfs dfs -cat <path>');
      return hdfs.cat(state, p);
    }
    case '-rm':
    case 'rm': {
      const p = rest[0];
      if (!p) return fail('usage: hdfs dfs -rm [-r] <path>');
      return hdfs.rm(state, p, flags.has('-r') || flags.has('-R') || flags.has('-f'));
    }
    case '-stat':
    case 'stat': {
      const p = rest[0];
      if (!p) return fail('usage: hdfs dfs -stat <path>');
      return hdfs.stat(state, p);
    }
    case '-setrep':
    case 'setrep': {
      // -setrep [-w] <n> <path>
      const n = Number(rest[0]);
      const p = rest[1];
      if (!Number.isFinite(n) || !p) return fail('usage: hdfs dfs -setrep [-w] <n> <path>');
      return hdfs.setReplication(state, n, p, flags.has('-w'));
    }
    default:
      return fail(`hdfs dfs: unknown operation '${op}'`);
  }
}

function dispatchDfsAdmin(state: ClusterState, args: string[]): CommandResult {
  const op = args[0];
  if (op === '-report') return hdfs.report(state);
  if (op === '-safemode') {
    const action = args[1];
    if (action !== 'enter' && action !== 'leave') {
      return fail('usage: hdfs dfsadmin -safemode enter|leave');
    }
    return hdfs.safemode(state, action);
  }
  return fail(`hdfs dfsadmin: unknown operation '${op ?? ''}'`);
}

function dispatchYarn(state: ClusterState, args: string[]): CommandResult {
  const surface = args[0];
  const rest = args.slice(1);

  if (surface === 'node') {
    if (rest[0] === '-list' || rest.length === 0) return yarn.nodeList(state);
    return fail('usage: yarn node -list');
  }

  if (surface === 'application') {
    const op = rest[0];
    if (op === '-list' || !op) {
      const all = rest.includes('-appStates') || rest.includes('ALL') || rest.includes('-all');
      return yarn.appList(state, all);
    }
    if (op === '-status') {
      const id = rest[1];
      if (!id) return fail('usage: yarn application -status <appId>');
      return yarn.appStatus(state, id);
    }
    if (op === '-kill') {
      const id = rest[1];
      if (!id) return fail('usage: yarn application -kill <appId>');
      return yarn.appKill(state, id);
    }
    return fail(`yarn application: unknown op '${op}'`);
  }

  if (surface === 'jar') {
    // yarn jar wordcount.jar <in> <out> [combiner]
    const jar = rest[0];
    const input = rest[1];
    const output = rest[2];
    const combiner = rest[3] === 'combiner' || rest.includes('combiner');
    if (!jar || !input || !output) {
      return fail('usage: yarn jar wordcount.jar <in> <out> [combiner]');
    }
    return mr.submitWordCount(state, input, output, combiner);
  }

  return fail(`yarn: unknown subcommand '${surface ?? ''}'. Try 'yarn node -list' or 'yarn jar ...'.`);
}

function dispatchMapReduce(state: ClusterState, args: string[]): CommandResult {
  const surface = args[0];
  const rest = args.slice(1);
  if (surface === 'job') {
    const op = rest[0];
    if (op === '-list' || !op) return mr.jobList(state);
    if (op === '-status') {
      const id = rest[1];
      if (!id) return fail('usage: mapreduce job -status <jobId>');
      return mr.jobStatus(state, id);
    }
    if (op === '-advance') {
      const id = rest[1];
      if (!id) return fail('usage: mapreduce job -advance <jobId>');
      return mr.advanceJob(state, id);
    }
    return fail(`mapreduce job: unknown op '${op}'`);
  }
  return fail(`mapreduce: unknown subcommand '${surface ?? ''}'`);
}

export function formatResult(result: CommandResult): { text: string; events: VisualEvent[] } {
  return {
    text: result.lines.join('\n'),
    events: result.events ?? [],
  };
}
