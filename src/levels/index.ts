/**
 * Level definitions for LearnHadoop.
 * Goals use partial state; compare is subset / don't-care semantics.
 */
import type { PartialClusterState } from '../engine/types';

export type LevelSeries =
  | 'hdfs-basics'
  | 'replication'
  | 'mapreduce'
  | 'yarn'
  | 'e2e';

export type Difficulty = 1 | 2 | 3 | 4 | 5;

export interface Level {
  id: string;
  name: string;
  series: LevelSeries;
  difficulty: Difficulty;
  startDialog: string;
  hint?: string;
  startState?: PartialClusterState;
  goalState: PartialClusterState;
  /** Golf par (command count). */
  par: number;
  /** Optional free-form objective checklist shown in the rail. */
  objectives: string[];
}

function fileGoal(path: string): PartialClusterState {
  return {
    hdfs: {
      nodes: {
        [path]: {
          path,
          sizeBytes: 1,
          blockIds: ['x'],
          owner: 'hdfs',
          createdTick: 1,
          isDirectory: false,
        },
      } as never,
    },
  };
}

function dirGoal(path: string): PartialClusterState {
  return {
    hdfs: {
      nodes: {
        [path]: {
          path,
          sizeBytes: 0,
          blockIds: [],
          owner: 'hdfs',
          createdTick: 1,
          isDirectory: true,
        },
      } as never,
    },
  };
}

/** Goal: path must exist as non-directory; size/blocks don't-care via compare holes. */
function existsFile(path: string): PartialClusterState {
  return {
    hdfs: {
      nodes: {
        [path]: {
          path,
          isDirectory: false,
        },
      } as never,
    },
  };
}

function existsDir(path: string): PartialClusterState {
  return {
    hdfs: {
      nodes: {
        [path]: {
          path,
          isDirectory: true,
        },
      } as never,
    },
  };
}

function missingPath(path: string): PartialClusterState {
  // Encode "must not exist" by checking after — we handle this with a custom goal marker.
  // For now, use a sentinel object the compare won't see; app checks path absence separately.
  return {
    hdfs: {
      nodes: {
        [path]: undefined,
      } as never,
    },
  };
}

function jobDone(marker: string): PartialClusterState {
  return {
    mapreduce: {
      jobs: {
        [marker]: {
          id: marker,
          state: 'DONE',
        } as never,
      } as never,
    },
  };
}

function nodeAlive(id: string, alive: boolean): PartialClusterState {
  return {
    hdfs: {
      datanodes: {
        [id]: {
          id,
          alive,
        } as never,
      } as never,
    },
  };
}

function underReplicatedAtMost(n: number): PartialClusterState {
  // Handled specially in evaluateLevel via custom checkers.
  void n;
  return {};
}

export const LEVELS: Level[] = [
  // ── HDFS Basics ──────────────────────────────────────────────
  {
    id: 'hdfs-01-mkdir',
    name: 'Make a directory',
    series: 'hdfs-basics',
    difficulty: 1,
    par: 1,
    startDialog:
      'Welcome to LearnHadoop. You are operating a small cluster with a NameNode and four DataNodes.\n' +
      'Create the directory `/user/analyst/data` on HDFS.',
    hint: 'hdfs dfs -mkdir -p /user/analyst/data',
    goalState: existsDir('/user/analyst/data'),
    objectives: ['Create `/user/analyst/data`'],
  },
  {
    id: 'hdfs-02-put',
    name: 'Put a file (blocks appear)',
    series: 'hdfs-basics',
    difficulty: 1,
    par: 2,
    startDialog:
      'Upload `sales.csv` into `/user/analyst/data`.\n' +
      'Watch how the NameNode splits it into blocks and places replicas on DataNodes.',
    hint: 'hdfs dfs -mkdir /user/analyst/data  then  hdfs dfs -put sales.csv /user/analyst/data 2 "region,amount"',
    startState: undefined,
    goalState: existsFile('/user/analyst/data/sales.csv'),
    objectives: ['Create parent dirs if needed', 'Put `sales.csv` under `/user/analyst/data`'],
  },
  {
    id: 'hdfs-03-ls-stat',
    name: 'Inspect the filesystem',
    series: 'hdfs-basics',
    difficulty: 1,
    par: 2,
    startDialog:
      'A file is already on the cluster. List `/user/analyst/data` and `stat` the file to inspect its blocks.',
    hint: 'hdfs dfs -ls /user/analyst/data   and   hdfs dfs -stat /user/analyst/data/events.log',
    startState: {
      hdfs: {
        nodes: {
          '/user': {
            path: '/user',
            sizeBytes: 0,
            blockIds: [],
            owner: 'hdfs',
            createdTick: 0,
            isDirectory: true,
          },
          '/user/analyst': {
            path: '/user/analyst',
            sizeBytes: 0,
            blockIds: [],
            owner: 'hdfs',
            createdTick: 0,
            isDirectory: true,
          },
          '/user/analyst/data': {
            path: '/user/analyst/data',
            sizeBytes: 0,
            blockIds: [],
            owner: 'hdfs',
            createdTick: 0,
            isDirectory: true,
          },
        } as never,
      },
      mapreduce: {
        fileContents: {
          '/user/analyst/data/events.log': 'login ok\nlogin fail\npurchase',
        },
      },
    },
    // Goal: tick advanced after ls and stat (approx). We use a soft goal — file must exist.
    goalState: existsFile('/user/analyst/data/events.log'),
    objectives: ['`hdfs dfs -ls` the data dir', '`hdfs dfs -stat` the file'],
  },
  {
    id: 'hdfs-04-get-rm',
    name: 'Get and remove',
    series: 'hdfs-basics',
    difficulty: 2,
    par: 3,
    startDialog:
      'Copy `/data/raw/input.txt` to local as `input.txt`, then delete it from HDFS.',
    hint: 'hdfs dfs -get /data/raw/input.txt input.txt  then  hdfs dfs -rm /data/raw/input.txt',
    startState: {
      hdfs: {
        nodes: {
          '/data': {
            path: '/data',
            sizeBytes: 0,
            blockIds: [],
            owner: 'hdfs',
            createdTick: 0,
            isDirectory: true,
          },
          '/data/raw': {
            path: '/data/raw',
            sizeBytes: 0,
            blockIds: [],
            owner: 'hdfs',
            createdTick: 0,
            isDirectory: true,
          },
          '/data/raw/input.txt': {
            path: '/data/raw/input.txt',
            sizeBytes: 128,
            blockIds: ['blk_seed0', 'blk_seed1'],
            owner: 'hdfs',
            createdTick: 0,
            isDirectory: false,
          },
        } as never,
        blocks: {
          blk_seed0: { id: 'blk_seed0', sizeBytes: 64, replicas: ['dn1', 'dn2', 'dn3'] },
          blk_seed1: { id: 'blk_seed1', sizeBytes: 64, replicas: ['dn2', 'dn3', 'dn4'] },
        } as never,
        datanodes: {
          dn1: {
            id: 'dn1',
            host: 'node1',
            capacityBlocks: 32,
            alive: true,
            blocks: ['blk_seed0'],
          },
          dn2: {
            id: 'dn2',
            host: 'node2',
            capacityBlocks: 32,
            alive: true,
            blocks: ['blk_seed0', 'blk_seed1'],
          },
          dn3: {
            id: 'dn3',
            host: 'node3',
            capacityBlocks: 32,
            alive: true,
            blocks: ['blk_seed0', 'blk_seed1'],
          },
          dn4: {
            id: 'dn4',
            host: 'node4',
            capacityBlocks: 32,
            alive: true,
            blocks: ['blk_seed1'],
          },
        } as never,
      },
    },
    goalState: {
      hdfs: {
        nodes: {
          '/data/raw/input.txt': undefined,
        } as never,
      },
    },
    objectives: ['`get` the file locally', '`rm` it from HDFS'],
  },

  // ── Replication ──────────────────────────────────────────────
  {
    id: 'rep-01-setrep',
    name: 'Raise replication',
    series: 'replication',
    difficulty: 2,
    par: 1,
    startDialog:
      'The file `/logs/app.log` is replicated 2×. Raise replication to 3 and watch new replica tiles fly out.',
    hint: 'hdfs dfs -setrep 3 /logs/app.log',
    startState: {
      hdfs: {
        replication: 2,
        nodes: {
          '/logs': {
            path: '/logs',
            sizeBytes: 0,
            blockIds: [],
            owner: 'hdfs',
            createdTick: 0,
            isDirectory: true,
          },
          '/logs/app.log': {
            path: '/logs/app.log',
            sizeBytes: 64,
            blockIds: ['blk_r0'],
            owner: 'hdfs',
            createdTick: 0,
            isDirectory: false,
          },
        } as never,
        blocks: {
          blk_r0: { id: 'blk_r0', sizeBytes: 64, replicas: ['dn1', 'dn2'] },
        } as never,
        datanodes: {
          dn1: { id: 'dn1', host: 'node1', capacityBlocks: 32, alive: true, blocks: ['blk_r0'] },
          dn2: { id: 'dn2', host: 'node2', capacityBlocks: 32, alive: true, blocks: ['blk_r0'] },
          dn3: { id: 'dn3', host: 'node3', capacityBlocks: 32, alive: true, blocks: [] },
          dn4: { id: 'dn4', host: 'node4', capacityBlocks: 32, alive: true, blocks: [] },
        } as never,
      },
    },
    goalState: {
      hdfs: {
        blocks: {
          blk_r0: { id: 'blk_r0', replicas: ['dn1', 'dn2', 'dn3'] } as never,
        } as never,
      },
    },
    objectives: ['setrep to 3 on `/logs/app.log`', 'blk_r0 has 3 replicas'],
  },
  {
    id: 'rep-02-kill-dn',
    name: 'Survive a DataNode loss',
    series: 'replication',
    difficulty: 3,
    par: 2,
    startDialog:
      'A block is safely replicated. Kill DataNode `dn2` and verify the cluster re-replicates so the file stays available.\n' +
      'Then use `hdfs dfsadmin -report` to confirm.',
    hint: 'kill-datanode dn2   then   hdfs dfsadmin -report',
    startState: {
      hdfs: {
        nodes: {
          '/db': {
            path: '/db',
            sizeBytes: 0,
            blockIds: [],
            owner: 'hdfs',
            createdTick: 0,
            isDirectory: true,
          },
          '/db/tbl.csv': {
            path: '/db/tbl.csv',
            sizeBytes: 64,
            blockIds: ['blk_k0'],
            owner: 'hdfs',
            createdTick: 0,
            isDirectory: false,
          },
        } as never,
        blocks: {
          blk_k0: { id: 'blk_k0', sizeBytes: 64, replicas: ['dn1', 'dn2', 'dn3'] },
        } as never,
        datanodes: {
          dn1: { id: 'dn1', host: 'node1', capacityBlocks: 32, alive: true, blocks: ['blk_k0'] },
          dn2: { id: 'dn2', host: 'node2', capacityBlocks: 32, alive: true, blocks: ['blk_k0'] },
          dn3: { id: 'dn3', host: 'node3', capacityBlocks: 32, alive: true, blocks: ['blk_k0'] },
          dn4: { id: 'dn4', host: 'node4', capacityBlocks: 32, alive: true, blocks: [] },
        } as never,
      },
    },
    goalState: nodeAlive('dn2', false),
    objectives: ['Kill `dn2`', 'Block still has 3 replicas (re-replicated)'],
  },
  {
    id: 'rep-03-recovery',
    name: 'Bring the node back',
    series: 'replication',
    difficulty: 2,
    par: 2,
    startDialog: 'DataNode `dn3` is down. Bring it back online with `start-datanode dn3`.',
    hint: 'start-datanode dn3',
    startState: {
      hdfs: {
        datanodes: {
          dn3: { id: 'dn3', host: 'node3', capacityBlocks: 32, alive: false, blocks: [] },
        } as never,
      },
    },
    goalState: nodeAlive('dn3', true),
    objectives: ['`start-datanode dn3`'],
  },

  // ── MapReduce ────────────────────────────────────────────────
  {
    id: 'mr-01-put-text',
    name: 'Stage input text',
    series: 'mapreduce',
    difficulty: 2,
    par: 2,
    startDialog:
      'Before a MapReduce job, put text data on HDFS.\n' +
      'Put local file `notes.txt` with contents `the quick brown fox` into `/in`.',
    hint: 'hdfs dfs -mkdir /in   then   hdfs dfs -put notes.txt /in 1 the quick brown fox',
    goalState: existsFile('/in/notes.txt'),
    objectives: ['Create `/in`', 'Put `notes.txt` with text contents'],
  },
  {
    id: 'mr-02-wordcount',
    name: 'Run wordcount',
    series: 'mapreduce',
    difficulty: 3,
    par: 5,
    startDialog:
      'Input is staged at `/in/notes.txt`. Submit the wordcount job:\n' +
      '  yarn jar wordcount.jar /in/notes.txt /out\n' +
      'Then drive the pipeline with `mapreduce job -advance <jobId>` until DONE (map → shuffle → reduce).',
    hint: 'yarn jar wordcount.jar /in/notes.txt /out   then mapreduce job -list  and -advance <id> three times',
    startState: {
      hdfs: {
        nodes: {
          '/in': {
            path: '/in',
            sizeBytes: 0,
            blockIds: [],
            owner: 'hdfs',
            createdTick: 0,
            isDirectory: true,
          },
          '/in/notes.txt': {
            path: '/in/notes.txt',
            sizeBytes: 64,
            blockIds: ['blk_m0'],
            owner: 'hdfs',
            createdTick: 0,
            isDirectory: false,
          },
        } as never,
        blocks: {
          blk_m0: { id: 'blk_m0', sizeBytes: 64, replicas: ['dn1', 'dn2', 'dn3'] },
        } as never,
        datanodes: {
          dn1: { id: 'dn1', host: 'node1', capacityBlocks: 32, alive: true, blocks: ['blk_m0'] },
          dn2: { id: 'dn2', host: 'node2', capacityBlocks: 32, alive: true, blocks: ['blk_m0'] },
          dn3: { id: 'dn3', host: 'node3', capacityBlocks: 32, alive: true, blocks: ['blk_m0'] },
          dn4: { id: 'dn4', host: 'node4', capacityBlocks: 32, alive: true, blocks: [] },
        } as never,
      },
      mapreduce: {
        fileContents: {
          '/in/notes.txt': 'the quick brown fox the lazy dog',
        },
      },
    },
    goalState: existsDir('/out'),
    objectives: ['Submit wordcount job', 'Advance until job DONE', 'Output directory `/out` exists'],
  },
  {
    id: 'mr-03-output',
    name: 'Read the output',
    series: 'mapreduce',
    difficulty: 2,
    par: 4,
    startDialog: 'Run wordcount to completion and `cat` the reduce output under `/out`.',
    hint: 'yarn jar wordcount.jar /in/data /out; mapreduce job -advance <id> (×3); hdfs dfs -cat /out/_r-00000',
    startState: {
      hdfs: {
        nodes: {
          '/in': {
            path: '/in',
            sizeBytes: 0,
            blockIds: [],
            owner: 'hdfs',
            createdTick: 0,
            isDirectory: true,
          },
          '/in/data': {
            path: '/in/data',
            sizeBytes: 64,
            blockIds: ['blk_w0'],
            owner: 'hdfs',
            createdTick: 0,
            isDirectory: false,
          },
        } as never,
        blocks: {
          blk_w0: { id: 'blk_w0', sizeBytes: 64, replicas: ['dn1', 'dn2', 'dn3'] },
        } as never,
        datanodes: {
          dn1: { id: 'dn1', host: 'node1', capacityBlocks: 32, alive: true, blocks: ['blk_w0'] },
          dn2: { id: 'dn2', host: 'node2', capacityBlocks: 32, alive: true, blocks: ['blk_w0'] },
          dn3: { id: 'dn3', host: 'node3', capacityBlocks: 32, alive: true, blocks: ['blk_w0'] },
          dn4: { id: 'dn4', host: 'node4', capacityBlocks: 32, alive: true, blocks: [] },
        } as never,
      },
      mapreduce: {
        fileContents: {
          '/in/data': 'hadoop map reduce hadoop',
        },
      },
    },
    goalState: existsFile('/out/_r-00000'),
    objectives: ['Finish wordcount', 'Output part file exists'],
  },

  // ── YARN ─────────────────────────────────────────────────────
  {
    id: 'yarn-01-nodes',
    name: 'Inspect NodeManagers',
    series: 'yarn',
    difficulty: 1,
    par: 1,
    startDialog: 'List YARN nodes with `yarn node -list`.',
    hint: 'yarn node -list',
    goalState: {
      // Soft goal — any successful yarn node -list advances tick; we check via objectives in free play.
      tick: 1,
    },
    objectives: ['Run `yarn node -list`'],
  },
  {
    id: 'yarn-02-kill-nm',
    name: 'Lose a NodeManager',
    series: 'yarn',
    difficulty: 3,
    par: 2,
    startDialog:
      'Kill NodeManager `nm2` while looking at `yarn node -list`. If a job is running, its AM may be lost — YARN will fail the app.',
    hint: 'kill-nodemanager nm2',
    startState: {
      yarn: {
        nodeManagers: {
          nm2: {
            id: 'nm2',
            host: 'node2',
            memoryMb: 4096,
            vcores: 4,
            alive: true,
            containers: [],
          },
        } as never,
      },
    },
    goalState: {
      yarn: {
        nodeManagers: {
          nm2: {
            id: 'nm2',
            host: 'node2',
            memoryMb: 4096,
            vcores: 4,
            alive: false,
            containers: [],
          },
        } as never,
      },
    },
    objectives: ['`kill-nodemanager nm2`', 'Node shows LOST'],
  },
  {
    id: 'yarn-03-app-lifecycle',
    name: 'Kill an application',
    series: 'yarn',
    difficulty: 3,
    par: 6,
    startDialog:
      'Submit a job, list applications, then kill the YARN application before it finishes.\n' +
      'Use `yarn application -list`, note the app id, `yarn application -kill <id>`.',
    hint: 'stage input, yarn jar ..., yarn application -list, yarn application -kill <id>',
    startState: {
      hdfs: {
        nodes: {
          '/in': {
            path: '/in',
            sizeBytes: 0,
            blockIds: [],
            owner: 'hdfs',
            createdTick: 0,
            isDirectory: true,
          },
          '/in/x.txt': {
            path: '/in/x.txt',
            sizeBytes: 64,
            blockIds: ['blk_y0'],
            owner: 'hdfs',
            createdTick: 0,
            isDirectory: false,
          },
        } as never,
        blocks: {
          blk_y0: { id: 'blk_y0', sizeBytes: 64, replicas: ['dn1', 'dn2', 'dn3'] },
        } as never,
        datanodes: {
          dn1: { id: 'dn1', host: 'node1', capacityBlocks: 32, alive: true, blocks: ['blk_y0'] },
          dn2: { id: 'dn2', host: 'node2', capacityBlocks: 32, alive: true, blocks: ['blk_y0'] },
          dn3: { id: 'dn3', host: 'node3', capacityBlocks: 32, alive: true, blocks: ['blk_y0'] },
          dn4: { id: 'dn4', host: 'node4', capacityBlocks: 32, alive: true, blocks: [] },
        } as never,
      },
      mapreduce: {
        fileContents: { '/in/x.txt': 'one two three' },
      },
    },
    goalState: {
      yarn: {
        applications: {
          // any app in KILLED — checked specially
        },
      },
    },
    objectives: ['Submit wordcount', 'Find app id', 'Kill the application'],
  },

  // ── E2E ──────────────────────────────────────────────────────
  {
    id: 'e2e-01-pipeline',
    name: 'Full pipeline',
    series: 'e2e',
    difficulty: 4,
    par: 8,
    startDialog:
      'End-to-end: create `/in`, put `words.txt` with text `hadoop scales data`, run wordcount to `/out`, advance the job to DONE.',
    hint: 'mkdir /in; put words.txt /in 1 hadoop scales data; yarn jar wordcount.jar /in/words.txt /out; advance ×3',
    goalState: existsFile('/out/_r-00000'),
    objectives: ['Put input', 'Run wordcount', 'Job DONE', 'Output exists'],
  },
  {
    id: 'e2e-02-fault',
    name: 'Pipeline under failure',
    series: 'e2e',
    difficulty: 5,
    par: 10,
    startDialog:
      'Put a multi-block file, kill one DataNode (watch re-replication), then complete a wordcount job successfully.',
    hint: 'put with sizeUnits 2+; kill-datanode dn4; yarn jar ...; advance to DONE',
    goalState: existsFile('/out/_r-00000'),
    objectives: [
      'Put a multi-block file',
      'Kill a DataNode and re-replicate',
      'Complete wordcount',
    ],
  },
];

export function getLevel(id: string): Level | undefined {
  return LEVELS.find((l) => l.id === id);
}

export function levelsBySeries(): Record<LevelSeries, Level[]> {
  const out = {
    'hdfs-basics': [] as Level[],
    replication: [] as Level[],
    mapreduce: [] as Level[],
    yarn: [] as Level[],
    e2e: [] as Level[],
  };
  for (const l of LEVELS) out[l.series].push(l);
  return out;
}

export function nextLevel(id: string): Level | undefined {
  const idx = LEVELS.findIndex((l) => l.id === id);
  if (idx < 0) return LEVELS[0];
  return LEVELS[idx + 1];
}

// silence unused in some goal helpers
void fileGoal;
void dirGoal;
void missingPath;
void jobDone;
void underReplicatedAtMost;
