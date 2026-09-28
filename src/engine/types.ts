/**
 * Core domain types for the LearnHadoop cluster simulator.
 * Pure data — no DOM, no framework.
 */

export type DataNodeId = string;
export type NodeManagerId = string;
export type BlockId = string;
export type JobId = string;
export type AppId = string;
export type ContainerId = string;
export type FilePath = string;

export interface DataNode {
  id: DataNodeId;
  host: string;
  capacityBlocks: number;
  alive: boolean;
  /** Block ids stored on this node. */
  blocks: BlockId[];
}

export interface Block {
  id: BlockId;
  sizeBytes: number;
  /** DataNode ids that hold a replica. */
  replicas: DataNodeId[];
}

export interface HdfsFile {
  path: FilePath;
  sizeBytes: number;
  blockIds: BlockId[];
  owner: string;
  createdTick: number;
  isDirectory: boolean;
}

export interface HdfsState {
  blockSizeBytes: number;
  replication: number;
  /** Directories and files keyed by absolute path. */
  nodes: Record<FilePath, HdfsFile>;
  blocks: Record<BlockId, Block>;
  datanodes: Record<DataNodeId, DataNode>;
  namenodeStarted: boolean;
  safemode: boolean;
  nextBlockNum: number;
}

export type ContainerKind = 'map' | 'reduce' | 'am';

export interface Container {
  id: ContainerId;
  appId: AppId;
  kind: ContainerKind;
  memoryMb: number;
  vcores: number;
  nodeId: NodeManagerId;
}

export type YarnAppState =
  | 'NEW'
  | 'SUBMITTED'
  | 'ACCEPTED'
  | 'RUNNING'
  | 'FINISHED'
  | 'FAILED'
  | 'KILLED';

export interface NodeManager {
  id: NodeManagerId;
  host: string;
  memoryMb: number;
  vcores: number;
  alive: boolean;
  containers: ContainerId[];
}

export interface YarnApp {
  id: AppId;
  name: string;
  state: YarnAppState;
  progress: number;
  /** Job id when this app wraps a MapReduce job. */
  jobId?: JobId;
  amContainerId?: ContainerId;
  diagnostics?: string;
}

export interface YarnState {
  nodeManagers: Record<NodeManagerId, NodeManager>;
  applications: Record<AppId, YarnApp>;
  containers: Record<ContainerId, Container>;
  nextAppNum: number;
  nextContainerNum: number;
}

export type MrJobState =
  | 'PENDING'
  | 'MAP'
  | 'SHUFFLE'
  | 'REDUCE'
  | 'DONE'
  | 'FAILED'
  | 'KILLED';

export type TaskState = 'PENDING' | 'RUNNING' | 'DONE' | 'FAILED';

export interface MrTask {
  index: number;
  state: TaskState;
  containerId?: ContainerId;
  attempt: number;
}

export interface MrJob {
  id: JobId;
  name: string;
  appId: AppId;
  inputPath: FilePath;
  outputPath: FilePath;
  state: MrJobState;
  splits: number;
  numMaps: number;
  numReduces: number;
  combiner: boolean;
  maps: MrTask[];
  reduces: MrTask[];
  outputFiles: FilePath[];
  /** Simulated word counts for the wordcount template. */
  wordCounts?: Record<string, number>;
}

export interface MrState {
  jobs: Record<JobId, MrJob>;
  nextJobNum: number;
  /** Wordcount corpus used when putting/reading text files. */
  fileContents: Record<FilePath, string>;
}

export interface ClusterState {
  tick: number;
  hdfs: HdfsState;
  yarn: YarnState;
  mapreduce: MrState;
}

/** Partial cluster state used for level start/goal definitions. */
export type PartialClusterState = {
  tick?: number;
  hdfs?: Partial<HdfsState>;
  yarn?: Partial<YarnState>;
  mapreduce?: Partial<MrState>;
};

export interface CommandResult {
  ok: boolean;
  /** Terminal output lines. */
  lines: string[];
  /** Optional animation events for the visual layer. */
  events?: VisualEvent[];
  /** True if the command mutated cluster state. */
  mutated?: boolean;
  error?: boolean;
}

export type VisualEvent =
  | { kind: 'put-blocks'; path: FilePath; blockIds: BlockId[]; replicas: Record<BlockId, DataNodeId[]> }
  | { kind: 'replicate'; blockId: BlockId; to: DataNodeId[] }
  | { kind: 'node-down'; nodeId: string }
  | { kind: 'node-up'; nodeId: string }
  | { kind: 'job-start'; jobId: JobId }
  | { kind: 'job-stage'; jobId: JobId; stage: MrJobState }
  | { kind: 'map-progress'; jobId: JobId; done: number; total: number }
  | { kind: 'reduce-progress'; jobId: JobId; done: number; total: number }
  | { kind: 'job-done'; jobId: JobId }
  | { kind: 'log'; message: string; level?: 'info' | 'warn' | 'error' };

export interface CommandContext {
  state: ClusterState;
}
