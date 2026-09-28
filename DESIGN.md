# LearnHadoop — Design & Architecture

Interactive Hadoop visualization and tutorial, modeled after LearnGitBranching (LGB).
Scope: full HDFS + YARN + MapReduce stack, LGB-depth product (sandbox, levels, golf,
undo/reset, level builder, import/export, share links, i18n scaffold).

## Style anchor

**Cluster ops console crossed with a block puzzle.**
Feel: a Bloomberg-style ops terminal for a small Hadoop cluster, drawn with the
legibility of a modern educational sandbox (LGB) and the tactile block grammar of
Minecraft-style storage. Not a marketing site. Not a dashboard template.

Reference points:
- LGB's command bar + free canvas + modal level builder workflow
- Apache Hadoop's yellow elephant identity used as accent, not as wallpaper
- Physical instrumentation panels (Nagra / mixing desk) for node cards and meters

## Palette

| Role | Hex | Use |
|------|-----|-----|
| Background | `#0F1419` | app canvas |
| Surface | `#1A2332` | panels, node cards |
| Surface raised | `#243044` | hover, active rows |
| Ink | `#E7ECF3` | primary text |
| Ink muted | `#8B9BB4` | labels, secondary |
| Accent (Hadoop) | `#FFD100` | NameNode, active blocks, primary CTA |
| Accent secondary | `#2DD4BF` | DataNode life, success paths, MR reduce |
| Danger | `#F87171` | dead nodes, failed jobs |
| Warn | `#FBBF24` | under-replicated, speculative |
| Map stage | `#60A5FA` | mapper data tint |
| Shuffle | `#C084FC` | shuffle/partition particles |
| Reduce | `#2DD4BF` | reduce / output |

Max 3 color families per diagram; accents only on meaning, never decoration.

## Typography

| Role | Family | Size / Weight |
|------|--------|---------------|
| Display / title | `"JetBrains Mono", "IBM Plex Mono", ui-monospace, monospace` | 20–28px / 500 |
| Body | `"Inter", "Segoe UI", system-ui, sans-serif` | 14px / 400 |
| Terminal | `"JetBrains Mono", ui-monospace, monospace` | 13px / 400 |
| Caption / metric | JetBrains Mono | 11px / 400 |
| CJK fallback | `"PingFang SC", "Microsoft YaHei", sans-serif` | as above |

Scale contrast: one loud display size (level title), everything else dense and
operational. Weight 500 for labels that must be scanned, 400 otherwise.

## Layout system

- Full-viewport app shell, CSS grid.
- **Top bar** (56px): brand, mode (Sandbox / Level), level series nav, undo/reset/help.
- **Left rail** (280px, collapsible): level brief, objective checklist, golf score.
- **Main canvas**: SVG/HTML visualization (topology or pipeline). Min height 420px.
- **Bottom terminal** (140–200px, resizable): command input + scrollable log.
- Spacing rhythm: 4px base; 8 / 12 / 16 / 24 / 32.
- Density: high. Node cards compact. No hero whitespace.
- Max content width inside panels: 720px for long prose; canvas is free.

### Responsive

- < 960px: left rail becomes overlay drawer; terminal docks bottom.
- < 640px: canvas stacks above terminal; node cards reflow to column.

## Signature moments

1. **Block flight on put.** File is split into blocks; each block flies as a
   tile from the client edge to a DataNode, then clones to replica peers with a
   short stagger. Under-replicated blocks pulse amber.
2. **MapReduce river.** Input splits slide into mappers (blue tint), emit as
   purple particles through a shuffle fan-in, coalesce into reducers (teal),
   and write output blocks back to HDFS. One continuous left-to-right path.

## Domain model (engine)

Pure TypeScript. No DOM. Serializable to JSON (export / undo / level goals).

```ts
ClusterState {
  tick: number
  hdfs: HdfsState
  yarn: YarnState
  mapreduce: MrState
}

HdfsState {
  blockSizeBytes: number          // simulated, default 64 "units"
  replication: number             // default 3
  files: Record<string, HdfsFile> // path -> file
  blocks: Record<string, Block>   // blockId -> block
  datanodes: Record<string, DataNode>
  namenode: { started: boolean }
}

HdfsFile { path, sizeBytes, blockIds[], owner, createdTick }
Block    { id, sizeBytes, replicas: DataNodeId[] }
DataNode { id, host, capacityBlocks, usedBlocks, alive, blocks: BlockId[] }

YarnState {
  nodeManagers: Record<string, NodeManager>
  applications: Record<string, YarnApp>
  nextAppId: number
}

NodeManager { id, host, memoryMb, vcores, usedMemoryMb, usedVcores, alive, containers: Container[] }
Container   { id, appId, memoryMb, vcores, nodeId, kind: 'map'|'reduce'|'am' }
YarnApp     { id, name, state: 'NEW'|'SUBMITTED'|'ACCEPTED'|'RUNNING'|'FINISHED'|'FAILED'|'KILLED', progress }

MrState {
  jobs: Record<string, MrJob>
}

MrJob {
  id, name, inputPath, outputPath
  state: 'PENDING'|'MAP'|'SHUFFLE'|'REDUCE'|'DONE'|'FAILED'
  splits: number
  maps: TaskState[]
  reduces: TaskState[]
  combiner: boolean
  outputFiles: string[]
}
```

### Goal compare

`compareCluster(current, goal) -> { ok, diffs[] }`
- Order-independent for maps/sets of ids (blocks, replicas, containers).
- Paths exact. Job state must match if present in goal.
- Missing keys in goal = don't-care (LGB tree-compare semantics).

## Command surface (CLI)

Sandbox accepts a Hadoop-ish shell. Simulation helpers mirror LGB's
`git fakeCreateRemote`.

```
hdfs dfs -mkdir <path>
hdfs dfs -put <local> <hdfsPath>     # splits into blocks, places replicas
hdfs dfs -get <hdfsPath> [local]
hdfs dfs -ls [path]
hdfs dfs -cat <hdfsPath>
hdfs dfs -rm [-r] <path>
hdfs dfs -stat <path>
hdfs dfs -setrep [-w] <n> <path>
hdfs dfsadmin -report
hdfs dfsadmin -safemode enter|leave
yarn node -list
yarn application -list [-appStates ALL]
yarn application -status <id>
yarn application -kill <id>
yarn jar wordcount.jar <in> <out> [combiner]
mapreduce job -status <id>
# simulation helpers
kill-datanode <id> | start-datanode <id>
kill-nodemanager <id> | start-nodemanager <id>
# app
levels | sandbox | level <id> | build level | import level
undo | reset | help | clear | goal | solution?
```

## Level system

JSON (TypeScript objects) levels:

```ts
Level {
  id, name, difficulty: 1|2|3|4|5
  series: 'hdfs-basics'|'replication'|'mapreduce'|'yarn'|'e2e'
  startState: ClusterState (partial, merged onto defaults)
  goalState: ClusterState (partial)
  startDialog?: string
  hint?: string
  solutionCommandCount?: number  // golf par
}
```

Series (initial content):
1. **hdfs-basics** — mkdir, put, ls, get, rm; see blocks appear
2. **replication** — setrep, kill a DataNode, watch re-replication
3. **mapreduce** — wordcount job; follow map/shuffle/reduce
4. **yarn** — containers, kill NM, app failure/retry
5. **e2e** — put data + run job + verify output under constraints

Golf: count successful commands that mutate state (or match a verb allowlist).
Lower is better. Par stored per level.

Share links: `?NODEMO&command=<urlencoded;&chain>` and `?level=<id>`.
Level builder produces JSON; `import level` accepts paste; gist id optional later.

## Architecture / module map

```
src/
  engine/     # pure state machines (hdfs, yarn, mapreduce, cluster, compare, serialize)
  commands/   # lexer/parser + dispatchers per surface
  visuals/    # DOM/SVG renderers + animation queue
  app/        # shell, terminal, levels runner, builder, undo stack, i18n
  levels/     # level definitions as data
  style/      # tokens + layout
tests/        # vitest unit tests for engine + compare + parser
```

Dependency direction: `app → visuals → engine`, `app → commands → engine`.
Engine never imports DOM.

## Tech

- Vite 6 + TypeScript 5 (strict)
- Vanilla DOM + SVG (no React)
- Vitest for unit tests
- No backend; 100% client-side (LGB parity)

## Build / run

```bash
npm install
npm run dev      # vite
npm test         # vitest
npm run build    # tsc --noEmit && vite build
```

## Explicit non-goals (v1)

- Real Hadoop RPC / real cluster
- Kerberos, HDFS Federation, YARN timeline server
- Full i18n translations (scaffold + en_US only)
- Gist import (paste JSON is enough; URL gist later)

## Product honesty note

This is a *simulation and teaching tool*, not a Hadoop compatibility layer.
Command semantics are pedagogically faithful (block split, replication factor,
pipeline stages) but simplified (fixed block size in abstract units, no network
latency model beyond animation timing, wordcount as the only MR job template).
