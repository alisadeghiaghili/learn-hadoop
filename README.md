# LearnHadoop

An interactive Hadoop visualization and tutorial — modeled on
[LearnGitBranching](https://github.com/pcottle/learnGitBranching).
Learn **HDFS**, **YARN**, and **MapReduce** by typing commands against a simulated
cluster and watching blocks, containers, and job stages move.

100% client-side. No backend.

## Quick start

```bash
npm install
npm run dev      # http://localhost:5173
npm test
npm run build
```

## What you can do

- **Sandbox** — free play on a 4-DataNode / 3-NodeManager cluster
- **Levels** — 15 challenges across 5 series (HDFS basics, replication, MapReduce, YARN, end-to-end)
- **Golf** — solve levels in fewer commands than par
- **Undo / Reset** — LGB-style recovery
- **Level builder** — capture the current cluster as a level and export JSON
- **Import level** — paste level JSON and play it
- **Share links** — `?level=hdfs-01-mkdir` or `?NODEMO&command=hdfs%20dfs%20-mkdir%20/x`

## Command surface

```
hdfs dfs -mkdir|-put|-get|-ls|-cat|-rm|-stat|-setrep ...
hdfs dfsadmin -report | -safemode enter|leave
yarn node -list
yarn application -list|-status|-kill
yarn jar wordcount.jar <in> <out> [combiner]
mapreduce job -list|-status|-advance
kill-datanode|start-datanode
kill-nodemanager|start-nodemanager
help | levels | level <id> | goal | undo | reset
```

MapReduce jobs run in discrete stages. After submitting a job, drive it with
`mapreduce job -advance <jobId>` through map → shuffle → reduce so you can watch
each phase.

## Architecture

```
src/
  engine/     pure cluster state machines (HDFS, YARN, MapReduce) + goal compare
  commands/   tokenizer + dispatcher
  visuals/    SVG topology / pipeline + animations
  app/        shell, terminal, undo, levels, builder
  levels/     level data
  style/      design tokens + layout
tests/        vitest unit tests
```

Engine is DOM-free and fully serializable (undo snapshots, level import/export).

See [DESIGN.md](./DESIGN.md) for visual direction and domain model.

## Honesty note

This is a **teaching simulator**, not a Hadoop compatibility layer. Block split,
replication, and job stages are pedagogically faithful but simplified (abstract
block size, wordcount as the MR template, discrete stage advances).
