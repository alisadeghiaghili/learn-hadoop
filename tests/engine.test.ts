import { describe, expect, it } from 'vitest';
import {
  createDefaultCluster,
  mkdir,
  put,
  ls,
  rm,
  setReplication,
  killDataNode,
  startDataNode,
  report,
  cat,
} from '../src/engine';
import { parseCommand, splitChain, tokenize } from '../src/commands';
import { dispatch } from '../src/commands/dispatch';
import { compareCluster } from '../src/engine/compare';
import { evaluateLevel } from '../src/engine/levelEval';
import { getLevel, LEVELS } from '../src/levels';
import { wordCount, advanceJob } from '../src/engine/mapreduce';

describe('HDFS engine', () => {
  it('mkdir creates nested path when parent exists', () => {
    const s = createDefaultCluster();
    expect(mkdir(s, '/user').ok).toBe(true);
    expect(mkdir(s, '/user/analyst').ok).toBe(true);
    expect(s.hdfs.nodes['/user/analyst']?.isDirectory).toBe(true);
  });

  it('mkdir fails when parent is missing', () => {
    const s = createDefaultCluster();
    const r = mkdir(s, '/user/analyst/data');
    expect(r.ok).toBe(false);
  });

  it('mkdir -p creates intermediate parents', () => {
    const s = createDefaultCluster();
    const r = mkdir(s, '/user/analyst/data', true);
    expect(r.ok).toBe(true);
    expect(s.hdfs.nodes['/user']?.isDirectory).toBe(true);
    expect(s.hdfs.nodes['/user/analyst']?.isDirectory).toBe(true);
    expect(s.hdfs.nodes['/user/analyst/data']?.isDirectory).toBe(true);
  });

  it('put places blocks with replication', () => {
    const s = createDefaultCluster();
    mkdir(s, '/in');
    const r = put(s, 'f.txt', '/in', 2, 'hello world');
    expect(r.ok).toBe(true);
    const file = s.hdfs.nodes['/in/f.txt'];
    expect(file?.blockIds.length).toBe(2);
    for (const bid of file!.blockIds) {
      expect(s.hdfs.blocks[bid]?.replicas.length).toBe(3);
    }
  });

  it('put into directory lands as child', () => {
    const s = createDefaultCluster();
    mkdir(s, '/in');
    put(s, 'f.txt', '/in', 1);
    expect(s.hdfs.nodes['/in/f.txt']).toBeTruthy();
  });

  it('ls lists children', () => {
    const s = createDefaultCluster();
    mkdir(s, '/a');
    put(s, 'x', '/a', 1);
    const r = ls(s, '/a');
    expect(r.lines.join('\n')).toContain('/a/x');
  });

  it('rm deletes file and blocks', () => {
    const s = createDefaultCluster();
    mkdir(s, '/a');
    put(s, 'x', '/a', 1);
    const before = Object.keys(s.hdfs.blocks).length;
    expect(before).toBeGreaterThan(0);
    expect(rm(s, '/a/x').ok).toBe(true);
    expect(s.hdfs.nodes['/a/x']).toBeUndefined();
    expect(Object.keys(s.hdfs.blocks).length).toBe(0);
  });

  it('setrep increases replica count', () => {
    const s = createDefaultCluster();
    // Use only 2 live DNs for the initial put? Default has 4, replication 3.
    mkdir(s, '/a');
    put(s, 'x', '/a', 1);
    // Force replication 2 by setrep down then up
    setReplication(s, 2, '/a/x');
    const bid = s.hdfs.nodes['/a/x']!.blockIds[0]!;
    expect(s.hdfs.blocks[bid]!.replicas.length).toBe(2);
    setReplication(s, 3, '/a/x');
    expect(s.hdfs.blocks[bid]!.replicas.length).toBe(3);
  });

  it('kill-datanode re-replicates blocks', () => {
    const s = createDefaultCluster();
    mkdir(s, '/a');
    put(s, 'x', '/a', 1);
    const bid = s.hdfs.nodes['/a/x']!.blockIds[0]!;
    const before = s.hdfs.blocks[bid]!.replicas.slice();
    const victim = before[0]!;
    const r = killDataNode(s, victim);
    expect(r.ok).toBe(true);
    expect(s.hdfs.datanodes[victim]!.alive).toBe(false);
    expect(s.hdfs.blocks[bid]!.replicas.length).toBe(3);
    expect(s.hdfs.blocks[bid]!.replicas).not.toContain(victim);
  });

  it('start-datanode brings node back', () => {
    const s = createDefaultCluster();
    killDataNode(s, 'dn1');
    expect(startDataNode(s, 'dn1').ok).toBe(true);
    expect(s.hdfs.datanodes['dn1']!.alive).toBe(true);
  });

  it('report shows live/dead counts', () => {
    const s = createDefaultCluster();
    killDataNode(s, 'dn4');
    const r = report(s);
    expect(r.lines.join('\n')).toContain('Live datanodes (3)');
    expect(r.lines.join('\n')).toContain('Dead datanodes (1)');
  });

  it('cat returns seeded text', () => {
    const s = createDefaultCluster();
    mkdir(s, '/in');
    put(s, 't.txt', '/in', 1, 'alpha beta');
    expect(cat(s, '/in/t.txt').lines).toContain('alpha beta');
  });
});

describe('parser', () => {
  it('tokenizes quotes', () => {
    expect(tokenize("hdfs dfs -put a 'hello world' /x")).toEqual([
      'hdfs',
      'dfs',
      '-put',
      'a',
      'hello world',
      '/x',
    ]);
  });

  it('parses program and flags', () => {
    const c = parseCommand('hdfs dfs -rm -r /x');
    expect(c?.program).toBe('hdfs');
    expect(c?.flags).toContain('-r');
  });

  it('splits chains', () => {
    expect(splitChain('a; b; c')).toEqual(['a', 'b', 'c']);
    expect(splitChain('put x "a;b" /y')).toEqual(['put x "a;b" /y']);
  });
});

describe('dispatch', () => {
  it('runs mkdir via shell syntax', () => {
    const s = createDefaultCluster();
    const r = dispatch(s, parseCommand('hdfs dfs -mkdir /x')!);
    expect(r.ok).toBe(true);
    expect(s.hdfs.nodes['/x']).toBeTruthy();
  });

  it('runs put with text', () => {
    const s = createDefaultCluster();
    dispatch(s, parseCommand('hdfs dfs -mkdir /in')!);
    const r = dispatch(s, parseCommand('hdfs dfs -put notes.txt /in 1 the quick brown fox')!);
    expect(r.ok).toBe(true);
    expect(s.mapreduce.fileContents['/in/notes.txt']).toBe('the quick brown fox');
  });

  it('unknown command errors', () => {
    const s = createDefaultCluster();
    const r = dispatch(s, parseCommand('foo bar')!);
    expect(r.ok).toBe(false);
  });

  it('help lists commands', () => {
    const s = createDefaultCluster();
    const r = dispatch(s, parseCommand('help')!);
    expect(r.lines.join('\n')).toContain('hdfs dfs -mkdir');
  });
});

describe('MapReduce', () => {
  it('wordCount counts tokens', () => {
    const c = wordCount('the cat the hat');
    expect(c.the).toBe(2);
    expect(c.cat).toBe(1);
  });

  it('full job pipeline to DONE writes output', () => {
    const s = createDefaultCluster();
    dispatch(s, parseCommand('hdfs dfs -mkdir /in')!);
    dispatch(s, parseCommand('hdfs dfs -put n.txt /in 1 the quick brown fox the lazy dog')!);
    const r = dispatch(s, parseCommand('yarn jar wordcount.jar /in/n.txt /out')!);
    expect(r.ok).toBe(true);
    const jobId = Object.keys(s.mapreduce.jobs)[0]!;
    expect(s.mapreduce.jobs[jobId]!.state).toBe('MAP');

    advanceJob(s, jobId);
    expect(s.mapreduce.jobs[jobId]!.state).toBe('SHUFFLE');
    advanceJob(s, jobId);
    expect(s.mapreduce.jobs[jobId]!.state).toBe('REDUCE');
    advanceJob(s, jobId);
    expect(s.mapreduce.jobs[jobId]!.state).toBe('DONE');
    expect(s.hdfs.nodes['/out/_r-00000']).toBeTruthy();
    expect(s.mapreduce.fileContents['/out/_r-00000']).toContain('the\t2');
  });
});

describe('compare + levels', () => {
  it('subset goal matches', () => {
    const s = createDefaultCluster();
    mkdir(s, '/a');
    const result = compareCluster(s, {
      hdfs: {
        nodes: {
          '/a': { path: '/a', isDirectory: true },
        } as never,
      },
    });
    expect(result.ok).toBe(true);
  });

  it('absence constraint fails when present', () => {
    const s = createDefaultCluster();
    mkdir(s, '/a');
    const result = compareCluster(s, {
      hdfs: {
        nodes: { '/a': undefined } as never,
      },
    });
    expect(result.ok).toBe(false);
  });

  it('absence constraint passes when missing', () => {
    const s = createDefaultCluster();
    const result = compareCluster(s, {
      hdfs: {
        nodes: { '/nope': undefined } as never,
      },
    });
    expect(result.ok).toBe(true);
  });

  it('every level has required fields', () => {
    for (const lv of LEVELS) {
      expect(lv.id).toBeTruthy();
      expect(lv.par).toBeGreaterThan(0);
      expect(lv.objectives.length).toBeGreaterThan(0);
      expect(lv.goalState).toBeTruthy();
    }
  });

  it('level hdfs-01 completes after mkdir', () => {
    const lv = getLevel('hdfs-01-mkdir')!;
    const s = createDefaultCluster();
    // parent path needs to exist — level goal is existsDir with only path+isDirectory;
    // user must create parents. createDefault only has '/'.
    mkdir(s, '/user');
    mkdir(s, '/user/analyst');
    mkdir(s, '/user/analyst/data');
    const evalR = evaluateLevel(s, lv, 3);
    expect(evalR.complete).toBe(true);
  });
});
