/**
 * SVG visualization of the Hadoop cluster: NameNode, DataNodes, blocks,
 * YARN NodeManagers, and a MapReduce pipeline lane.
 */
import type { ClusterState, VisualEvent, BlockId, DataNodeId } from '../engine/types';

export type ViewMode = 'topology' | 'pipeline';

export interface VisualsOptions {
  root: HTMLElement;
  onModeChange?: (mode: ViewMode) => void;
}

const COLORS = {
  bg: '#0F1419',
  surface: '#1A2332',
  raised: '#243044',
  ink: '#E7ECF3',
  muted: '#8B9BB4',
  accent: '#FFD100',
  secondary: '#2DD4BF',
  danger: '#F87171',
  warn: '#FBBF24',
  map: '#60A5FA',
  shuffle: '#C084FC',
  reduce: '#2DD4BF',
};

export class ClusterVisuals {
  private root: HTMLElement;
  private svg: SVGSVGElement;
  private mode: ViewMode = 'topology';
  private state: ClusterState | null = null;
  private animLayer: SVGGElement;

  constructor(opts: VisualsOptions) {
    this.root = opts.root;
    this.root.classList.add('visuals');
    this.root.innerHTML = '';
    this.svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    this.svg.setAttribute('viewBox', '0 0 960 520');
    this.svg.setAttribute('width', '100%');
    this.svg.setAttribute('height', '100%');
    this.svg.classList.add('cluster-svg');
    this.root.appendChild(this.svg);

    const bg = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
    bg.setAttribute('width', '960');
    bg.setAttribute('height', '520');
    bg.setAttribute('fill', COLORS.bg);
    this.svg.appendChild(bg);

    this.animLayer = document.createElementNS('http://www.w3.org/2000/svg', 'g');
    this.animLayer.setAttribute('class', 'anim-layer');
    this.svg.appendChild(this.animLayer);
  }

  setMode(mode: ViewMode): void {
    this.mode = mode;
    this.render();
  }

  setState(state: ClusterState): void {
    this.state = state;
    this.render();
  }

  async playEvents(events: VisualEvent[]): Promise<void> {
    for (const ev of events) {
      await this.playEvent(ev);
    }
    this.render();
  }

  private async playEvent(ev: VisualEvent): Promise<void> {
    switch (ev.kind) {
      case 'put-blocks':
        await this.animatePut(ev.path, ev.blockIds, ev.replicas);
        break;
      case 'replicate':
        await this.animateReplicate(ev.blockId, ev.to);
        break;
      case 'node-down':
        this.flashNode(ev.nodeId, COLORS.danger);
        await wait(280);
        break;
      case 'node-up':
        this.flashNode(ev.nodeId, COLORS.secondary);
        await wait(280);
        break;
      case 'job-stage':
        await this.animateStage(ev.stage);
        break;
      case 'job-done':
        this.burst(480, 260, COLORS.secondary);
        await wait(200);
        break;
      default:
        break;
    }
    this.render();
  }

  private flashNode(id: string, color: string): void {
    const el = this.svg.querySelector(`[data-node-id="${id}"]`);
    if (!el) return;
    el.setAttribute('stroke', color);
    el.setAttribute('stroke-width', '3');
    setTimeout(() => {
      el.setAttribute('stroke', COLORS.muted);
      el.setAttribute('stroke-width', '1');
    }, 400);
  }

  private async animatePut(
    path: string,
    blockIds: BlockId[],
    replicas: Record<BlockId, DataNodeId[]>,
  ): Promise<void> {
    void path;
    const dnPos = this.datanodePositions();
    for (let i = 0; i < blockIds.length; i++) {
      const bid = blockIds[i]!;
      const targets = replicas[bid] ?? [];
      // Client tile -> NameNode -> DataNodes
      for (const tid of targets.slice(0, 3)) {
        const pos = dnPos[tid];
        if (!pos) continue;
        await this.flyTile(80, 80, pos.x + 30, pos.y + 20, COLORS.accent, bid);
      }
    }
    await wait(80);
  }

  private async animateReplicate(blockId: BlockId, to: DataNodeId[]): Promise<void> {
    const dnPos = this.datanodePositions();
    const src = Object.values(dnPos)[0];
    for (const tid of to) {
      const pos = dnPos[tid];
      if (!pos || !src) continue;
      await this.flyTile(src.x + 30, src.y + 20, pos.x + 30, pos.y + 20, COLORS.secondary, blockId);
    }
  }

  private async animateStage(stage: string): Promise<void> {
    void stage;
    this.burst(480, 200, COLORS.map);
    await wait(160);
    this.burst(480, 280, COLORS.shuffle);
    await wait(160);
    this.burst(480, 360, COLORS.reduce);
    await wait(160);
  }

  private burst(x: number, y: number, color: string): void {
    for (let i = 0; i < 6; i++) {
      const c = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
      const ang = (i / 6) * Math.PI * 2;
      c.setAttribute('cx', String(x + Math.cos(ang) * 20));
      c.setAttribute('cy', String(y + Math.sin(ang) * 20));
      c.setAttribute('r', '3');
      c.setAttribute('fill', color);
      this.animLayer.appendChild(c);
      setTimeout(() => c.remove(), 350);
    }
  }

  private flyTile(
    x0: number,
    y0: number,
    x1: number,
    y1: number,
    color: string,
    label: string,
  ): Promise<void> {
    return new Promise((resolve) => {
      const g = document.createElementNS('http://www.w3.org/2000/svg', 'g');
      const rect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
      rect.setAttribute('width', '28');
      rect.setAttribute('height', '18');
      rect.setAttribute('rx', '3');
      rect.setAttribute('fill', color);
      const text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
      text.setAttribute('x', '14');
      text.setAttribute('y', '13');
      text.setAttribute('text-anchor', 'middle');
      text.setAttribute('font-size', '8');
      text.setAttribute('font-family', 'JetBrains Mono, monospace');
      text.setAttribute('fill', '#0F1419');
      text.textContent = label.replace('blk_', '');
      g.appendChild(rect);
      g.appendChild(text);
      this.animLayer.appendChild(g);

      const start = performance.now();
      const dur = 320;
      const step = (now: number) => {
        const t = Math.min(1, (now - start) / dur);
        const x = x0 + (x1 - x0) * t;
        const y = y0 + (y1 - y0) * t - Math.sin(t * Math.PI) * 24;
        g.setAttribute('transform', `translate(${x},${y})`);
        if (t < 1) {
          requestAnimationFrame(step);
        } else {
          g.remove();
          resolve();
        }
      };
      requestAnimationFrame(step);
    });
  }

  private datanodePositions(): Record<string, { x: number; y: number }> {
    const ids = this.state ? Object.keys(this.state.hdfs.datanodes) : [];
    const pos: Record<string, { x: number; y: number }> = {};
    const n = Math.max(ids.length, 1);
    ids.forEach((id, i) => {
      pos[id] = { x: 160 + i * (640 / n), y: 160 };
    });
    return pos;
  }

  render(): void {
    // Keep defs/bg/anim, clear content group.
    const existing = this.svg.querySelector('g.content');
    if (existing) existing.remove();
    const content = document.createElementNS('http://www.w3.org/2000/svg', 'g');
    content.setAttribute('class', 'content');
    this.svg.insertBefore(content, this.animLayer);

    if (!this.state) {
      this.drawText(content, 480, 260, 'No cluster state', COLORS.muted, 14, 'middle');
      return;
    }

    if (this.mode === 'pipeline') {
      this.renderPipeline(content);
    } else {
      this.renderTopology(content);
    }
  }

  private renderTopology(g: SVGGElement): void {
    const s = this.state!;
    // NameNode
    this.drawNodeCard(g, 40, 40, 200, 72, 'NameNode', s.hdfs.namenodeStarted ? COLORS.accent : COLORS.danger, [
      `files: ${Object.keys(s.hdfs.nodes).length}`,
      `blocks: ${Object.keys(s.hdfs.blocks).length}`,
      `rep: ${s.hdfs.replication}`,
    ], 'nn');

    // DataNodes
    const dns = Object.values(s.hdfs.datanodes);
    dns.forEach((dn, i) => {
      const x = 40 + i * 220;
      const y = 160;
      const stroke = dn.alive ? COLORS.secondary : COLORS.danger;
      this.drawNodeCard(g, x, y, 200, 110, dn.id + ' (' + dn.host + ')', stroke, [
        dn.alive ? 'In Service' : 'DOWN',
        `blocks: ${dn.blocks.length}/${dn.capacityBlocks}`,
        dn.blocks.slice(0, 6).join(' ') + (dn.blocks.length > 6 ? '…' : ''),
      ], dn.id);

      // Block tiles
      dn.blocks.slice(0, 8).forEach((bid, j) => {
        const bx = x + 10 + (j % 4) * 46;
        const by = y + 78 + Math.floor(j / 4) * 22;
        this.drawBlockTile(g, bx, by, bid, this.blockColor(bid));
      });
    });

    // YARN strip
    this.drawText(g, 40, 340, 'YARN', COLORS.muted, 12, 'start');
    const nms = Object.values(s.yarn.nodeManagers);
    nms.forEach((nm, i) => {
      const x = 40 + i * 220;
      const stroke = nm.alive ? COLORS.map : COLORS.danger;
      const memUsed = nm.containers.reduce(
        (a, cid) => a + (s.yarn.containers[cid]?.memoryMb ?? 0),
        0,
      );
      this.drawNodeCard(g, x, 360, 200, 88, nm.id, stroke, [
        nm.alive ? 'RUNNING' : 'LOST',
        `mem ${memUsed}/${nm.memoryMb}`,
        `containers: ${nm.containers.length}`,
      ], nm.id);
    });

    // Job strip
    const jobs = Object.values(s.mapreduce.jobs);
    this.drawText(g, 40, 480, `Jobs: ${jobs.length}`, COLORS.muted, 11, 'start');
    jobs.forEach((j, i) => {
      this.drawText(
        g,
        120 + i * 160,
        480,
        `${j.id} ${j.state}`,
        j.state === 'DONE' ? COLORS.secondary : j.state === 'FAILED' ? COLORS.danger : COLORS.accent,
        11,
        'start',
      );
    });
  }

  private renderPipeline(g: SVGGElement): void {
    const s = this.state!;
    const jobs = Object.values(s.mapreduce.jobs);
    const job = jobs[jobs.length - 1];

    this.drawText(g, 480, 36, 'MapReduce pipeline', COLORS.ink, 16, 'middle');

    // Lanes: Input → Map → Shuffle → Reduce → Output
    const stages = [
      { label: 'Input', x: 80, color: COLORS.muted },
      { label: 'Map', x: 260, color: COLORS.map },
      { label: 'Shuffle', x: 440, color: COLORS.shuffle },
      { label: 'Reduce', x: 620, color: COLORS.reduce },
      { label: 'Output', x: 800, color: COLORS.accent },
    ];

    for (const st of stages) {
      this.drawNodeCard(g, st.x, 100, 140, 64, st.label, st.color, [], 'stage-' + st.label);
    }

    // Arrows
    for (let i = 0; i < stages.length - 1; i++) {
      const a = stages[i]!;
      const b = stages[i + 1]!;
      this.drawArrow(g, a.x + 140, 132, b.x, 132, COLORS.muted);
    }

    // Active stage highlight
    if (job) {
      const activeIndex =
        job.state === 'PENDING'
          ? 0
          : job.state === 'MAP'
            ? 1
            : job.state === 'SHUFFLE'
              ? 2
              : job.state === 'REDUCE'
                ? 3
                : 4;
      const st = stages[activeIndex]!;
      this.drawNodeCard(g, st.x, 100, 140, 64, st.label, st.color, [job.state], 'stage-active', 2.5);
    }

    // Flow particles
    if (job && job.state !== 'DONE') {
      const y = 220;
      this.drawText(g, 80, y, `job ${job.id}`, COLORS.ink, 13, 'start');
      this.drawText(
        g,
        80,
        y + 24,
        `maps ${job.maps.filter((m) => m.state === 'DONE').length}/${job.numMaps}` +
          `  reduces ${job.reduces.filter((r) => r.state === 'DONE').length}/${job.numReduces}`,
        COLORS.muted,
        12,
        'start',
      );
      this.drawText(
        g,
        80,
        y + 48,
        `combiner: ${job.combiner ? 'on' : 'off'}  input: ${job.inputPath}`,
        COLORS.muted,
        12,
        'start',
      );
    } else if (job) {
      this.drawText(g, 80, 220, `job ${job.id} DONE`, COLORS.secondary, 13, 'start');
      this.drawText(g, 80, 244, `output: ${job.outputFiles.join(', ')}`, COLORS.muted, 12, 'start');
    } else {
      this.drawText(g, 80, 220, 'No job running. Submit with yarn jar wordcount.jar <in> <out>', COLORS.muted, 12, 'start');
    }

    // Sample tokens for wordcount if present
    if (job?.wordCounts) {
      const words = Object.entries(job.wordCounts).slice(0, 12);
      this.drawText(g, 80, 320, 'Word counts', COLORS.muted, 11, 'start');
      words.forEach(([w, n], i) => {
        const x = 80 + (i % 6) * 140;
        const yy = 350 + Math.floor(i / 6) * 28;
        this.drawText(g, x, yy, `${w}  ${n}`, COLORS.ink, 13, 'start');
      });
    }
  }

  private blockColor(bid: BlockId): string {
    void bid;
    return COLORS.accent;
  }

  private drawNodeCard(
    g: SVGGElement,
    x: number,
    y: number,
    w: number,
    h: number,
    title: string,
    stroke: string,
    lines: string[],
    nodeId: string,
    strokeWidth = 1,
  ): void {
    const rect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
    rect.setAttribute('x', String(x));
    rect.setAttribute('y', String(y));
    rect.setAttribute('width', String(w));
    rect.setAttribute('height', String(h));
    rect.setAttribute('rx', '8');
    rect.setAttribute('fill', COLORS.surface);
    rect.setAttribute('stroke', stroke);
    rect.setAttribute('stroke-width', String(strokeWidth));
    rect.setAttribute('data-node-id', nodeId);
    g.appendChild(rect);

    this.drawText(g, x + 12, y + 18, title, COLORS.ink, 13, 'start');
    lines.forEach((ln, i) => {
      this.drawText(g, x + 12, y + 38 + i * 16, ln, COLORS.muted, 11, 'start');
    });
  }

  private drawBlockTile(g: SVGGElement, x: number, y: number, label: string, fill: string): void {
    const rect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
    rect.setAttribute('x', String(x));
    rect.setAttribute('y', String(y));
    rect.setAttribute('width', '42');
    rect.setAttribute('height', '18');
    rect.setAttribute('rx', '3');
    rect.setAttribute('fill', fill);
    g.appendChild(rect);
    this.drawText(g, x + 21, y + 12, label.replace('blk_', 'b'), '#0F1419', 9, 'middle');
  }

  private drawArrow(g: SVGGElement, x0: number, y0: number, x1: number, y1: number, stroke: string): void {
    const line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
    line.setAttribute('x1', String(x0));
    line.setAttribute('y1', String(y0));
    line.setAttribute('x2', String(x1));
    line.setAttribute('y2', String(y1));
    line.setAttribute('stroke', stroke);
    line.setAttribute('stroke-width', '1.5');
    line.setAttribute('marker-end', 'url(#arrow)');
    g.appendChild(line);
  }

  private drawText(
    g: SVGGElement,
    x: number,
    y: number,
    text: string,
    fill: string,
    size: number,
    anchor: 'start' | 'middle' | 'end',
  ): void {
    const t = document.createElementNS('http://www.w3.org/2000/svg', 'text');
    t.setAttribute('x', String(x));
    t.setAttribute('y', String(y));
    t.setAttribute('fill', fill);
    t.setAttribute('font-size', String(size));
    t.setAttribute('font-family', 'JetBrains Mono, ui-monospace, monospace');
    t.setAttribute('text-anchor', anchor);
    t.textContent = text;
    g.appendChild(t);
  }
}

function wait(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}
