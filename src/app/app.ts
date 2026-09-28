/**
 * Application shell: terminal, undo stack, levels, builder, share links.
 */
import {
  cloneState,
  createDefaultCluster,
  exportStatePretty,
  materialize,
  type ClusterState,
  type CommandResult,
  type PartialClusterState,
} from '../engine';
import { evaluateLevel } from '../engine/levelEval';
import { parseCommand, splitChain, type ParsedCommand } from '../commands';
import { dispatch } from '../commands/dispatch';
import { ClusterVisuals, type ViewMode } from '../visuals';
import { LEVELS, getLevel, nextLevel, levelsBySeries, type Level } from '../levels';

export interface SolvedRecord {
  [levelId: string]: { commands: number; par: number; at: string };
}

const STORAGE_KEY = 'learn-hadoop-solved-v1';

export class App {
  private state: ClusterState = createDefaultCluster();
  private undoStack: string[] = [];
  private commandCount = 0;
  private level: Level | null = null;
  private levelStartState: ClusterState | null = null;
  private solved: SolvedRecord = {};
  private visuals!: ClusterVisuals;
  private viewMode: ViewMode = 'topology';

  private els!: {
    app: HTMLElement;
    modeBadge: HTMLElement;
    levelTitle: HTMLElement;
    levelDialog: HTMLElement;
    objectives: HTMLElement;
    golf: HTMLElement;
    hintBox: HTMLElement;
    canvasHost: HTMLElement;
    log: HTMLElement;
    input: HTMLInputElement;
    levelList: HTMLElement;
  };

  constructor() {
    this.loadSolved();
  }

  start(): void {
    this.buildDom();
    this.parseUrl();
    this.refresh();
    this.els.input.focus();
    this.printBanner();
  }

  private loadSolved(): void {
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      if (raw) this.solved = JSON.parse(raw) as SolvedRecord;
    } catch {
      this.solved = {};
    }
  }

  private saveSolved(): void {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(this.solved));
  }

  private buildDom(): void {
    const app = document.getElementById('app');
    if (!app) throw new Error('#app missing');
    app.innerHTML = `
      <header class="topbar">
        <div class="brand">Learn<span>Hadoop</span></div>
        <div class="top-actions">
          <div class="seg">
            <button type="button" data-action="view-topology" class="seg-btn active">Topology</button>
            <button type="button" data-action="view-pipeline" class="seg-btn">Pipeline</button>
          </div>
          <span id="mode-badge" class="badge">sandbox</span>
          <button type="button" data-action="levels">Levels</button>
          <button type="button" data-action="undo">Undo</button>
          <button type="button" data-action="reset">Reset</button>
          <button type="button" data-action="build-level">Build level</button>
          <button type="button" data-action="import-level">Import</button>
          <button type="button" data-action="export-state">Export</button>
          <button type="button" data-action="help">Help</button>
        </div>
      </header>
      <div class="shell">
        <aside class="rail">
          <div class="rail-section">
            <h2 id="level-title">Sandbox</h2>
            <div id="level-dialog" class="dialog"></div>
          </div>
          <div class="rail-section">
            <h3>Objectives</h3>
            <ul id="objectives"></ul>
          </div>
          <div class="rail-section">
            <h3>Golf</h3>
            <div id="golf" class="golf"></div>
          </div>
          <div class="rail-section">
            <h3>Hint</h3>
            <div id="hint-box" class="hint"></div>
          </div>
          <div class="rail-section">
            <h3>Levels</h3>
            <div id="level-list" class="level-list"></div>
          </div>
        </aside>
        <main class="main">
          <div id="canvas-host" class="canvas-host"></div>
          <section class="terminal">
            <div id="log" class="log" aria-live="polite"></div>
            <div class="input-row">
              <span class="prompt">hdfs&gt;</span>
              <input id="cmd" type="text" spellcheck="false" autocomplete="off" placeholder="type help" />
            </div>
          </section>
        </main>
      </div>
      <div id="modal" class="modal hidden"></div>
    `;

    this.els = {
      app,
      modeBadge: app.querySelector('#mode-badge')!,
      levelTitle: app.querySelector('#level-title')!,
      levelDialog: app.querySelector('#level-dialog')!,
      objectives: app.querySelector('#objectives')!,
      golf: app.querySelector('#golf')!,
      hintBox: app.querySelector('#hint-box')!,
      canvasHost: app.querySelector('#canvas-host')!,
      log: app.querySelector('#log')!,
      input: app.querySelector('#cmd')!,
      levelList: app.querySelector('#level-list')!,
    };

    this.visuals = new ClusterVisuals({ root: this.els.canvasHost });

    app.addEventListener('click', (e) => {
      const btn = (e.target as HTMLElement).closest('[data-action]');
      if (!btn) return;
      const action = btn.getAttribute('data-action')!;
      void this.onAction(action, btn);
    });

    this.els.input.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        void this.runLine(this.els.input.value);
        this.els.input.value = '';
      }
    });
  }

  private async onAction(action: string, el: Element): Promise<void> {
    switch (action) {
      case 'view-topology':
        this.viewMode = 'topology';
        this.visuals.setMode('topology');
        this.setSeg('view-topology');
        break;
      case 'view-pipeline':
        this.viewMode = 'pipeline';
        this.visuals.setMode('pipeline');
        this.setSeg('view-pipeline');
        break;
      case 'levels':
        this.els.levelList.scrollIntoView({ behavior: 'smooth' });
        this.renderLevelList();
        break;
      case 'undo':
        this.undo();
        break;
      case 'reset':
        this.resetLevel();
        break;
      case 'help':
        await this.runLine('help');
        break;
      case 'build-level':
        this.openBuilder();
        break;
      case 'import-level':
        this.openImport();
        break;
      case 'export-state':
        this.openExport();
        break;
      case 'load-level': {
        const id = el.getAttribute('data-level-id')!;
        this.loadLevel(id);
        break;
      }
      case 'show-hint':
        this.els.hintBox.classList.toggle('visible');
        break;
      default:
        break;
    }
  }

  private setSeg(active: string): void {
    document.querySelectorAll('.seg-btn').forEach((b) => {
      b.classList.toggle('active', b.getAttribute('data-action') === active);
    });
  }

  private printBanner(): void {
    this.logLine('info', 'LearnHadoop — interactive HDFS + YARN + MapReduce tutorial');
    this.logLine('info', "Type 'help' for commands, 'levels' for challenges.");
  }

  private parseUrl(): void {
    const params = new URLSearchParams(location.search);
    const levelId = params.get('level');
    if (levelId && getLevel(levelId)) {
      this.loadLevel(levelId);
      return;
    }
    const command = params.get('command');
    if (command) {
      const line = decodeURIComponent(command);
      void this.runLine(line, { fromUrl: true });
    }
  }

  private logLine(kind: 'in' | 'out' | 'err' | 'info' | 'ok', text: string): void {
    const div = document.createElement('div');
    div.className = `log-line ${kind}`;
    div.textContent = text;
    this.els.log.appendChild(div);
    this.els.log.scrollTop = this.els.log.scrollHeight;
  }

  private logBlock(kind: 'out' | 'err' | 'ok', lines: string[]): void {
    for (const ln of lines) this.logLine(kind, ln);
  }

  async runLine(line: string, opts: { fromUrl?: boolean } = {}): Promise<void> {
    const chains = splitChain(line);
    for (const cmdLine of chains) {
      await this.runSingle(cmdLine, opts);
    }
  }

  private async runSingle(line: string, opts: { fromUrl?: boolean } = {}): Promise<void> {
    const cmd = parseCommand(line);
    if (!cmd) return;
    if (!opts.fromUrl) this.logLine('in', line);

    // Snapshot for undo.
    this.undoStack.push(JSON.stringify(this.state));
    if (this.undoStack.length > 80) this.undoStack.shift();

    const before = this.commandCount;
    const result = dispatch(this.state, cmd, {
      appCommand: (c, s) => this.appCommand(c, s),
    });

    if (result.error) {
      this.logBlock('err', result.lines);
      // Don't count failed commands toward golf; still keep undo entry? pop it.
      this.undoStack.pop();
    } else {
      if (result.lines.length) {
        this.logBlock(result.mutated ? 'ok' : 'out', result.lines);
      }
      if (result.mutated || cmd.program === 'levels' || cmd.program === 'goal' || cmd.program === 'help') {
        // count mutating commands + explicit learning actions
      }
      if (result.mutated) this.commandCount += 1;

      if (result.events?.length) {
        await this.visuals.playEvents(result.events);
      }
    }

    this.refresh();

    // Level complete?
    if (this.level && result.ok) {
      const evalResult = evaluateLevel(this.state, this.level, this.commandCount);
      if (evalResult.complete) {
        this.onLevelComplete();
      }
    }

    void before;
  }

  private appCommand(cmd: ParsedCommand, state: ClusterState): CommandResult | null {
    switch (cmd.program) {
      case 'sandbox':
        this.level = null;
        this.levelStartState = null;
        this.state = createDefaultCluster();
        this.commandCount = 0;
        this.undoStack = [];
        return { ok: true, lines: ['Switched to sandbox (fresh cluster).'], mutated: true };

      case 'levels': {
        const lines: string[] = ['Available levels:'];
        const by = levelsBySeries();
        for (const [series, list] of Object.entries(by)) {
          lines.push('', `## ${series}`);
          for (const l of list) {
            const mark = this.solved[l.id] ? '✓' : ' ';
            lines.push(`  [${mark}] ${l.id}  ${l.name}  (par ${l.par})`);
          }
        }
        return { ok: true, lines, mutated: false };
      }

      case 'level': {
        const id = cmd.args[0];
        if (!id) return { ok: false, lines: ['usage: level <id>'], error: true };
        const lv = getLevel(id);
        if (!lv) return { ok: false, lines: [`Unknown level '${id}'`], error: true };
        this.loadLevel(id);
        return { ok: true, lines: [`Loaded level ${lv.id}: ${lv.name}`], mutated: true };
      }

      case 'goal':
        if (!this.level) return { ok: true, lines: ['Sandbox — no goal. Try levels.'], mutated: false };
        return {
          ok: true,
          lines: [
            `Goal for ${this.level.id}`,
            ...this.level.objectives.map((o) => '  - ' + o),
            'Hint: ' + (this.level.hint ?? '(none)'),
          ],
          mutated: false,
        };

      case 'hint':
        return {
          ok: true,
          lines: [this.level?.hint ?? 'No hint (sandbox).'],
          mutated: false,
        };

      case 'undo':
        this.undo();
        return { ok: true, lines: ['Undo.'], mutated: false };

      case 'reset':
        this.resetLevel();
        return { ok: true, lines: ['Reset.'], mutated: false };

      case 'export': {
        if (cmd.args[0] === 'state' || cmd.args[0] === 'level' || !cmd.args.length) {
          return {
            ok: true,
            lines: exportStatePretty(state).split('\n').slice(0, 40).concat(['… (truncated; use Export button for full JSON)']),
            mutated: false,
          };
        }
        return null;
      }

      case 'build':
        if (cmd.args[0] === 'level') {
          this.openBuilder();
          return { ok: true, lines: ['Opened level builder.'], mutated: false };
        }
        return null;

      case 'import':
        if (cmd.args[0] === 'level') {
          this.openImport();
          return { ok: true, lines: ['Paste level JSON in the dialog.'], mutated: false };
        }
        return null;

      default:
        return null;
    }
  }

  private undo(): void {
    const snap = this.undoStack.pop();
    if (!snap) {
      this.logLine('err', 'Nothing to undo.');
      return;
    }
    this.state = JSON.parse(snap) as ClusterState;
    if (this.commandCount > 0) this.commandCount -= 1;
    this.refresh();
    this.logLine('ok', 'Undid last command.');
  }

  private resetLevel(): void {
    if (this.levelStartState) {
      this.state = cloneState(this.levelStartState);
      this.commandCount = 0;
      this.undoStack = [];
      this.refresh();
      this.logLine('ok', `Reset level ${this.level?.id ?? ''}.`);
    } else {
      this.state = createDefaultCluster();
      this.commandCount = 0;
      this.undoStack = [];
      this.refresh();
      this.logLine('ok', 'Reset sandbox.');
    }
  }

  private loadLevel(id: string): void {
    const lv = getLevel(id);
    if (!lv) return;
    this.level = lv;
    this.state = materialize(lv.startState as PartialClusterState);
    this.levelStartState = cloneState(this.state);
    this.commandCount = 0;
    this.undoStack = [];
    this.els.modeBadge.textContent = 'level · ' + lv.id;
    this.refresh();
    this.logLine('info', `— Level ${lv.id}: ${lv.name} —`);
    this.logBlock('out', lv.startDialog.split('\n'));
  }

  private onLevelComplete(): void {
    if (!this.level) return;
    const id = this.level.id;
    const prev = this.solved[id];
    const record = {
      commands: this.commandCount,
      par: this.level.par,
      at: new Date().toISOString(),
    };
    if (!prev || prev.commands > record.commands) {
      this.solved[id] = record;
      this.saveSolved();
    }
    this.logLine('ok', `✓ Level complete: ${this.level.name}`);
    if (this.commandCount <= this.level.par) {
      this.logLine('ok', `Golf: ${this.commandCount} (par ${this.level.par}) — par matched or beaten.`);
    } else {
      this.logLine('ok', `Golf: ${this.commandCount} (par ${this.level.par}) — can you do it in fewer?`);
    }
    const nxt = nextLevel(id);
    if (nxt) {
      this.logLine('info', `Next: level ${nxt.id} — ${nxt.name}`);
      this.logLine('info', `Load it with: level ${nxt.id}`);
    }
    // Mark complete in UI (don't auto-advance — LGB-style).
    this.els.levelTitle.textContent = this.level.name + '  ✓';
  }

  private refresh(): void {
    this.visuals.setState(this.state);
    if (this.viewMode === 'pipeline') this.visuals.setMode('pipeline');

    if (this.level) {
      this.els.levelTitle.textContent = this.level.name;
      this.els.levelDialog.textContent = this.level.startDialog;
      this.els.objectives.innerHTML = '';
      for (const o of this.level.objectives) {
        const li = document.createElement('li');
        li.textContent = o;
        this.els.objectives.appendChild(li);
      }
      const evalR = evaluateLevel(this.state, this.level, this.commandCount);
      this.els.golf.innerHTML = `
        <div>commands: <strong>${this.commandCount}</strong></div>
        <div>par: <strong>${this.level.par}</strong></div>
        <div>status: <strong>${evalR.complete ? 'COMPLETE' : 'in progress'}</strong></div>
      `;
      this.els.hintBox.textContent = this.level.hint ?? '';
    } else {
      this.els.levelTitle.textContent = 'Sandbox';
      this.els.levelDialog.textContent =
        'Mess with the cluster freely. undo / reset work here too. levels to learn.';
      this.els.objectives.innerHTML = '<li class="muted">No active level</li>';
      this.els.golf.innerHTML = `<div>commands: <strong>${this.commandCount}</strong></div>`;
      this.els.hintBox.textContent = '';
    }
    this.renderLevelList();
  }

  private renderLevelList(): void {
    const by = levelsBySeries();
    let html = '';
    for (const [series, list] of Object.entries(by)) {
      html += `<div class="series">${series}</div>`;
      for (const l of list) {
        const mark = this.solved[l.id] ? '✓' : '';
        const active = this.level?.id === l.id ? 'active' : '';
        html += `<button type="button" class="level-item ${active}" data-action="load-level" data-level-id="${l.id}">${mark} ${l.id}</button>`;
      }
    }
    this.els.levelList.innerHTML = html;
  }

  private openModal(title: string, bodyHtml: string, onMount?: (root: HTMLElement) => void): void {
    const modal = document.getElementById('modal')!;
    modal.classList.remove('hidden');
    modal.innerHTML = `
      <div class="modal-card">
        <div class="modal-head">
          <h2>${title}</h2>
          <button type="button" data-close class="ghost">Close</button>
        </div>
        <div class="modal-body">${bodyHtml}</div>
      </div>
    `;
    modal.querySelector('[data-close]')!.addEventListener('click', () => {
      modal.classList.add('hidden');
      modal.innerHTML = '';
    });
    onMount?.(modal);
  }

  private openBuilder(): void {
    this.openModal(
      'Level builder',
      `
      <p class="muted">Build a level from the current cluster as start state. Goal is checked live against a text objective list for now; export JSON and share.</p>
      <label>id <input id="b-id" value="custom-01" /></label>
      <label>name <input id="b-name" value="Custom challenge" /></label>
      <label>series
        <select id="b-series">
          <option>hdfs-basics</option><option>replication</option><option>mapreduce</option><option>yarn</option><option>e2e</option>
        </select>
      </label>
      <label>par <input id="b-par" type="number" value="5" /></label>
      <label>dialog <textarea id="b-dialog" rows="3"></textarea></label>
      <label>hint <input id="b-hint" /></label>
      <label>objectives (one per line) <textarea id="b-obj" rows="3"></textarea></label>
      <button type="button" id="b-generate">Generate JSON</button>
      <pre id="b-out" class="code"></pre>
      `,
      (root) => {
        root.querySelector('#b-generate')!.addEventListener('click', () => {
          const id = (root.querySelector('#b-id') as HTMLInputElement).value;
          const name = (root.querySelector('#b-name') as HTMLInputElement).value;
          const series = (root.querySelector('#b-series') as HTMLSelectElement).value;
          const par = Number((root.querySelector('#b-par') as HTMLInputElement).value);
          const dialog = (root.querySelector('#b-dialog') as HTMLTextAreaElement).value;
          const hint = (root.querySelector('#b-hint') as HTMLInputElement).value;
          const obj = (root.querySelector('#b-obj') as HTMLTextAreaElement).value
            .split('\n')
            .map((s) => s.trim())
            .filter(Boolean);

          const level = {
            id,
            name,
            series,
            difficulty: 2,
            par,
            startDialog: dialog,
            hint,
            startState: this.state,
            goalState: {},
            objectives: obj,
          };
          (root.querySelector('#b-out') as HTMLElement).textContent = JSON.stringify(level, null, 2);
        });
      },
    );
  }

  private openImport(): void {
    this.openModal(
      'Import level',
      `<label>Level JSON <textarea id="i-json" rows="14" class="code"></textarea></label>
       <button type="button" id="i-load">Load level</button>
       <div id="i-msg" class="muted"></div>`,
      (root) => {
        root.querySelector('#i-load')!.addEventListener('click', () => {
          try {
            const raw = (root.querySelector('#i-json') as HTMLTextAreaElement).value;
            const lv = JSON.parse(raw) as Level & { startState?: PartialClusterState };
            if (!lv.id || !lv.name) throw new Error('level needs id and name');
            // Register ephemerally by injecting into LEVELS if missing.
            if (!getLevel(lv.id)) {
              (LEVELS as Level[]).push({
                id: lv.id,
                name: lv.name,
                series: (lv.series as Level['series']) ?? 'e2e',
                difficulty: (lv.difficulty as Level['difficulty']) ?? 2,
                startDialog: lv.startDialog ?? '',
                hint: lv.hint,
                startState: lv.startState,
                goalState: lv.goalState ?? {},
                par: lv.par ?? 99,
                objectives: lv.objectives ?? [],
              });
            }
            this.loadLevel(lv.id);
            document.getElementById('modal')!.classList.add('hidden');
          } catch (e) {
            (root.querySelector('#i-msg') as HTMLElement).textContent = String(e);
          }
        });
      },
    );
  }

  private openExport(): void {
    const json = exportStatePretty(this.state);
    this.openModal(
      'Export cluster state',
      `<pre class="code" id="e-json"></pre>`,
      (root) => {
        (root.querySelector('#e-json') as HTMLElement).textContent = json;
      },
    );
  }
}
