/**
 * Shell-like command parser for the LearnHadoop terminal.
 */
export interface ParsedCommand {
  /** Full raw line. */
  raw: string;
  /** Program: hdfs | yarn | mapreduce | kill-datanode | ... | levels | undo ... */
  program: string;
  /** Remaining tokens after program (and subcommand tokens). */
  args: string[];
  /** Flags like -mkdir become cmd tokens; -r style flags collected here. */
  flags: string[];
}

/**
 * Tokenize a command line, honoring simple quotes.
 */
export function tokenize(line: string): string[] {
  const tokens: string[] = [];
  let cur = '';
  let quote: '"' | "'" | null = null;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i]!;
    if (quote) {
      if (ch === quote) {
        quote = null;
      } else {
        cur += ch;
      }
      continue;
    }
    if (ch === '"' || ch === "'") {
      quote = ch;
      continue;
    }
    if (/\s/.test(ch)) {
      if (cur) {
        tokens.push(cur);
        cur = '';
      }
      continue;
    }
    cur += ch;
  }
  if (cur) tokens.push(cur);
  return tokens;
}

export function parseCommand(line: string): ParsedCommand | null {
  const trimmed = line.trim();
  if (!trimmed || trimmed.startsWith('#')) return null;

  const tokens = tokenize(trimmed);
  if (!tokens.length) return null;

  const program = tokens[0]!;
  const rest = tokens.slice(1);
  const flags = rest.filter((t) => /^-{1,2}\w/.test(t));
  const args = rest;

  return { raw: trimmed, program, args, flags };
}

/** Split a line on `;` for command chains (share links). Respect quotes. */
export function splitChain(line: string): string[] {
  const parts: string[] = [];
  let cur = '';
  let quote: '"' | "'" | null = null;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i]!;
    if (quote) {
      cur += ch;
      if (ch === quote) quote = null;
      continue;
    }
    if (ch === '"' || ch === "'") {
      quote = ch;
      cur += ch;
      continue;
    }
    if (ch === ';') {
      if (cur.trim()) parts.push(cur.trim());
      cur = '';
      continue;
    }
    cur += ch;
  }
  if (cur.trim()) parts.push(cur.trim());
  return parts;
}
