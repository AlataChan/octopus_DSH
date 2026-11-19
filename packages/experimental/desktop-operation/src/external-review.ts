/** On-demand read-only delegation through the current desktop user's native CLIs. */
import { access, readdir, readFile, stat } from 'node:fs/promises'
import { constants } from 'node:fs'
import { homedir } from 'node:os'
import { delimiter, dirname, isAbsolute, join } from 'node:path'
import { createInterface } from 'node:readline'
import type { Context } from '@deepseek-ai/cordis'
import type { SubprocessSpawnSpec } from '@deepseek-ai/dsh-subprocess'
import { defineTool } from '@deepseek-ai/dsh-tools'
import z from '@deepseek-ai/schemastery'

/** Loader-facing plugin name. */
export const name = 'desktop-external-review'
/** Services required to register and run a review. */
export const inject = ['tools', 'subprocess']

type Reviewer = 'codex' | 'codebuddy'

/** Maximum duration and buffered output of each external review. */
export interface Config {
  timeoutMs: number
  maxBytes: number
}
/** Deployment limits validated on profile load. */
export const Config: z<Config> = z.object({
  timeoutMs: z.number().step(1).min(1).max(2147483647).default(180000),
  maxBytes: z.number().step(1).min(1024).default(4 * 1024 * 1024),
})

function fields(value: unknown): Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value) ? value as Record<string, unknown> : {}
}

function decode(text: string): unknown {
  try { return JSON.parse(text) } catch (_invalidJson) {
    throw new Error('CLI 返回了无法识别的响应，请检查 CLI 版本。')
  }
}

async function directories(path: string) {
  try {
    return (await readdir(path, { withFileTypes: true })).filter(item => item.isDirectory()).map(item => item.name).sort().reverse()
  } catch (error) {
    if (['ENOENT', 'ENOTDIR', 'EACCES'].includes((error as NodeJS.ErrnoException).code ?? '')) return []
    throw error
  }
}

async function executable(path: string) {
  try {
    await access(path, constants.X_OK)
    return (await stat(path)).isFile()
  } catch (error) {
    if (['ENOENT', 'ENOTDIR', 'EACCES'].includes((error as NodeJS.ErrnoException).code ?? '')) return false
    throw error
  }
}

async function locate(agent: Reviewer) {
  const home = homedir()
  const nvm = join(home, '.nvm/versions/node')
  const bins = [...(process.env.PATH ?? '').split(delimiter).filter(isAbsolute), join(home, '.local/bin'), join(home, '.npm-global/bin'), '/opt/homebrew/bin', '/usr/local/bin', ...(await directories(nvm)).map(version => join(nvm, version, 'bin'))]
  for (const dir of new Set(bins)) {
    const file = join(dir, agent)
    if (await executable(file)) return file
  }
  if (agent === 'codebuddy') {
    const cache = join(process.env.npm_config_cache ?? join(home, '.npm'), '_npx')
    for (const dir of await directories(cache)) {
      const pkg = join(cache, dir, 'node_modules/@tencent-ai/codebuddy-code')
      const file = join(pkg, 'bin/codebuddy')
      if (await executable(file) && fields(decode(await readFile(join(pkg, 'package.json'), 'utf8'))).name === '@tencent-ai/codebuddy-code') return file
    }
  }
  throw new Error(agent === 'codex' ? '未找到 Codex CLI。请安装 Codex CLI，再运行 codex login。' : '未找到 CodeBuddy CLI。请安装腾讯 CodeBuddy CLI 并运行 codebuddy 登录。仅安装 WorkBuddy 桌面端不足以执行委派。')
}

const buddyTools = ['--tools', 'Read,Grep,Glob', '--allowedTools', 'Read,Grep,Glob', '--permission-mode', 'dontAsk', '--strict-mcp-config']

function failure(agent: Reviewer, detail: string): Error {
  if (/not logged in|authentication required|unauthorized|401\b|login required|please log in/i.test(detail)) {
    return new Error(agent === 'codex' ? 'Codex CLI 需要登录，请运行 codex login。' : 'CodeBuddy CLI 需要登录，请运行 codebuddy 完成登录。')
  }
  if (/socket|network|tls|econn|enotfound|timed? ?out|fetch failed/i.test(detail)) return new Error(agent + ' 连接失败，请检查网络及代理。')
  return new Error(agent + ' CLI 请求失败，请在终端检查 CLI 版本、账户状态及服务可用性。')
}

type Launch = (argv: string[], stdio: SubprocessSpawnSpec['stdio']) => ReturnType<Context['subprocess']['spawn']>

async function checkBuddy(launch: Launch, cwd: string, maxBytes: number) {
  const child = launch(['--acp', ...buddyTools], { stdin: 'pipe', stdout: 'pipe', stderr: { maxBytes } })
  const input = child.stdin
  if (!input || !child.stdout) {
    child.terminate()
    await child.done
    throw failure('codebuddy', '')
  }
  const lines = createInterface({ input: child.stdout })
  const request = (id: number, method: string, params: object) => input.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n')
  // A short-lived CLI can exit before reading initialize.
  input.on('error', () => { lines.close() })
  const close = () => { lines.close() }
  const closed = child.done.then(close, close)
  try {
    request(1, 'initialize', { protocolVersion: 1, clientCapabilities: {} })
    let bytes = 0
    for await (const line of lines) {
      bytes += Buffer.byteLength(line)
      if (bytes > maxBytes) throw failure('codebuddy', '')
      const row = fields(decode(line))
      const error = fields(row.error)
      const result = fields(row.result)
      if (row.error) throw failure('codebuddy', error.code === -32000 ? 'authentication required' : typeof error.message === 'string' ? error.message : '')
      if (row.id === 1 && result.protocolVersion === 1) request(2, 'session/new', { cwd, mcpServers: [] })
      if (row.id === 2 && typeof result.sessionId === 'string' && result.sessionId) return
    }
    throw failure('codebuddy', child.collected.stderr?.readFrom(0).text ?? '')
  } finally {
    lines.close()
    child.terminate()
    await closed
  }
}

async function run(ctx: Context, config: Config, agent: Reviewer, cwd: string, signal: AbortSignal, prompt?: string) {
  const file = await locate(agent)
  const deadline = AbortSignal.any([signal, AbortSignal.timeout(config.timeoutMs)])
  const launch: Launch = (args, stdio) => ctx.subprocess.spawn({
    argv: [file, ...args], cwd, stdio, signal: deadline, graceMs: 1000,
    env: { PATH: [dirname(process.execPath), dirname(file), process.env.PATH].filter(Boolean).join(delimiter),
      ...Object.fromEntries(Object.entries(process.env).filter(([key]) => /^(https?_proxy|all_proxy|no_proxy)$/i.test(key))) },
  })
  async function batch(args: string[], data?: string) {
    const output = { maxBytes: config.maxBytes }
    const child = launch(args, { stdin: data === undefined ? 'ignore' : { data }, stdout: output, stderr: output })
    const outcome = await child.done
    deadline.throwIfAborted()
    const stdout = child.collected.stdout?.readFrom(0)
    const stderr = child.collected.stderr?.readFrom(0).text ?? ''
    if (!stdout) throw failure(agent, stderr)
    if (outcome.exitCode !== 0 || stdout.lossy) throw failure(agent, stdout.text + stderr)
    return { stdout: stdout.text, stderr }
  }
  if (agent === 'codex') {
    const status = await batch(['login', 'status'])
    const detail = status.stdout + status.stderr
    if (!/logged in/i.test(detail) || /not logged in/i.test(detail)) throw failure(agent, detail)
  } else {
    try { await checkBuddy(launch, cwd, config.maxBytes) } finally { deadline.throwIfAborted() }
  }
  if (prompt === undefined) return agent + ' CLI 已安装并已登录。'
  if (agent === 'codex') {
    const { stdout, stderr } = await batch(['exec', '--sandbox', 'read-only', '--ephemeral', '--skip-git-repo-check', '--json', '-'], prompt)
    const events = stdout.trim().split('\n').filter(Boolean).map(line => fields(decode(line)))
    const last = events.findLast(event => event.type === 'item.completed' && fields(event.item).type === 'agent_message')
    const message = fields(last?.item).text
    if (events.at(-1)?.type === 'turn.completed' && typeof message === 'string' && message.trim()) return message
    throw failure(agent, stderr)
  }
  const { stdout, stderr } = await batch(['-p', ...buddyTools, '--no-session-persistence', '--output-format', 'json'], prompt)
  if (!stdout.trim()) throw failure(agent, stderr)
  const parsed = decode(stdout)
  const result = fields(Array.isArray(parsed) ? parsed.at(-1) : parsed)
  if (result.type === 'result' && result.subtype === 'success' && !result.is_error && typeof result.result === 'string' && result.result.trim()) return result.result
  throw failure(agent, stderr + (typeof result.result === 'string' ? result.result : ''))
}

/**
 * Register independent tools. Omitting the prompt checks installation and login without inference.
 * @param ctx - desktop Host services.
 * @param config - per-invocation resource limits.
 */
export function apply(ctx: Context, config: Config): void {
  for (const agent of ['codex', 'codebuddy'] as const) {
    ctx.tools.register(defineTool({
      name: 'desktop_review_' + agent,
      timeoutMs: config.timeoutMs,
      description: 'Use the local ' + agent + ' CLI for a read-only workspace review when requested. Omit prompt to check installation and login.',
      parameters: { prompt: { type: 'string', description: 'Review question with workspace-relative file names.' } },
      output: { schema: { type: 'string' }, render: (_args, value) => [{ type: 'text', text: value }] },
      async execute(args, exec) {
        const cwd = exec.agent?.session.header.cwd
        if (!cwd) throw new Error('请先选择工作文件夹。')
        return run(ctx, config, agent, cwd, exec.signal, args.prompt)
      },
    }))
  }
}
