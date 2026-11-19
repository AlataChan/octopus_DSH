/** Exercise the packaged review tools through the bundled local subprocess runtime. */
import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { createRequire, registerHooks } from 'node:module'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { pathToFileURL } from 'node:url'

const resources = resolve(process.argv[2])
const live = process.argv[3] === '--live'
const harness = createRequire(join(resources, 'harness/package.json'))
const plugin = join(resources, 'profile-plugins/@deepseek-ai/dsh-experimental-desktop-operation')
assert.match(await readFile(join(plugin, 'cordis.patch.yml'), 'utf8'), /name: '@deepseek-ai\/dsh-experimental-desktop-operation\/external-review'/)
const skill = await readFile(join(resources, 'bundled-skills/external-code-review/SKILL.md'), 'utf8')
registerHooks({
  resolve(specifier, context, next) {
    if (specifier.startsWith('@deepseek-ai/')) return { shortCircuit: true, url: pathToFileURL(harness.resolve(specifier)).href }
    return next(specifier, context)
  },
})
const { Context } = await import('@deepseek-ai/cordis')
const { default: Local } = await import('@deepseek-ai/dsh-subprocess-local')
const manifest = JSON.parse(await readFile(join(plugin, 'package.json'), 'utf8'))
const { apply } = await import(pathToFileURL(join(plugin, manifest.exports['./external-review'].default)))
const cwd = await mkdtemp(join(tmpdir(), 'desktop-external-review-'))
const fiber = new Context()
const runtime = fiber.plugin(Local)
await runtime
try {
  const tools = new Map()
  apply({ tools: { register(tool) { tools.set(tool.name, tool) } }, subprocess: fiber.subprocess }, { timeoutMs: 180000, maxBytes: 4194304 })
  const call = (agent, prompt) => {
    const name = 'desktop_review_' + agent
    assert.ok(skill.includes('`' + name + '`'))
    return tools.get(name).execute(prompt ? { prompt } : {}, { agent: { session: { header: { cwd } } }, signal: new AbortController().signal })
  }
  if (live) {
    await Promise.all(['codex', 'codebuddy'].map(async agent => {
      const token = randomUUID()
      await writeFile(join(cwd, agent + '.txt'), token)
      const answer = await call(agent, 'Read ' + agent + '.txt in this workspace and return the exact contents.')
      assert.ok(answer.includes(token), agent + ' failed the fresh file review')
      console.log(agent + ' completed a live review')
    }))
  } else {
    process.env.HOME = cwd
    process.env.PATH = cwd
    delete process.env.npm_config_cache
    // PATH fixtures precede even globally installed tools on the packaging host.
    const fixture = (agent, script) => writeFile(join(cwd, agent), '#!/usr/bin/env node\n' + script, { mode: 0o755 })
    await fixture('codex', 'console.error("Not logged in"); process.exitCode = 1')
    await fixture('codebuddy', `const lines = require('node:readline').createInterface({ input: process.stdin })
lines.on('line', line => {
  const {id} = JSON.parse(line)
  console.log(JSON.stringify(id === 1 ? {jsonrpc:'2.0',id,result:{protocolVersion:1}} : {jsonrpc:'2.0',id,error:{code:-32000,message:'Authentication required'}}))
})`)
    await assert.rejects(call('codex'), /codex login/)
    await assert.rejects(call('codebuddy'), /运行 codebuddy 完成登录/)
    await fixture('codex', 'console.error("Logged in using ChatGPT")')
    assert.match(await call('codex'), /已登录/)
    await assert.rejects(call('codebuddy'), /需要登录/)
    await fixture('codex', 'console.error("TLS connection failed"); process.exitCode = 1')
    await assert.rejects(call('codex'), /网络及代理/)
  }
} finally {
  await runtime.dispose()
  await rm(cwd, { recursive: true, force: true })
}
