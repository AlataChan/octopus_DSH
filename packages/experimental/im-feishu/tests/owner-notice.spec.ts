/**
 * Production-entrypoint ingress plugin, SQLite domain isolation, restricted handoff,
 * FeishuOutboundClient HTTP, and restart deduplication tests for WeChat owner notices.
 */
import { createHash } from 'node:crypto'
import { createServer, type IncomingMessage, type ServerResponse } from 'node:http'
import {
  chmodSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  realpathSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs'
import { rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'
import { EventDispatcher } from '@larksuiteoapi/node-sdk'
import { afterEach, describe, expect, it } from 'vitest'
import { z } from 'zod'
import { Context } from '@deepseek-ai/cordis'
import { provideCmdline } from '@deepseek-ai/dsh-cmdline'
import Storage from '@deepseek-ai/dsh-storage'
import { DomainFacility } from '@deepseek-ai/dsh-storage-domain'
import { SqliteStorageBackend } from '@deepseek-ai/dsh-storage-sqlite'
import type { FeishuWSClientLike } from '../src/feishu-transport.ts'
import { apply as applyIngressPlugin } from '../src/ingress-plugin.ts'
import {
  FeishuOutboundClient,
  IngressStorage,
  OwnerNoticeDispatcher,
  OwnerNoticeStorage,
  TenantIpcClient,
  TenantIpcServer,
  encodeOperatorBindingKey,
  formatOwnerNoticeId,
  ingressDomainSpec,
  ownerNoticeDomainSpec,
  ownerNoticesCapability,
  parseFeishuAppId,
  parseFeishuChatId,
  parseFeishuEventId,
  parseFeishuSenderId,
  parseFeishuTenantId,
  parseOperatorId,
  parseTargetTenantId,
  type HttpFetchFn,
  type TenantRuntimeIpcHandler,
} from '../src/index.ts'
import { Operations } from '../../../../scripts/linux-operations/operations.mjs'
import { WechatExchangeProjector } from '../../../../scripts/linux-operations/wechat-inbox.mjs'

const feishuMessageRequestSchema = z.looseObject({
  content: z.string(),
  reply_id: z.string().optional(),
})
const feishuTextContentSchema = z.object({ text: z.string() })

type FeishuMessageRequestBody = z.infer<typeof feishuMessageRequestSchema>

interface RecordedRequest {
  readonly url: string
  readonly body: FeishuMessageRequestBody
}

interface HttpStubController {
  readonly port: number
  readonly sentRequests: RecordedRequest[]
  setBehavior(fn: (req: IncomingMessage, bodyText: string, res: ServerResponse) => Promise<void> | void): void
  close(): Promise<void>
}

async function startFeishuStubServer(): Promise<HttpStubController> {
  const sentRequests: RecordedRequest[] = []
  let customMessageBehavior: (
    req: IncomingMessage,
    bodyText: string,
    res: ServerResponse,
  ) => Promise<void> | void = (_req, _bodyText, res) => {
    const seq = sentRequests.length
    res.writeHead(200, { 'Content-Type': 'application/json' })
    res.end(JSON.stringify({ code: 0, msg: 'ok', data: { message_id: `om_real_${seq}` } }))
  }

  const server = createServer((req, res) => {
    const chunks: Uint8Array[] = []
    req.on('data', (chunk: Uint8Array) => {
      chunks.push(chunk)
    })
    req.on('end', () => {
      const bodyText = Buffer.concat(chunks).toString('utf8')
      if (req.url === '/open-apis/auth/v3/tenant_access_token/internal') {
        res.writeHead(200, { 'Content-Type': 'application/json' })
        res.end(JSON.stringify({ code: 0, msg: 'ok', tenant_access_token: 'tenant-tok-test', expire: 7200 }))
        return
      }
      if (req.url?.startsWith('/open-apis/im/v1/messages')) {
        const parsed = bodyText
          ? feishuMessageRequestSchema.parse(JSON.parse(bodyText))
          : feishuMessageRequestSchema.parse({ content: '' })
        sentRequests.push({ url: req.url, body: parsed })
        void Promise.resolve(customMessageBehavior(req, bodyText, res))
        return
      }
      res.writeHead(404)
      res.end(JSON.stringify({ code: 404 }))
    })
  })

  await new Promise<void>((resolveReady) => {
    server.listen(0, '127.0.0.1', () => {
      resolveReady()
    })
  })
  const address = server.address()
  if (!address || typeof address === 'string') {
    throw new Error('Failed to bind local test HTTP server')
  }

  return {
    port: address.port,
    sentRequests,
    setBehavior(fn) {
      customMessageBehavior = fn
    },
    close: () => {
      server.closeAllConnections()
      return new Promise<void>((resolveClose, rejectClose) => {
        server.close((err) => {
          if (err) rejectClose(err)
          else resolveClose()
        })
      })
    },
  }
}

async function createNoticeFixture(options: { dir?: string; stub: HttpStubController; timeoutMs?: number }) {
  const dir = options.dir ?? mkdtempSync(join(tmpdir(), 'dsh-owner-notice-'))
  const ctx = new Context()
  await ctx.plugin(Storage)

  const ingressSqlite = new SqliteStorageBackend({ path: join(dir, 'ingress.sqlite'), journalMode: 'wal' })
  const noticeSqlite = new SqliteStorageBackend({ path: join(dir, 'owner-notices.sqlite'), journalMode: 'wal' })
  const unregisterIngress = ctx.storage.backend.register('sqlite:ingress', ingressSqlite)
  const unregisterNotice = ctx.storage.backend.register('sqlite:notice', noticeSqlite)

  const ingressFacility = new DomainFacility(ctx, { backend: 'sqlite:ingress', routes: {} })
  const noticeFacility = new DomainFacility(ctx, { backend: 'sqlite:notice', routes: {} })
  const ingressStorage = new IngressStorage(await ingressFacility.open(ingressDomainSpec))
  const noticeStorage = new OwnerNoticeStorage(await noticeFacility.open(ownerNoticeDomainSpec))
  await noticeStorage.reconcileInFlightNotices()

  const appId = parseFeishuAppId('cli_notice_app')
  const tenantId = parseFeishuTenantId('tenant_feishu')
  const chatId = parseFeishuChatId('oc_owner_chat')
  const senderId = parseFeishuSenderId('ou_owner_sender')
  const operatorId = parseOperatorId('owner-1')
  const targetTenantId = parseTargetTenantId('octopus')
  const bindingKey = encodeOperatorBindingKey('feishu', appId, tenantId, chatId, senderId)

  if (!ingressStorage.getBinding(bindingKey)) {
    await ingressStorage.putBinding({
      bindingKey,
      operatorId,
      targetTenantId,
      provider: 'feishu',
      appId,
      tenantId,
      chatId,
      senderId,
      authorizedAt: new Date(1_760_000_000_000).toISOString(),
      revokedAt: null,
    })
  }

  const localFetch: HttpFetchFn = (url, init) => {
    const parsed = new URL(url)
    return fetch(`http://127.0.0.1:${options.stub.port}${parsed.pathname}${parsed.search}`, init)
  }

  const outboundClient = new FeishuOutboundClient({
    appId,
    appSecret: 'test-only-secret',
    domain: 'https://open.feishu.cn',
    timeoutMs: options.timeoutMs ?? 2000,
    fetchFn: localFetch,
  })

  const dispatcher = new OwnerNoticeDispatcher(
    noticeStorage,
    outboundClient,
    key => ingressStorage.getBinding(key),
    { bindingKey, operatorId, targetTenantId, appId, chatId },
  )

  return {
    dir,
    ctx,
    ingressStorage,
    noticeStorage,
    outboundClient,
    dispatcher,
    bindingKey,
    chatId,
    operatorId,
    targetTenantId,
    appId,
    tenantId,
    senderId,
    close: async () => {
      await ingressSqlite.close()
      await noticeSqlite.close()
      unregisterIngress()
      unregisterNotice()
      await ctx.fiber.dispose()
    },
  }
}

async function seedFrozenExecution(options: {
  dir: string
  wechatRoot: string
  opsStateDir: string
  wechatConfigPath: string
  bindingKey: string
  operatorId: string
  targetTenantId: string
  jobText: string
  nowMs: number
}): Promise<{ jobId: string; receiptPath: string; rawReceipt: string }> {
  const ingressSeed = join(options.dir, 'seed-ingress.sqlite')
  const ctx = new Context()
  await ctx.plugin(Storage)
  const sqlite = new SqliteStorageBackend({ path: ingressSeed, journalMode: 'wal' })
  const unreg = ctx.storage.backend.register('sqlite:seed', sqlite)
  const facility = new DomainFacility(ctx, { backend: 'sqlite:seed', routes: {} })
  const ingress = new IngressStorage(await facility.open(ingressDomainSpec))
  const parsedTuple = z.tuple([z.literal('feishu'), z.string(), z.string(), z.string(), z.string()]).parse(JSON.parse(options.bindingKey))
  await ingress.putBinding({
    bindingKey: options.bindingKey,
    operatorId: parseOperatorId(options.operatorId),
    targetTenantId: parseTargetTenantId(options.targetTenantId),
    provider: 'feishu',
    appId: parseFeishuAppId(parsedTuple[1]),
    tenantId: parseFeishuTenantId(parsedTuple[2]),
    chatId: parseFeishuChatId(parsedTuple[3]),
    senderId: parseFeishuSenderId(parsedTuple[4]),
    authorizedAt: new Date(options.nowMs).toISOString(),
    revokedAt: null,
  })
  const deliveryKey = JSON.stringify([...parsedTuple, 'seed-evt-1'])
  const delivery = await ingress.recordDeliveryReceived({
    deliveryKey,
    operatorId: options.operatorId,
    targetTenantId: options.targetTenantId,
    prompt: `微信发送 默认联系人\n${options.jobText}`,
  })
  await ingress.updateDeliveryStatus(deliveryKey, 'in_flight', 'session-seed')
  await sqlite.close()
  unreg()
  await ctx.fiber.dispose()

  const accountSha = createHash('sha256').update('own-wechat').digest('hex')
  const policyPath = join(options.dir, 'policy.json')
  const policy: Record<string, unknown> = {
    socket: join(options.dir, 'broker.sock'),
    state: options.opsStateDir,
    ingressDatabase: ingressSeed,
    bindingKey: options.bindingKey,
    operatorId: options.operatorId,
    tenantId: options.targetTenantId,
    services: {},
    containers: [],
    wechatRoot: options.wechatRoot,
    wechatConfig: options.wechatConfigPath,
    wechatRecipientAlias: '默认联系人',
    limits: { logLines: 50, previewSeconds: 600, workerPollSeconds: 2, maxTextBytes: 1000, commandSeconds: 10 },
  }
  writeFileSync(policyPath, JSON.stringify(policy), { mode: 0o600 })

  let jobId = ''
  const ops = new Operations(
    policy,
    policyPath,
    async (op: Record<string, unknown>) => {
      const id = String(op.id)
      writeFileSync(
        join(options.wechatRoot, 'adhoc', 'outbox', `${id}.json`),
        JSON.stringify({
          schema_version: 1,
          kind: 'send',
          job_id: id,
          account_sha256: accountSha,
          recipient_id: 'wxid_alice',
          recipient_alias: '默认联系人',
          text: options.jobText,
          text_sha256: createHash('sha256').update(options.jobText).digest('hex'),
          created_at: options.nowMs,
          expires_at: options.nowMs + 600_000,
          confirmation_request_id: delivery.requestId,
        }),
        { mode: 0o640 },
      )
      return { queued: true, operation: id }
    },
    () => Date.now(),
  )
  const prepared = ops.prepare({ kind: 'wechat_send', payload: { recipient: '默认联系人', text: options.jobText } })
  jobId = prepared.id
  await ops.run(jobId)
  ops.close()
  chmodSync(join(options.opsStateDir, 'operations.sqlite'), 0o600)

  const receiptPath = join(options.wechatRoot, 'adhoc', 'receipts', `${jobId}.json`)
  const rawReceipt = JSON.stringify({ state: 'sent_verified', recipient_alias: '默认联系人', checked_at: options.nowMs / 1000 })
  writeFileSync(receiptPath, rawReceipt, { mode: 0o600 })
  return { jobId, receiptPath, rawReceipt }
}

async function bootIngressInstance(options: {
  stateDirectory: string
  stub: HttpStubController
  binding: {
    bindingKey: string
    operatorId: string
    targetTenantId: string
    appId: string
    tenantId: string
    chatId: string
    senderId: string
    revokedAt?: string | null
  }
  channelKind?: 'feishu' | 'dingtalk'
  ownerNotices?: Record<string, unknown>
  dispatchIntervalMs?: number
  shutdownTimeoutMs?: number
  tenantSocketPath?: string
}) {
  const ctx = new Context()
  await ctx.plugin(Storage)
  const readyCallbacks: Array<() => void> = []
  let exitCode: number | undefined
  provideCmdline(ctx, {
    args: [],
    exit: (code: number) => {
      exitCode = code
    },
    ready: {
      onReady: (cb: () => void) => {
        readyCallbacks.push(cb)
        return () => {}
      },
    },
  })

  const dispatcher = new EventDispatcher({})
  const wsClient: FeishuWSClientLike = {
    start: async () => {},
    close: () => {},
    getConnectionStatus: () => ({ state: 'connected' }),
  }
  const fetchFn: HttpFetchFn = (url, init) => {
    const parsed = new URL(url)
    return fetch(`http://127.0.0.1:${options.stub.port}${parsed.pathname}${parsed.search}`, init)
  }

  const rawConfig: Record<string, unknown> = {
    ingressId: 'prod-entry-test',
    stateDirectory: options.stateDirectory,
    dispatchIntervalMs: options.dispatchIntervalMs ?? 25,
    shutdownTimeoutMs: options.shutdownTimeoutMs ?? 5000,
    tenants: {
      [options.binding.targetTenantId]: {
        socketPath: options.tenantSocketPath ?? join(options.stateDirectory, 'unused-tenant.sock'),
      },
    },
    operatorBindings: [{
      bindingKey: options.binding.bindingKey,
      operatorId: options.binding.operatorId,
      targetTenantId: options.binding.targetTenantId,
      provider: 'feishu',
      appId: options.binding.appId,
      tenantId: options.binding.tenantId,
      chatId: options.binding.chatId,
      senderId: options.binding.senderId,
      authorizedAt: new Date(1_760_000_000_000).toISOString(),
      revokedAt: options.binding.revokedAt ?? null,
    }],
    ...(options.ownerNotices ? { ownerNotices: options.ownerNotices } : {}),
  }

  if ((options.channelKind ?? 'feishu') === 'feishu') {
    rawConfig.channel = {
      kind: 'feishu',
      appId: options.binding.appId,
      appSecret: 'test-only-secret',
    }
  } else {
    rawConfig.channel = {
      kind: 'dingtalk',
      clientId: 'ding-client',
      clientSecret: 'ding-secret',
      expectedCorpId: 'ding-corp',
    }
  }

  try {
    await applyIngressPlugin(
      ctx,
      rawConfig as Parameters<typeof applyIngressPlugin>[1],
      {
        clientFactory: (targetTenantId, endpoint) => new TenantIpcClient({
          socketPath: endpoint.socketPath,
          ingressId: 'prod-entry-test',
          targetTenantId,
        }),
        feishu: {
          fetchFn,
          clientFactory: {
            createWSClient: () => wsClient,
            createEventDispatcher: () => dispatcher,
          },
        },
      },
    )
  } catch (error) {
    await ctx.fiber.dispose().catch(() => undefined)
    throw error
  }

  for (const cb of readyCallbacks) cb()
  return {
    ctx,
    get exitCode() {
      return exitCode
    },
    dispose: () => ctx.fiber.dispose(),
  }
}

async function readStoredNotices(stateDirectory: string) {
  const ctx = new Context()
  await ctx.plugin(Storage)
  const noticeSqlite = new SqliteStorageBackend({ path: join(stateDirectory, 'owner-notices.sqlite'), journalMode: 'wal' })
  const unreg = ctx.storage.backend.register('sqlite:inspect-notice', noticeSqlite)
  const facility = new DomainFacility(ctx, { backend: 'sqlite:inspect-notice', routes: {} })
  const storage = new OwnerNoticeStorage(await facility.open(ownerNoticeDomainSpec))
  const list = [...storage.listNotices()]
  await noticeSqlite.close()
  unreg()
  await ctx.fiber.dispose()
  return list
}

describe('WeChat Feishu owner notice production integration', () => {
  const tempDirs: string[] = []
  const stubs: HttpStubController[] = []

  afterEach(async () => {
    while (stubs.length) {
      const stub = stubs.pop()
      await stub?.close()
    }
    while (tempDirs.length) {
      const dir = tempDirs.pop()
      if (dir) {
        try {
          chmodSync(dir, 0o700)
          for (const entry of readdirSync(dir, { recursive: true, withFileTypes: true })) {
            if (entry.isSymbolicLink()) continue
            const path = join(entry.parentPath ?? dir, entry.name)
            try {
              chmodSync(path, entry.isDirectory() ? 0o700 : 0o600)
            } catch {
              /* Ignore already removed entry during test cleanup. */
            }
          }
        } catch {
          /* Ignore cleanup chmod fallback. */
        }
        await rm(dir, { recursive: true, force: true, maxRetries: 10, retryDelay: 25 })
      }
    }
  })

  it('persists notices separately from u_im_ingress_deliveries, classifies 4xx/5xx/timeouts, and recovers crashed in_flight notices without retrying', async () => {
    const stub = await startFeishuStubServer()
    stubs.push(stub)
    const f = await createNoticeFixture({ stub, timeoutMs: 80 })
    tempDirs.push(f.dir)

    const record = await f.noticeStorage.enqueueNotice({
      noticeKey: 'inbound:batch-1',
      kind: 'inbound_alert',
      conversationKey: 'friend:alice',
      bindingKey: f.bindingKey,
      chatId: f.chatId,
      operatorId: f.operatorId,
      targetTenantId: f.targetTenantId,
      sourceRef: 'batch-1',
      body: '<think>internal</think>Alice\u202e说: Bearer secret-tok-99 确认执行 123456',
    })
    expect(record.noticeId).toBe(formatOwnerNoticeId('inbound:batch-1'))
    expect(record.untrusted).toBe(true)
    expect(record.authority).toBe('notice_only')
    expect(record.body).not.toContain('secret-tok-99')
    expect(record.body).not.toContain('\u202e')

    const first = await f.dispatcher.dispatchNotice('inbound:batch-1')
    expect(first.status).toBe('sent')
    expect(first.messageId).toBe('om_real_1')
    expect(f.noticeStorage.getThreadMessageId('friend:alice')).toBe('om_real_1')

    // Sequential alert for the same conversation reuses Feishu reply thread ID automatically.
    await f.noticeStorage.enqueueNotice({
      noticeKey: 'inbound:batch-2',
      kind: 'inbound_alert',
      conversationKey: 'friend:alice',
      bindingKey: f.bindingKey,
      chatId: f.chatId,
      operatorId: f.operatorId,
      targetTenantId: f.targetTenantId,
      sourceRef: 'batch-2',
      body: '第二条汇总消息',
    })
    const second = await f.dispatcher.dispatchNotice('inbound:batch-2')
    expect(second.status).toBe('sent')
    expect(stub.sentRequests[1]?.url).toBe('/open-apis/im/v1/messages/om_real_1/reply')

    // 4xx -> failed; timeout -> unknown; omitted message_id -> unknown
    await f.noticeStorage.enqueueNotice({
      noticeKey: 'receipt:job4xx:failed_before_submit',
      kind: 'outbound_receipt',
      bindingKey: f.bindingKey,
      chatId: f.chatId,
      operatorId: f.operatorId,
      targetTenantId: f.targetTenantId,
      sourceRef: 'job4xx',
      body: 'Permanent 4xx rejection',
    })
    stub.setBehavior((_req, _body, res) => {
      res.writeHead(400, { 'Content-Type': 'application/json' })
      res.end(JSON.stringify({ code: 400, msg: 'bad request' }))
    })
    expect((await f.dispatcher.dispatchNotice('receipt:job4xx:failed_before_submit')).status).toBe('failed')

    await f.noticeStorage.enqueueNotice({
      noticeKey: 'inbound:batch-timeout',
      kind: 'inbound_alert',
      bindingKey: f.bindingKey,
      chatId: f.chatId,
      operatorId: f.operatorId,
      targetTenantId: f.targetTenantId,
      sourceRef: 'batch-timeout',
      body: 'Timeout test payload',
    })
    stub.setBehavior(() => {})
    expect((await f.dispatcher.dispatchNotice('inbound:batch-timeout')).status).toBe('unknown')

    await f.noticeStorage.enqueueNotice({
      noticeKey: 'inbound:batch-missing-id',
      kind: 'inbound_alert',
      bindingKey: f.bindingKey,
      chatId: f.chatId,
      operatorId: f.operatorId,
      targetTenantId: f.targetTenantId,
      sourceRef: 'batch-missing-id',
      body: 'Missing id payload',
    })
    stub.setBehavior((_req, _body, res) => {
      res.writeHead(200, { 'Content-Type': 'application/json' })
      res.end(JSON.stringify({ code: 0, msg: 'ok', data: {} }))
    })
    expect((await f.dispatcher.dispatchNotice('inbound:batch-missing-id')).status).toBe('unknown')

    // Simulate claimed in_flight crash, close, reopen -> transitions to unknown and never retries.
    await f.noticeStorage.enqueueNotice({
      noticeKey: 'inbound:crash-batch',
      kind: 'inbound_alert',
      bindingKey: f.bindingKey,
      chatId: f.chatId,
      operatorId: f.operatorId,
      targetTenantId: f.targetTenantId,
      sourceRef: 'crash-batch',
      body: 'Crash recovery test',
    })
    await f.noticeStorage.recordAttempt('inbound:crash-batch')
    await f.close()

    const sendsBeforeRestart = stub.sentRequests.length
    const f2 = await createNoticeFixture({ dir: f.dir, stub })
    expect(f2.noticeStorage.getNotice('inbound:crash-batch')?.status).toBe('unknown')
    await f2.dispatcher.dispatchPending()
    expect(stub.sentRequests.length).toBe(sendsBeforeRestart)
    expect(f2.ingressStorage.listDeliveries()).toHaveLength(0)
    await f2.close()
  })

  it('boots real ingress-plugin entrypoint with kernel-isolated wechatRoot, sends once, restarts without duplicate, and drains cleanly', async () => {
    const stub = await startFeishuStubServer()
    stubs.push(stub)
    const dir = realpathSync(mkdtempSync(join(tmpdir(), 'dsh-prod-entry-')))
    tempDirs.push(dir)

    const wechatRoot = join(dir, 'wechat')
    const opsStateDir = join(dir, 'ops-state')
    const handoffDir = join(dir, 'handoff')
    const ingressStateDir = join(dir, 'ingress-state')
    mkdirSync(join(wechatRoot, 'config'), { recursive: true, mode: 0o750 })
    mkdirSync(join(wechatRoot, 'anchors'), { recursive: true, mode: 0o750 })
    mkdirSync(join(wechatRoot, 'adhoc', 'outbox'), { recursive: true, mode: 0o2750 })
    mkdirSync(join(wechatRoot, 'adhoc', 'receipts'), { recursive: true, mode: 0o700 })
    mkdirSync(opsStateDir, { recursive: true, mode: 0o700 })
    mkdirSync(handoffDir, { recursive: true, mode: 0o750 })
    mkdirSync(ingressStateDir, { recursive: true, mode: 0o700 })
    chmodSync(wechatRoot, 0o750)
    chmodSync(join(wechatRoot, 'adhoc'), 0o750)
    chmodSync(join(wechatRoot, 'adhoc', 'outbox'), 0o2750)
    chmodSync(join(wechatRoot, 'adhoc', 'receipts'), 0o700)
    chmodSync(opsStateDir, 0o700)
    chmodSync(handoffDir, 0o750)
    chmodSync(ingressStateDir, 0o700)

    for (const anchorName of ['account.png', 'own.png', 'friend-search.png', 'friend-sidebar.png', 'friend-card.png']) {
      writeFileSync(join(wechatRoot, 'anchors', anchorName), anchorName, { mode: 0o600 })
    }
    const wechatConfigPath = join(wechatRoot, 'config', 'config.json')
    writeFileSync(
      wechatConfigPath,
      JSON.stringify({
        account_id: 'own-wechat',
        recipient_id: 'wxid_alice',
        recipient_query: '默认联系人',
        enabled: true,
        direct_enabled: true,
        account_anchor: join(wechatRoot, 'anchors', 'account.png'),
        own_chat_anchor: join(wechatRoot, 'anchors', 'own.png'),
        recipient_anchor: join(wechatRoot, 'anchors', 'friend-search.png'),
        recipient_sidebar_anchor: join(wechatRoot, 'anchors', 'friend-sidebar.png'),
        recipient_card_anchor: join(wechatRoot, 'anchors', 'friend-card.png'),
      }),
      { mode: 0o600 },
    )

    const appId = 'cli_notice_app'
    const tenantId = 'tenant_feishu'
    const chatId = 'oc_owner_chat'
    const senderId = 'ou_owner_sender'
    const operatorId = 'owner-1'
    const targetTenantId = 'octopus'
    const bindingKey = encodeOperatorBindingKey('feishu', parseFeishuAppId(appId), parseFeishuTenantId(tenantId), parseFeishuChatId(chatId), parseFeishuSenderId(senderId))
    const nowMs = Date.now()

    const { jobId, receiptPath, rawReceipt } = await seedFrozenExecution({
      dir,
      wechatRoot,
      opsStateDir,
      wechatConfigPath,
      bindingKey,
      operatorId,
      targetTenantId,
      jobText: '请三点参会',
      nowMs,
    })

    // Project via WechatExchangeProjector (broker half) -> restricted handoffDir (0750 / 0640)
    const projector = new WechatExchangeProjector({
      socket: join(dir, 'broker.sock'),
      state: opsStateDir,
      ingressDatabase: join(dir, 'seed-ingress.sqlite'),
      bindingKey,
      operatorId,
      tenantId: targetTenantId,
      services: {},
      containers: [],
      wechatRoot,
      wechatConfig: wechatConfigPath,
      wechatRecipientAlias: '默认联系人',
      limits: { logLines: 50, previewSeconds: 600, workerPollSeconds: 2, maxTextBytes: 1000, commandSeconds: 10 },
      wechatNotifications: {
        enabled: true,
        handoffDir,
      },
    }, { clock: () => nowMs })
    const projected = projector.projectOnce()
    projector.close()
    expect(projected.receipts?.rejected).toEqual([])
    expect(projected.receipts?.projected).toBe(1)

    // Revoke all access on wechatRoot and opsStateDir to prove at OS kernel level that ingress only reads handoffDir.
    chmodSync(wechatRoot, 0o000)
    chmodSync(opsStateDir, 0o000)

    const inst1 = await bootIngressInstance({
      stateDirectory: ingressStateDir,
      stub,
      binding: { bindingKey, operatorId, targetTenantId, appId, tenantId, chatId, senderId },
      ownerNotices: {
        enabled: true,
        handoffDirectory: handoffDir,
        bindingKey,
        operatorId,
        targetTenantId,
        feishuAppId: appId,
        feishuChatId: chatId,
        pollIntervalMs: 25,
      },
    })

    const deadline = Date.now() + 3000
    while (stub.sentRequests.length < 1 && Date.now() < deadline) {
      await delay(25)
    }
    expect(stub.sentRequests).toHaveLength(1)
    const sentText = feishuTextContentSchema.parse(JSON.parse(stub.sentRequests[0]?.body.content ?? '')).text
    expect(sentText).toMatch(/wn_[0-9a-f]{12}/)
    expect(sentText).toContain(jobId)
    expect(sentText).toContain('sent_verified')
    expect(inst1.exitCode).toBeUndefined()
    await inst1.dispose()

    // Cold restart with the same handoff JSON still present on disk -> must NEVER send a duplicate.
    const inst2 = await bootIngressInstance({
      stateDirectory: ingressStateDir,
      stub,
      binding: { bindingKey, operatorId, targetTenantId, appId, tenantId, chatId, senderId },
      ownerNotices: {
        enabled: true,
        handoffDirectory: handoffDir,
        bindingKey,
        operatorId,
        targetTenantId,
        feishuAppId: appId,
        feishuChatId: chatId,
        pollIntervalMs: 25,
      },
    })
    await delay(150)
    await inst2.dispose()
    expect(stub.sentRequests).toHaveLength(1)

    const stored = await readStoredNotices(ingressStateDir)
    expect(stored).toHaveLength(1)
    expect(stored[0]?.status).toBe('sent')
    expect(stored[0]?.messageId).toBe('om_real_1')

    chmodSync(wechatRoot, 0o750)
    chmodSync(opsStateDir, 0o700)
    expect(readFileSync(receiptPath, 'utf8')).toBe(rawReceipt)
  })

  it('keeps notifications disabled when flag is off and rejects missing-Feishu, wrong/revoked owner, or loose handoff files', async () => {
    const stub = await startFeishuStubServer()
    stubs.push(stub)
    const dir = realpathSync(mkdtempSync(join(tmpdir(), 'dsh-prod-neg-')))
    tempDirs.push(dir)

    const appId = 'cli_notice_app'
    const tenantId = 'tenant_feishu'
    const chatId = 'oc_owner_chat'
    const senderId = 'ou_owner_sender'
    const operatorId = 'owner-1'
    const targetTenantId = 'octopus'
    const bindingKey = encodeOperatorBindingKey('feishu', parseFeishuAppId(appId), parseFeishuTenantId(tenantId), parseFeishuChatId(chatId), parseFeishuSenderId(senderId))

    const handoffDir = join(dir, 'handoff')
    mkdirSync(handoffDir, { mode: 0o750 })
    chmodSync(handoffDir, 0o750)
    writeFileSync(
      join(handoffDir, 'sample.json'),
      JSON.stringify({
        schemaVersion: 1,
        noticeKey: 'receipt:aaaaaaaaaaaaaaaaaaaaaaaa:sent_verified',
        noticeId: formatOwnerNoticeId('receipt:aaaaaaaaaaaaaaaaaaaaaaaa:sent_verified'),
        kind: 'outbound_receipt',
        conversationKey: null,
        sourceRef: 'aaaaaaaaaaaaaaaaaaaaaaaa',
        body: '[微信任务终态回执 wn_000000000000] test',
        createdAt: Date.now(),
        untrusted: true,
        authority: 'notice_only',
      }),
      { mode: 0o640 },
    )
    chmodSync(join(handoffDir, 'sample.json'), 0o640)

    // 1. Default-off (omitted / enabled: false): no polling, no owner-notices.sqlite, zero sends
    const disabledState = join(dir, 'disabled-state')
    mkdirSync(disabledState, { mode: 0o700 })
    chmodSync(disabledState, 0o700)
    const disabledInst = await bootIngressInstance({
      stateDirectory: disabledState,
      stub,
      binding: { bindingKey, operatorId, targetTenantId, appId, tenantId, chatId, senderId },
      ownerNotices: { enabled: false },
    })
    await delay(100)
    await disabledInst.dispose()
    expect(stub.sentRequests).toHaveLength(0)
    expect(existsSync(join(disabledState, 'owner-notices.sqlite'))).toBe(false)

    // 2. Enabled without Feishu channel (DingTalk) fails closed at startup
    const noFeishuState = join(dir, 'nofeishu-state')
    mkdirSync(noFeishuState, { mode: 0o700 })
    chmodSync(noFeishuState, 0o700)
    await expect(bootIngressInstance({
      stateDirectory: noFeishuState,
      stub,
      channelKind: 'dingtalk',
      binding: { bindingKey, operatorId, targetTenantId, appId, tenantId, chatId, senderId },
      ownerNotices: {
        enabled: true,
        handoffDirectory: handoffDir,
        bindingKey,
        operatorId,
        targetTenantId,
      },
    })).rejects.toThrow('Owner notifications require an active Feishu channel')

    // 3. Symlinked or world-readable handoffDir fails closed at startup
    const linkHandoff = join(dir, 'handoff-link')
    symlinkSync(handoffDir, linkHandoff)
    const linkState = join(dir, 'link-state')
    mkdirSync(linkState, { mode: 0o700 })
    chmodSync(linkState, 0o700)
    await expect(bootIngressInstance({
      stateDirectory: linkState,
      stub,
      binding: { bindingKey, operatorId, targetTenantId, appId, tenantId, chatId, senderId },
      ownerNotices: {
        enabled: true,
        handoffDirectory: linkHandoff,
        bindingKey,
        operatorId,
        targetTenantId,
      },
    })).rejects.toThrow(/non-symlinked directory|canonical/)

    const worldHandoff = join(dir, 'world-handoff')
    mkdirSync(worldHandoff, { mode: 0o755 })
    chmodSync(worldHandoff, 0o755)
    const worldState = join(dir, 'world-state')
    mkdirSync(worldState, { mode: 0o700 })
    chmodSync(worldState, 0o700)
    await expect(bootIngressInstance({
      stateDirectory: worldState,
      stub,
      binding: { bindingKey, operatorId, targetTenantId, appId, tenantId, chatId, senderId },
      ownerNotices: {
        enabled: true,
        handoffDirectory: worldHandoff,
        bindingKey,
        operatorId,
        targetTenantId,
      },
    })).rejects.toThrow(/world-accessible/)

    // 4. Revoked operator binding rejects delivery with 0 HTTP requests
    const revokedState = join(dir, 'revoked-state')
    mkdirSync(revokedState, { mode: 0o700 })
    chmodSync(revokedState, 0o700)
    const revokedInst = await bootIngressInstance({
      stateDirectory: revokedState,
      stub,
      binding: {
        bindingKey,
        operatorId,
        targetTenantId,
        appId,
        tenantId,
        chatId,
        senderId,
        revokedAt: new Date(1_760_000_000_000).toISOString(),
      },
      ownerNotices: {
        enabled: true,
        handoffDirectory: handoffDir,
        bindingKey,
        operatorId,
        targetTenantId,
        feishuAppId: appId,
        feishuChatId: chatId,
        pollIntervalMs: 25,
      },
    })
    await delay(120)
    await revokedInst.dispose()
    expect(stub.sentRequests).toHaveLength(0)
    const revokedNotices = await readStoredNotices(revokedState)
    expect(revokedNotices).toHaveLength(1)
    expect(revokedNotices[0]?.status).toBe('failed')
    expect(revokedNotices[0]?.error).toContain('revoked')

    // 5. Wrong configured owner (feishuChatId mismatch) also fails closed without sending HTTP
    const wrongOwnerState = join(dir, 'wrong-owner-state')
    mkdirSync(wrongOwnerState, { mode: 0o700 })
    chmodSync(wrongOwnerState, 0o700)
    const wrongInst = await bootIngressInstance({
      stateDirectory: wrongOwnerState,
      stub,
      binding: { bindingKey, operatorId, targetTenantId, appId, tenantId, chatId, senderId },
      ownerNotices: {
        enabled: true,
        handoffDirectory: handoffDir,
        bindingKey,
        operatorId,
        targetTenantId,
        feishuAppId: appId,
        feishuChatId: 'oc_different_chat',
        pollIntervalMs: 25,
      },
    })
    await delay(120)
    await wrongInst.dispose()
    expect(stub.sentRequests).toHaveLength(0)
    const wrongNotices = await readStoredNotices(wrongOwnerState)
    expect(wrongNotices).toHaveLength(1)
    expect(wrongNotices[0]?.status).toBe('failed')
  })

  it('decouples bounded single-flight noticePump from ordinary IM replies, drains active sends cleanly on shutdown, and avoids duplicates on restart', async () => {
    expect(ownerNoticesCapability).toEqual({
      supported: true,
      schemaVersion: 1,
      configKey: 'ownerNotices',
      handoffSchemaVersion: 1,
      boundedNoticePump: true,
    })

    const stub = await startFeishuStubServer()
    stubs.push(stub)
    const dir = realpathSync(mkdtempSync(join(tmpdir(), 'dsh-p-')))
    tempDirs.push(dir)

    const handoffDir = join(dir, 'handoff')
    const ingressStateDir = join(dir, 'ingress-state')
    const tenantSocketPath = join(dir, 't.sock')
    mkdirSync(handoffDir, { mode: 0o750 })
    chmodSync(handoffDir, 0o750)
    mkdirSync(ingressStateDir, { mode: 0o700 })
    chmodSync(ingressStateDir, 0o700)

    const appId = 'cli_notice_app'
    const tenantId = 'tenant_feishu'
    const chatId = 'oc_owner_chat'
    const senderId = 'ou_owner_sender'
    const operatorId = 'owner-1'
    const targetTenantId = 'octopus'
    const bindingKey = encodeOperatorBindingKey(
      'feishu',
      parseFeishuAppId(appId),
      parseFeishuTenantId(tenantId),
      parseFeishuChatId(chatId),
      parseFeishuSenderId(senderId),
    )

    const keys = [
      'inbound:conv_pump:msg_1',
      'inbound:conv_pump:msg_2',
      'inbound:conv_pump:msg_3',
    ] as const
    const baseMs = Date.now() - 5000
    keys.forEach((noticeKey, index) => {
      const noticeId = formatOwnerNoticeId(noticeKey)
      writeFileSync(
        join(handoffDir, `0${index + 1}-${noticeId}.json`),
        JSON.stringify({
          schemaVersion: 1,
          noticeKey,
          noticeId,
          kind: 'inbound_alert',
          conversationKey: 'conv_pump',
          sourceRef: `msg_${index + 1}`,
          body: `[微信新消息 ${noticeId}] 第${index + 1}条`,
          createdAt: baseMs + index,
          untrusted: true,
          authority: 'notice_only',
        }),
        { mode: 0o640 },
      )
      chmodSync(join(handoffDir, `0${index + 1}-${noticeId}.json`), 0o640)
    })

    let hold1StartedResolve!: () => void
    const hold1StartedPromise = new Promise<void>((resolve) => { hold1StartedResolve = resolve })
    let releaseHold1!: () => void
    const hold1Gate = new Promise<void>((resolve) => { releaseHold1 = resolve })

    let hold2StartedResolve!: () => void
    const hold2StartedPromise = new Promise<void>((resolve) => { hold2StartedResolve = resolve })
    let releaseHold2!: () => void
    const hold2Gate = new Promise<void>((resolve) => { releaseHold2 = resolve })

    let humanReplyResolve!: () => void
    const humanReplyPromise = new Promise<void>((resolve) => { humanReplyResolve = resolve })

    let noticeAttemptCount = 0
    let activeNoticeCount = 0
    let maxActiveNoticeCount = 0

    stub.setBehavior(async (_req, bodyText, res) => {
      const parsed = feishuMessageRequestSchema.parse(JSON.parse(bodyText))
      const text = feishuTextContentSchema.parse(JSON.parse(parsed.content)).text
      if (text === 'human reply ok') {
        res.writeHead(200, { 'Content-Type': 'application/json' })
        res.end(JSON.stringify({ code: 0, msg: 'ok', data: { message_id: 'om_human_1' } }))
        humanReplyResolve()
        return
      }
      noticeAttemptCount += 1
      const attempt = noticeAttemptCount
      activeNoticeCount += 1
      maxActiveNoticeCount = Math.max(maxActiveNoticeCount, activeNoticeCount)
      try {
        if (attempt === 1) {
          hold1StartedResolve()
          await hold1Gate
        } else if (attempt === 2) {
          hold2StartedResolve()
          await hold2Gate
        }
        if (!res.writableEnded) {
          res.writeHead(200, { 'Content-Type': 'application/json' })
          res.end(JSON.stringify({ code: 0, msg: 'ok', data: { message_id: `om_notice_${attempt}` } }))
        }
      } finally {
        activeNoticeCount -= 1
      }
    })

    const submittedDeliveries = new Map<string, { requestId: string; sessionId: string }>()
    const ipcHandler: TenantRuntimeIpcHandler = {
      handleSubmitDelivery: async (req) => {
        const sessionId = req.sessionId ?? 'session-human-1'
        submittedDeliveries.set(req.deliveryKey, { requestId: req.requestId, sessionId })
        return {
          id: req.id,
          type: 'submit_delivery_ack',
          deliveryKey: req.deliveryKey,
          sessionId,
          requestId: req.requestId,
          status: 'completed',
          resultText: 'human reply ok',
          error: null,
        }
      },
      handleGetDelivery: async (req) => {
        const submitted = submittedDeliveries.get(req.deliveryKey)
        const reconciliation = submitted
          ? {
            state: 'completed' as const,
            requestId: submitted.requestId,
            sessionId: submitted.sessionId,
            resultText: 'human reply ok',
          }
          : { state: 'not_found' as const }
        return {
          id: req.id,
          type: 'get_delivery_ack',
          deliveryKey: req.deliveryKey,
          reconciliation,
        }
      },
      handleCancelDelivery: async req => ({
        id: req.id,
        type: 'cancel_delivery_ack',
        deliveryKey: req.deliveryKey,
        status: 'not_found',
      }),
      handleGetApprovalContext: async req => ({
        id: req.id,
        type: 'get_approval_context_ack',
        sessionId: req.sessionId,
        context: null,
      }),
    }
    const ipcServer = new TenantIpcServer(tenantSocketPath, targetTenantId, ipcHandler)
    await ipcServer.listen()

    let inst1Disposed = false
    let inst1!: Awaited<ReturnType<typeof bootIngressInstance>>
    try {
      inst1 = await bootIngressInstance({
        stateDirectory: ingressStateDir,
        stub,
        tenantSocketPath,
        dispatchIntervalMs: 20,
        shutdownTimeoutMs: 3000,
        binding: { bindingKey, operatorId, targetTenantId, appId, tenantId, chatId, senderId },
        ownerNotices: {
          enabled: true,
          handoffDirectory: handoffDir,
          bindingKey,
          operatorId,
          targetTenantId,
          feishuAppId: appId,
          feishuChatId: chatId,
          pollIntervalMs: 20,
          maxIngestPerCycle: 2,
          maxSendsPerCycle: 1,
        },
      })

      await Promise.race([
        hold1StartedPromise,
        delay(1500).then(() => {
          throw new Error(`hold1 did not start (noticeAttemptCount=${noticeAttemptCount}, active=${activeNoticeCount})`)
        }),
      ])
      await delay(80)
      expect(noticeAttemptCount).toBe(1)
      expect(maxActiveNoticeCount).toBe(1)

      await inst1.ctx.imIngress.handleIncomingMessage({
        provider: 'feishu',
        appId: parseFeishuAppId(appId),
        tenantId: parseFeishuTenantId(tenantId),
        chatId: parseFeishuChatId(chatId),
        senderId: parseFeishuSenderId(senderId),
        eventId: parseFeishuEventId('evt_human_concurrent_1'),
        messageId: 'om_in_human_1',
        createTime: Date.now(),
        chatType: 'p2p',
        text: 'hello while notice held',
      })

      await Promise.race([
        humanReplyPromise,
        delay(2000).then(() => {
          throw new Error('Ordinary human IM reply was blocked while owner notice HTTP request was held')
        }),
      ])
      expect(noticeAttemptCount).toBe(1)
      expect(activeNoticeCount).toBe(1)

      releaseHold1()
      await Promise.race([
        hold2StartedPromise,
        delay(1500).then(() => {
          throw new Error(`hold2 did not start (noticeAttemptCount=${noticeAttemptCount}, active=${activeNoticeCount})`)
        }),
      ])
      await delay(60)
      expect(noticeAttemptCount).toBe(2)
      expect(maxActiveNoticeCount).toBe(1)

      let disposeSettled = false
      const disposePromise = inst1.dispose().then(() => {
        disposeSettled = true
        inst1Disposed = true
      })
      await delay(60)
      expect(disposeSettled).toBe(false)
      expect(noticeAttemptCount).toBe(2)

      releaseHold2()
      await Promise.race([
        disposePromise,
        delay(2000).then(() => {
          throw new Error(`disposePromise did not settle (noticeAttemptCount=${noticeAttemptCount}, active=${activeNoticeCount})`)
        }),
      ])
      expect(inst1.exitCode).toBeUndefined()
      expect(noticeAttemptCount).toBe(2)
      expect(maxActiveNoticeCount).toBe(1)

      const storedAfterShutdown = await readStoredNotices(ingressStateDir)
      expect(storedAfterShutdown.filter(row => row.status === 'sent')).toHaveLength(2)
      expect(
        storedAfterShutdown
          .filter(row => row.noticeKey === keys[2])
          .every(row => row.status === 'pending' && row.attempts === 0),
      ).toBe(true)

      const inst2 = await bootIngressInstance({
        stateDirectory: ingressStateDir,
        stub,
        tenantSocketPath,
        dispatchIntervalMs: 25,
        shutdownTimeoutMs: 3000,
        binding: { bindingKey, operatorId, targetTenantId, appId, tenantId, chatId, senderId },
        ownerNotices: {
          enabled: true,
          handoffDirectory: handoffDir,
          bindingKey,
          operatorId,
          targetTenantId,
          feishuAppId: appId,
          feishuChatId: chatId,
          pollIntervalMs: 25,
          maxIngestPerCycle: 20,
          maxSendsPerCycle: 5,
        },
      })
      const deadline = Date.now() + 3000
      while (noticeAttemptCount < 3 && Date.now() < deadline) {
        await delay(25)
      }
      await delay(80)
      await inst2.dispose()

      expect(noticeAttemptCount).toBe(3)
      expect(maxActiveNoticeCount).toBe(1)
      const storedFinal = await readStoredNotices(ingressStateDir)
      expect(storedFinal).toHaveLength(3)
      expect(storedFinal.every(row => row.status === 'sent' && row.attempts === 1)).toBe(true)
      expect(storedFinal.map(row => row.messageId).sort()).toEqual([
        'om_notice_1',
        'om_notice_2',
        'om_notice_3',
      ])
    } finally {
      releaseHold1()
      releaseHold2()
      if (!inst1Disposed && inst1) {
        await inst1.dispose().catch(() => undefined)
      }
      await ipcServer.close().catch(() => undefined)
    }
  })
})
