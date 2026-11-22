/** Broker exchange projection, pinned producer UIDs, frozen-job verification, and sliding-window tests. */
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import { connect } from 'node:net'
import {
  chmodSync,
  existsSync,
  lstatSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  realpathSync,
  rmSync,
  statSync,
  symlinkSync,
  unlinkSync,
  writeFileSync,
} from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import { validatePolicy, wechatNotificationsPolicy } from '../policy.mjs'
import { digest, Operations, resolveWechatRegistry } from '../operations.mjs'
import {
  createBrokerProjectionLoop,
  dispatch,
  perform,
  reconcileManualReplyConfigView,
  setManualReplyControl,
  startBroker,
} from '../broker.mjs'
import {
  WechatExchangeProjector,
  formatNoticeId,
  readTrustedManualReplyView,
  resolveActionableInboundNotice,
  resolveManualReplyViewPath,
  sanitizeUntrustedNoticeText,
} from '../wechat-inbox.mjs'

function makeIngressDb(dbPath, {
  bindingKey,
  operatorId = 'owner',
  targetTenantId = 'octopus',
  requestId = 'human-1',
  prompt = '请准备',
  receivedAt = 1_760_000_000_000,
}) {
  const db = new DatabaseSync(dbPath)
  db.exec('CREATE TABLE IF NOT EXISTS u_im_ingress_operators (key TEXT PRIMARY KEY, value TEXT NOT NULL)')
  db.exec('CREATE TABLE IF NOT EXISTS u_im_ingress_deliveries (key TEXT PRIMARY KEY, value TEXT NOT NULL)')
  db.prepare(
    'INSERT INTO u_im_ingress_operators(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
  ).run(
    bindingKey,
    JSON.stringify({
      bindingKey,
      operatorId,
      targetTenantId,
      provider: 'feishu',
      appId: 'app',
      tenantId: 'org',
      chatId: 'chat',
      senderId: 'owner',
      authorizedAt: new Date(1_760_000_000_000).toISOString(),
      revokedAt: null,
    }),
  )
  const identity = JSON.parse(bindingKey)
  const deliveryKey = JSON.stringify([...identity, requestId])
  db.prepare('DELETE FROM u_im_ingress_deliveries').run()
  db.prepare(
    'INSERT INTO u_im_ingress_deliveries(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
  ).run(
    deliveryKey,
    JSON.stringify({
      deliveryKey,
      operatorId,
      targetTenantId,
      requestId,
      prompt,
      receivedAt: new Date(receivedAt).toISOString(),
      status: 'in_flight',
    }),
  )
  db.close()
}

function setupFixture(t, { wechatNotifications, statUid } = {}) {
  const root = realpathSync(mkdtempSync(join(tmpdir(), 'octopus-inbox-')))
  chmodSync(root, 0o750)
  const wechatRoot = join(root, 'wechat')
  const stateDir = join(root, 'state')
  const handoffDir = join(root, 'handoff')
  mkdirSync(join(wechatRoot, 'config'), { recursive: true, mode: 0o750 })
  mkdirSync(join(wechatRoot, 'anchors'), { recursive: true, mode: 0o750 })
  mkdirSync(join(wechatRoot, 'inbox', 'events'), { recursive: true, mode: 0o700 })
  mkdirSync(join(wechatRoot, 'inbox', 'evidence'), { recursive: true, mode: 0o700 })
  mkdirSync(join(wechatRoot, 'adhoc', 'outbox'), { recursive: true, mode: 0o2750 })
  mkdirSync(join(wechatRoot, 'adhoc', 'receipts'), { recursive: true, mode: 0o700 })
  mkdirSync(stateDir, { recursive: true, mode: 0o700 })
  mkdirSync(handoffDir, { recursive: true, mode: 0o750 })

  // Mirror real host directory permissions (0750 / 2750 / 0700; zero world bits).
  for (const [dir, mode] of [
    [wechatRoot, 0o750],
    [join(wechatRoot, 'config'), 0o750],
    [join(wechatRoot, 'anchors'), 0o750],
    [join(wechatRoot, 'inbox'), 0o750],
    [join(wechatRoot, 'inbox', 'events'), 0o700],
    [join(wechatRoot, 'inbox', 'evidence'), 0o700],
    [join(wechatRoot, 'adhoc'), 0o750],
    [join(wechatRoot, 'adhoc', 'outbox'), 0o2750],
    [join(wechatRoot, 'adhoc', 'receipts'), 0o700],
    [stateDir, 0o700],
    [handoffDir, 0o750],
  ]) {
    chmodSync(dir, mode)
  }

  for (const name of [
    'account.png',
    'own.png',
    'alice-s.png',
    'alice-side.png',
    'alice-card.png',
    'muted-s.png',
    'muted-side.png',
    'muted-card.png',
    'ops-g.png',
    'ops-m1.png',
  ]) {
    const p = join(wechatRoot, 'anchors', name)
    writeFileSync(p, name, { mode: 0o600 })
    chmodSync(p, 0o600)
  }

  const wechatConfigPath = join(wechatRoot, 'config', 'config.json')
  const sidecarPath = join(wechatRoot, 'config', 'octopus-contacts.json')
  const accountId = 'own-wechat-account'
  const accountSha = digest(accountId)

  writeFileSync(
    wechatConfigPath,
    JSON.stringify({
      account_id: accountId,
      recipient_id: 'wxid_alice',
      recipient_query: 'Alice',
      enabled: true,
      direct_enabled: true,
      group_enabled: true,
      account_anchor: join(wechatRoot, 'anchors', 'account.png'),
      own_chat_anchor: join(wechatRoot, 'anchors', 'own.png'),
    }),
    { mode: 0o600 },
  )
  chmodSync(wechatConfigPath, 0o600)

  writeFileSync(
    sidecarPath,
    JSON.stringify({
      schema_version: 1,
      account_sha256: accountSha,
      contacts: [
        {
          id: 'alice',
          type: 'friend',
          alias: 'Alice别名',
          display_name: 'Alice',
          enabled: true,
          direct_enabled: true,
          recipient_id: 'wxid_alice',
          recipient_query: 'Alice',
          recipient_anchor: join(wechatRoot, 'anchors', 'alice-s.png'),
          recipient_sidebar_anchor: join(wechatRoot, 'anchors', 'alice-side.png'),
          recipient_card_anchor: join(wechatRoot, 'anchors', 'alice-card.png'),
        },
        {
          id: 'ops_group',
          type: 'group',
          alias: '运维群',
          display_name: '运维值班群',
          enabled: true,
          group_enabled: true,
          group_id: 'grp_ops_1',
          group_name: '运维值班群',
          group_query: '运维值班群',
          group_name_read_mode: 'native_copy',
          group_anchor: join(wechatRoot, 'anchors', 'ops-g.png'),
          group_member_anchors: [join(wechatRoot, 'anchors', 'ops-m1.png')],
        },
        {
          id: 'muted_friend',
          type: 'friend',
          alias: '停用好友',
          display_name: 'MutedFriend',
          enabled: false,
          direct_enabled: false,
          recipient_id: 'wxid_muted',
          recipient_query: 'MutedFriend',
          recipient_anchor: join(wechatRoot, 'anchors', 'muted-s.png'),
          recipient_sidebar_anchor: join(wechatRoot, 'anchors', 'muted-side.png'),
          recipient_card_anchor: join(wechatRoot, 'anchors', 'muted-card.png'),
        },
      ],
    }),
    { mode: 0o640 },
  )
  chmodSync(sidecarPath, 0o640)

  let nowMs = 1_760_000_000_000
  const bindingKey = JSON.stringify(['feishu', 'app', 'org', 'chat', 'owner'])
  const ingressDbPath = join(root, 'ingress.sqlite')
  makeIngressDb(ingressDbPath, { bindingKey, receivedAt: nowMs })

  const policyPath = join(root, 'policy.json')
  const normalizedNotif = wechatNotifications
    ? (wechatNotifications.enabled
      ? {
        ...(Object.hasOwn(wechatNotifications, 'handoffDir') ? {} : { handoffDir }),
        ...(Object.hasOwn(wechatNotifications, 'receiptProducerUid')
          ? {}
          : { receiptProducerUid: statSync(join(wechatRoot, 'adhoc', 'receipts')).uid }),
        ...(Object.hasOwn(wechatNotifications, 'inboundProducerUid')
          ? {}
          : { inboundProducerUid: statSync(join(wechatRoot, 'inbox', 'events')).uid }),
        ...(Object.hasOwn(wechatNotifications, 'outboxProducerUid')
          ? {}
          : { outboxProducerUid: statSync(join(wechatRoot, 'adhoc', 'outbox')).uid }),
        ...wechatNotifications,
      }
      : wechatNotifications)
    : undefined

  const manualReplyCfgPath = join(wechatRoot, 'config', 'octopus-manual-reply.json')
  const effectiveNotif = wechatNotificationsPolicy(normalizedNotif ? { wechatNotifications: normalizedNotif } : {})
  writeFileSync(
    manualReplyCfgPath,
    JSON.stringify({
      schema_version: 1,
      account_sha256: accountSha,
      enabled: effectiveNotif.enabled,
      manual_reply_enabled: effectiveNotif.manualReplyEnabled,
      max_age_seconds: effectiveNotif.manualReplyMaxAgeSeconds,
      paused: false,
      lease_expires_at: null,
    }),
    { mode: 0o640 },
  )
  chmodSync(manualReplyCfgPath, 0o640)

  const policy = {
    socket: join(root, 'broker.sock'),
    state: stateDir,
    ingressDatabase: ingressDbPath,
    bindingKey,
    operatorId: 'owner',
    tenantId: 'octopus',
    services: { probe: 'octopus-probe.service' },
    containers: ['probe'],
    wechatRoot,
    wechatConfig: wechatConfigPath,
    wechatContactsConfig: sidecarPath,
    wechatContacts: [
      { id: 'alice', type: 'friend', alias: 'Alice别名', displayName: 'Alice', enabled: true, capabilities: { directSend: true } },
      { id: 'ops_group', type: 'group', alias: '运维群', displayName: '运维值班群', enabled: true, capabilities: { groupSend: true } },
      { id: 'muted_friend', type: 'friend', alias: '停用好友', displayName: 'MutedFriend', enabled: false, capabilities: { directSend: false } },
    ],
    limits: { logLines: 50, previewSeconds: 600, workerPollSeconds: 2, maxTextBytes: 1000, commandSeconds: 10 },
    ...(normalizedNotif !== undefined ? { wechatNotifications: normalizedNotif } : {}),
  }
  writeFileSync(policyPath, JSON.stringify(policy), { mode: 0o600 })

  // Always seed real operations.sqlite so frozen-job relationship checks exercise production storage.
  let opsReqSeq = 1
  const createFrozenJob = async ({ kind, payload, receiptData, visibility }) => {
    const reqId = `human-job-${opsReqSeq++}`
    const prompt = kind === 'wechat_send'
      ? `给${payload.recipient}发微信：${payload.text}`
      : kind === 'wechat_send_group'
        ? `给${payload.recipient}发群消息：${payload.text}`
        : kind === 'wechat_publish_moment'
          ? `发${visibility === 'friends' ? '好友可见' : '仅自己可见'}朋友圈：${payload.text}`
          : '请准备'
    makeIngressDb(ingressDbPath, { bindingKey, requestId: reqId, prompt, receivedAt: nowMs })
    const ops = new Operations(
      policy,
      policyPath,
      async (op) => {
        const outboxObj = kind === 'wechat_publish_moment'
          ? {
            schema_version: 2,
            channel: 'adhoc_moments',
            kind: 'publish',
            job_id: op.id,
            account_sha256: accountSha,
            visibility: op.visibility,
            binding_sha256: op.targetProof,
            text: op.payload.text,
            text_sha256: digest(op.payload.text),
            created_at: nowMs,
            expires_at: nowMs + 600_000,
            confirmation_request_id: reqId,
          }
          : kind === 'wechat_send_group'
            ? {
              schema_version: 2,
              channel: 'group',
              kind: 'send',
              job_id: op.id,
              account_sha256: accountSha,
              contact_id: op.contactId,
              group_alias: op.recipientAlias,
              binding_sha256: op.targetProof,
              text: op.payload.text,
              text_sha256: digest(op.payload.text),
              created_at: nowMs,
              expires_at: nowMs + 600_000,
              confirmation_request_id: reqId,
            }
            : {
              schema_version: 2,
              kind: 'send',
              job_id: op.id,
              account_sha256: accountSha,
              contact_id: op.contactId,
              recipient_alias: op.recipientAlias,
              binding_sha256: op.targetProof,
              text: op.payload.text,
              text_sha256: digest(op.payload.text),
              created_at: nowMs,
              expires_at: nowMs + 600_000,
              confirmation_request_id: reqId,
            }
        const outboxPath = join(wechatRoot, 'adhoc', 'outbox', `${op.id}.json`)
        writeFileSync(outboxPath, JSON.stringify(outboxObj), { mode: 0o640 })
        chmodSync(outboxPath, 0o640)
        return { queued: true, operation: op.id }
      },
      () => nowMs,
    )
    const prep = ops.prepare({ kind, payload: visibility ? { ...payload, visibility } : payload })
    await ops.run(prep.id)
    ops.close()
    chmodSync(join(stateDir, 'operations.sqlite'), 0o600)

    const receiptPath = join(wechatRoot, 'adhoc', 'receipts', `${prep.id}.json`)
    const rawReceipt = JSON.stringify(receiptData)
    writeFileSync(receiptPath, rawReceipt, { mode: 0o600 })
    chmodSync(receiptPath, 0o600)
    return { jobId: prep.id, receiptPath, rawReceipt }
  }

  // Initialize operations.sqlite table cleanly even before any job is queued.
  const initOps = new Operations(policy, policyPath, async () => ({ ok: true }), () => nowMs)
  initOps.close()
  chmodSync(join(stateDir, 'operations.sqlite'), 0o600)

  let projector = new WechatExchangeProjector(policy, {
    clock: () => nowMs,
    ...(statUid ? { statUid } : {}),
  })

  const writeEvidence = (name, content = 'png-bytes-' + name) => {
    const path = join(wechatRoot, 'inbox', 'evidence', name)
    writeFileSync(path, content, { mode: 0o600 })
    chmodSync(path, 0o600)
    return { relPath: `/home/wechat/.gamma-moments/inbox/evidence/${name}`, hostPath: path, sha256: digest(content) }
  }

  const writeEvent = (event, { fileName, mode = 0o600 } = {}) => {
    const name = fileName ?? `${event.event_id}.json`
    const target = join(wechatRoot, 'inbox', 'events', name)
    writeFileSync(target, JSON.stringify(event), { mode })
    chmodSync(target, mode)
    return target
  }

  const readHandoffs = () => readdirSync(handoffDir)
    .filter(name => name.endsWith('.json'))
    .sort()
    .map(name => ({
      name,
      mode: statSync(join(handoffDir, name)).mode & 0o777,
      data: JSON.parse(readFileSync(join(handoffDir, name), 'utf8')),
    }))

  t.after(() => {
    projector.close()
    rmSync(root, { recursive: true, force: true })
  })

  return {
    root,
    policyPath,
    wechatRoot,
    stateDir,
    handoffDir,
    ingressDbPath,
    policy,
    accountSha,
    time: val => { nowMs = val },
    now: () => nowMs,
    get projector() { return projector },
    reopen: () => {
      projector.close()
      projector = new WechatExchangeProjector(policy, {
        clock: () => nowMs,
        ...(statUid ? { statUid } : {}),
      })
    },
    createFrozenJob,
    writeEvidence,
    writeEvent,
    readHandoffs,
  }
}

test('owner ingress can read captured records for one WeChat post or bound chat, but quoted or revoked requests fail', async t => {
  const f = setupFixture(t, { wechatNotifications: { enabled: true } })
  const request = { action: 'wechat_inbox_read' }
  const journal = { policy: f.policy, policyPath: f.policyPath, clock: f.now }
  const evidence = f.writeEvidence('inbox-read.png')
  const proof = resolveWechatRegistry(f.policy).contacts.find(c => c.id === 'alice').targetProof
  const ownerPrompt = prompt => makeIngressDb(f.ingressDbPath, {
    bindingKey: f.policy.bindingKey,
    prompt,
    receivedAt: f.now(),
  })
  const event = (eventId, observedAt, text, fields) => f.writeEvent({
    schema_version: 1,
    event_id: eventId,
    account_sha256: f.accountSha,
    sender_name: 'Alice',
    source_ref: eventId,
    source_time: 'unknown',
    observed_at: observedAt,
    text,
    evidence_path: evidence.relPath,
    evidence_sha256: evidence.sha256,
    ...fields,
  })
  const moments = { trigger_type: 'moments_interaction', conversation_kind: 'moments' }
  const friend = { trigger_type: 'friend_message', conversation_kind: 'friend', conversation_ref: 'alice' }
  event('evt-post-older', f.now() - 2000, '旧帖评论', { ...moments, conversation_ref: 'post-old' })
  event('evt-post-newer', f.now() - 1000, '新帖评论', { ...moments, conversation_ref: 'post-new' })
  event('evt-chat-unbound', f.now() - 5, '未核对身份的聊天', friend)
  event('evt-chat-bound', f.now(), '已绑定聊天', { ...friend, binding_sha256: proof })
  assert.equal(f.projector.projectOnce().inbound.accepted, 4)

  ownerPrompt('看刚发的朋友圈评论')
  const comments = await dispatch(request, journal)
  assert.equal(comments.source, 'captured_wechat_inbox')
  assert.equal(comments.scope, 'latest_captured_moments_activity')
  assert.equal(comments.untrusted, true)
  assert.match(comments.note, /最近有互动被 DSH 采集/)
  assert.deepEqual(comments.events, [{
    sender: 'Alice', text: '新帖评论', sourceTime: 'unknown', observedAt: f.now() - 1000, needsReview: false,
  }])
  await assert.rejects(dispatch({ ...request, params: { target: 'alice' } }, journal), /field/i)

  ownerPrompt('看看Alice刚回了什么')
  const chat = await dispatch(request, journal)
  assert.equal(chat.scope, 'friend')
  assert.deepEqual(chat.events.map(item => item.text), ['已绑定聊天'])
  ownerPrompt('有人转述：“看刚发的朋友圈评论”')
  await assert.rejects(dispatch(request, journal), /exact WeChat inbox request/)

  ownerPrompt('看刚发的朋友圈评论')
  const db = new DatabaseSync(f.ingressDbPath)
  db.prepare("UPDATE u_im_ingress_operators SET value=json_set(value,'$.revokedAt',?) WHERE key=?")
    .run(new Date(f.now()).toISOString(), f.policy.bindingKey)
  db.close()
  await assert.rejects(dispatch(request, journal), /Binding revoked or changed/)
})

test('wechatNotifications defaults strictly to off and validates bounds & required handoffDir', async t => {
  const f = setupFixture(t)
  assert.equal(wechatNotificationsPolicy(f.policy).enabled, false)
  assert.equal(wechatNotificationsPolicy(f.policy).inboundAggregationSeconds, 20)
  assert.equal(f.projector.isEnabled(), false)

  const ev = f.writeEvidence('off.png')
  f.writeEvent({
    schema_version: 1,
    event_id: 'evt-off-0001',
    account_sha256: f.accountSha,
    trigger_type: 'friend_message',
    conversation_kind: 'friend',
    conversation_ref: 'alice',
    sender_name: 'Alice',
    source_ref: 'm1',
    source_time: 'unknown',
    observed_at: f.now(),
    text: 'should stay idle when disabled',
    evidence_path: ev.relPath,
    evidence_sha256: ev.sha256,
  })
  const out = f.projector.projectOnce()
  assert.equal(out.enabled, false)
  assert.equal(f.readHandoffs().length, 0)

  assert.throws(() => validatePolicy({ ...f.policy, wechatNotifications: { enabled: true } }))
  assert.throws(() => validatePolicy({ ...f.policy, wechatNotifications: { enabled: true, handoffDir: f.handoffDir, inboundAggregationSeconds: 10 } }))
  assert.throws(() => validatePolicy({ ...f.policy, wechatNotifications: { enabled: true, handoffDir: f.handoffDir, inboundAggregationSeconds: 25, inboundMaxBatchWaitSeconds: 20 } }))
  assert.throws(() => validatePolicy({ ...f.policy, wechatNotifications: { enabled: true, handoffDir: f.handoffDir, receiptProducerUid: -1 } }))
  assert.throws(() => validatePolicy({ ...f.policy, wechatNotifications: { enabled: true, handoffDir: f.handoffDir, unknownKey: 1 } }))
  assert.throws(() => validatePolicy({ ...f.policy, wechatNotifications: { enabled: true, handoffDir: f.handoffDir, notifyOnReceiptStates: ['bad_state'] } }))
})

test('rejects world-readable/writable modes, mismatched real fstat UID, symlinks, and escaping inputs', t => {
  // 1. Real kernel lstatSync/fstatSync rejects pinned UID mismatches across receipt, inbound, and outbox roles
  const wrongUid = (process.getuid?.() ?? 501) + 1
  const mismatchedReceipt = setupFixture(t, {
    wechatNotifications: { enabled: true, receiptProducerUid: wrongUid },
  })
  const wrongUidReceipt = join(mismatchedReceipt.wechatRoot, 'adhoc', 'receipts', '111111111111111111111111.json')
  writeFileSync(wrongUidReceipt, JSON.stringify({ state: 'sent_verified' }), { mode: 0o600 })
  chmodSync(wrongUidReceipt, 0o600)
  assert.throws(() => mismatchedReceipt.projector.ingestTerminalReceipts(), /not pinned|does not match pinned/)

  const mismatchedInbound = setupFixture(t, {
    wechatNotifications: { enabled: true, inboundProducerUid: wrongUid },
  })
  assert.throws(() => mismatchedInbound.projector.ingestInboundEvents(), /not pinned|does not match pinned/)

  const mismatchedOutbox = setupFixture(t, {
    wechatNotifications: { enabled: true, outboxProducerUid: wrongUid },
  })
  assert.throws(() => mismatchedOutbox.projector.ingestTerminalReceipts(), /not pinned|does not match pinned/)

  const unpinnedAll = setupFixture(t, {
    wechatNotifications: {
      enabled: true,
      receiptProducerUid: null,
      inboundProducerUid: null,
      outboxProducerUid: null,
    },
  })
  if (process.getuid?.() === 0) {
    assert.throws(() => unpinnedAll.projector.ingestInboundEvents(), /Pinned inboundProducerUid UID is required/)
    assert.throws(() => unpinnedAll.projector.ingestTerminalReceipts(), /Pinned receiptProducerUid UID is required/)
    const unpinnedOutboxOnly = setupFixture(t, {
      wechatNotifications: { enabled: true, outboxProducerUid: null },
    })
    assert.throws(() => unpinnedOutboxOnly.projector.ingestTerminalReceipts(), /Pinned outboxProducerUid UID is required/)
  } else {
    assert.equal(unpinnedAll.projector.ingestInboundEvents().enabled, true)
    assert.equal(unpinnedAll.projector.ingestTerminalReceipts().enabled, true)
  }

  // 2. Rejects world-readable (0644), writable (0666), symlinked, oversized, and escaping event files
  const f = setupFixture(t, { wechatNotifications: { enabled: true } })
  const ev = f.writeEvidence('valid.png')
  const baseEvent = {
    schema_version: 1,
    event_id: 'evt-valid-001',
    account_sha256: f.accountSha,
    trigger_type: 'friend_message',
    conversation_kind: 'friend',
    conversation_ref: 'alice',
    sender_name: 'Alice',
    source_ref: 'm1',
    source_time: '15:00',
    observed_at: f.now(),
    text: '正常消息',
    evidence_path: ev.relPath,
    evidence_sha256: ev.sha256,
  }

  const externalJson = join(f.root, 'external-event.json')
  writeFileSync(externalJson, JSON.stringify({ ...baseEvent, event_id: 'evt-symlink-1' }), { mode: 0o600 })
  chmodSync(externalJson, 0o600)
  symlinkSync(externalJson, join(f.wechatRoot, 'inbox', 'events', 'evt-symlink-1.json'))

  f.writeEvent({ ...baseEvent, event_id: 'evt-worldread-1' }, { mode: 0o644 })
  f.writeEvent({ ...baseEvent, event_id: 'evt-writable-1' }, { mode: 0o666 })

  const oversized = join(f.wechatRoot, 'inbox', 'events', 'evt-oversize-1.json')
  writeFileSync(oversized, JSON.stringify({ ...baseEvent, event_id: 'evt-oversize-1', text: 'A'.repeat(35_000) }), { mode: 0o600 })
  chmodSync(oversized, 0o600)

  f.writeEvent({ ...baseEvent, event_id: 'evt-wrongacct-1', account_sha256: digest('other-wechat') })
  f.writeEvent({ ...baseEvent, event_id: 'evt-unknown-1', injected_command: 'run' })
  f.writeEvent({ ...baseEvent, event_id: 'evt-realid-1' }, { fileName: 'evt-aliased-1.json' })
  f.writeEvent({ ...baseEvent, event_id: 'evt-kind-bad-1', conversation_kind: 'group', conversation_ref: 'ops_group' })
  f.writeEvent({ ...baseEvent, event_id: 'evt-muted-001', conversation_ref: 'muted_friend' })

  const outsideFile = join(f.root, 'secret.txt')
  writeFileSync(outsideFile, 'secret', { mode: 0o600 })
  f.writeEvent({
    ...baseEvent,
    event_id: 'evt-escape-1',
    evidence_path: join(f.wechatRoot, 'inbox', 'evidence', '..', '..', 'secret.txt'),
    evidence_sha256: digest('secret'),
  })
  f.writeEvent({ ...baseEvent, event_id: 'evt-baddigest-1', evidence_sha256: '0'.repeat(64) })
  f.writeEvent(baseEvent)

  f.time(f.now() + 25_000)
  const res = f.projector.projectOnce()
  assert.equal(res.inbound.accepted, 1)
  assert.equal(res.inbound.rejected.length, 11)
  const handoffs = f.readHandoffs()
  assert.equal(handoffs.length, 1)
  assert.equal(handoffs[0].mode, 0o640)
  assert.match(handoffs[0].data.body, /正常消息/)
})

test('distinct production role UIDs (root 0 / worker 1000 / rogue 999) isolate outbox, receipts, and future inbound producer', async t => {
  const uidMap = new Map()
  const f = setupFixture(t, {
    wechatNotifications: {
      enabled: true,
      handoffGid: 2000,
      receiptProducerUid: 1000,
      inboundProducerUid: 1000,
      outboxProducerUid: 0,
    },
    statUid: (_stat, path) => {
      if (uidMap.has(path)) return uidMap.get(path)
      if (path.includes('/adhoc/receipts') || path.includes('/inbox/events') || path.includes('/inbox/evidence')) {
        return 1000
      }
      return 0
    },
  })

  // Legitimate worker (uid 1000) receipt + root (uid 0) outbox + frozen op in operations.sqlite
  const validJob = await f.createFrozenJob({
    kind: 'wechat_send',
    payload: { recipient: 'Alice别名', text: '不同UID测试' },
    receiptData: { state: 'sent_verified', recipient_alias: 'Alice别名', checked_at: f.now() / 1000 },
  })
  // Spoofed receipt supposedly written by uid 999 (or uid 0) against real frozen job -> rejected!
  const spoofJob = await f.createFrozenJob({
    kind: 'wechat_send',
    payload: { recipient: 'Alice别名', text: '冒充uid测试' },
    receiptData: { state: 'sent_verified', recipient_alias: 'Alice别名', checked_at: f.now() / 1000 },
  })
  uidMap.set(spoofJob.receiptPath, 999)

  // Worker (1000) trying to write outbox (must be root 0) -> rejected!
  const tamperedOutboxJob = await f.createFrozenJob({
    kind: 'wechat_send',
    payload: { recipient: 'Alice别名', text: '篡改outbox属主' },
    receiptData: { state: 'sent_verified', recipient_alias: 'Alice别名', checked_at: f.now() / 1000 },
  })
  uidMap.set(join(f.wechatRoot, 'adhoc', 'outbox', `${tamperedOutboxJob.jobId}.json`), 1000)

  // Future inbound producer under uid 1000 writing event + evidence, plus rogue uid 999 event
  const ev = f.writeEvidence('worker1000.png')
  f.writeEvent({
    schema_version: 1,
    event_id: 'evt-worker-1000',
    account_sha256: f.accountSha,
    trigger_type: 'friend_message',
    conversation_kind: 'friend',
    conversation_ref: 'alice',
    sender_name: 'Alice',
    source_ref: 'msg-w1000',
    source_time: '12:00',
    observed_at: f.now(),
    text: '来自UID1000采集器的合法消息',
    evidence_path: ev.relPath,
    evidence_sha256: ev.sha256,
  })
  const roguePath = f.writeEvent({
    schema_version: 1,
    event_id: 'evt-rogue-0999',
    account_sha256: f.accountSha,
    trigger_type: 'friend_message',
    conversation_kind: 'friend',
    conversation_ref: 'alice',
    sender_name: 'Attacker',
    source_ref: 'msg-r999',
    source_time: '12:00',
    observed_at: f.now(),
    text: '不该信任的消息',
    evidence_path: ev.relPath,
    evidence_sha256: ev.sha256,
  })
  uidMap.set(roguePath, 999)

  f.time(f.now() + 25_000)
  const out = f.projector.projectOnce()
  assert.equal(out.receipts.projected, 1)
  assert.equal(out.receipts.rejected.length, 2)
  assert.equal(out.receipts.rejected.filter(item => item.reason.includes('does not match pinned producer uid')).length, 2)
  assert.equal(out.inbound.accepted, 1)
  assert.equal(out.inbound.rejected.length, 1)
  assert.match(out.inbound.rejected[0].reason, /does not match pinned producer uid/)

  const handoffs = f.readHandoffs()
  assert.equal(handoffs.length, 2)
  assert.ok(handoffs.some(h => h.data.sourceRef === validJob.jobId && h.data.untrusted === true && h.data.authority === 'notice_only'))
  assert.ok(handoffs.some(h => h.data.body.includes('来自UID1000采集器的合法消息')))
  assert.equal(existsSync(join(f.handoffDir, `${digest(`receipt:${spoofJob.jobId}:sent_verified`).slice(0, 32)}.json`)), false)
})

test('rejects orphan receipts, unexecuted prepared jobs, or outbox text/proof mismatches against operations.sqlite', async t => {
  const f = setupFixture(t, { wechatNotifications: { enabled: true } })

  // 1. Orphan receipt without matching outbox or frozen operation
  const orphanId = 'aaaaaaaaaaaaaaaaaaaaaaaa'
  writeFileSync(join(f.wechatRoot, 'adhoc', 'receipts', `${orphanId}.json`), JSON.stringify({ state: 'sent_verified' }), { mode: 0o600 })
  chmodSync(join(f.wechatRoot, 'adhoc', 'receipts', `${orphanId}.json`), 0o600)

  // 2. Prepared-only operation in operations.sqlite (never run -> state='prepared') paired with forged outbox & receipt
  const ops = new Operations(f.policy, f.policyPath, async () => ({ ok: true }), () => f.now())
  const prepOnly = ops.prepare({ kind: 'wechat_send', payload: { recipient: 'Alice别名', text: '仅预览未执行' } })
  ops.close()
  chmodSync(join(f.stateDir, 'operations.sqlite'), 0o600)
  const fakeOutbox = {
    schema_version: 2,
    kind: 'send',
    job_id: prepOnly.id,
    account_sha256: f.accountSha,
    contact_id: 'alice',
    recipient_alias: 'Alice别名',
    binding_sha256: prepOnly.targetProof,
    text: '仅预览未执行',
    text_sha256: digest('仅预览未执行'),
    created_at: f.now(),
    expires_at: f.now() + 600_000,
    confirmation_request_id: 'human-1',
  }
  writeFileSync(join(f.wechatRoot, 'adhoc', 'outbox', `${prepOnly.id}.json`), JSON.stringify(fakeOutbox), { mode: 0o640 })
  chmodSync(join(f.wechatRoot, 'adhoc', 'outbox', `${prepOnly.id}.json`), 0o640)
  writeFileSync(join(f.wechatRoot, 'adhoc', 'receipts', `${prepOnly.id}.json`), JSON.stringify({ state: 'sent_verified' }), { mode: 0o600 })
  chmodSync(join(f.wechatRoot, 'adhoc', 'receipts', `${prepOnly.id}.json`), 0o600)

  // 3. Valid executed job whose outbox text_sha256 is later tampered
  const tampered = await f.createFrozenJob({
    kind: 'wechat_send',
    payload: { recipient: 'Alice别名', text: '原始批准正文' },
    receiptData: { state: 'sent_verified', recipient_alias: 'Alice别名', checked_at: f.now() / 1000 },
  })
  const tamperedOutboxPath = join(f.wechatRoot, 'adhoc', 'outbox', `${tampered.jobId}.json`)
  const mutated = JSON.parse(readFileSync(tamperedOutboxPath, 'utf8'))
  mutated.text = '替换后的正文'
  mutated.text_sha256 = digest('替换后的正文')
  writeFileSync(tamperedOutboxPath, JSON.stringify(mutated), { mode: 0o640 })
  chmodSync(tamperedOutboxPath, 0o640)

  const out = f.projector.projectOnce()
  assert.equal(out.receipts.projected, 0)
  assert.equal(out.receipts.rejected.length, 3)
  assert.equal(f.readHandoffs().length, 0)
})

test('20s sliding window, event_id dedup, terminal receipts, bounded context, and retention handoff cleanup work cohesively', async t => {
  const f = setupFixture(t, {
    wechatNotifications: {
      enabled: true,
      inboundAggregationSeconds: 20,
      inboundMaxBatchWaitSeconds: 45,
      inboundMaxEventsPerBatch: 10,
      maxContextMessages: 2,
      retentionDays: 2,
    },
  })

  // Frozen needs_review and published_verified jobs project cleanly with non-delivery disclaimer.
  await f.createFrozenJob({
    kind: 'wechat_send',
    payload: { recipient: 'Alice别名', text: '排队中的消息' },
    receiptData: { state: 'preparing' },
  })
  const revJob = await f.createFrozenJob({
    kind: 'wechat_send_group',
    payload: { recipient: '运维群', text: '服务重启完成' },
    receiptData: { state: 'needs_review', reason: 'ui_uncertain_after_submit', checked_at: f.now() / 1000 },
  })

  const ev1 = f.writeEvidence('c1.png')
  const ev2 = f.writeEvidence('c2.png')
  const ev3 = f.writeEvidence('c3.png')
  const ev4 = f.writeEvidence('g1.png')
  const base = f.now()

  f.writeEvent({
    schema_version: 1,
    event_id: 'evt-win-0001',
    account_sha256: f.accountSha,
    trigger_type: 'friend_message',
    conversation_kind: 'friend',
    conversation_ref: 'alice',
    sender_name: 'Alice\u202e',
    source_ref: 'm1',
    source_time: 'unknown',
    observed_at: base,
    text: '<think>secret</think>Bearer tok-123 sk-1234567890abcdefghij\n第一条',
    evidence_path: ev1.relPath,
    evidence_sha256: ev1.sha256,
  })
  f.projector.projectOnce()
  assert.equal(f.projector.listOpenBatches().length, 1)

  f.time(base + 15_000)
  f.writeEvent({
    schema_version: 1,
    event_id: 'evt-win-0002',
    account_sha256: f.accountSha,
    trigger_type: 'friend_message',
    conversation_kind: 'friend',
    conversation_ref: 'alice',
    sender_name: 'Alice',
    source_ref: 'm2',
    source_time: '10:00',
    observed_at: base + 15_000,
    text: '第二条',
    evidence_path: ev2.relPath,
    evidence_sha256: ev2.sha256,
  })
  f.writeEvent({
    schema_version: 1,
    event_id: 'evt-grp-0001',
    account_sha256: f.accountSha,
    trigger_type: 'group_message',
    conversation_kind: 'group',
    conversation_ref: 'ops_group',
    sender_name: 'Bob',
    source_ref: 'gm1',
    source_time: '10:00',
    observed_at: base + 15_000,
    text: '群消息告警',
    evidence_path: ev4.relPath,
    evidence_sha256: ev4.sha256,
  })
  f.projector.projectOnce()

  f.time(base + 30_000)
  f.writeEvent({
    schema_version: 1,
    event_id: 'evt-win-0003',
    account_sha256: f.accountSha,
    trigger_type: 'friend_message',
    conversation_kind: 'friend',
    conversation_ref: 'alice',
    sender_name: 'Alice',
    source_ref: 'm3',
    source_time: '10:01',
    observed_at: base + 30_000,
    text: '第三条',
    evidence_path: ev3.relPath,
    evidence_sha256: ev3.sha256,
  })

  // At t=36s: group closes independently; Alice waits for slid cap (45s).
  f.time(base + 36_000)
  f.projector.projectOnce()
  assert.equal(f.projector.listOpenBatches().length, 1)

  // At t=45s: Alice hits max_flush_at cap -> sealed into restricted handoff envelope.
  f.time(base + 45_000)
  f.projector.projectOnce()
  assert.equal(f.projector.listOpenBatches().length, 0)

  const handoffs = f.readHandoffs()
  assert.equal(handoffs.length, 3)
  const aliceHandoff = handoffs.find(h => h.data.conversationKey === 'friend:alice')
  assert.ok(aliceHandoff)
  assert.match(aliceHandoff.data.noticeId, /^wn_[0-9a-f]{12}$/)
  assert.match(aliceHandoff.data.body, /共 3 条/)
  assert.match(aliceHandoff.data.body, /来源时间未知\(unknown\)/)
  assert.doesNotMatch(aliceHandoff.data.body, /tok-123|sk-1234567890abcdefghij|<think>|\u202e/)
  assert.equal(aliceHandoff.data.chatId, undefined)
  assert.equal(aliceHandoff.data.operatorId, undefined)

  // Bounded conversation_context kept only latest 2 for friend:alice
  assert.deepEqual(f.projector.listContext('friend:alice').map(c => c.event_id), ['evt-win-0002', 'evt-win-0003'])
  // Source receipt stayed completely unmodified.
  assert.equal(lstatSync(revJob.receiptPath).isFile(), true)
  assert.equal(readFileSync(revJob.receiptPath, 'utf8'), revJob.rawReceipt)

  // Reopen & rescan must duplicate 0 events/receipts.
  f.reopen()
  const rescan = f.projector.projectOnce()
  assert.equal(rescan.inbound.accepted, 0)
  assert.equal(rescan.inbound.duplicates, 4)
  assert.equal(rescan.receipts.projected, 0)
  assert.equal(rescan.receipts.duplicates, 1)

  // Retention prune removes expired context and stale handoff files cleanly.
  f.time(base + 3 * 86_400_000)
  const pruned = f.projector.pruneExpiredRecords(f.now())
  assert.equal(pruned.handoffFilesRemoved, 3)
  assert.equal(f.readHandoffs().length, 0)
  assert.equal(sanitizeUntrustedNoticeText('ok\u0000\u202etest'), 'ok test')
  assert.match(formatNoticeId('inbound:batch-x'), /^wn_[0-9a-f]{12}$/)
})

test('createBrokerProjectionLoop returns null when disabled and runs non-overlapping start/stop when enabled', async t => {
  const off = setupFixture(t)
  assert.equal(createBrokerProjectionLoop(off.policy), null)

  const f = setupFixture(t, {
    wechatNotifications: {
      enabled: true,
      projectionIntervalSeconds: 1,
    },
  })
  const job = await f.createFrozenJob({
    kind: 'wechat_send',
    payload: { recipient: 'Alice别名', text: 'broker loop notice' },
    receiptData: { state: 'sent_verified', recipient_alias: 'Alice别名', checked_at: f.now() / 1000 },
  })
  const loop = createBrokerProjectionLoop(f.policy, { clock: () => f.now() })
  assert.ok(loop)
  loop.start()
  try {
    const handoffs = f.readHandoffs()
    assert.equal(handoffs.length, 1)
    assert.equal(handoffs[0].data.sourceRef, job.jobId)
  } finally {
    loop.stop()
  }
})

test('manualReplyEnabled defaults off when notifications enabled and requires enabled=true', t => {
  const f = setupFixture(t, { wechatNotifications: { enabled: true } })
  assert.equal(wechatNotificationsPolicy(f.policy).enabled, true)
  assert.equal(wechatNotificationsPolicy(f.policy).manualReplyEnabled, false)
  assert.equal(wechatNotificationsPolicy(f.policy).manualReplyMaxAgeSeconds, 3600)

  assert.throws(() => validatePolicy({
    ...f.policy,
    wechatNotifications: { ...f.policy.wechatNotifications, enabled: false, manualReplyEnabled: true },
  }), /manualReplyEnabled requires/)
  assert.throws(() => validatePolicy({
    ...f.policy,
    wechatNotifications: { ...f.policy.wechatNotifications, manualReplyEnabled: true, manualReplyMaxAgeSeconds: 30 },
  }), /manualReplyMaxAgeSeconds/)

  const ev = f.writeEvidence('default-off.png')
  const aliceProof = resolveWechatRegistry(f.policy).contacts.find(c => c.id === 'alice').targetProof
  f.writeEvent({
    schema_version: 1,
    event_id: 'evt-default-off-01',
    account_sha256: f.accountSha,
    trigger_type: 'friend_message',
    conversation_kind: 'friend',
    conversation_ref: 'alice',
    binding_sha256: aliceProof,
    sender_name: 'Alice',
    source_ref: 'msg-off-1',
    source_time: '12:00',
    observed_at: f.now(),
    text: '默认关闭测试',
    evidence_path: ev.relPath,
    evidence_sha256: ev.sha256,
  })
  f.time(f.now() + 25_000)
  const [batch] = f.projector.projectOnce().sealed
  assert.throws(
    () => resolveActionableInboundNotice(f.policy, batch.noticeId, f.now()),
    /WeChat notice reply is disabled/,
  )
})

test('wechat_reply_notice happy paths (friend + group), wechat_status flag, two-turn confirm, duplicate protection, and receipt handoff', async t => {
  const f = setupFixture(t, {
    wechatNotifications: {
      enabled: true,
      manualReplyEnabled: true,
      manualReplyMaxAgeSeconds: 1800,
    },
  })
  assert.equal(wechatNotificationsPolicy(f.policy).manualReplyEnabled, true)

  const setIngressPrompt = ({ requestId, prompt, operatorId = 'owner', targetTenantId = 'octopus', revokedAt = null, omitDelivery = false }) => {
    const db = new DatabaseSync(f.ingressDbPath)
    db.prepare(
      'INSERT INTO u_im_ingress_operators(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
    ).run(
      f.policy.bindingKey,
      JSON.stringify({
        bindingKey: f.policy.bindingKey,
        operatorId,
        targetTenantId,
        provider: 'feishu',
        appId: 'app',
        tenantId: 'org',
        chatId: 'chat',
        senderId: operatorId,
        authorizedAt: new Date(1_760_000_000_000).toISOString(),
        revokedAt,
      }),
    )
    db.prepare('DELETE FROM u_im_ingress_deliveries').run()
    if (!omitDelivery) {
      const identity = JSON.parse(f.policy.bindingKey)
      db.prepare('INSERT INTO u_im_ingress_deliveries(key, value) VALUES(?, ?)').run(
        JSON.stringify([...identity, requestId]),
        JSON.stringify({
          deliveryKey: JSON.stringify([...identity, requestId]),
          operatorId,
          targetTenantId,
          requestId,
          prompt,
          receivedAt: new Date(f.now()).toISOString(),
          status: 'in_flight',
        }),
      )
    }
    db.close()
  }

  const friendEv = f.writeEvidence('reply-friend.png')
  const groupEv = f.writeEvidence('reply-group.png')
  const registry = resolveWechatRegistry(f.policy)
  const aliceProof = registry.contacts.find(c => c.id === 'alice').targetProof
  const opsGroupProof = registry.contacts.find(c => c.id === 'ops_group').targetProof
  f.writeEvent({
    schema_version: 1,
    event_id: 'evt-reply-alice-1',
    account_sha256: f.accountSha,
    trigger_type: 'friend_message',
    conversation_kind: 'friend',
    conversation_ref: 'alice',
    binding_sha256: aliceProof,
    sender_name: 'Alice',
    source_ref: 'msg-alice-1',
    source_time: '13:01',
    observed_at: f.now(),
    text: '下午茶订好了吗？',
    evidence_path: friendEv.relPath,
    evidence_sha256: friendEv.sha256,
  })
  f.writeEvent({
    schema_version: 1,
    event_id: 'evt-reply-group-1',
    account_sha256: f.accountSha,
    trigger_type: 'group_message',
    conversation_kind: 'group',
    conversation_ref: 'ops_group',
    binding_sha256: opsGroupProof,
    sender_name: '值班同事',
    source_ref: 'msg-grp-1',
    source_time: '13:02',
    observed_at: f.now() + 1000,
    text: '发布窗口已打开',
    evidence_path: groupEv.relPath,
    evidence_sha256: groupEv.sha256,
  })
  f.time(f.now() + 25_000)
  const projected = f.projector.projectOnce()
  assert.equal(projected.sealed.length, 2)
  const friendNoticeId = projected.sealed.find(b => b.conversationKey === 'friend:alice').noticeId
  const groupNoticeId = projected.sealed.find(b => b.conversationKey === 'group:ops_group').noticeId

  const ops = new Operations(f.policy, f.policyPath, perform, () => f.now())
  t.after(() => ops.close())

  // wechat_status surfaces manualReplyEnabled truthfully without leaking raw body/secrets.
  mkdirSync(join(f.wechatRoot, 'receipts'), { recursive: true, mode: 0o750 })
  writeFileSync(
    join(f.wechatRoot, 'receipts', 'worker.json'),
    JSON.stringify({ checked_at: new Date(f.now()).toISOString(), state: 'ready' }),
    { mode: 0o640 },
  )
  setIngressPrompt({ requestId: 'req-status', prompt: '查看微信状态' })
  const statusRes = await dispatch({ action: 'wechat_status', params: {} }, ops)
  assert.equal(statusRes.manualReplyEnabled, true)

  // 1. Registered friend reply (no space between 回复 and wn_ also supported) -> same-turn direct: true.
  const friendReplyText = '已经订好了，三点见'
  setIngressPrompt({ requestId: 'req-reply-friend', prompt: `回复${friendNoticeId}：${friendReplyText}` })
  const friendPrep = ops.prepare({
    kind: 'wechat_reply_notice',
    payload: { noticeId: friendNoticeId, text: friendReplyText },
  })
  assert.equal(friendPrep.direct, true)
  assert.equal(friendPrep.contactId, 'alice')
  assert.equal(friendPrep.contactType, 'friend')
  assert.equal(friendPrep.noticeId, friendNoticeId)
  // Duplicate prepare in the same request returns exact frozen id without creating a second op.
  const friendPrepDup = ops.prepare({
    kind: 'wechat_reply_notice',
    payload: { noticeId: friendNoticeId, text: friendReplyText },
  })
  assert.equal(friendPrepDup.id, friendPrep.id)
  const friendQueued = await dispatch({ action: 'execute', params: { id: friendPrep.id } }, ops)
  assert.equal(friendQueued.state, 'queued')
  const friendOutbox = JSON.parse(readFileSync(join(f.wechatRoot, 'adhoc', 'outbox', `${friendPrep.id}.json`), 'utf8'))
  assert.equal(friendOutbox.channel, undefined)
  assert.equal(friendOutbox.contact_type, 'friend')
  assert.equal(friendOutbox.origin_notice_id, friendNoticeId)
  assert.equal(friendOutbox.contact_id, 'alice')
  assert.equal(friendOutbox.text, friendReplyText)
  assert.equal(friendOutbox.confirmation_request_id, 'req-reply-friend')

  // Same request cannot enqueue a second distinct direct WeChat send or reply!
  const secondInSameTurn = ops.prepare({
    kind: 'wechat_reply_notice',
    payload: { noticeId: groupNoticeId, text: '同回合第二个回复' },
  })
  assert.equal(secondInSameTurn.direct, false)
  await assert.rejects(() => ops.run(secondInSameTurn.id), /需要本人在飞书回复：确认执行/)
  // Even if the prompt in the same request matched a second direct send or reply, run MUST block priorClaim.
  setIngressPrompt({ requestId: 'req-reply-friend', prompt: `回复 ${groupNoticeId}：同回合偷发群回复` })
  const forgedSecondDirect = ops.prepare({
    kind: 'wechat_reply_notice',
    payload: { noticeId: groupNoticeId, text: '同回合偷发群回复' },
  })
  assert.equal(forgedSecondDirect.direct, true)
  await assert.rejects(() => ops.run(forgedSecondDirect.id), /Only one direct WeChat operation is authorized/)
  setIngressPrompt({ requestId: 'req-reply-friend', prompt: '给Alice别名发微信：同一回合偷发第二条' })
  const secondDirectSend = ops.prepare({
    kind: 'wechat_send',
    payload: { recipient: 'Alice别名', text: '同一回合偷发第二条' },
  })
  assert.equal(secondDirectSend.direct, true)
  await assert.rejects(() => ops.run(secondDirectSend.id), /Only one direct WeChat operation is authorized/)

  // 2. Registered group reply using spaced English command -> same-turn direct: true + terminal receipt projection!
  const groupReplyText = '收到，立即开始灰度发布'
  setIngressPrompt({ requestId: 'req-reply-group', prompt: `Please reply to notice ${groupNoticeId}: ${groupReplyText}` })
  const groupPrep = ops.prepare({
    kind: 'wechat_reply_notice',
    payload: { noticeId: groupNoticeId, text: groupReplyText },
  })
  assert.equal(groupPrep.direct, true)
  assert.equal(groupPrep.contactId, 'ops_group')
  assert.equal(groupPrep.contactType, 'group')
  const groupQueued = await dispatch({ action: 'execute', params: { id: groupPrep.id } }, ops)
  assert.equal(groupQueued.state, 'queued')
  // Idempotent run re-check returns queued without touching outbox again.
  const groupQueuedAgain = await ops.run(groupPrep.id)
  assert.equal(groupQueuedAgain.state, 'queued')
  const groupOutboxPath = join(f.wechatRoot, 'adhoc', 'outbox', `${groupPrep.id}.json`)
  const groupOutbox = JSON.parse(readFileSync(groupOutboxPath, 'utf8'))
  assert.equal(groupOutbox.channel, 'group')
  assert.equal(groupOutbox.origin_notice_id, groupNoticeId)
  assert.equal(groupOutbox.contact_id, 'ops_group')

  // Simulate terminal worker receipts for both replies -> Feishu handoff notices emitted via existing projector.
  const friendReceiptPath = join(f.wechatRoot, 'adhoc', 'receipts', `${friendPrep.id}.json`)
  const groupReceiptPath = join(f.wechatRoot, 'adhoc', 'receipts', `${groupPrep.id}.json`)
  writeFileSync(friendReceiptPath, JSON.stringify({ state: 'sent_verified', recipient_alias: 'Alice别名', checked_at: f.now() / 1000 }), { mode: 0o600 })
  chmodSync(friendReceiptPath, 0o600)
  writeFileSync(groupReceiptPath, JSON.stringify({ state: 'sent_verified', group_alias: '运维群', checked_at: f.now() / 1000 }), { mode: 0o600 })
  chmodSync(groupReceiptPath, 0o600)
  const receiptProj = f.projector.projectOnce()
  assert.equal(receiptProj.receipts.projected, 2)
  assert.equal(receiptProj.receipts.rejected.length, 0)
  const handoffs = f.readHandoffs()
  assert.ok(handoffs.some(h => h.data.kind === 'outbound_receipt' && h.data.sourceRef === friendPrep.id))
  assert.ok(handoffs.some(h => h.data.kind === 'outbound_receipt' && h.data.sourceRef === groupPrep.id))

  // Tampered origin_notice_id in outbox causes receipt projection to fail closed!
  const receiptTamper = await f.createFrozenJob({
    kind: 'wechat_send',
    payload: { recipient: 'Alice别名', text: 'dummy' },
    receiptData: { state: 'sent_verified', recipient_alias: 'Alice别名', checked_at: f.now() / 1000 },
  })
  void receiptTamper
  writeFileSync(groupOutboxPath, JSON.stringify({ ...groupOutbox, origin_notice_id: friendNoticeId }), { mode: 0o640 })
  chmodSync(groupOutboxPath, 0o640)
  const badReceiptPath = join(f.wechatRoot, 'adhoc', 'receipts', `${groupPrep.id}.json`)
  writeFileSync(badReceiptPath, JSON.stringify({ state: 'failed_before_submit', reason: 'tamper_check', checked_at: f.now() / 1000 }), { mode: 0o600 })
  chmodSync(badReceiptPath, 0o600)
  const tamperedProj = f.projector.projectOnce()
  assert.equal(tamperedProj.receipts.rejected.length, 1)
  assert.match(tamperedProj.receipts.rejected[0].reason, /Outbox notice origin mismatch/)

  // 3. Two-turn frozen preview confirmation succeeds for non-direct prompts when owner sends 确认执行 <id>.
  setIngressPrompt({ requestId: 'req-two-turn-1', prompt: `帮我拟一条给 ${friendNoticeId} 的回复` })
  const twoTurnPrep = ops.prepare({
    kind: 'wechat_reply_notice',
    payload: { noticeId: friendNoticeId, text: '两步确认后的正式回复' },
  })
  assert.equal(twoTurnPrep.direct, false)
  await assert.rejects(() => ops.run(twoTurnPrep.id), /需要本人在飞书回复：确认执行/)
  f.time(f.now() + 1000)
  setIngressPrompt({ requestId: 'req-two-turn-2', prompt: `确认执行 ${twoTurnPrep.id}` })
  const twoTurnDone = await ops.run(twoTurnPrep.id)
  assert.equal(twoTurnDone.state, 'queued')
})

test('wechat_reply_notice rejects invalid authority, prompt modifiers, mismatches, size caps, invalid/legacy/ambiguous/expired/rebound origins', async t => {
  const f = setupFixture(t, {
    wechatNotifications: {
      enabled: true,
      manualReplyEnabled: true,
      manualReplyMaxAgeSeconds: 120,
    },
  })

  const setDelivery = ({ requestId, prompt, operatorId = 'owner', targetTenantId = 'octopus', revokedAt = null, status = 'in_flight', emptyDeliveries = false }) => {
    const db = new DatabaseSync(f.ingressDbPath)
    db.prepare(
      'INSERT INTO u_im_ingress_operators(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
    ).run(
      f.policy.bindingKey,
      JSON.stringify({
        bindingKey: f.policy.bindingKey,
        operatorId,
        targetTenantId,
        provider: 'feishu',
        appId: 'app',
        tenantId: 'org',
        chatId: 'chat',
        senderId: operatorId,
        authorizedAt: new Date(1_760_000_000_000).toISOString(),
        revokedAt,
      }),
    )
    db.prepare('DELETE FROM u_im_ingress_deliveries').run()
    if (!emptyDeliveries) {
      const identity = JSON.parse(f.policy.bindingKey)
      db.prepare('INSERT INTO u_im_ingress_deliveries(key, value) VALUES(?, ?)').run(
        JSON.stringify([...identity, requestId]),
        JSON.stringify({
          deliveryKey: JSON.stringify([...identity, requestId]),
          operatorId,
          targetTenantId,
          requestId,
          prompt,
          receivedAt: new Date(f.now()).toISOString(),
          status,
        }),
      )
    }
    db.close()
  }

  const evAlice = f.writeEvidence('neg-alice.png')
  const evAliceMix = f.writeEvidence('neg-mix.png')
  const evMoments = f.writeEvidence('neg-mom.png')
  const negRegistry = resolveWechatRegistry(f.policy)
  const aliceNegProof = negRegistry.contacts.find(c => c.id === 'alice').targetProof
  const opsGroupNegProof = negRegistry.contacts.find(c => c.id === 'ops_group').targetProof

  // Clean friend event -> mixed into needs_review batch
  f.writeEvent({
    schema_version: 1,
    event_id: 'evt-neg-clean',
    account_sha256: f.accountSha,
    trigger_type: 'friend_message',
    conversation_kind: 'friend',
    conversation_ref: 'alice',
    binding_sha256: aliceNegProof,
    sender_name: 'Alice',
    source_ref: 'msg-clean-1',
    source_time: '14:00',
    observed_at: f.now(),
    text: '可以回复我吗？',
    evidence_path: evAlice.relPath,
    evidence_sha256: evAlice.sha256,
  })
  // Needs-review event -> review-only notice
  f.writeEvent({
    schema_version: 1,
    event_id: 'evt-neg-review',
    account_sha256: f.accountSha,
    trigger_type: 'friend_message',
    conversation_kind: 'friend',
    conversation_ref: 'alice',
    binding_sha256: aliceNegProof,
    sender_name: 'Alice',
    source_ref: 'msg-rev-1',
    source_time: '14:00',
    observed_at: f.now() + 100,
    needs_review: true,
    reason: 'ocr_ambiguous',
    text: '有疑义消息',
    evidence_path: evAliceMix.relPath,
    evidence_sha256: evAliceMix.sha256,
  })
  // Note: because needs_review immediately triggers sealing of evt-neg-clean+evt-neg-review into ONE mixed batch!
  // Let's also ingest moments_interaction and needs_human separately after advancing clock!
  f.time(f.now() + 5000)
  const mixedRes = f.projector.projectOnce()
  assert.equal(mixedRes.sealed.length, 1)
  const mixedBatchNoticeId = mixedRes.sealed[0].noticeId

  // Now create a PURE clean friend notice B (since previous batch sealed, new batch opens cleanly).
  f.writeEvent({
    schema_version: 1,
    event_id: 'evt-neg-clean-only',
    account_sha256: f.accountSha,
    trigger_type: 'friend_message',
    conversation_kind: 'friend',
    conversation_ref: 'alice',
    binding_sha256: aliceNegProof,
    sender_name: 'Alice',
    source_ref: 'msg-clean-2',
    source_time: '14:01',
    observed_at: f.now(),
    text: '纯净消息',
    evidence_path: evAlice.relPath,
    evidence_sha256: evAlice.sha256,
  })
  // Open batch not yet sealed: peek its notice_id directly from sqlite before advancing 25s
  f.projector.projectOnce()
  const openBatchNoticeId = f.projector.db.prepare(
    "SELECT notice_id FROM inbound_batches WHERE status='open'",
  ).get().notice_id

  // Seal it cleanly after window
  f.time(f.now() + 25_000)
  f.projector.projectOnce()
  const cleanNoticeId = openBatchNoticeId

  // needs_human event (different contact group ops_group)
  f.writeEvent({
    schema_version: 1,
    event_id: 'evt-neg-human',
    account_sha256: f.accountSha,
    trigger_type: 'needs_human',
    conversation_kind: 'group',
    conversation_ref: 'ops_group',
    sender_name: 'Unknown',
    source_ref: 'msg-nh-1',
    source_time: '14:02',
    observed_at: f.now(),
    text: '需人工处理',
    evidence_path: null,
    evidence_sha256: null,
  })
  // moments interaction event
  f.writeEvent({
    schema_version: 1,
    event_id: 'evt-neg-moments',
    account_sha256: f.accountSha,
    trigger_type: 'moments_interaction',
    conversation_kind: 'moments',
    conversation_ref: 'self',
    sender_name: 'Bob',
    source_ref: 'msg-mom-1',
    source_time: '14:02',
    observed_at: f.now() + 50,
    text: '点赞了你的朋友圈',
    evidence_path: evMoments.relPath,
    evidence_sha256: evMoments.sha256,
  })
  const specialRes = f.projector.projectOnce()
  const needsHumanNoticeId = specialRes.sealed.find(b => b.conversationKey === 'group:ops_group').noticeId
  f.time(f.now() + 25_000)
  const momRes = f.projector.projectOnce()
  const momentsNoticeId = momRes.sealed.find(b => b.conversationKey === 'moments:self').noticeId

  // Also create an outbound_receipt notice_id via frozen job
  await f.createFrozenJob({
    kind: 'wechat_send',
    payload: { recipient: 'Alice别名', text: '回执测试' },
    receiptData: { state: 'sent_verified', recipient_alias: 'Alice别名', checked_at: f.now() / 1000 },
  })
  const receiptProj = f.projector.projectOnce()
  assert.equal(receiptProj.receipts.projected, 1)
  const receiptNoticeId = f.projector.db.prepare(
    "SELECT notice_id FROM projected_notices WHERE kind='outbound_receipt'",
  ).get().notice_id

  const ops = new Operations(f.policy, f.policyPath, perform, () => f.now())
  t.after(() => ops.close())

  // Open batch cannot be resolved before sealing (simulate creating an open batch on ops_group)
  f.writeEvent({
    schema_version: 1,
    event_id: 'evt-neg-open',
    account_sha256: f.accountSha,
    trigger_type: 'group_message',
    conversation_kind: 'group',
    conversation_ref: 'ops_group',
    binding_sha256: opsGroupNegProof,
    sender_name: 'Colleague',
    source_ref: 'msg-open-1',
    source_time: '14:03',
    observed_at: f.now(),
    text: '尚未封口消息',
    evidence_path: evAlice.relPath,
    evidence_sha256: evAlice.sha256,
  })
  f.projector.projectOnce()
  const unsealedId = f.projector.db.prepare(
    "SELECT notice_id FROM inbound_batches WHERE status='open' AND conversation_key='group:ops_group'",
  ).get().notice_id
  setDelivery({ requestId: 'req-open', prompt: `回复 ${unsealedId}：不行` })
  assert.throws(() => ops.prepare({ kind: 'wechat_reply_notice', payload: { noticeId: unsealedId, text: '不行' } }), /Unknown WeChat notice id/)

  // Invalid / unknown / free-form override / origin-type refusals
  setDelivery({ requestId: 'req-bad-args', prompt: `回复 ${cleanNoticeId}：正常正文` })
  assert.throws(
    () => ops.prepare({ kind: 'wechat_reply_notice', payload: { noticeId: cleanNoticeId, text: '正常正文', recipient: 'ops_group' } }),
    /Unknown field/,
  )
  assert.throws(
    () => ops.prepare({ kind: 'wechat_reply_notice', payload: { noticeId: 'bad-id', text: '正常正文' } }),
    /Invalid WeChat notice identifier/,
  )
  assert.throws(
    () => ops.prepare({ kind: 'wechat_reply_notice', payload: { noticeId: 'wn_000000000000', text: '正常正文' } }),
    /Unknown WeChat notice id/,
  )
  assert.throws(
    () => ops.prepare({ kind: 'wechat_reply_notice', payload: { noticeId: receiptNoticeId, text: '正常正文' } }),
    /Cannot reply to an outbound receipt notice/,
  )
  assert.throws(
    () => ops.prepare({ kind: 'wechat_reply_notice', payload: { noticeId: mixedBatchNoticeId, text: '正常正文' } }),
    /review-only or lacks verified intake/,
  )
  assert.throws(
    () => ops.prepare({ kind: 'wechat_reply_notice', payload: { noticeId: needsHumanNoticeId, text: '正常正文' } }),
    /review-only or lacks verified intake/,
  )
  assert.throws(
    () => ops.prepare({ kind: 'wechat_reply_notice', payload: { noticeId: momentsNoticeId, text: '正常正文' } }),
    /review-only or lacks verified intake|does not support direct reply/,
  )
  assert.throws(
    () => ops.prepare({ kind: 'wechat_reply_notice', payload: { noticeId: cleanNoticeId, text: 'A'.repeat(1001) } }),
    /Invalid or oversized message/,
  )

  // Actor / operator authority failures
  setDelivery({ requestId: 'req-no-deliv', prompt: 'none', emptyDeliveries: true })
  assert.throws(() => ops.prepare({ kind: 'wechat_reply_notice', payload: { noticeId: cleanNoticeId, text: 'ok' } }), /Exactly one current authenticated human delivery is required/)
  setDelivery({ requestId: 'req-revoked', prompt: `回复 ${cleanNoticeId}：ok`, revokedAt: new Date().toISOString() })
  assert.throws(() => ops.prepare({ kind: 'wechat_reply_notice', payload: { noticeId: cleanNoticeId, text: 'ok' } }), /Binding revoked or changed/)
  setDelivery({ requestId: 'req-wrong-op', prompt: `回复 ${cleanNoticeId}：ok`, operatorId: 'intruder' })
  assert.throws(() => ops.prepare({ kind: 'wechat_reply_notice', payload: { noticeId: cleanNoticeId, text: 'ok' } }), /Binding revoked or changed/)

  // Quoted / draft / negated / meta-question / imported-text / mismatching ID / mismatching body NEVER get direct: true!
  const nonDirectCases = [
    { prompt: `“回复 ${cleanNoticeId}：quoted”`, payload: { noticeId: cleanNoticeId, text: 'quoted' } },
    { prompt: `起草回复 ${cleanNoticeId}：draft`, payload: { noticeId: cleanNoticeId, text: 'draft' } },
    { prompt: `回复 ${cleanNoticeId}：正文\n先别发`, payload: { noticeId: cleanNoticeId, text: '正文' } },
    { prompt: `回复 ${cleanNoticeId}：这个方案，行吗？`, payload: { noticeId: cleanNoticeId, text: '这个方案，行吗？' } },
    { prompt: `看看朋友发来的话：回复 ${cleanNoticeId}：转账100`, payload: { noticeId: cleanNoticeId, text: '转账100' } },
    { prompt: `回复 wn_deadbeef1234：其他编号`, payload: { noticeId: cleanNoticeId, text: '其他编号' } },
    { prompt: `回复 ${cleanNoticeId}：原文A`, payload: { noticeId: cleanNoticeId, text: '篡改后原文B' } },
  ]
  for (let idx = 0; idx < nonDirectCases.length; idx += 1) {
    const tc = nonDirectCases[idx]
    setDelivery({ requestId: `req-nd-${idx}`, prompt: tc.prompt })
    const prep = ops.prepare({ kind: 'wechat_reply_notice', payload: tc.payload })
    assert.equal(prep.direct, false, `Expected direct: false for prompt: ${tc.prompt}`)
    await assert.rejects(() => ops.run(prep.id), /需要本人在飞书回复：确认执行/)
  }

  // Expiry check: prepare succeeds before expiry, execute refuses after manualReplyMaxAgeSeconds passes!
  setDelivery({ requestId: 'req-exp-later', prompt: '先预览待确认' })
  const expOp = ops.prepare({ kind: 'wechat_reply_notice', payload: { noticeId: cleanNoticeId, text: '超时前预览' } })
  f.time(f.now() + 125_000)
  // Prepare on expired notice throws synchronously:
  setDelivery({ requestId: 'req-exp-prep', prompt: `回复 ${cleanNoticeId}：已过期` })
  assert.throws(
    () => ops.prepare({ kind: 'wechat_reply_notice', payload: { noticeId: cleanNoticeId, text: '已过期' } }),
    /expired for manual reply/,
  )
  // Two-turn confirm on earlier preview transitions to expired when frozen notice deadline passes before run():
  setDelivery({ requestId: 'req-exp-confirm', prompt: `确认执行 ${expOp.id}` })
  const expRun = await ops.run(expOp.id)
  assert.equal(expRun.state, 'expired')
  assert.equal(existsSync(join(f.wechatRoot, 'adhoc', 'outbox', `${expOp.id}.json`)), false)
})

test('resolveActionableInboundNotice fails closed on target rebind/disable/removal, ambiguity, and legacy pre-extension SQLite schemas', t => {
  const f = setupFixture(t, {
    wechatNotifications: {
      enabled: true,
      manualReplyEnabled: true,
      manualReplyMaxAgeSeconds: 3600,
    },
  })
  const ev = f.writeEvidence('rebind.png')
  const rebindRegistry = resolveWechatRegistry(f.policy)
  const aliceRebindProof = rebindRegistry.contacts.find(c => c.id === 'alice').targetProof
  f.writeEvent({
    schema_version: 1,
    event_id: 'evt-rebind-1',
    account_sha256: f.accountSha,
    trigger_type: 'friend_message',
    conversation_kind: 'friend',
    conversation_ref: 'alice',
    binding_sha256: aliceRebindProof,
    sender_name: 'Alice',
    source_ref: 'msg-rb-1',
    source_time: '15:00',
    observed_at: f.now(),
    text: '绑定检查',
    evidence_path: ev.relPath,
    evidence_sha256: ev.sha256,
  })
  f.time(f.now() + 25_000)
  const [sealed] = f.projector.projectOnce().sealed
  const resolved = resolveActionableInboundNotice(f.policy, sealed.noticeId, f.now())
  assert.equal(resolved.contact.id, 'alice')

  // 1. Disabling or changing anchors/recipient_id in sidecar fails resolution fail-closed.
  const sidecarPath = f.policy.wechatContactsConfig
  const origSidecar = readFileSync(sidecarPath, 'utf8')
  const parsedSidecar = JSON.parse(origSidecar)
  writeFileSync(sidecarPath, JSON.stringify({
    ...parsedSidecar,
    contacts: parsedSidecar.contacts.map(c => (c.id === 'alice' ? { ...c, enabled: false } : c)),
  }), { mode: 0o640 })
  assert.throws(() => resolveActionableInboundNotice(f.policy, sealed.noticeId, f.now()), /target is disabled/)

  writeFileSync(sidecarPath, JSON.stringify({
    ...parsedSidecar,
    contacts: parsedSidecar.contacts.map(c => (c.id === 'alice' ? { ...c, recipient_id: 'wxid_evil_rebound' } : c)),
  }), { mode: 0o640 })
  assert.throws(() => resolveActionableInboundNotice(f.policy, sealed.noticeId, f.now()), /target identity or anchors changed/)

  writeFileSync(sidecarPath, JSON.stringify({
    ...parsedSidecar,
    contacts: parsedSidecar.contacts.filter(c => c.id !== 'alice'),
  }), { mode: 0o640 })
  assert.throws(() => resolveActionableInboundNotice(f.policy, sealed.noticeId, f.now()), /binding is invalid or unbound/)
  const strippedPolicy = {
    ...f.policy,
    wechatContacts: f.policy.wechatContacts.filter(c => c.id !== 'alice'),
  }
  assert.throws(() => resolveActionableInboundNotice(strippedPolicy, sealed.noticeId, f.now()), /no longer registered/)

  writeFileSync(sidecarPath, origSidecar, { mode: 0o640 })

  // 2. Account ID change fails closed.
  const wechatCfgPath = f.policy.wechatConfig
  const origWechatCfg = readFileSync(wechatCfgPath, 'utf8')
  writeFileSync(wechatCfgPath, JSON.stringify({ ...JSON.parse(origWechatCfg), account_id: 'acct-other' }), { mode: 0o640 })
  assert.throws(() => resolveActionableInboundNotice(f.policy, sealed.noticeId, f.now()), /account binding changed/)
  writeFileSync(wechatCfgPath, origWechatCfg, { mode: 0o640 })

  // 3. Synthetic un-indexed ledger with duplicate notice_id fails closed as ambiguous.
  const ambigDir = realpathSync(mkdtempSync(join(tmpdir(), 'octopus-ambig-db-')))
  t.after(() => rmSync(ambigDir, { recursive: true, force: true }))
  chmodSync(ambigDir, 0o700)
  const ambigDb = new DatabaseSync(join(ambigDir, 'wechat-inbox.sqlite'))
  ambigDb.exec(`
    CREATE TABLE inbound_batches (
      batch_id TEXT PRIMARY KEY, conversation_key TEXT NOT NULL, conversation_kind TEXT NOT NULL,
      conversation_name TEXT NOT NULL, notice_id TEXT NOT NULL, status TEXT NOT NULL,
      opened_at INTEGER NOT NULL, last_event_at INTEGER NOT NULL, flush_after INTEGER NOT NULL,
      max_flush_at INTEGER NOT NULL, event_count INTEGER NOT NULL DEFAULT 0, sealed_at INTEGER,
      account_sha256 TEXT, contact_id TEXT, target_proof TEXT, reply_eligible INTEGER NOT NULL DEFAULT 0
    );
    CREATE TABLE projected_notices (
      notice_key TEXT PRIMARY KEY, notice_id TEXT NOT NULL, kind TEXT NOT NULL,
      conversation_key TEXT, conversation_kind TEXT, source_ref TEXT NOT NULL,
      handoff_file TEXT NOT NULL, body TEXT NOT NULL, created_at INTEGER NOT NULL,
      account_sha256 TEXT, contact_id TEXT, target_proof TEXT,
      reply_eligible INTEGER NOT NULL DEFAULT 0, source_observed_at INTEGER
    );
  `)
  for (const suffix of ['a', 'b']) {
    ambigDb.prepare(`
      INSERT INTO projected_notices(
        notice_key, notice_id, kind, conversation_key, conversation_kind, source_ref, handoff_file, body, created_at,
        account_sha256, contact_id, target_proof, reply_eligible, source_observed_at
      ) VALUES(?, ?, 'inbound_alert', 'friend:alice', 'friend', ?, 'dup.json', 'dup', ?, ?, 'alice', ?, 1, ?)
    `).run(`inbound:dup-${suffix}`, sealed.noticeId, `dup-${suffix}`, f.now(), f.accountSha, resolved.contact.targetProof, f.now())
  }
  ambigDb.close()
  chmodSync(join(ambigDir, 'wechat-inbox.sqlite'), 0o600)
  assert.throws(
    () => resolveActionableInboundNotice({ ...f.policy, state: ambigDir }, sealed.noticeId, f.now()),
    /Ambiguous WeChat notice id/,
  )

  // 4. Pre-extension SQLite database opened via openPrivateProjectionDatabase keeps old rows reply_eligible = 0, AND un-migrated read-only table fails closed.
  const legacyDir = realpathSync(mkdtempSync(join(tmpdir(), 'octopus-legacy-db-')))
  t.after(() => rmSync(legacyDir, { recursive: true, force: true }))
  chmodSync(legacyDir, 0o700)
  const legacyDbPath = join(legacyDir, 'wechat-inbox.sqlite')
  const legacyDb = new DatabaseSync(legacyDbPath)
  legacyDb.exec(`
    CREATE TABLE inbound_batches (
      batch_id TEXT PRIMARY KEY,
      conversation_key TEXT NOT NULL,
      conversation_kind TEXT NOT NULL,
      conversation_name TEXT NOT NULL,
      notice_id TEXT NOT NULL,
      status TEXT NOT NULL,
      opened_at INTEGER NOT NULL,
      last_event_at INTEGER NOT NULL,
      flush_after INTEGER NOT NULL,
      max_flush_at INTEGER NOT NULL,
      event_count INTEGER NOT NULL DEFAULT 0,
      sealed_at INTEGER
    );
    CREATE TABLE projected_notices (
      notice_key TEXT PRIMARY KEY,
      notice_id TEXT NOT NULL,
      kind TEXT NOT NULL,
      conversation_key TEXT,
      source_ref TEXT NOT NULL,
      handoff_file TEXT NOT NULL,
      body TEXT NOT NULL,
      created_at INTEGER NOT NULL
    );
  `)
  const legacyNoticeId = formatNoticeId('inbound:legacy-b1')
  legacyDb.prepare(
    "INSERT INTO inbound_batches(batch_id, conversation_key, conversation_kind, conversation_name, notice_id, status, opened_at, last_event_at, flush_after, max_flush_at, event_count, sealed_at) VALUES('legacy-b1','friend:alice','friend','Alice',?,'sealed',1,1,1,1,1,1)",
  ).run(legacyNoticeId)
  legacyDb.prepare(
    "INSERT INTO projected_notices(notice_key, notice_id, kind, conversation_key, source_ref, handoff_file, body, created_at) VALUES('inbound:legacy-b1',?,'inbound_alert','friend:alice','legacy-b1','legacy.json','old',1)",
  ).run(legacyNoticeId)
  legacyDb.close()
  chmodSync(legacyDbPath, 0o600)

  const legacyPolicy = {
    ...f.policy,
    state: legacyDir,
    wechatNotifications: { ...f.policy.wechatNotifications },
  }
  assert.throws(
    () => resolveActionableInboundNotice(legacyPolicy, legacyNoticeId, 10),
    /Notice ledger lacks verified intake binding columns/,
  )
  // Opening through WechatExchangeProjector migrates schema backward-compatibly, yet legacy rows stay non-replyable!
  const migratedProj = new WechatExchangeProjector(legacyPolicy, { clock: () => f.now() })
  migratedProj.close()
  assert.throws(
    () => resolveActionableInboundNotice(legacyPolicy, legacyNoticeId, 10),
    /review-only or lacks verified intake origin/,
  )
})

test('explicit observation-time binding_sha256 is required for actionable friend/group events and resists pre-ingestion rebind and legacy inferred rows', async t => {
  const f = setupFixture(t, {
    wechatNotifications: {
      enabled: true,
      manualReplyEnabled: true,
      manualReplyMaxAgeSeconds: 3600,
    },
  })
  const initRegistry = resolveWechatRegistry(f.policy)
  const aliceProof = initRegistry.contacts.find(c => c.id === 'alice').targetProof
  const opsGroupProof = initRegistry.contacts.find(c => c.id === 'ops_group').targetProof

  // 1. Friend & group events missing binding_sha256 OR with explicit binding_sha256: null still project strictly review-only handoff alerts.
  const evNoProof = f.writeEvidence('no-proof.png')
  const evNullProof = f.writeEvidence('null-proof.png')
  f.writeEvent({
    schema_version: 1,
    event_id: 'evt-missing-proof-1',
    account_sha256: f.accountSha,
    trigger_type: 'friend_message',
    conversation_kind: 'friend',
    conversation_ref: 'alice',
    sender_name: 'Alice',
    source_ref: 'msg-mp-1',
    source_time: '16:00',
    observed_at: f.now(),
    text: '没有观测绑定哈希的消息',
    evidence_path: evNoProof.relPath,
    evidence_sha256: evNoProof.sha256,
  })
  f.writeEvent({
    schema_version: 1,
    event_id: 'evt-null-proof-1',
    account_sha256: f.accountSha,
    trigger_type: 'group_message',
    conversation_kind: 'group',
    conversation_ref: 'ops_group',
    binding_sha256: null,
    sender_name: 'OpsUser',
    source_ref: 'msg-np-1',
    source_time: '16:00',
    observed_at: f.now() + 100,
    text: '显式空绑定哈希的群消息',
    evidence_path: evNullProof.relPath,
    evidence_sha256: evNullProof.sha256,
  })
  f.time(f.now() + 25_000)
  const reviewOnlyRes = f.projector.projectOnce()
  assert.equal(reviewOnlyRes.inbound.accepted, 2)
  assert.equal(reviewOnlyRes.sealed.length, 2)
  assert.equal(f.readHandoffs().length, 2)
  for (const sealed of reviewOnlyRes.sealed) {
    assert.throws(
      () => resolveActionableInboundNotice(f.policy, sealed.noticeId, f.now()),
      /review-only or lacks verified intake origin/,
    )
  }

  // 2. Mixed batch combining one valid proof event + one missing-proof event stays strictly non-actionable.
  const evValid = f.writeEvidence('mix-valid.png')
  f.writeEvent({
    schema_version: 1,
    event_id: 'evt-mix-valid-1',
    account_sha256: f.accountSha,
    trigger_type: 'friend_message',
    conversation_kind: 'friend',
    conversation_ref: 'alice',
    binding_sha256: aliceProof,
    sender_name: 'Alice',
    source_ref: 'msg-mv-1',
    source_time: '16:01',
    observed_at: f.now(),
    text: '第一条有绑定',
    evidence_path: evValid.relPath,
    evidence_sha256: evValid.sha256,
  })
  f.writeEvent({
    schema_version: 1,
    event_id: 'evt-mix-legacy-2',
    account_sha256: f.accountSha,
    trigger_type: 'friend_message',
    conversation_kind: 'friend',
    conversation_ref: 'alice',
    sender_name: 'Alice',
    source_ref: 'msg-mv-2',
    source_time: '16:01',
    observed_at: f.now() + 200,
    text: '第二条缺失绑定',
    evidence_path: evNoProof.relPath,
    evidence_sha256: evNoProof.sha256,
  })
  f.time(f.now() + 25_000)
  const mixProj = f.projector.projectOnce()
  assert.equal(mixProj.sealed.length, 1)
  assert.throws(
    () => resolveActionableInboundNotice(f.policy, mixProj.sealed[0].noticeId, f.now()),
    /review-only or lacks verified intake origin/,
  )

  // 3. Positive explicit proof for BOTH friend and group passes end-to-end.
  f.writeEvent({
    schema_version: 1,
    event_id: 'evt-valid-friend-pos',
    account_sha256: f.accountSha,
    trigger_type: 'friend_message',
    conversation_kind: 'friend',
    conversation_ref: 'alice',
    binding_sha256: aliceProof,
    sender_name: 'Alice',
    source_ref: 'msg-pos-f',
    source_time: '16:02',
    observed_at: f.now(),
    text: '有效好友消息',
    evidence_path: evValid.relPath,
    evidence_sha256: evValid.sha256,
  })
  f.writeEvent({
    schema_version: 1,
    event_id: 'evt-valid-group-pos',
    account_sha256: f.accountSha,
    trigger_type: 'group_message',
    conversation_kind: 'group',
    conversation_ref: 'ops_group',
    binding_sha256: opsGroupProof,
    sender_name: 'OpsUser',
    source_ref: 'msg-pos-g',
    source_time: '16:02',
    observed_at: f.now() + 100,
    text: '有效群消息',
    evidence_path: evValid.relPath,
    evidence_sha256: evValid.sha256,
  })
  f.time(f.now() + 25_000)
  const posProj = f.projector.projectOnce()
  const posFriendId = posProj.sealed.find(b => b.conversationKey === 'friend:alice').noticeId
  const posGroupId = posProj.sealed.find(b => b.conversationKey === 'group:ops_group').noticeId
  assert.equal(resolveActionableInboundNotice(f.policy, posFriendId, f.now()).contact.id, 'alice')
  assert.equal(resolveActionableInboundNotice(f.policy, posGroupId, f.now()).contact.id, 'ops_group')

  // 4. Persisted pre-upgrade DB rows with inferred actionability (reply_eligible=1, target_proof=<hash>, but ev.bindingSha256=null/missing) remain non-actionable after upgrade!
  const legacyInferredRow = f.projector.db.prepare('SELECT payload FROM ingested_events WHERE event_id=?').get('evt-valid-friend-pos')
  const inferredObj = JSON.parse(legacyInferredRow.payload)
  delete inferredObj.bindingSha256
  f.projector.db.prepare('UPDATE ingested_events SET payload=? WHERE event_id=?').run(JSON.stringify(inferredObj), 'evt-valid-friend-pos')
  assert.throws(
    () => resolveActionableInboundNotice(f.policy, posFriendId, f.now()),
    /non-actionable or mismatched origin event/,
  )

  // 5. Pre-ingestion rebind: event observed with old proof before alice rebinds in sidecar is rejected fail-closed at ingestion!
  const origSidecar = readFileSync(f.policy.wechatContactsConfig, 'utf8')
  const parsedSidecar = JSON.parse(origSidecar)
  writeFileSync(
    f.policy.wechatContactsConfig,
    JSON.stringify({
      ...parsedSidecar,
      contacts: parsedSidecar.contacts.map(c => (c.id === 'alice' ? { ...c, recipient_id: 'wxid_alice_rebound_preingest' } : c)),
    }),
    { mode: 0o640 },
  )
  f.writeEvent({
    schema_version: 1,
    event_id: 'evt-rebound-preingest',
    account_sha256: f.accountSha,
    trigger_type: 'friend_message',
    conversation_kind: 'friend',
    conversation_ref: 'alice',
    binding_sha256: aliceProof,
    sender_name: 'Alice',
    source_ref: 'msg-rpi-1',
    source_time: '16:03',
    observed_at: f.now(),
    text: '观测后换绑期间的消息',
    evidence_path: evValid.relPath,
    evidence_sha256: evValid.sha256,
  })
  const rebindIngest = f.projector.projectOnce()
  assert.equal(rebindIngest.inbound.accepted, 0)
  const rejectedRebind = rebindIngest.inbound.rejected.find(r => r.file === 'evt-rebound-preingest.json')
  assert.ok(rejectedRebind)
  assert.match(rejectedRebind.reason, /binding_sha256 mismatch/)
  writeFileSync(f.policy.wechatContactsConfig, origSidecar, { mode: 0o640 })
})

test('wechat_reply_notice clamps outbox deadline to notice lifetime (observed/created age), never extends frozen deadlines, and honors live paused lease', async t => {
  const f = setupFixture(t, {
    wechatNotifications: {
      enabled: true,
      manualReplyEnabled: true,
      manualReplyMaxAgeSeconds: 60,
    },
  })
  const registry = resolveWechatRegistry(f.policy)
  const aliceProof = registry.contacts.find(c => c.id === 'alice').targetProof
  const setPrompt = (requestId, prompt) => {
    const db = new DatabaseSync(f.ingressDbPath)
    db.prepare('DELETE FROM u_im_ingress_deliveries').run()
    const identity = JSON.parse(f.policy.bindingKey)
    db.prepare('INSERT INTO u_im_ingress_deliveries(key, value) VALUES(?, ?)').run(
      JSON.stringify([...identity, requestId]),
      JSON.stringify({
        deliveryKey: JSON.stringify([...identity, requestId]),
        operatorId: 'owner',
        targetTenantId: 'octopus',
        requestId,
        prompt,
        receivedAt: new Date(f.now()).toISOString(),
        status: 'in_flight',
      }),
    )
    db.close()
  }

  // Exact Blocker 2 scenario: maxAge=60s, observed 59s ago -> only 1s (1000ms) remains at T0, NOT 600s!
  const ev = f.writeEvidence('short-ttl.png')
  const obsAt = f.now()
  f.writeEvent({
    schema_version: 1,
    event_id: 'evt-short-ttl-1',
    account_sha256: f.accountSha,
    trigger_type: 'friend_message',
    conversation_kind: 'friend',
    conversation_ref: 'alice',
    binding_sha256: aliceProof,
    sender_name: 'Alice',
    source_ref: 'msg-st-1',
    source_time: '17:00',
    observed_at: obsAt,
    text: '还剩1秒过期测试',
    evidence_path: ev.relPath,
    evidence_sha256: ev.sha256,
  })
  // Advance 25s to seal at obsAt+25s, then advance to obsAt+59s (1s left out of 60s maxAge!)
  f.time(obsAt + 25_000)
  const [sealed] = f.projector.projectOnce().sealed
  f.time(obsAt + 59_000)

  const ops = new Operations(f.policy, f.policyPath, perform, () => f.now())
  t.after(() => ops.close())
  setPrompt('req-short-1', `回复 ${sealed.noticeId}：1秒极限响应`)
  const shortPrep = ops.prepare({
    kind: 'wechat_reply_notice',
    payload: { noticeId: sealed.noticeId, text: '1秒极限响应' },
  })
  assert.equal(shortPrep.expiresAt, obsAt + 60_000)
  assert.equal(shortPrep.noticeDeadline, obsAt + 60_000)

  // Raising manualReplyMaxAgeSeconds to 3600 BEFORE execute MUST NEVER extend frozen shortPrep.expiresAt!
  ops.policy = {
    ...f.policy,
    wechatNotifications: { ...f.policy.wechatNotifications, manualReplyMaxAgeSeconds: 3600 },
  }
  const shortRun = await ops.run(shortPrep.id)
  assert.equal(shortRun.state, 'queued')
  const outboxPath = join(f.wechatRoot, 'adhoc', 'outbox', `${shortPrep.id}.json`)
  const outbox = JSON.parse(readFileSync(outboxPath, 'utf8'))
  assert.equal(outbox.expires_at, obsAt + 60_000)
  assert.equal(outbox.origin_notice_id, sealed.noticeId)
  assert.equal(outbox.origin_notice_key, sealed.noticeKey)
  assert.equal(outbox.origin_notice_observed_at, obsAt)
  assert.equal(outbox.origin_notice_created_at, obsAt + 25_000)
  // verifyFrozenOperationForOutbox rejects extended outbox TTL or altered metadata!
  writeFileSync(outboxPath, JSON.stringify({ ...outbox, expires_at: obsAt + 120_000 }), { mode: 0o640 })
  chmodSync(outboxPath, 0o640)
  const spoofReceiptPath = join(f.wechatRoot, 'adhoc', 'receipts', `${shortPrep.id}.json`)
  writeFileSync(spoofReceiptPath, JSON.stringify({ state: 'sent_verified', recipient_alias: 'Alice别名', checked_at: f.now() / 1000 }), { mode: 0o600 })
  chmodSync(spoofReceiptPath, 0o600)
  const ttlTamperProj = f.projector.projectOnce()
  assert.equal(ttlTamperProj.receipts.rejected.length, 1)
  assert.match(ttlTamperProj.receipts.rejected[0].reason, /exceeds frozen notice limit/)

  // Recomputing & clamping downward on execute refuses queue admission if lowered maxAge already expired!
  const ev2 = f.writeEvidence('clamp-down.png')
  const t2 = f.now()
  f.writeEvent({
    schema_version: 1,
    event_id: 'evt-clamp-down-1',
    account_sha256: f.accountSha,
    trigger_type: 'friend_message',
    conversation_kind: 'friend',
    conversation_ref: 'alice',
    binding_sha256: aliceProof,
    sender_name: 'Alice',
    source_ref: 'msg-cd-1',
    source_time: '17:01',
    observed_at: t2,
    text: '中途调短寿命测试',
    evidence_path: ev2.relPath,
    evidence_sha256: ev2.sha256,
  })
  f.time(t2 + 25_000)
  const [sealed2] = f.projector.projectOnce().sealed
  ops.policy = {
    ...f.policy,
    wechatNotifications: { ...f.policy.wechatNotifications, manualReplyMaxAgeSeconds: 1800 },
  }
  setManualReplyControl(ops.policy, { action: 'resume', nowMs: f.now() })
  setPrompt('req-clamp-1', '先预览待确认')
  const clampOp = ops.prepare({
    kind: 'wechat_reply_notice',
    payload: { noticeId: sealed2.noticeId, text: '先预览待确认回复' },
  })
  assert.equal(clampOp.expiresAt, t2 + 25_000 + f.policy.limits.previewSeconds * 1000)
  // Advance 40s (so 65s since observation); operator lowers maxAge to 60s -> already expired at execute -> zero outbox admission!
  f.time(t2 + 65_000)
  ops.policy = {
    ...f.policy,
    wechatNotifications: { ...f.policy.wechatNotifications, manualReplyMaxAgeSeconds: 60 },
  }
  setPrompt('req-clamp-2', `确认执行 ${clampOp.id}`)
  const clampExpiredRun = await ops.run(clampOp.id)
  assert.equal(clampExpiredRun.state, 'needs_review')
  assert.match(clampExpiredRun.error, /expired/)
  assert.equal(existsSync(join(f.wechatRoot, 'adhoc', 'outbox', `${clampOp.id}.json`)), false)

  // Live pause via setManualReplyControl blocks subsequent prepare & perform cleanly!
  f.time(t2 + 70_000)
  const ev3 = f.writeEvidence('pause-perf.png')
  f.writeEvent({
    schema_version: 1,
    event_id: 'evt-pause-perf-1',
    account_sha256: f.accountSha,
    trigger_type: 'friend_message',
    conversation_kind: 'friend',
    conversation_ref: 'alice',
    binding_sha256: aliceProof,
    sender_name: 'Alice',
    source_ref: 'msg-pp-1',
    source_time: '17:02',
    observed_at: f.now(),
    text: '暂停开关测试',
    evidence_path: ev3.relPath,
    evidence_sha256: ev3.sha256,
  })
  f.time(f.now() + 25_000)
  const [sealed3] = f.projector.projectOnce().sealed
  ops.policy = {
    ...f.policy,
    wechatNotifications: { ...f.policy.wechatNotifications, manualReplyMaxAgeSeconds: 1800 },
  }
  setManualReplyControl(ops.policy, { action: 'resume', nowMs: f.now() })
  setPrompt('req-pause-1', '预览后暂停')
  const pauseOp = ops.prepare({ kind: 'wechat_reply_notice', payload: { noticeId: sealed3.noticeId, text: '暂停测试回复' } })
  setManualReplyControl(ops.policy, { action: 'pause', nowMs: f.now() })
  f.time(f.now() + 1000)
  setPrompt('req-pause-2', `确认执行 ${pauseOp.id}`)
  const pausedRun = await ops.run(pauseOp.id)
  assert.equal(pausedRun.state, 'needs_review')
  assert.match(pausedRun.error, /paused/)
  assert.equal(existsSync(join(f.wechatRoot, 'adhoc', 'outbox', `${pauseOp.id}.json`)), false)
})

test('createBrokerProjectionLoop, startBroker, and perform preserve pause, reduced age, and finite lease, never synthesize missing/corrupt overrides, and honor CLI pause/resume', async t => {
  const f = setupFixture(t, {
    wechatNotifications: {
      enabled: true,
      manualReplyEnabled: true,
      manualReplyMaxAgeSeconds: 3600,
    },
  })
  const viewPath = resolveManualReplyViewPath(f.policy)
  const registry = resolveWechatRegistry(f.policy)
  const aliceProof = registry.contacts.find(c => c.id === 'alice').targetProof

  // Write valid friend event & worker heartbeat cleanly for production startBroker socket tests.
  const ev = f.writeEvidence('codex-repro.png')
  f.writeEvent({
    schema_version: 1,
    event_id: 'evt-codex-repro-1',
    account_sha256: f.accountSha,
    trigger_type: 'friend_message',
    conversation_kind: 'friend',
    conversation_ref: 'alice',
    binding_sha256: aliceProof,
    sender_name: 'Alice',
    source_ref: 'msg-cr-1',
    source_time: '18:00',
    observed_at: f.now(),
    text: 'Codex 暂停保真测试',
    evidence_path: ev.relPath,
    evidence_sha256: ev.sha256,
  })
  f.time(f.now() + 25_000)
  const [sealed] = f.projector.projectOnce().sealed

  mkdirSync(join(f.wechatRoot, 'receipts'), { recursive: true, mode: 0o750 })
  writeFileSync(
    join(f.wechatRoot, 'receipts', 'worker.json'),
    JSON.stringify({ checked_at: new Date().toISOString(), state: 'ready' }),
    { mode: 0o640 },
  )

  // 1. Exact Codex repro: paused=true, max_age_seconds=60, finite lease_expires_at BEFORE projection loop start/tick.
  const finiteLease = Date.now() + 300_000
  writeFileSync(viewPath, JSON.stringify({
    schema_version: 1,
    account_sha256: f.accountSha,
    enabled: true,
    manual_reply_enabled: true,
    max_age_seconds: 60,
    paused: true,
    lease_expires_at: finiteLease,
  }), { mode: 0o640 })
  chmodSync(viewPath, 0o640)

  const loop = createBrokerProjectionLoop(f.policy, { policyPath: f.policyPath, clock: () => f.now() })
  assert.ok(loop)
  loop.start()
  try {
    const afterStart = readTrustedManualReplyView(f.policy)
    assert.equal(afterStart.paused, true)
    assert.equal(afterStart.max_age_seconds, 60)
    assert.equal(afterStart.lease_expires_at, finiteLease)
    assert.equal(afterStart.manual_reply_enabled, true)

    // Verify syncLivePolicyView reads updated policyPath on tick without throwing: lower ceiling 120 -> 90 while preserving pause+lease.
    writeFileSync(viewPath, JSON.stringify({ ...afterStart, max_age_seconds: 120 }), { mode: 0o640 })
    writeFileSync(
      f.policyPath,
      JSON.stringify({ ...f.policy, wechatNotifications: { ...f.policy.wechatNotifications, manualReplyMaxAgeSeconds: 90 } }),
      { mode: 0o600 },
    )
    const liveReloadLoop = createBrokerProjectionLoop(f.policy, { policyPath: f.policyPath, clock: () => f.now() })
    liveReloadLoop.start()
    liveReloadLoop.stop()
    const afterReloadTick = readTrustedManualReplyView(f.policy)
    assert.equal(afterReloadTick.max_age_seconds, 90)
    assert.equal(afterReloadTick.paused, true)
    assert.equal(afterReloadTick.lease_expires_at, finiteLease)
    writeFileSync(f.policyPath, JSON.stringify(f.policy), { mode: 0o600 })

    // Passive tighten-only policy sync also preserves paused=false + finite lease + reduced max_age_seconds=60.
    writeFileSync(viewPath, JSON.stringify({
      schema_version: 1,
      account_sha256: f.accountSha,
      enabled: true,
      manual_reply_enabled: true,
      max_age_seconds: 60,
      paused: false,
      lease_expires_at: finiteLease,
    }), { mode: 0o640 })
    const reconciledActive = reconcileManualReplyConfigView(f.policy)
    assert.equal(reconciledActive.paused, false)
    assert.equal(reconciledActive.max_age_seconds, 60)
    assert.equal(reconciledActive.lease_expires_at, finiteLease)

    // Policy disable tightens monotonically without clearing paused/lease.
    writeFileSync(viewPath, JSON.stringify({ ...reconciledActive, paused: true }), { mode: 0o640 })
    const tighterPolicy = { ...f.policy, wechatNotifications: { ...f.policy.wechatNotifications, manualReplyEnabled: false } }
    reconcileManualReplyConfigView(tighterPolicy)
    const afterTighten = readTrustedManualReplyView(f.policy)
    assert.equal(afterTighten.manual_reply_enabled, false)
    assert.equal(afterTighten.paused, true)
    assert.equal(afterTighten.max_age_seconds, 60)
    assert.equal(afterTighten.lease_expires_at, finiteLease)
  } finally {
    loop.stop()
  }

  // 2. Account rebind during passive reconcile forces fail-closed without authorizing the new account.
  const origCfg = readFileSync(f.policy.wechatConfig, 'utf8')
  writeFileSync(viewPath, JSON.stringify({
    schema_version: 1,
    account_sha256: f.accountSha,
    enabled: true,
    manual_reply_enabled: true,
    max_age_seconds: 120,
    paused: false,
    lease_expires_at: finiteLease,
  }), { mode: 0o640 })
  writeFileSync(f.policy.wechatConfig, JSON.stringify({ ...JSON.parse(origCfg), account_id: 'rebound-acct' }), { mode: 0o600 })
  reconcileManualReplyConfigView(f.policy)
  const reboundView = JSON.parse(readFileSync(viewPath, 'utf8'))
  assert.equal(reboundView.account_sha256, f.accountSha)
  assert.equal(reboundView.enabled, false)
  assert.equal(reboundView.manual_reply_enabled, false)
  writeFileSync(f.policy.wechatConfig, origCfg, { mode: 0o600 })

  // 3. Missing OR corrupt/unreadable view is NEVER synthesized or overwritten on reconcile/perform/prepare.
  unlinkSync(viewPath)
  assert.equal(reconcileManualReplyConfigView(f.policy), null)
  assert.equal(existsSync(viewPath), false)
  assert.throws(() => resolveActionableInboundNotice(f.policy, sealed.noticeId, f.now()), /config view is missing/)

  writeFileSync(viewPath, '{corrupt-json', { mode: 0o640 })
  assert.equal(reconcileManualReplyConfigView(f.policy), null)
  assert.equal(readFileSync(viewPath, 'utf8'), '{corrupt-json')
  assert.throws(() => resolveActionableInboundNotice(f.policy, sealed.noticeId, f.now()))
  await assert.rejects(
    () => perform({ id: 'a'.repeat(24), kind: 'wechat_reply_notice', noticeId: sealed.noticeId, payload: { noticeId: sealed.noticeId, text: 'fail' }, confirmationRequestId: 'r1' }, f.policy, { now: f.now() }),
    /.+/,
  )
  assert.equal(readFileSync(viewPath, 'utf8'), '{corrupt-json')

  // 4. Production startBroker + real Unix domain socket + broker manual-reply pause/resume CLI!
  const sockDir = realpathSync(mkdtempSync('/tmp/octopus-sock-'))
  t.after(() => rmSync(sockDir, { recursive: true, force: true }))
  chmodSync(sockDir, 0o750)
  const sockPolicy = { ...f.policy, socket: join(sockDir, 'b.sock') }
  writeFileSync(f.policyPath, JSON.stringify(sockPolicy), { mode: 0o600 })

  // Restore paused finite-lease override BEFORE booting startBroker().
  writeFileSync(viewPath, JSON.stringify({
    schema_version: 1,
    account_sha256: f.accountSha,
    enabled: true,
    manual_reply_enabled: true,
    max_age_seconds: 60,
    paused: true,
    lease_expires_at: Date.now() + 300_000,
  }), { mode: 0o640 })

  // Make event fresh relative to wall clock because startBroker uses Date.now().
  const wallNow = Date.now()
  f.projector.db.prepare('UPDATE inbound_batches SET opened_at=?, last_event_at=?, sealed_at=?').run(wallNow - 5000, wallNow - 5000, wallNow)
  f.projector.db.prepare('UPDATE projected_notices SET source_observed_at=?, created_at=?').run(wallNow - 5000, wallNow)
  f.projector.db.prepare('SELECT batch_id, payload FROM ingested_events').all().forEach(() => {})
  f.projector.db.prepare('UPDATE ingested_events SET observed_at=?').run(wallNow - 5000)

  const rpc = req => new Promise((resolvePromise, rejectPromise) => {
    const client = connect(sockPolicy.socket, () => client.write(JSON.stringify(req) + '\n'))
    let buf = ''
    client.on('data', chunk => { buf += chunk.toString() })
    client.on('end', () => resolvePromise(JSON.parse(buf.trim())))
    client.on('error', rejectPromise)
  })

  const server = startBroker(f.policyPath)
  t.after(() => new Promise(resolveClose => {
    if (!server.listening) {
      if (existsSync(sockPolicy.socket)) unlinkSync(sockPolicy.socket)
      resolveClose()
      return
    }
    server.once('close', resolveClose)
    server.close()
    server.closeAllConnections?.()
  }))
  await new Promise(resolvePromise => {
    if (existsSync(sockPolicy.socket)) resolvePromise()
    else server.once('listening', resolvePromise)
  })

  const stPaused = await rpc({ action: 'wechat_status' })
  assert.equal(stPaused.ok, true)
  assert.equal(stPaused.result.manualReplyEnabled, false)
  assert.equal(stPaused.result.manualReplyPaused, true)
  assert.equal(stPaused.result.manualReplyMaxAgeSeconds, 60)
  assert.ok(Number.isSafeInteger(stPaused.result.manualReplyLeaseExpiresAt))

  // startBroker startup & its projection loop did NOT overwrite paused=true or max_age_seconds=60!
  const persistedAfterBoot = readTrustedManualReplyView(sockPolicy)
  assert.equal(persistedAfterBoot.paused, true)
  assert.equal(persistedAfterBoot.max_age_seconds, 60)

  // Ordinary in-flight prepare over socket FAILS CLOSED while paused!
  const db = new DatabaseSync(f.ingressDbPath)
  db.prepare('DELETE FROM u_im_ingress_deliveries').run()
  const identity = JSON.parse(sockPolicy.bindingKey)
  db.prepare('INSERT INTO u_im_ingress_deliveries(key, value) VALUES(?, ?)').run(
    JSON.stringify([...identity, 'req-sock-1']),
    JSON.stringify({
      deliveryKey: JSON.stringify([...identity, 'req-sock-1']),
      operatorId: 'owner',
      targetTenantId: 'octopus',
      requestId: 'req-sock-1',
      prompt: `回复 ${sealed.noticeId}：暂停态拒发`,
      receivedAt: new Date(Date.now()).toISOString(),
      status: 'in_flight',
    }),
  )
  const blockedPrep = await rpc({ action: 'prepare', params: { kind: 'wechat_reply_notice', payload: { noticeId: sealed.noticeId, text: '暂停态拒发' } } })
  assert.equal(blockedPrep.ok, false)
  assert.match(blockedPrep.error, /paused/)

  // Explicit CLI `broker.mjs <policy> manual-reply resume --lease-seconds 120 --max-age-seconds 90` clears pause with finite lease!
  const brokerCliPath = join(process.cwd(), 'scripts', 'linux-operations', 'broker.mjs')
  const cliOut = JSON.parse(execFileSync(
    process.execPath,
    [brokerCliPath, f.policyPath, 'manual-reply', 'resume', '--lease-seconds', '120', '--max-age-seconds', '90'],
    { encoding: 'utf8' },
  ))
  assert.equal(cliOut.action, 'resume')
  assert.equal(cliOut.view.paused, false)
  assert.equal(cliOut.view.max_age_seconds, 90)
  assert.ok(cliOut.view.lease_expires_at > Date.now() && cliOut.view.lease_expires_at <= Date.now() + 125_000)

  const stResumed = await rpc({ action: 'wechat_status' })
  assert.equal(stResumed.result.manualReplyEnabled, true)
  assert.equal(stResumed.result.manualReplyPaused, false)
  assert.equal(stResumed.result.manualReplyMaxAgeSeconds, 90)
  db.close()
})

test('projector and auto-reply handoffs recover crash/write interruptions boundedly without duplicate notices or terminal resend', async t => {
  const f = setupFixture(t, { wechatNotifications: { enabled: true } })
  const ev = f.writeEvidence('handoff-retry-1.png')
  const evFile = f.writeEvent({
    schema_version: 1,
    event_id: 'evt-handoff-retry-001',
    account_sha256: f.accountSha,
    trigger_type: 'friend_message',
    conversation_kind: 'friend',
    conversation_ref: 'alice',
    sender_name: 'Alice',
    source_ref: 'm-handoff-1',
    source_time: '16:10',
    observed_at: f.now(),
    text: '测试写入中断后恢复',
    evidence_path: ev.relPath,
    evidence_sha256: ev.sha256,
  })
  assert.equal(existsSync(evFile), true)
  f.time(f.now() + 21_000)

  // 1. Exact Codex repro: fail writeHandoffFile once during sealDueBatches().
  const origWriteHandoff = f.projector.writeHandoffFile.bind(f.projector)
  f.projector.writeHandoffFile = () => {
    throw new Error('injected handoff write error')
  }
  assert.throws(() => f.projector.projectOnce(), /injected handoff write error/)
  const afterFailRows = f.projector.listProjectedNotices()
  assert.equal(afterFailRows.length, 1)
  assert.equal(afterFailRows[0].handoff_status, 'pending')
  assert.match(afterFailRows[0].last_handoff_error, /injected handoff write error/)
  assert.equal(f.readHandoffs().length, 0)

  // Restore after restart and re-run projectOnce(): handoff file is flushed with deterministic noticeId & 0o640 mode.
  f.projector.writeHandoffFile = origWriteHandoff
  f.reopen()
  const recoveredInbound = f.projector.projectOnce()
  assert.equal(recoveredInbound.handoffsRecovered, 1)
  assert.equal(recoveredInbound.sealed.length, 0)
  const afterRecoverRows = f.projector.listProjectedNotices()
  assert.equal(afterRecoverRows.length, 1)
  assert.equal(afterRecoverRows[0].notice_id, afterFailRows[0].notice_id)
  assert.equal(afterRecoverRows[0].notice_key, afterFailRows[0].notice_key)
  assert.equal(afterRecoverRows[0].handoff_status, 'written')
  const handoffs1 = f.readHandoffs()
  assert.equal(handoffs1.length, 1)
  assert.equal(handoffs1[0].mode, 0o640)
  assert.equal(handoffs1[0].data.noticeId, afterFailRows[0].notice_id)

  // Healthy subsequent projectOnce() performs ZERO redundant writeHandoffFile calls and keeps single row/file.
  let healthyWrites = 0
  const origRestored = f.projector.writeHandoffFile.bind(f.projector)
  f.projector.writeHandoffFile = (...args) => {
    healthyWrites += 1
    return origRestored(...args)
  }
  const healthyRun = f.projector.projectOnce()
  assert.equal(healthyRun.handoffsRecovered, 0)
  assert.equal(healthyWrites, 0)
  assert.equal(f.projector.listProjectedNotices().length, 1)
  assert.equal(f.readHandoffs().length, 1)
  f.projector.writeHandoffFile = origRestored

  // 2. Same DB->filesystem crash/write recovery for outbound terminal receipts.
  const job = await f.createFrozenJob({
    kind: 'wechat_send',
    payload: { recipient: 'Alice别名', text: '回执中断恢复测试' },
    receiptData: { state: 'sent_verified', recipient_alias: 'Alice别名', checked_at: Math.trunc(f.now() / 1000) },
  })
  const origForReceipt = f.projector.writeHandoffFile.bind(f.projector)
  f.projector.writeHandoffFile = () => {
    throw new Error('injected receipt handoff failure')
  }
  const failReceiptRun = f.projector.projectOnce()
  assert.equal(failReceiptRun.receipts.projected, 0)
  assert.equal(failReceiptRun.receipts.rejected.length, 1)
  const receiptRowPending = f.projector.getProjectedNotice(`receipt:${job.jobId}:sent_verified`)
  assert.ok(receiptRowPending)
  assert.equal(receiptRowPending.handoff_status, 'pending')
  assert.equal(f.readHandoffs().length, 1)

  f.projector.writeHandoffFile = origForReceipt
  f.reopen()
  const recoverReceiptRun = f.projector.projectOnce()
  assert.equal(recoverReceiptRun.receipts.recovered, 1)
  assert.equal(recoverReceiptRun.receipts.duplicates, 1)
  assert.equal(recoverReceiptRun.handoffsRecovered, 1)
  const receiptRowWritten = f.projector.getProjectedNotice(`receipt:${job.jobId}:sent_verified`)
  assert.equal(receiptRowWritten.notice_id, receiptRowPending.notice_id)
  assert.equal(receiptRowWritten.handoff_status, 'written')
  assert.equal(f.projector.listProjectedNotices().length, 2)
  assert.equal(f.readHandoffs().length, 2)

  // Healthy repeat receipt scan writes nothing and does not duplicate.
  let repeatReceiptWrites = 0
  const origRepeat = f.projector.writeHandoffFile.bind(f.projector)
  f.projector.writeHandoffFile = (...args) => {
    repeatReceiptWrites += 1
    return origRepeat(...args)
  }
  const cleanRepeat = f.projector.projectOnce()
  assert.equal(cleanRepeat.receipts.recovered, 0)
  assert.equal(cleanRepeat.receipts.duplicates, 1)
  assert.equal(cleanRepeat.handoffsRecovered, 0)
  assert.equal(repeatReceiptWrites, 0)
  assert.equal(f.readHandoffs().length, 2)
  f.projector.writeHandoffFile = origRepeat
})
