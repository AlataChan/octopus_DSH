/** Authorization and crash controls use private SQLite state and never contact production services. */
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, rmSync, symlinkSync } from 'node:fs'
import { spawnSync } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { DatabaseSync } from 'node:sqlite'
import { Operations, authority, digest } from '../operations.mjs'
import { validatePolicy } from '../policy.mjs'
import { dispatch, redact, perform } from '../broker.mjs'

function fixture(t) {
  const root = mkdtempSync(join(tmpdir(), 'octopus-ops-'))
  const policyPath = join(root, 'policy.json')
  const bindingKey = JSON.stringify(['feishu', 'app', 'org', 'chat', 'owner'])
  const policy = { socket: join(root, 'broker.sock'), state: join(root, 'state'), ingressDatabase: join(root, 'ingress.sqlite'),
    bindingKey, operatorId: 'owner', tenantId: 'octopus', services: { probe: 'octopus-probe.service' }, containers: ['probe'],
    wechatRoot: root, wechatConfig: join(root, 'wechat.json'), wechatRecipientAlias: '默认联系人',
    limits: { logLines: 50, previewSeconds: 600, workerPollSeconds: 2, maxTextBytes: 1000, commandSeconds: 10 } }
  writeFileSync(policyPath, JSON.stringify(policy))
  writeFileSync(policy.wechatConfig, JSON.stringify({ account_id: 'own-wechat', recipient_id: 'friend', recipient_query: 'fixture', enabled: true, direct_enabled: true }))
  let now = 100000
  let actor = { requestId: 'human-1', prompt: '请准备操作', receivedAt: now }
  let executions = 0
  let journal = new Operations(policy, policyPath, async () => { executions++; return { ok: true } }, () => now, () => actor)
  t.after(() => { journal.close(); rmSync(root, { recursive: true, force: true }) })
  return { root, policy, policyPath, get journal() { return journal }, get executions() { return executions },
    actor: value => { actor = value }, time: value => { now = value },
    reopen: () => { journal.close(); journal = new Operations(policy, policyPath, async () => { executions++ }, () => now, () => actor) },
    replaceJournal: next => { journal.close(); journal = next } }
}

test('strict targets and fields reject command and path injection', t => {
  const f = fixture(t)
  for (const payload of [{ target: 'probe; reboot' }, { target: '../../etc/shadow' }, { target: 'probe', command: 'id' }]) {
    assert.throws(() => f.journal.prepare({ kind: 'restart_service', payload }))
  }
  assert.throws(() => f.journal.prepare({ kind: 'shell', payload: {} }))
  assert.throws(() => f.journal.prepare({ kind: 'wechat_send', payload: { recipient: 'stranger', text: 'hello' } }))
  assert.throws(() => validatePolicy({ ...f.policy, limits: { ...f.policy.limits, commandSeconds: Infinity } }))
})

test('symlinked release entry executes startup validation instead of silently exiting', t => {
  const f = fixture(t)
  const link = join(f.root, 'broker-link.mjs')
  symlinkSync(fileURLToPath(new URL('../broker.mjs', import.meta.url)), link)
  const result = spawnSync(process.execPath, [link, join(f.root, 'missing-policy.json')], { encoding: 'utf8', timeout: 5000 })
  assert.notEqual(result.status, 0)
  assert.match(result.stderr, /missing-policy/)
})

test('service changes still require exact human confirmation and keep frozen arguments', async t => {
  const f = fixture(t)
  const input = { kind: 'restart_service', payload: { target: 'probe' } }
  const op = f.journal.prepare(input)
  input.payload.target = 'modified'
  assert.equal(f.journal.get(op.id).payload.target, 'probe')
  await assert.rejects(f.journal.run(op.id), /本人/)
  f.actor({ requestId: 'human-2', prompt: `确认执行 ${op.id}`, receivedAt: 99999 })
  await assert.rejects(f.journal.run(op.id), /本人/)
  f.actor({ requestId: 'human-2', prompt: `确认执行 ${op.id}`, receivedAt: 100001 })
  assert.equal((await f.journal.run(op.id)).state, 'completed')
  await f.journal.run(op.id)
  assert.equal(f.executions, 1)
})

test('exact structured human instructions authorize only their current request', async t => {
  const f = fixture(t)
  f.actor({ requestId: 'human-1', prompt: '重启服务 probe', receivedAt: 100000 })
  const op = f.journal.prepare({ kind: 'restart_service', payload: { target: 'probe' } })
  assert.equal(op.id, f.journal.prepare({ kind: 'restart_service', payload: { target: 'probe' } }).id)
  assert.equal((await f.journal.run(op.id)).state, 'completed')
  assert.equal(f.executions, 1)
})

test('reordered arguments cannot produce a second direct send for one human request', async t => {
  const f = fixture(t)
  f.actor({ requestId: 'human-1', prompt: '微信发送 默认联系人\n正文', receivedAt: 100000 })
  const first = f.journal.prepare({ kind: 'wechat_send', payload: { recipient: '默认联系人', text: '正文' } })
  const second = f.journal.prepare({ kind: 'wechat_send', payload: { text: '正文', recipient: '默认联系人' } })
  assert.equal(first.id, second.id)
  await f.journal.run(first.id)
  await f.journal.run(second.id)
  assert.equal(f.executions, 1)
})

test('administrator recipient changes cannot redirect a previously frozen message', async t => {
  const f = fixture(t)
  f.actor({ requestId: 'human-1', prompt: '微信发送 默认联系人\n正文', receivedAt: 100000 })
  const operation = f.journal.prepare({ kind: 'wechat_send', payload: { recipient: '默认联系人', text: '正文' } })
  writeFileSync(f.policy.wechatConfig, JSON.stringify({ account_id: 'own-wechat', recipient_id: 'different-person', enabled: true, direct_enabled: true }))
  f.journal.execute = perform
  const result = await f.journal.run(operation.id)
  assert.equal(result.state, 'needs_review')
  assert.match(result.error, /binding changed/)
})

test('expiry and crash recovery never replay a claimed action', async t => {
  const f = fixture(t)
  const expired = f.journal.prepare({ kind: 'restart_service', payload: { target: 'probe' } })
  f.time(800000)
  assert.equal((await f.journal.run(expired.id)).state, 'expired')
  f.actor({ requestId: 'human-3', prompt: 'prepare', receivedAt: 800000 })
  const crash = f.journal.prepare({ kind: 'restart_service', payload: { target: 'probe' } })
  f.journal.save({ ...crash, state: 'executing' })
  f.reopen()
  assert.equal((await f.journal.run(crash.id)).state, 'needs_review')
  assert.equal(f.executions, 0)
})

test('limits are backed up and restored; stale configuration preview refuses changes', async t => {
  const f = fixture(t)
  const op = f.journal.prepare({ kind: 'set_limits', payload: { logLines: 80 } })
  f.actor({ requestId: 'human-2', prompt: `确认执行 ${op.id}`, receivedAt: 100001 })
  assert.equal((await f.journal.run(op.id)).state, 'completed')
  assert.equal(JSON.parse(readFileSync(f.policyPath)).limits.logLines, 80)
  const restore = f.journal.prepare({ kind: 'restore_limits', payload: { operation: op.id } })
  f.actor({ requestId: 'human-3', prompt: `确认执行 ${restore.id}`, receivedAt: 100002 })
  assert.equal((await f.journal.run(restore.id)).state, 'completed')
  assert.equal(JSON.parse(readFileSync(f.policyPath)).limits.logLines, 50)
  const stale = f.journal.prepare({ kind: 'set_limits', payload: { logLines: 90 } })
  writeFileSync(f.policyPath, JSON.stringify({ ...f.policy, limits: { ...f.policy.limits, logLines: 70 } }))
  f.actor({ requestId: 'human-4', prompt: `确认执行 ${stale.id}`, receivedAt: 100003 })
  assert.equal((await f.journal.run(stale.id)).state, 'needs_review')
  assert.equal(JSON.parse(readFileSync(f.policyPath)).limits.logLines, 70)
})

test('confirmation authority comes from a current non-revoked matching ingress binding', t => {
  const f = fixture(t)
  const db = new DatabaseSync(f.policy.ingressDatabase)
  t.after(() => db.close())
  db.exec('CREATE TABLE u_im_ingress_operators(key TEXT PRIMARY KEY,value TEXT); CREATE TABLE u_im_ingress_deliveries(key TEXT PRIMARY KEY,value TEXT)')
  const binding = { revokedAt: null, operatorId: 'owner', targetTenantId: 'octopus' }
  db.prepare('INSERT INTO u_im_ingress_operators VALUES(?,?)').run(f.policy.bindingKey, JSON.stringify(binding))
  const row = { status: 'in_flight', operatorId: 'owner', targetTenantId: 'octopus', requestId: 'request', prompt: 'authentic',
    receivedAt: new Date(100000).toISOString(), deliveryKey: JSON.stringify(['feishu', 'app', 'org', 'chat', 'owner', 'event']) }
  db.prepare('INSERT INTO u_im_ingress_deliveries VALUES(?,?)').run('one', JSON.stringify(row))
  assert.equal(authority(f.policy, 100001).prompt, 'authentic')
  assert.throws(() => authority(f.policy, 900000))
  db.prepare('UPDATE u_im_ingress_operators SET value=?').run(JSON.stringify({ ...binding, revokedAt: 'now' }))
  assert.throws(() => authority(f.policy, 100001), /revoked/)
  db.prepare('UPDATE u_im_ingress_operators SET value=?').run(JSON.stringify(binding))
  db.prepare('UPDATE u_im_ingress_deliveries SET value=?').run(JSON.stringify({ ...row,
    deliveryKey: JSON.stringify(['feishu', 'app', 'org', 'other-chat', 'owner', 'event']) }))
  assert.throws(() => authority(f.policy, 100001), /Exactly one/)
  db.prepare('UPDATE u_im_ingress_deliveries SET value=?').run(JSON.stringify(row))
  db.prepare('INSERT INTO u_im_ingress_deliveries VALUES(?,?)').run('second', JSON.stringify({ ...row, requestId: 'overlapping' }))
  assert.throws(() => authority(f.policy, 100001), /Exactly one/)
})

test('broker rejects extra request arguments and redacts credential assignments', async t => {
  const f = fixture(t)
  await assert.rejects(dispatch({ action: 'service_logs', params: { target: 'probe', command: 'cat /etc/shadow' } }, f.journal))
  await assert.rejects(dispatch({ action: 'unknown', params: {} }, f.journal))
  assert(!redact('API_KEY=secret Authorization: Bearer abc.xyz').includes('secret'))
  assert(!redact('API_KEY=secret Authorization: Bearer abc.xyz').includes('abc.xyz'))
})

test('natural-language WeChat instructions execute in their authenticated turn without confirmation', async t => {
  const f = fixture(t)
  f.actor({ requestId: 'human-1', prompt: '给 fixture 发微信：微信调通了', receivedAt: 100000 })
  const input = { kind: 'wechat_send', payload: { recipient: 'fixture', text: '微信调通了' } }
  const op = f.journal.prepare(input)
  assert.equal(op.direct, true)
  assert.equal(op.payload.recipient, '默认联系人')
  assert.equal(f.journal.prepare({ kind: 'wechat_send', payload: { recipient: '默认联系人', text: '微信调通了' } }).id, op.id)
  input.payload.text = 'changed after preparation'
  f.actor({ requestId: 'human-2', prompt: '检查服务器状态', receivedAt: 100001 })
  await assert.rejects(f.journal.run(op.id), /本人/)
  f.actor({ requestId: 'human-1', prompt: '给 fixture 发微信：微信调通了', receivedAt: 100000 })
  assert.equal((await f.journal.run(op.id)).state, 'queued')
  assert.equal(f.journal.get(op.id).payload.text, '微信调通了')
  await f.journal.run(op.id)
  assert.equal(f.executions, 1)
})

function multiContactFixture(t, overrides = {}) {
  const root = mkdtempSync(join(tmpdir(), 'octopus-ops-multi-'))
  const policyPath = join(root, 'policy.json')
  const configDir = join(root, 'config')
  mkdirSync(join(configDir, 'contacts', 'friend-2'), { recursive: true })
  mkdirSync(join(configDir, 'contacts', 'friend-3'), { recursive: true })
  writeFileSync(join(configDir, 'account.png'), 'account-anchor-bytes')
  writeFileSync(join(configDir, 'own-chat.png'), 'own-chat-anchor-bytes')
  writeFileSync(join(configDir, 'recipient.png'), 'f1-search')
  writeFileSync(join(configDir, 'recipient-sidebar.png'), 'f1-sidebar')
  writeFileSync(join(configDir, 'recipient-card.png'), 'f1-card')
  writeFileSync(join(configDir, 'contacts', 'friend-2', 'recipient.png'), 'f2-search')
  writeFileSync(join(configDir, 'contacts', 'friend-2', 'recipient-sidebar.png'), 'f2-sidebar')
  writeFileSync(join(configDir, 'contacts', 'friend-2', 'recipient-card.png'), 'f2-card')
  writeFileSync(join(configDir, 'contacts', 'friend-3', 'recipient.png'), 'f3-search')
  writeFileSync(join(configDir, 'contacts', 'friend-3', 'recipient-sidebar.png'), 'f3-sidebar')
  writeFileSync(join(configDir, 'contacts', 'friend-3', 'recipient-card.png'), 'f3-card')
  mkdirSync(join(root, 'receipts'), { recursive: true })
  writeFileSync(join(root, 'receipts', 'worker.json'), JSON.stringify({ checked_at: new Date().toISOString(), state: 'ready' }))

  const bindingKey = JSON.stringify(['feishu', 'app', 'org', 'chat', 'owner'])
  const wechatContacts = overrides.wechatContacts ?? [
    { id: 'friend-1', type: 'friend', alias: '阿明-北京', displayName: '阿明', enabled: true, capabilities: { directSend: true }, legacy: true },
    { id: 'friend-2', type: 'friend', alias: '好友二', displayName: '阿强', enabled: true, capabilities: { directSend: true } },
    { id: 'friend-3', type: 'friend', alias: '阿明-上海', displayName: '阿明', enabled: true, capabilities: { directSend: true } },
  ]
  const policy = {
    socket: join(root, 'broker.sock'),
    state: join(root, 'state'),
    ingressDatabase: join(root, 'ingress.sqlite'),
    bindingKey,
    operatorId: 'owner',
    tenantId: 'octopus',
    services: { probe: 'octopus-probe.service' },
    containers: ['probe'],
    wechatRoot: root,
    wechatConfig: join(configDir, 'config.json'),
    wechatContacts,
    limits: { logLines: 50, previewSeconds: 600, workerPollSeconds: 2, maxTextBytes: 1000, commandSeconds: 10 },
    ...overrides.policy,
  }
  const wechatConfig = overrides.wechatConfig ?? {
    enabled: true,
    moments_enabled: true,
    direct_enabled: true,
    account_id: 'own-wechat',
    account_anchor: '/home/wechat/.gamma-moments/config/account.png',
    own_chat_anchor: '/home/wechat/.gamma-moments/config/own-chat.png',
    recipient_id: 'wx-friend-1',
    recipient_query: '阿明',
    recipient_anchor: '/home/wechat/.gamma-moments/config/recipient.png',
    recipient_sidebar_anchor: '/home/wechat/.gamma-moments/config/recipient-sidebar.png',
    recipient_card_anchor: '/home/wechat/.gamma-moments/config/recipient-card.png',
    visibility: 'private',
  }
  const contactsSidecar = overrides.contactsSidecar ?? {
    schema_version: 1,
    account_sha256: digest('own-wechat'),
    contacts: [
      {
        id: 'friend-1', type: 'friend', alias: '阿明-北京', display_name: '阿明', enabled: true, direct_enabled: true, legacy: true,
        recipient_id: 'wx-friend-1', recipient_query: '阿明',
        recipient_anchor: '/home/wechat/.gamma-moments/config/recipient.png',
        recipient_sidebar_anchor: '/home/wechat/.gamma-moments/config/recipient-sidebar.png',
        recipient_card_anchor: '/home/wechat/.gamma-moments/config/recipient-card.png',
      },
      {
        id: 'friend-2', type: 'friend', alias: '好友二', display_name: '阿强', enabled: true, direct_enabled: true,
        recipient_id: 'wx-friend-2', recipient_query: '阿强',
        recipient_anchor: '/home/wechat/.gamma-moments/config/contacts/friend-2/recipient.png',
        recipient_sidebar_anchor: '/home/wechat/.gamma-moments/config/contacts/friend-2/recipient-sidebar.png',
        recipient_card_anchor: '/home/wechat/.gamma-moments/config/contacts/friend-2/recipient-card.png',
      },
      {
        id: 'friend-3', type: 'friend', alias: '阿明-上海', display_name: '阿明', enabled: true, direct_enabled: true,
        recipient_id: 'wx-friend-3', recipient_query: '阿明',
        recipient_anchor: '/home/wechat/.gamma-moments/config/contacts/friend-3/recipient.png',
        recipient_sidebar_anchor: '/home/wechat/.gamma-moments/config/contacts/friend-3/recipient-sidebar.png',
        recipient_card_anchor: '/home/wechat/.gamma-moments/config/contacts/friend-3/recipient-card.png',
      },
    ],
  }
  const sidecarPath = join(configDir, 'octopus-contacts.json')
  policy.wechatContactsConfig = sidecarPath
  writeFileSync(policyPath, JSON.stringify(policy))
  writeFileSync(policy.wechatConfig, JSON.stringify(wechatConfig))
  writeFileSync(sidecarPath, JSON.stringify(contactsSidecar))
  let now = Date.now()
  let actor = { requestId: 'human-1', prompt: '微信发送', receivedAt: now }
  const journal = new Operations(policy, policyPath, perform, () => now, () => actor)
  t.after(() => { journal.close(); rmSync(root, { recursive: true, force: true }) })
  return { root, configDir, sidecarPath, policy, policyPath, wechatConfig, contactsSidecar, journal, actor: v => { actor = v }, time: v => { now = v } }
}

test('three-contact policy validates strict friend registry and rejects groups, duplicates, or mixed modes', t => {
  const f = multiContactFixture(t)
  assert.equal(validatePolicy(f.policy).wechatContacts.length, 3)
  assert.throws(() => validatePolicy({ ...f.policy, wechatRecipientAlias: '默认联系人' }))
  assert.throws(() => validatePolicy({ ...f.policy, wechatContacts: [{ ...f.policy.wechatContacts[0], type: 'group' }] }))
  assert.throws(() => validatePolicy({
    ...f.policy,
    wechatContacts: [
      f.policy.wechatContacts[0],
      { ...f.policy.wechatContacts[1], alias: f.policy.wechatContacts[0].alias },
    ],
  }))
  assert.throws(() => validatePolicy({
    ...f.policy,
    wechatContacts: [
      f.policy.wechatContacts[0],
      { ...f.policy.wechatContacts[1], id: f.policy.wechatContacts[0].id },
    ],
  }))
  assert.throws(() => validatePolicy({
    ...f.policy,
    wechatContacts: [
      f.policy.wechatContacts[0],
      { ...f.policy.wechatContacts[1], legacy: true },
    ],
  }))
})

test('three registered friends are separately selectable while ambiguous display names and raw IDs are rejected', async t => {
  const f = multiContactFixture(t)
  const status = await dispatch({ action: 'wechat_status', params: {} }, f.journal)
  assert.equal(status.contacts.length, 3)
  assert.equal(status.contacts.every(c => c.bound === true && c.directEnabled === true && !('recipient_id' in c)), true)
  const serializedStatus = JSON.stringify(status)
  assert.equal(serializedStatus.includes('wx-friend-'), false)
  assert.equal(serializedStatus.includes('own-wechat'), false)
  assert.equal(serializedStatus.includes('.png'), false)

  assert.throws(() => f.journal.prepare({ kind: 'wechat_send', payload: { recipient: '阿明', text: '测试重名' } }), /ambiguous|Ambiguous/i)
  assert.throws(() => f.journal.prepare({ kind: 'wechat_send', payload: { recipient: 'wx-friend-2', text: '禁止微信号' } }))
  assert.throws(() => f.journal.prepare({ kind: 'wechat_send', payload: { recipient: 'stranger', text: '未知联系人' } }))

  f.actor({ requestId: 'human-f2', prompt: '给阿强发微信：给阿强', receivedAt: Date.now() })
  const byDisplay = f.journal.prepare({ kind: 'wechat_send', payload: { recipient: '阿强', text: '给阿强' } })
  const byId = f.journal.prepare({ kind: 'wechat_send', payload: { recipient: 'friend-2', text: '给阿强' } })
  const byAlias = f.journal.prepare({ kind: 'wechat_send', payload: { recipient: '好友二', text: '给阿强' } })
  assert.equal(byDisplay.id, byId.id)
  assert.equal(byId.id, byAlias.id)
  assert.equal(byId.contactId, 'friend-2')

  const ran2 = await f.journal.run(byId.id)
  assert.equal(ran2.state, 'queued')
  const job2 = JSON.parse(readFileSync(join(f.root, 'adhoc', 'outbox', `${byId.id}.json`), 'utf8'))
  assert.equal(job2.schema_version, 2)
  assert.equal(job2.contact_id, 'friend-2')
  assert.equal(job2.contact_type, 'friend')
  assert.equal(job2.recipient_id, 'wx-friend-2')
  assert.equal(job2.recipient_alias, '好友二')
  assert.match(job2.binding_sha256, /^[0-9a-f]{64}$/)

  f.actor({ requestId: 'human-f3', prompt: '微信发给阿明-上海：给上海阿明', receivedAt: Date.now() })
  const op3 = f.journal.prepare({ kind: 'wechat_send', payload: { recipient: '阿明-上海', text: '给上海阿明' } })
  assert.equal(op3.contactId, 'friend-3')
  const ran3 = await f.journal.run(op3.id)
  assert.equal(ran3.state, 'queued')
  const job3 = JSON.parse(readFileSync(join(f.root, 'adhoc', 'outbox', `${op3.id}.json`), 'utf8'))
  assert.equal(job3.recipient_id, 'wx-friend-3')
})

test('disabled, unbound, rebound, and anchor-tampered contacts fail closed', async t => {
  const f = multiContactFixture(t, {
    wechatContacts: [
      { id: 'friend-1', type: 'friend', alias: '阿明-北京', displayName: '阿明', enabled: true, capabilities: { directSend: true }, legacy: true },
      { id: 'friend-2', type: 'friend', alias: '好友二', displayName: '阿强', enabled: false, capabilities: { directSend: true } },
      { id: 'friend-3', type: 'friend', alias: '好友三', displayName: '小李', enabled: true, capabilities: { directSend: true } },
    ],
    wechatConfig: {
      enabled: true, direct_enabled: true, account_id: 'own-wechat',
      account_anchor: '/home/wechat/.gamma-moments/config/account.png',
      own_chat_anchor: '/home/wechat/.gamma-moments/config/own-chat.png',
      recipient_id: 'wx-friend-1', recipient_query: '阿明',
    },
    contactsSidecar: {
      schema_version: 1,
      account_sha256: digest('own-wechat'),
      contacts: [
        {
          id: 'friend-1', type: 'friend', alias: '阿明-北京', display_name: '阿明', enabled: true, direct_enabled: true, legacy: true,
          recipient_id: 'wx-friend-1', recipient_query: '阿明',
          recipient_anchor: '/home/wechat/.gamma-moments/config/recipient.png',
          recipient_sidebar_anchor: '/home/wechat/.gamma-moments/config/recipient-sidebar.png',
          recipient_card_anchor: '/home/wechat/.gamma-moments/config/recipient-card.png',
        },
        {
          id: 'friend-2', type: 'friend', alias: '好友二', display_name: '阿强', enabled: false, direct_enabled: false,
          recipient_id: 'wx-friend-2', recipient_query: '阿强',
          recipient_anchor: '/home/wechat/.gamma-moments/config/contacts/friend-2/recipient.png',
          recipient_sidebar_anchor: '/home/wechat/.gamma-moments/config/contacts/friend-2/recipient-sidebar.png',
          recipient_card_anchor: '/home/wechat/.gamma-moments/config/contacts/friend-2/recipient-card.png',
        },
        {
          id: 'friend-3', type: 'friend', alias: '好友三', display_name: '小李', enabled: true, direct_enabled: true,
          recipient_id: '', recipient_query: '小李',
          recipient_anchor: '', recipient_sidebar_anchor: '', recipient_card_anchor: '',
        },
      ],
    },
  })
  const status = await dispatch({ action: 'wechat_status', params: {} }, f.journal)
  assert.equal(status.contacts[0].bound, true)
  assert.equal(status.contacts[1].directEnabled, false)
  assert.equal(status.contacts[2].bound, false)
  assert.throws(() => f.journal.prepare({ kind: 'wechat_send', payload: { recipient: '好友二', text: '停用好友' } }), /disabled/i)
  assert.throws(() => f.journal.prepare({ kind: 'wechat_send', payload: { recipient: '好友三', text: '未绑定好友' } }), /binding/i)

  f.actor({ requestId: 'human-tamper-f', prompt: '给阿明-北京发微信：准备后篡改锚点', receivedAt: Date.now() })
  const op1 = f.journal.prepare({ kind: 'wechat_send', payload: { recipient: '阿明-北京', text: '准备后篡改锚点' } })
  writeFileSync(join(f.configDir, 'recipient-card.png'), 'tampered-card-bytes')
  const tamperedResult = await f.journal.run(op1.id)
  assert.equal(tamperedResult.state, 'needs_review')
  assert.match(tamperedResult.error, /binding changed/)
})

test('legacy prepared operation remains bound only to migrated original contact even after alias rename', async t => {
  const f = fixture(t)
  f.actor({ requestId: 'human-1', prompt: '微信发送 默认联系人\n旧待办', receivedAt: 100000 })
  const prepared = f.journal.prepare({ kind: 'wechat_send', payload: { recipient: '默认联系人', text: '旧待办' } })
  // Simulate an exact pre-Phase-1 SQLite operation record (no contactId, legacy targetProof).
  const legacyOp = {
    id: prepared.id,
    kind: prepared.kind,
    payload: prepared.payload,
    fingerprint: prepared.fingerprint,
    state: 'prepared',
    createdAt: prepared.createdAt,
    expiresAt: prepared.expiresAt,
    actorRequestId: prepared.actorRequestId,
    direct: true,
    targetProof: digest(JSON.stringify(['own-wechat', 'friend'])),
    recipientDisplayName: 'fixture',
  }
  f.journal.save(legacyOp)

  const configDir = join(f.root, 'config')
  mkdirSync(join(configDir, 'contacts', 'friend-2'), { recursive: true })
  writeFileSync(join(configDir, 'recipient.png'), 'legacy-search')
  writeFileSync(join(configDir, 'recipient-sidebar.png'), 'legacy-sidebar')
  writeFileSync(join(configDir, 'recipient-card.png'), 'legacy-card')
  writeFileSync(join(configDir, 'contacts', 'friend-2', 'recipient.png'), 'f2-search')
  writeFileSync(join(configDir, 'contacts', 'friend-2', 'recipient-sidebar.png'), 'f2-sidebar')
  writeFileSync(join(configDir, 'contacts', 'friend-2', 'recipient-card.png'), 'f2-card')
  const sidecarPath = join(configDir, 'octopus-contacts.json')
  // Rename the migrated legacy contact alias while also giving friend-2 the old alias to prove alias alone never redirects.
  const migratedPolicy = {
    ...f.policy,
    wechatContactsConfig: sidecarPath,
    wechatContacts: [
      { id: 'default', type: 'friend', alias: '阿明-北京', displayName: 'fixture', enabled: true, capabilities: { directSend: true }, legacy: true },
      { id: 'friend-2', type: 'friend', alias: '默认联系人', displayName: '阿强', enabled: true, capabilities: { directSend: true } },
    ],
  }
  delete migratedPolicy.wechatRecipientAlias
  writeFileSync(f.policyPath, JSON.stringify(migratedPolicy))
  writeFileSync(sidecarPath, JSON.stringify({
    schema_version: 1,
    account_sha256: digest('own-wechat'),
    contacts: [
      {
        id: 'default', type: 'friend', alias: '阿明-北京', display_name: 'fixture', enabled: true, direct_enabled: true, legacy: true,
        recipient_id: 'friend', recipient_query: 'fixture',
        recipient_anchor: '/home/wechat/.gamma-moments/config/recipient.png',
        recipient_sidebar_anchor: '/home/wechat/.gamma-moments/config/recipient-sidebar.png',
        recipient_card_anchor: '/home/wechat/.gamma-moments/config/recipient-card.png',
      },
      {
        id: 'friend-2', type: 'friend', alias: '默认联系人', display_name: '阿强', enabled: true, direct_enabled: true,
        recipient_id: 'wx-friend-2', recipient_query: '阿强',
        recipient_anchor: '/home/wechat/.gamma-moments/config/contacts/friend-2/recipient.png',
        recipient_sidebar_anchor: '/home/wechat/.gamma-moments/config/contacts/friend-2/recipient-sidebar.png',
        recipient_card_anchor: '/home/wechat/.gamma-moments/config/contacts/friend-2/recipient-card.png',
      },
    ],
  }))
  const migratedJournal = new Operations(migratedPolicy, f.policyPath, perform, () => 100001, () => ({ requestId: 'human-2', prompt: `确认执行 ${legacyOp.id}`, receivedAt: 100001 }))
  f.replaceJournal(migratedJournal)
  // First verify an unconfirmed pre-upgrade direct=true row cannot run without second-turn confirmation even on matching requestId.
  migratedJournal.resolveAuthority = () => ({ requestId: 'human-1', prompt: '微信发送 默认联系人\n旧待办', receivedAt: 100000 })
  await assert.rejects(migratedJournal.run(legacyOp.id), /确认执行/)
  migratedJournal.resolveAuthority = () => ({ requestId: 'human-2', prompt: `确认执行 ${legacyOp.id}`, receivedAt: 100001 })
  const executed = await migratedJournal.run(legacyOp.id)
  assert.equal(executed.state, 'queued')
  const queued = JSON.parse(readFileSync(join(f.root, 'adhoc', 'outbox', `${legacyOp.id}.json`), 'utf8'))
  assert.equal(queued.recipient_id, 'friend')
  assert.equal(queued.recipient_alias, '阿明-北京')

  // Mismatched account_id or mismatched recipient_id still rejects confirmed legacy prepared operations.
  const wrongRecipientOp = { ...legacyOp, id: '11'.repeat(12), state: 'prepared', targetProof: digest(JSON.stringify(['own-wechat', 'wrong-friend'])) }
  migratedJournal.save(wrongRecipientOp)
  migratedJournal.resolveAuthority = () => ({ requestId: 'human-3', prompt: `确认执行 ${wrongRecipientOp.id}`, receivedAt: 100001 })
  const wrongRecipientRes = await migratedJournal.run(wrongRecipientOp.id)
  assert.equal(wrongRecipientRes.state, 'needs_review')
  assert.match(wrongRecipientRes.error, /binding changed/)

  const wrongAccountOp = { ...legacyOp, id: '22'.repeat(12), state: 'prepared', targetProof: digest(JSON.stringify(['other-account', 'friend'])) }
  migratedJournal.save(wrongAccountOp)
  migratedJournal.resolveAuthority = () => ({ requestId: 'human-4', prompt: `确认执行 ${wrongAccountOp.id}`, receivedAt: 100001 })
  const wrongAccountRes = await migratedJournal.run(wrongAccountOp.id)
  assert.equal(wrongAccountRes.state, 'needs_review')
  assert.match(wrongAccountRes.error, /binding changed/)
})

test('Gamma-shaped config rewrite preserves sidecar contacts and fails closed on account, channel, or recipient change', async t => {
  const f = multiContactFixture(t)
  // Simulate Gamma deploy_wechat_moments.py overwriting config/config.json from scratch (no DSH keys).
  writeFileSync(f.policy.wechatConfig, JSON.stringify({
    enabled: true,
    moments_enabled: true,
    direct_enabled: true,
    account_id: 'own-wechat',
    recipient_id: 'wx-friend-1',
    recipient_query: '阿明',
    recipient_anchor: '/home/wechat/.gamma-moments/config/recipient.png',
    recipient_sidebar_anchor: '/home/wechat/.gamma-moments/config/recipient-sidebar.png',
    recipient_card_anchor: '/home/wechat/.gamma-moments/config/recipient-card.png',
    own_chat_anchor: '/home/wechat/.gamma-moments/config/own-chat.png',
    moments_not_before: '2026-11-22T00:00:00+08:00',
    account_anchor: '/home/wechat/.gamma-moments/config/account.png',
    visibility: 'friends',
  }))
  const statusAfterDeploy = await dispatch({ action: 'wechat_status', params: {} }, f.journal)
  assert.equal(statusAfterDeploy.contacts.length, 3)
  assert.equal(statusAfterDeploy.contacts.every(c => c.bound && c.directEnabled), true)
  f.actor({ requestId: 'human-gamma', prompt: '给好友二发微信：Gamma重部署后仍可用', receivedAt: Date.now() })
  const op2 = f.journal.prepare({ kind: 'wechat_send', payload: { recipient: '好友二', text: 'Gamma重部署后仍可用' } })
  assert.equal((await f.journal.run(op2.id)).state, 'queued')

  // Changing recipient_id in Gamma config fails closed on the legacy contact while account change fails all contacts.
  f.actor({ requestId: 'human-gamma-2', prompt: '给阿明-北京发微信：旧联系人变更校验', receivedAt: Date.now() })
  const preparedLegacy = f.journal.prepare({ kind: 'wechat_send', payload: { recipient: '阿明-北京', text: '旧联系人变更校验' } })
  writeFileSync(f.policy.wechatConfig, JSON.stringify({ ...f.wechatConfig, recipient_id: 'wx-other-legacy' }))
  assert.equal((await f.journal.run(preparedLegacy.id)).state, 'needs_review')
  assert.throws(() => f.journal.prepare({ kind: 'wechat_send', payload: { recipient: '阿明-北京', text: '不可再选' } }), /binding/i)

  writeFileSync(f.policy.wechatConfig, JSON.stringify({ ...f.wechatConfig, account_id: 'hijacked-account' }))
  const statusAfterAccountChange = await dispatch({ action: 'wechat_status', params: {} }, f.journal)
  assert.equal(statusAfterAccountChange.contacts.every(c => c.bound === false), true)
  assert.throws(() => f.journal.prepare({ kind: 'wechat_send', payload: { recipient: '好友二', text: '换号拒绝' } }), /binding/i)
})

function phase2GroupAndAdhocFixture(t, overrides = {}) {
  const root = mkdtempSync(join(tmpdir(), 'octopus-ops-p2-'))
  const policyPath = join(root, 'policy.json')
  const configDir = join(root, 'config')
  mkdirSync(join(configDir, 'groups', 'ops-group'), { recursive: true })
  mkdirSync(join(configDir, 'groups', 'backup-group'), { recursive: true })
  writeFileSync(join(configDir, 'account.png'), 'p2-account-anchor')
  writeFileSync(join(configDir, 'own-chat.png'), 'p2-own-chat-anchor')
  writeFileSync(join(configDir, 'recipient.png'), 'p2-friend-search')
  writeFileSync(join(configDir, 'recipient-sidebar.png'), 'p2-friend-sidebar')
  writeFileSync(join(configDir, 'recipient-card.png'), 'p2-friend-card')
  writeFileSync(join(configDir, 'groups', 'ops-group', 'group-search.png'), 'ops-group-search')
  writeFileSync(join(configDir, 'groups', 'ops-group', 'member-1.png'), 'ops-member-1')
  writeFileSync(join(configDir, 'groups', 'ops-group', 'member-2.png'), 'ops-member-2')
  writeFileSync(join(configDir, 'groups', 'ops-group', 'group-name.png'), 'ops-group-name')
  writeFileSync(join(configDir, 'groups', 'backup-group', 'group-search.png'), 'bak-group-search')
  writeFileSync(join(configDir, 'groups', 'backup-group', 'member-1.png'), 'bak-member-1')
  mkdirSync(join(root, 'receipts'), { recursive: true })
  writeFileSync(join(root, 'receipts', 'worker.json'), JSON.stringify({ checked_at: new Date().toISOString(), state: 'ready' }))

  const bindingKey = JSON.stringify(['feishu', 'app', 'org', 'chat', 'owner'])
  const wechatContacts = overrides.wechatContacts ?? [
    { id: 'friend-1', type: 'friend', alias: '阿明-主号', displayName: '阿明', enabled: true, capabilities: { directSend: true }, legacy: true },
    { id: 'group-ops', type: 'group', alias: '策略投研群', displayName: '策略投研正式群', enabled: true, capabilities: { groupSend: true } },
    { id: 'group-bak', type: 'group', alias: '只读核验群', displayName: '备份观察群', enabled: true, capabilities: { groupSend: false } },
  ]
  const policy = {
    socket: join(root, 'broker.sock'),
    state: join(root, 'state'),
    ingressDatabase: join(root, 'ingress.sqlite'),
    bindingKey,
    operatorId: 'owner',
    tenantId: 'octopus',
    services: { probe: 'octopus-probe.service' },
    containers: ['probe'],
    wechatRoot: root,
    wechatConfig: join(configDir, 'config.json'),
    wechatContacts,
    ...(overrides.wechatAdhocMoments ? { wechatAdhocMoments: overrides.wechatAdhocMoments } : {}),
    limits: { logLines: 50, previewSeconds: 600, workerPollSeconds: 2, maxTextBytes: 1000, commandSeconds: 10 },
  }
  const wechatConfig = overrides.wechatConfig ?? {
    enabled: true,
    moments_enabled: false,
    direct_enabled: true,
    account_id: 'own-wechat',
    account_anchor: '/home/wechat/.gamma-moments/config/account.png',
    own_chat_anchor: '/home/wechat/.gamma-moments/config/own-chat.png',
    recipient_id: 'wx-friend-1',
    recipient_query: '阿明',
  }
  const contactsSidecar = overrides.contactsSidecar ?? {
    schema_version: 1,
    account_sha256: digest('own-wechat'),
    ...(overrides.sidecarAdhocMoments ? { adhoc_moments: overrides.sidecarAdhocMoments } : {}),
    contacts: [
      {
        id: 'friend-1', type: 'friend', alias: '阿明-主号', display_name: '阿明', enabled: true, direct_enabled: true, legacy: true,
        recipient_id: 'wx-friend-1', recipient_query: '阿明',
        recipient_anchor: '/home/wechat/.gamma-moments/config/recipient.png',
        recipient_sidebar_anchor: '/home/wechat/.gamma-moments/config/recipient-sidebar.png',
        recipient_card_anchor: '/home/wechat/.gamma-moments/config/recipient-card.png',
      },
      {
        id: 'group-ops', type: 'group', alias: '策略投研群', display_name: '策略投研正式群', enabled: true, group_enabled: true,
        group_id: 'wx-group-ops', group_name: '策略投研正式群', group_query: '策略投研',
        group_anchor: '/home/wechat/.gamma-moments/config/groups/ops-group/group-search.png',
        group_member_anchors: [
          '/home/wechat/.gamma-moments/config/groups/ops-group/member-1.png',
          '/home/wechat/.gamma-moments/config/groups/ops-group/member-2.png',
        ],
        group_name_anchor: '/home/wechat/.gamma-moments/config/groups/ops-group/group-name.png',
        group_name_read_mode: 'auto',
      },
      {
        id: 'group-bak', type: 'group', alias: '只读核验群', display_name: '备份观察群', enabled: true, group_enabled: false,
        group_id: 'wx-group-bak', group_name: '备份观察群', group_query: '备份观察群',
        group_anchor: '/home/wechat/.gamma-moments/config/groups/backup-group/group-search.png',
        group_member_anchors: [
          '/home/wechat/.gamma-moments/config/groups/backup-group/member-1.png',
        ],
        group_name_read_mode: 'native_copy',
      },
    ],
  }
  const sidecarPath = join(configDir, 'octopus-contacts.json')
  policy.wechatContactsConfig = sidecarPath
  writeFileSync(policyPath, JSON.stringify(policy))
  writeFileSync(policy.wechatConfig, JSON.stringify(wechatConfig))
  writeFileSync(sidecarPath, JSON.stringify(contactsSidecar))
  let now = Date.now()
  let actor = { requestId: 'human-p2-1', prompt: '微信发送', receivedAt: now }
  const journal = new Operations(policy, policyPath, perform, () => now, () => actor)
  t.after(() => { journal.close(); rmSync(root, { recursive: true, force: true }) })
  return { root, configDir, sidecarPath, policy, policyPath, wechatConfig, contactsSidecar, journal, actor: v => { actor = v }, time: v => { now = v } }
}

test('group send, group probe, and friend send are typed separately and keep raw ids hidden', async t => {
  const f = phase2GroupAndAdhocFixture(t)
  assert.equal(validatePolicy(f.policy).wechatContacts.length, 3)
  assert.throws(() => validatePolicy({ ...f.policy, wechatContacts: [{ ...f.policy.wechatContacts[1], legacy: true }] }))
  assert.throws(() => validatePolicy({ ...f.policy, wechatContacts: [{ ...f.policy.wechatContacts[1], capabilities: { directSend: true } }] }))

  const status = await dispatch({ action: 'wechat_status', params: {} }, f.journal)
  assert.equal(status.directEnabled, true)
  assert.equal(status.groupEnabled, true)
  // Ad hoc Moments defaults to disabled when omitted in policy and sidecar.
  assert.equal(status.adhocMoments.enabled, false)
  const serialized = JSON.stringify(status)
  assert.equal(serialized.includes('wx-group-'), false)
  assert.equal(serialized.includes('wx-friend-'), false)
  assert.equal(serialized.includes('.png'), false)

  // Typed operations never cross between friends and groups or accept raw group_id.
  assert.throws(() => f.journal.prepare({ kind: 'wechat_send', payload: { recipient: '策略投研群', text: '混用好友类型' } }), /friend/i)
  assert.throws(() => f.journal.prepare({ kind: 'wechat_send_group', payload: { recipient: '阿明-主号', text: '混用群类型' } }), /friend|group/i)
  assert.throws(() => f.journal.prepare({ kind: 'wechat_send_group', payload: { recipient: 'wx-group-ops', text: '禁止原始群ID' } }), /not bound/i)
  assert.throws(() => f.journal.prepare({ kind: 'wechat_send_group', payload: { recipient: '只读核验群', text: '未开启发送' } }), /disabled/i)

  f.actor({ requestId: 'human-grp-1', prompt: '给策略投研正式群发群消息：盘后群纪要', receivedAt: Date.now() })
  const op = f.journal.prepare({ kind: 'wechat_send_group', payload: { recipient: '策略投研正式群', text: '盘后群纪要' } })
  assert.equal(op.direct, true)
  assert.equal(op.contactType, 'group')
  const ran = await f.journal.run(op.id)
  assert.equal(ran.state, 'queued')
  const queued = JSON.parse(readFileSync(join(f.root, 'adhoc', 'outbox', `${op.id}.json`), 'utf8'))
  assert.equal(queued.schema_version, 2)
  assert.equal(queued.channel, 'group')
  assert.equal(queued.kind, 'send')
  assert.equal(queued.group_id, 'wx-group-ops')
  assert.equal(queued.group_name, '策略投研正式群')
  assert.match(queued.binding_sha256, /^[0-9a-f]{64}$/)

  // Read-only group probe is permitted for an enrolled bound group even when groupSend is disabled.
  f.actor({ requestId: 'human-probe-1', prompt: '核验备份观察群', receivedAt: Date.now() })
  const probeOp = f.journal.prepare({ kind: 'wechat_probe_group', payload: { recipient: '只读核验群' } })
  assert.equal(probeOp.direct, true)
  assert.equal((await f.journal.run(probeOp.id)).state, 'queued')
  const probeJob = JSON.parse(readFileSync(join(f.root, 'adhoc', 'outbox', `${probeOp.id}.json`), 'utf8'))
  assert.equal(probeJob.channel, 'group')
  assert.equal(probeJob.kind, 'probe')
  assert.equal(probeJob.text, '')
  mkdirSync(join(f.root, 'adhoc', 'receipts'), { recursive: true })
  writeFileSync(join(f.root, 'adhoc', 'receipts', `${probeOp.id}.json`), JSON.stringify({ state: 'identity_verified', group_id: 'wx-group-bak' }))
  const probeReceipt = await dispatch({ action: 'receipt', params: { id: probeOp.id } }, f.journal)
  assert.equal(probeReceipt.state, 'completed')

  // Tampering group member anchor after prepare fails closed in perform().
  f.actor({ requestId: 'human-tamper-g', prompt: '给策略投研群发群消息：锚点篡改前', receivedAt: Date.now() })
  const preparedTamper = f.journal.prepare({ kind: 'wechat_send_group', payload: { recipient: '策略投研群', text: '锚点篡改前' } })
  writeFileSync(join(f.configDir, 'groups', 'ops-group', 'member-2.png'), 'tampered-member-anchor')
  const tamperRes = await f.journal.run(preparedTamper.id)
  assert.equal(tamperRes.state, 'needs_review')
  assert.match(tamperRes.error, /binding changed/)
})

test('ad hoc Moments enforces independent default-off toggle, visibility, viewport limits, and per-request IDs', async t => {
  // 1. Default-off even when daily moments_enabled is true; un-opted-in requests fail closed.
  const fOff = phase2GroupAndAdhocFixture(t, {
    wechatConfig: {
      enabled: true, moments_enabled: true, direct_enabled: true, account_id: 'own-wechat',
      account_anchor: '/home/wechat/.gamma-moments/config/account.png',
      own_chat_anchor: '/home/wechat/.gamma-moments/config/own-chat.png',
    },
  })
  assert.throws(() => fOff.journal.prepare({ kind: 'wechat_publish_moment', payload: { text: '默认关闭测试', visibility: 'private' } }), /disabled/i)

  // 2. Enabled independently even while daily moments_enabled is false, with no default visibility -> visibility required.
  const f = phase2GroupAndAdhocFixture(t, {
    wechatAdhocMoments: { enabled: true },
    sidecarAdhocMoments: { enabled: true, default_visibility: null },
  })
  assert.throws(() => f.journal.prepare({ kind: 'wechat_publish_moment', payload: { text: '缺少可见范围' } }), /visibility is required/i)
  assert.throws(() => f.journal.prepare({ kind: 'wechat_publish_moment', payload: { text: '无效范围', visibility: 'public' } }), /visibility/i)

  // Text validation rejects unedited limit/viewport/reserved-text violations without rewriting user text.
  const overChars = '中'.repeat(221)
  assert.throws(() => f.journal.prepare({ kind: 'wechat_publish_moment', payload: { text: overChars, visibility: 'private' } }), /220/)
  assert.throws(() => f.journal.prepare({ kind: 'wechat_publish_moment', payload: { text: '1\n2\n3\n4\n5', visibility: 'private' } }), /4 lines/)
  assert.throws(() => f.journal.prepare({ kind: 'wechat_publish_moment', payload: { text: ' 首尾空格拒绝', visibility: 'private' } }), /whitespace/)
  assert.throws(() => f.journal.prepare({ kind: 'wechat_publish_moment', payload: { text: 'x'.repeat(60), visibility: 'private' } }), /viewport/)
  assert.throws(() => f.journal.prepare({ kind: 'wechat_publish_moment', payload: { text: '发表', visibility: 'private' } }), /reserved/)
  assert.throws(() => f.journal.prepare({ kind: 'wechat_publish_moment', payload: { text: `${'中'.repeat(218)}\n\n\n#尾`, visibility: 'private' } }), /viewport/)

  // Duplicate model call in same authenticated request deduplicates; second distinct authenticated request produces a new 24-hex job ID.
  const text = '今日观察：按需朋友圈独立任务记录。'
  f.actor({ requestId: 'human-p2-1', prompt: `发好友可见朋友圈：${text}`, receivedAt: Date.now() })
  const firstPreview = f.journal.prepare({ kind: 'wechat_publish_moment', payload: { text, visibility: 'friends' } })
  const dupPreview = f.journal.prepare({ kind: 'wechat_publish_moment', payload: { text, visibility: 'friends' } })
  assert.equal(dupPreview.id, firstPreview.id)
  assert.equal(firstPreview.direct, true)
  assert.equal(firstPreview.visibility, 'friends')
  assert.equal(firstPreview.visibilitySource, 'owner_prompt')
  assert.equal(firstPreview.adhocLayout.textSha256, digest(text))
  const firstRun = await f.journal.run(firstPreview.id)
  assert.equal(firstRun.state, 'queued')

  f.actor({ requestId: 'human-p2-2', prompt: `发好友可见朋友圈：${text}`, receivedAt: Date.now() })
  const secondPreview = f.journal.prepare({ kind: 'wechat_publish_moment', payload: { text, visibility: 'friends' } })
  assert.notEqual(secondPreview.id, firstPreview.id)
  assert.match(secondPreview.id, /^[0-9a-f]{24}$/)
  const secondRun = await f.journal.run(secondPreview.id)
  assert.equal(secondRun.state, 'queued')

  const job1 = JSON.parse(readFileSync(join(f.root, 'adhoc', 'outbox', `${firstPreview.id}.json`), 'utf8'))
  const job2 = JSON.parse(readFileSync(join(f.root, 'adhoc', 'outbox', `${secondPreview.id}.json`), 'utf8'))
  assert.equal(job1.channel, 'adhoc_moments')
  assert.equal(job1.visibility, 'friends')
  assert.equal(job2.job_id, secondPreview.id)
  assert.equal(job1.text, text)

  mkdirSync(join(f.root, 'adhoc', 'receipts'), { recursive: true })
  writeFileSync(join(f.root, 'adhoc', 'receipts', `${firstPreview.id}.json`), JSON.stringify({ state: 'published_verified', proof: { evidence_file: `${firstPreview.id}.${digest(text).slice(0, 12)}.png` } }))
  const completed = await dispatch({ action: 'receipt', params: { id: firstPreview.id } }, f.journal)
  assert.equal(completed.state, 'completed')

  // 3. Explicit defaultVisibility is frozen and reflected in prepare preview; account anchor tamper fails perform().
  const fDef = phase2GroupAndAdhocFixture(t, {
    wechatAdhocMoments: { enabled: true, defaultVisibility: 'private' },
    sidecarAdhocMoments: { enabled: true, default_visibility: 'private' },
  })
  fDef.actor({ requestId: 'human-p2-def', prompt: '发朋友圈：使用配置的默认范围', receivedAt: Date.now() })
  const previewDef = fDef.journal.prepare({ kind: 'wechat_publish_moment', payload: { text: '使用配置的默认范围' } })
  assert.equal(previewDef.payload.visibility, 'private')
  assert.equal(previewDef.visibility, 'private')
  assert.equal(previewDef.visibilitySource, 'configured_default')
  assert.equal(previewDef.direct, true)
  writeFileSync(join(fDef.configDir, 'account.png'), 'tampered-account-anchor')
  const failedAccount = await fDef.journal.run(previewDef.id)
  assert.equal(failedAccount.state, 'needs_review')
  assert.match(failedAccount.error, /binding changed/)
})

test('real ingress authority blocks draft/negated/quoted/substituted/legacy direct writes while allowing confirmed previews and explicit direct turns', async t => {
  const base = phase2GroupAndAdhocFixture(t, {
    wechatAdhocMoments: { enabled: true, defaultVisibility: 'private' },
    sidecarAdhocMoments: { enabled: true, default_visibility: 'private' },
  })
  base.journal.close()
  const db = new DatabaseSync(base.policy.ingressDatabase)
  db.exec('CREATE TABLE u_im_ingress_operators(key TEXT PRIMARY KEY,value TEXT); CREATE TABLE u_im_ingress_deliveries(key TEXT PRIMARY KEY,value TEXT)')
  db.prepare('INSERT INTO u_im_ingress_operators VALUES(?,?)').run(
    base.policy.bindingKey,
    JSON.stringify({ revokedAt: null, operatorId: 'owner', targetTenantId: 'octopus' }),
  )
  let seq = 0
  let clockMs = 1_760_000_000_000
  const setDelivery = (prompt, requestId = `req-${++seq}`, receivedAt = clockMs) => {
    db.prepare('DELETE FROM u_im_ingress_deliveries').run()
    db.prepare('INSERT INTO u_im_ingress_deliveries VALUES(?,?)').run('active', JSON.stringify({
      status: 'in_flight',
      operatorId: 'owner',
      targetTenantId: 'octopus',
      requestId,
      prompt,
      receivedAt: new Date(receivedAt).toISOString(),
      deliveryKey: JSON.stringify(['feishu', 'app', 'org', 'chat', 'owner', `ev-${seq}`]),
    }))
    return requestId
  }
  const realJournal = new Operations(base.policy, base.policyPath, perform, () => clockMs)
  t.after(() => { realJournal.close(); db.close() })

  const { readdirSync } = await import('node:fs')
  const countJobs = () => {
    try { return readdirSync(join(base.root, 'adhoc', 'outbox')).filter(n => n.endsWith('.json')).length }
    catch { return 0 }
  }

  // 1. Draft-only turn cannot queue publish or send even inside a valid authenticated Feishu delivery.
  setDelivery('先别发，帮我起草一条发朋友圈的文案：今日盘后复盘', 'req-draft')
  const draftMoment = await dispatch({ action: 'prepare', params: { kind: 'wechat_publish_moment', payload: { text: '今日盘后复盘' } } }, realJournal)
  assert.equal(draftMoment.direct, false)
  await assert.rejects(dispatch({ action: 'execute', params: { id: draftMoment.id } }, realJournal), /确认执行/)
  const draftOther = await dispatch({ action: 'prepare', params: { kind: 'wechat_send', payload: { recipient: '阿明-主号', text: '顺便发私信' } } }, realJournal)
  assert.equal(draftOther.direct, false)
  await assert.rejects(dispatch({ action: 'execute', params: { id: draftOther.id } }, realJournal), /确认执行/)
  assert.equal(countJobs(), 0)

  // 2. Negated, quoted, hypothetical, question, status, and trailing-negation prompts stay direct=false and never execute directly.
  for (const unsafePrompt of [
    '不要给阿明-主号发微信：今晚开会',
    '别发朋友圈：今日复盘',
    '“发朋友圈：今日复盘”',
    '刚才客户说“给阿明-主号发微信：今晚开会”',
    '如果给阿明-主号发微信：今晚开会，会不会太晚？',
    '能否给策略投研群发群消息：收盘纪要？',
    '查看发朋友圈的状态：今日复盘',
    '给阿明-主号发微信：今晚开会\n（先别发，等我确认）',
  ]) {
    setDelivery(unsafePrompt, `req-neg-${++seq}`)
    const preview = await dispatch({
      action: 'prepare',
      params: { kind: 'wechat_publish_moment', payload: { text: '今日复盘' } },
    }, realJournal)
    assert.equal(preview.direct, false, `Expected direct=false for prompt: ${unsafePrompt}`)
    await assert.rejects(dispatch({ action: 'execute', params: { id: preview.id } }, realJournal), /确认执行/)
  }
  assert.equal(countJobs(), 0)

  // 3. Unrecognized conversational paraphrase preserves frozen preview + existing second-turn 确认执行 flow.
  setDelivery('帮我给策略投研群准备一条收盘总结，一会儿我看完再发', 'req-conv-1', clockMs)
  const convPreview = await dispatch({
    action: 'prepare',
    params: { kind: 'wechat_send_group', payload: { recipient: '策略投研群', text: '今日策略净值稳步回升' } },
  }, realJournal)
  assert.equal(convPreview.direct, false)
  await assert.rejects(dispatch({ action: 'execute', params: { id: convPreview.id } }, realJournal), /确认执行/)
  assert.equal(countJobs(), 0)

  clockMs += 1000
  setDelivery(`确认执行 ${convPreview.id}`, 'req-conv-2', clockMs)
  const convExecuted = await dispatch({ action: 'execute', params: { id: convPreview.id } }, realJournal)
  assert.equal(convExecuted.state, 'queued')
  assert.equal(countJobs(), 1)

  // 4. Explicit positive single-turn direct request rejects mismatched body, recipient, cross-channel, and visibility escalation.
  clockMs += 1000
  setDelivery('给阿明-主号发微信：按计划执行调仓', 'req-friend-explicit', clockMs)
  // Substituted body or wrong channel must not get direct authorization.
  const subBody = await dispatch({ action: 'prepare', params: { kind: 'wechat_send', payload: { recipient: '阿明-主号', text: '偷换的正文' } } }, realJournal)
  assert.equal(subBody.direct, false)
  await assert.rejects(dispatch({ action: 'execute', params: { id: subBody.id } }, realJournal), /确认执行/)
  const wrongChannel = await dispatch({ action: 'prepare', params: { kind: 'wechat_send_group', payload: { recipient: '策略投研群', text: '按计划执行调仓' } } }, realJournal)
  assert.equal(wrongChannel.direct, false)
  await assert.rejects(dispatch({ action: 'execute', params: { id: wrongChannel.id } }, realJournal), /确认执行/)

  const okFriend = await dispatch({ action: 'prepare', params: { kind: 'wechat_send', payload: { recipient: 'friend-1', text: '按计划执行调仓' } } }, realJournal)
  assert.equal(okFriend.direct, true)
  // Duplicate prepare under alias returns the same operation without creating a second job.
  const okFriendDup = await dispatch({ action: 'prepare', params: { kind: 'wechat_send', payload: { recipient: '阿明-主号', text: '按计划执行调仓' } } }, realJournal)
  assert.equal(okFriendDup.id, okFriend.id)
  const okFriendExec = await dispatch({ action: 'execute', params: { id: okFriend.id } }, realJournal)
  assert.equal(okFriendExec.state, 'queued')
  assert.equal(countJobs(), 2)

  // Trying to execute another forged direct=true operation with same actorRequestId fails single-claim scope.
  const injectedDirectId = 'ab'.repeat(12)
  realJournal.save({
    id: injectedDirectId,
    kind: 'wechat_publish_moment',
    payload: { text: '按计划执行调仓', visibility: 'private' },
    fingerprint: digest('forged'),
    state: 'prepared',
    createdAt: clockMs,
    expiresAt: clockMs + 600000,
    actorRequestId: 'req-friend-explicit',
    direct: true,
    directVersion: 2,
    directProof: digest(JSON.stringify(['direct_v2', 'wechat_publish_moment', 'req-friend-explicit', 'private', '按计划执行调仓'])),
    visibility: 'private',
    targetProof: digest('x'),
  })
  await assert.rejects(dispatch({ action: 'execute', params: { id: injectedDirectId } }, realJournal), /确认执行/)

  // 5. Moments visibility distinction: configured_default vs owner_prompt vs preview_argument.
  clockMs += 1000
  setDelivery('发朋友圈：使用默认仅自己可见', 'req-mom-def', clockMs)
  const escalatedVis = await dispatch({
    action: 'prepare',
    params: { kind: 'wechat_publish_moment', payload: { text: '使用默认仅自己可见', visibility: 'friends' } },
  }, realJournal)
  assert.equal(escalatedVis.direct, false)
  await assert.rejects(dispatch({ action: 'execute', params: { id: escalatedVis.id } }, realJournal), /确认执行/)
  const okDefMom = await dispatch({
    action: 'prepare',
    params: { kind: 'wechat_publish_moment', payload: { text: '使用默认仅自己可见' } },
  }, realJournal)
  assert.equal(okDefMom.direct, true)
  assert.equal(okDefMom.visibility, 'private')
  assert.equal(okDefMom.visibilitySource, 'configured_default')
  assert.equal((await dispatch({ action: 'execute', params: { id: okDefMom.id } }, realJournal)).state, 'queued')

  // Explicit owner request for friends visibility overrides default cleanly.
  clockMs += 1000
  setDelivery('发好友可见朋友圈：公开复盘观点', 'req-mom-friends-1', clockMs)
  const okFriendsMom = await dispatch({
    action: 'prepare',
    params: { kind: 'wechat_publish_moment', payload: { text: '公开复盘观点', visibility: 'friends' } },
  }, realJournal)
  assert.equal(okFriendsMom.direct, true)
  assert.equal(okFriendsMom.visibility, 'friends')
  assert.equal(okFriendsMom.visibilitySource, 'owner_prompt')
  assert.equal((await dispatch({ action: 'execute', params: { id: okFriendsMom.id } }, realJournal)).state, 'queued')

  // Second genuine separate owner request with identical text sends independently with a distinct 24-hex job ID.
  clockMs += 1000
  setDelivery('发好友可见朋友圈：公开复盘观点', 'req-mom-friends-2', clockMs)
  const okFriendsMom2 = await dispatch({
    action: 'prepare',
    params: { kind: 'wechat_publish_moment', payload: { text: '公开复盘观点', visibility: 'friends' } },
  }, realJournal)
  assert.notEqual(okFriendsMom2.id, okFriendsMom.id)
  assert.equal(okFriendsMom2.direct, true)
  assert.equal((await dispatch({ action: 'execute', params: { id: okFriendsMom2.id } }, realJournal)).state, 'queued')
  assert.equal(countJobs(), 5)

  // When defaultVisibility is unset (null), omitting visibility in prompt produces direct=false preview_argument requiring confirmation.
  const noDefPolicy = { ...base.policy, wechatAdhocMoments: { enabled: true } }
  const nextSidecar = { ...base.contactsSidecar, adhoc_moments: { enabled: true, default_visibility: null } }
  writeFileSync(base.policyPath, JSON.stringify(noDefPolicy))
  writeFileSync(base.sidecarPath, JSON.stringify(nextSidecar))
  const noDefJournal = new Operations(noDefPolicy, base.policyPath, perform, () => clockMs)
  t.after(() => noDefJournal.close())
  clockMs += 1000
  setDelivery('发朋友圈：未指明范围时必须确认', 'req-mom-nodef', clockMs)
  const noDefPreview = await dispatch({
    action: 'prepare',
    params: { kind: 'wechat_publish_moment', payload: { text: '未指明范围时必须确认', visibility: 'friends' } },
  }, noDefJournal)
  assert.equal(noDefPreview.direct, false)
  assert.equal(noDefPreview.visibilitySource, 'preview_argument')
  await assert.rejects(dispatch({ action: 'execute', params: { id: noDefPreview.id } }, noDefJournal), /确认执行/)

  // 6. Legacy pre-upgrade direct=true row stored without directVersion/directProof cannot execute directly after upgrade.
  const legacyUnsafeId = 'cd'.repeat(12)
  setDelivery('发好友可见朋友圈：偷渡正文', 'req-legacy-row', clockMs)
  noDefJournal.save({
    id: legacyUnsafeId,
    kind: 'wechat_publish_moment',
    payload: { text: '偷渡正文', visibility: 'friends' },
    fingerprint: digest('legacy-unsafe'),
    state: 'prepared',
    createdAt: clockMs - 500,
    expiresAt: clockMs + 600000,
    actorRequestId: 'req-legacy-row',
    direct: true,
    visibility: 'friends',
    targetProof: digest('stale'),
  })
  await assert.rejects(dispatch({ action: 'execute', params: { id: legacyUnsafeId } }, noDefJournal), /确认执行/)
  assert.equal(countJobs(), 5)

  // 7. Read-only probe and status continue to work normally without sending externally.
  clockMs += 1000
  setDelivery('查看备份观察群是否已绑定', 'req-ro-probe', clockMs)
  const roStatus = await dispatch({ action: 'wechat_status', params: {} }, noDefJournal)
  assert.equal(roStatus.groupEnabled, true)
  const roProbe = await dispatch({ action: 'prepare', params: { kind: 'wechat_probe_group', payload: { recipient: '只读核验群' } } }, noDefJournal)
  assert.equal(roProbe.direct, true)
  assert.equal((await dispatch({ action: 'execute', params: { id: roProbe.id } }, noDefJournal)).state, 'queued')
  assert.equal(countJobs(), 6)
})
