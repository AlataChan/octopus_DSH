/** Root-owned finite host operations; requests cannot supply commands, paths or recipients. */
import { createServer } from 'node:net'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { readFileSync, mkdirSync, chmodSync, chownSync, statSync, lstatSync, existsSync, unlinkSync, realpathSync, openSync, closeSync, fstatSync, constants } from 'node:fs'
import { join, dirname } from 'node:path'
import { freemem, totalmem, loadavg, cpus, uptime } from 'node:os'
import { pathToFileURL } from 'node:url'
import { fields, readPolicy, wechatAutoReplyPolicy, wechatNotificationsPolicy } from './policy.mjs'
import { Operations, QUEUED_WECHAT_KINDS, atomicJson, authority, computeAdhocMomentsBindingSha256, digest, resolveWechatRegistry, shanghaiDate, validateAdhocMomentText } from './operations.mjs'
import { WechatExchangeProjector, readCapturedWechatInbox, readTrustedManualReplyView, resolveActionableInboundNotice, resolveManualReplyViewPath } from './wechat-inbox.mjs'
import { canonicalTransportKey, checkOperatorBindingActive, createWechatAutoReplyPump, WechatAutoReplyStore } from './wechat-autoreply.mjs'
import { createCnPublishedReportProvider } from './wechat-report-facts.mjs'

const exec = promisify(execFile)
const MAX_FRAME = 16384

function receiptJson(path) {
  const fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW)
  try {
    const stat = fstatSync(fd)
    if (!stat.isFile() || stat.size > 16384) throw new Error('Invalid receipt file')
    try { return JSON.parse(readFileSync(fd, 'utf8')) } catch (error) { throw new Error('Receipt JSON is invalid', { cause: error }) }
  } finally { closeSync(fd) }
}

/** Remove common credential assignments and bearer values from bounded service output. */
export function redact(text) {
  return text.replace(/Bearer\s+[A-Za-z0-9._-]+/gi, 'Bearer [REDACTED]')
    .replace(/((?:api[_-]?key|app[_-]?secret|access[_-]?token|password|authorization)\s*[:=]\s*)[^\s,;]+/gi, '$1[REDACTED]')
}

/** Run only program/argv assembled by this module, with no inherited credentials. */
export async function command(program, args, policy) {
  const result = await exec(program, args, { timeout: policy.limits.commandSeconds * 1000,
    maxBuffer: 1024 * 1024, env: { PATH: '/usr/bin:/bin:/usr/sbin:/sbin', LANG: 'C.UTF-8', LC_ALL: 'C.UTF-8', HOME: '/root' } })
  return redact(result.stdout)
}

/** Resolve a root-configured unit alias without accepting a shell or a unit supplied by the model. */
export function unit(policy, target) {
  if (!Object.hasOwn(policy.services, target)) throw new Error('Unknown service alias')
  return policy.services[target]
}

function writeAdhocOutboxJob(policy, operationId, job) {
  const outbox = join(policy.wechatRoot, 'adhoc', 'outbox')
  const exchange = statSync(policy.wechatRoot)
  mkdirSync(outbox, { recursive: true, mode: 0o750 })
  if (process.getuid?.() === 0) {
    chownSync(join(policy.wechatRoot, 'adhoc'), 0, exchange.gid)
    chownSync(outbox, 0, exchange.gid)
  }
  chmodSync(join(policy.wechatRoot, 'adhoc'), 0o750)
  chmodSync(outbox, 0o2750)
  const path = join(outbox, `${operationId}.json`)
  if (existsSync(path)) throw new Error('Message queue entry already exists; reconcile instead of resending')
  atomicJson(path, job, 0o640)
  if (process.getuid?.() === 0) chownSync(path, 0, exchange.gid)
}

function readAccountIdForManualReplyView(policy) {
  if (!policy?.wechatConfig || !existsSync(policy.wechatConfig)) return ''
  const cfgRaw = JSON.parse(readFileSync(policy.wechatConfig, 'utf8'))
  return typeof cfgRaw?.account_id === 'string' ? cfgRaw.account_id.trim() : ''
}

function writeTrustedManualReplyView(policy, view) {
  const viewPath = resolveManualReplyViewPath(policy)
  if (!viewPath) throw new Error('Missing WeChat configuration for manual reply view')
  const configDir = dirname(viewPath)
  const dirStat = lstatSync(configDir)
  if (dirStat.isSymbolicLink() || !dirStat.isDirectory()) {
    throw new Error('Unsafe WeChat config directory for manual reply view')
  }
  if (existsSync(viewPath) && lstatSync(viewPath).isSymbolicLink()) {
    throw new Error('Manual reply config view must not be a symlink')
  }
  atomicJson(viewPath, view, 0o640)
  if (process.getuid?.() === 0) {
    chownSync(viewPath, 0, statSync(policy.wechatRoot).gid)
  }
  return viewPath
}

function viewsEqual(a, b) {
  return (
    a.schema_version === b.schema_version
    && a.account_sha256 === b.account_sha256
    && a.enabled === b.enabled
    && a.manual_reply_enabled === b.manual_reply_enabled
    && a.max_age_seconds === b.max_age_seconds
    && a.paused === b.paused
    && a.lease_expires_at === b.lease_expires_at
  )
}

/**
 * Passive tighten-only reconciliation across broker startup and projection ticks.
 * Preserves existing admin pause, reduced max-age, finite lease, and mismatched/unreadable state,
 * and never synthesizes a missing or corrupt `config/octopus-manual-reply.json`.
 */
export function reconcileManualReplyConfigView(policy) {
  const viewPath = resolveManualReplyViewPath(policy)
  if (!viewPath || !existsSync(viewPath)) return null
  let existing
  try {
    existing = readTrustedManualReplyView(policy)
  } catch {
    return null
  }
  if (!existing) return null

  const accountId = readAccountIdForManualReplyView(policy)
  const notifCfg = wechatNotificationsPolicy(policy)
  let nextView
  if (!accountId || existing.account_sha256 !== digest(accountId)) {
    nextView = {
      ...existing,
      enabled: false,
      manual_reply_enabled: false,
    }
  } else {
    nextView = {
      schema_version: 1,
      account_sha256: existing.account_sha256,
      enabled: existing.enabled === true && notifCfg.enabled === true,
      manual_reply_enabled: existing.manual_reply_enabled === true && notifCfg.enabled === true && notifCfg.manualReplyEnabled === true,
      max_age_seconds: Math.min(existing.max_age_seconds, notifCfg.manualReplyMaxAgeSeconds),
      paused: existing.paused === true,
      lease_expires_at: existing.lease_expires_at,
    }
  }
  if (!viewsEqual(existing, nextView)) {
    writeTrustedManualReplyView(policy, nextView)
  }
  return nextView
}

function resolveEffectiveManualReplyState(policy, nowMs = Date.now()) {
  const notifCfg = wechatNotificationsPolicy(policy)
  const policyCeiling = notifCfg.enabled && notifCfg.manualReplyEnabled
  const accountId = readAccountIdForManualReplyView(policy)
  const accountSha = accountId ? digest(accountId) : null
  const viewPath = resolveManualReplyViewPath(policy)
  if (!viewPath || !existsSync(viewPath)) {
    return {
      viewExists: false,
      viewError: null,
      effectiveEnabled: false,
      paused: false,
      maxAgeSeconds: notifCfg.manualReplyMaxAgeSeconds,
      leaseExpiresAt: null,
      policyEnabled: policyCeiling,
      accountMatched: false,
    }
  }
  try {
    const view = readTrustedManualReplyView(policy)
    const accountMatched = Boolean(accountSha && view && view.account_sha256 === accountSha)
    const leaseValid = view.lease_expires_at === null || nowMs < view.lease_expires_at
    const effectiveEnabled = Boolean(
      policyCeiling
      && view
      && view.enabled === true
      && view.manual_reply_enabled === true
      && view.paused === false
      && accountMatched
      && leaseValid,
    )
    return {
      viewExists: true,
      viewError: null,
      effectiveEnabled,
      paused: view.paused === true,
      maxAgeSeconds: Math.min(notifCfg.manualReplyMaxAgeSeconds, view.max_age_seconds),
      leaseExpiresAt: view.lease_expires_at,
      policyEnabled: policyCeiling,
      accountMatched,
    }
  } catch (error) {
    return {
      viewExists: true,
      viewError: error instanceof Error ? error.message : String(error),
      effectiveEnabled: false,
      paused: false,
      maxAgeSeconds: notifCfg.manualReplyMaxAgeSeconds,
      leaseExpiresAt: null,
      policyEnabled: policyCeiling,
      accountMatched: false,
    }
  }
}

/**
 * Explicit operator control (`status` | `pause` | `resume`) owning admin pause, reduced
 * max-age overrides, and finite leases within the deployment policy ceiling.
 */
export function setManualReplyControl(policy, options = {}) {
  const action = options.action
  const nowMs = options.nowMs ?? Date.now()
  if (action === 'status') {
    return resolveEffectiveManualReplyState(policy, nowMs)
  }
  if (action !== 'pause' && action !== 'resume') {
    throw new Error('Unsupported manual reply control action')
  }
  if (!Number.isSafeInteger(nowMs) || nowMs <= 0) {
    throw new Error('Invalid clock timestamp for manual reply control')
  }
  const notifCfg = wechatNotificationsPolicy(policy)
  const accountId = readAccountIdForManualReplyView(policy)
  if (!accountId) {
    throw new Error('WeChat account binding is missing')
  }
  const accountSha = digest(accountId)

  let nextMaxAge = notifCfg.manualReplyMaxAgeSeconds
  if (options.maxAgeSeconds !== undefined && options.maxAgeSeconds !== null) {
    if (!Number.isSafeInteger(options.maxAgeSeconds) || options.maxAgeSeconds < 60 || options.maxAgeSeconds > 86400) {
      throw new Error('manualReply maxAgeSeconds must be an integer between 60 and 86400')
    }
    nextMaxAge = Math.min(options.maxAgeSeconds, notifCfg.manualReplyMaxAgeSeconds)
  }
  let explicitLeaseDeadline
  if (options.leaseSeconds !== undefined && options.leaseSeconds !== null) {
    if (!Number.isSafeInteger(options.leaseSeconds) || options.leaseSeconds < 1 || options.leaseSeconds > 86400 * 30) {
      throw new Error('manualReply leaseSeconds must be a positive integer <= 2592000')
    }
    explicitLeaseDeadline = nowMs + options.leaseSeconds * 1000
  }

  const existing = readTrustedManualReplyView(policy)
  if (action === 'pause') {
    let nextLease = existing?.lease_expires_at ?? null
    if (explicitLeaseDeadline !== undefined) {
      nextLease = nextLease === null ? explicitLeaseDeadline : Math.min(nextLease, explicitLeaseDeadline)
    }
    const baseMaxAge = existing ? Math.min(existing.max_age_seconds, nextMaxAge) : nextMaxAge
    const view = {
      schema_version: 1,
      account_sha256: existing && existing.account_sha256 === accountSha ? existing.account_sha256 : accountSha,
      enabled: Boolean(notifCfg.enabled && (existing ? existing.enabled : true)),
      manual_reply_enabled: Boolean(notifCfg.enabled && notifCfg.manualReplyEnabled && (existing ? existing.manual_reply_enabled : true)),
      max_age_seconds: baseMaxAge,
      paused: true,
      lease_expires_at: nextLease,
    }
    writeTrustedManualReplyView(policy, view)
    return { action, view }
  }

  if (!notifCfg.enabled || !notifCfg.manualReplyEnabled) {
    throw new Error('Cannot resume manual reply while disabled in policy')
  }
  const view = {
    schema_version: 1,
    account_sha256: accountSha,
    enabled: true,
    manual_reply_enabled: true,
    max_age_seconds: nextMaxAge,
    paused: false,
    lease_expires_at: explicitLeaseDeadline ?? null,
  }
  writeTrustedManualReplyView(policy, view)
  return { action, view }
}

function noticeAuthOutboxFields(noticeAuth) {
  if (!noticeAuth) return {}
  return {
    origin_notice_id: noticeAuth.noticeId,
    origin_notice_key: noticeAuth.noticeKey,
    origin_notice_observed_at: noticeAuth.noticeObservedAt,
    origin_notice_created_at: noticeAuth.noticeCreatedAt,
  }
}

function enqueueGroupOutboxJob({ policy, registry, contact, operationId, text, nowMs, expiresAt, confirmationRequestId, isProbe = false, noticeAuth = null, automaticFields = {} }) {
  if (!registry.globalEnabled || (!isProbe && !registry.globalGroupEnabled) || !registry.accountId) {
    throw new Error('WeChat group messaging is not configured')
  }
  if (!contact || contact.type !== 'group' || !contact.bound) {
    throw new Error('WeChat group binding changed after preview')
  }
  if (!contact.enabled || (!isProbe && !contact.groupEnabled)) {
    throw new Error('WeChat target is disabled')
  }
  const bodyText = isProbe ? '' : text
  writeAdhocOutboxJob(policy, operationId, {
    schema_version: 2,
    channel: 'group',
    kind: isProbe ? 'probe' : 'send',
    job_id: operationId,
    account_sha256: digest(registry.accountId),
    contact_id: contact.id,
    contact_type: 'group',
    group_id: contact.groupId,
    group_name: contact.groupName,
    group_alias: contact.alias,
    binding_sha256: contact.targetProof,
    text: bodyText,
    text_sha256: digest(bodyText),
    created_at: nowMs,
    expires_at: expiresAt,
    confirmation_request_id: confirmationRequestId,
    ...noticeAuthOutboxFields(noticeAuth),
    ...automaticFields,
  })
  return { queued: true, operation: operationId, channel: 'group', contactId: contact.id, group: contact.alias }
}

function enqueueFriendOutboxJob({ policy, registry, contact, operationId, text, nowMs, expiresAt, confirmationRequestId, noticeAuth = null, automaticFields = {} }) {
  if (!registry.globalEnabled || !registry.globalDirectEnabled || !registry.accountId) {
    throw new Error('WeChat direct messaging is not configured')
  }
  if (!contact || contact.type !== 'friend' || !contact.bound) {
    throw new Error('WeChat identity binding changed after preview')
  }
  if (!contact.enabled || !contact.directEnabled) {
    throw new Error('WeChat target is disabled')
  }
  const job = contact.hasSidecar
    ? {
      schema_version: 2,
      kind: 'send',
      job_id: operationId,
      account_sha256: digest(registry.accountId),
      contact_id: contact.id,
      contact_type: contact.type,
      recipient_id: contact.recipientId,
      recipient_alias: contact.alias,
      binding_sha256: contact.targetProof,
      text,
      text_sha256: digest(text),
      created_at: nowMs,
      expires_at: expiresAt,
      confirmation_request_id: confirmationRequestId,
      ...noticeAuthOutboxFields(noticeAuth),
      ...automaticFields,
    }
    : {
      schema_version: 1,
      kind: 'send',
      job_id: operationId,
      account_sha256: digest(registry.accountId),
      recipient_id: contact.recipientId,
      recipient_alias: contact.alias,
      text,
      text_sha256: digest(text),
      created_at: nowMs,
      expires_at: expiresAt,
      confirmation_request_id: confirmationRequestId,
      ...noticeAuthOutboxFields(noticeAuth),
    }
  writeAdhocOutboxJob(policy, operationId, job)
  return { queued: true, operation: operationId, contactId: contact.id, recipient: contact.alias }
}

function writeAutoReplyPermit(policy, nowMs, { revoke = false } = {}) {
  const cfg = wechatAutoReplyPolicy(policy)
  if (!policy.wechatConfig || !existsSync(policy.wechatConfig)) return null
  const path = join(dirname(policy.wechatConfig), 'octopus-auto-reply.json')
  if (!cfg.enabled && !existsSync(path)) return null
  const accountId = readAccountIdForManualReplyView(policy)
  if (!accountId) return null
  const root = realpathSync(policy.wechatRoot)
  const configDir = dirname(path)
  const dir = lstatSync(configDir)
  const uid = process.getuid?.()
  if (
    !dir.isDirectory() || dir.isSymbolicLink() || (dir.mode & 0o022)
    || (dir.uid !== 0 && dir.uid !== uid) || realpathSync(configDir) !== configDir
    || !configDir.startsWith(root + '/')
    || (existsSync(path) && lstatSync(path).isSymbolicLink())
  ) throw new Error('Unsafe WeChat automatic reply permit location')
  const manual = readTrustedManualReplyView(policy)
  const accountSha = digest(accountId)
  const enabled = !revoke && cfg.enabled
    && wechatNotificationsPolicy(policy).enabled
    && manual?.enabled === true && manual.account_sha256 === accountSha
    && (manual.lease_expires_at === null || manual.lease_expires_at > nowMs)
  const paused = revoke || cfg.globalPaused || manual?.paused !== false
  const conversations = Object.fromEntries(Object.entries(cfg.conversations)
    .filter(([, rule]) => rule.mode === 'unattended').map(([key]) => [key, 'unattended']))
  const permit = {
    schema_version: 1, account_sha256: accountSha, policy_version: cfg.policyVersion,
    enabled: Boolean(enabled), paused, conversations,
    lease_expires_at: Math.min(nowMs + (revoke ? 0 : 30_000), manual?.lease_expires_at ?? Infinity),
  }
  atomicJson(path, permit, 0o640)
  if (uid === 0) chownSync(path, 0, statSync(policy.wechatRoot).gid)
  return permit
}

function enqueueAutomaticReply(journal, { decision, source, sourceText, policy, clock, dispatchBy }) {
  const nowMs = clock()
  if (nowMs >= dispatchBy) return false
  const live = readPolicy(journal.policyPath)
  if (live.state !== policy.state || live.wechatRoot !== policy.wechatRoot) return false
  const current = wechatAutoReplyPolicy(live)
  if (
    canonicalTransportKey(current.transport) !== canonicalTransportKey(wechatAutoReplyPolicy(policy).transport)
    || live.bindingKey !== policy.bindingKey || !checkOperatorBindingActive(live).ok
  ) return false
  const permit = writeAutoReplyPermit(live, nowMs)
  if (
    !permit?.enabled || permit.paused || permit.policy_version !== decision.policyVersion
    || permit.conversations[decision.conversationKey] !== 'unattended'
    || !decision.providerRunId || decision.eligibilityDecision !== 'unattended_candidate'
    || typeof decision.draftText !== 'string' || !decision.draftText.trim()
    || Buffer.byteLength(decision.draftText) > Math.min(live.limits.maxTextBytes, 4096)
  ) return false

  // A broker restart must recover an existing job by ID, never create another one
  // after a crash between journaling and writing the outbox file.
  const id = decision.taskId
  const existing = journal.db.prepare('SELECT value FROM operations WHERE id=?').get(id)
  if (existing) {
    const previous = JSON.parse(existing.value)
    if (previous.autoReply?.batchId !== decision.batchId) return false
    const path = join(live.wechatRoot, 'adhoc', 'outbox', `${id}.json`)
    if (!existsSync(path)) return false
    const queued = receiptJson(path)
    if (queued.auto_reply_task_id !== id || queued.text_sha256 !== digest(decision.draftText)
      || queued.expires_at !== previous.expiresAt || !Number.isSafeInteger(queued.expires_at)) {
      throw new Error('Previously queued automatic reply does not match its frozen job')
    }
    return { deadlineAt: queued.expires_at }
  }

  const registry = resolveWechatRegistry(live)
  const target = decision.frozenTarget
  if (!target || !registry.accountId || digest(registry.accountId) !== target.accountSha256) return false
  const contact = registry.contacts.find(item => item.id === target.contactId && item.type === target.contactType)
  if (!contact?.bound || !contact.hasSidecar || contact.targetProof !== target.targetProof) return false
  const expiresAt = Math.min(
    decision.batchExpiresAt,
    source.observed_at + current.maxInboundAgeSeconds * 1000,
    nowMs + 180_000,
  )
  if (!Number.isSafeInteger(expiresAt) || expiresAt <= nowMs) return false
  const confirmationRequestId = `wechat-auto:${id}`
  const operation = {
    id, kind: contact.type === 'group' ? 'wechat_send_group' : 'wechat_send',
    payload: { text: decision.draftText },
    createdAt: nowMs, expiresAt, confirmationRequestId,
    contactId: contact.id, contactType: contact.type, recipientAlias: contact.alias, targetProof: contact.targetProof,
    state: 'executing',
    autoReply: {
      batchId: decision.batchId, policyVersion: decision.policyVersion, providerRunId: decision.providerRunId, source, sourceText,
    },
  }
  journal.save(operation)
  try {
    const enqueue = contact.type === 'group' ? enqueueGroupOutboxJob : enqueueFriendOutboxJob
    operation.result = enqueue({
      policy: live, registry, contact, operationId: id, text: decision.draftText, nowMs, expiresAt, confirmationRequestId,
      automaticFields: {
        auto_reply_task_id: id, auto_reply_policy_version: decision.policyVersion, auto_reply_source: source,
      },
    })
    operation.state = 'queued'
    journal.save(operation)
  } catch (error) {
    operation.state = 'needs_review'
    journal.save(operation)
    throw error
  }
  return { deadlineAt: expiresAt }
}

/** Enqueue one immutable message or Moments request for the serialized desktop worker. */
export async function perform(operation, policy, context = {}) {
  const nowMs = typeof context?.now === 'number' ? context.now : Date.now()
  if (operation.kind === 'restart_container') {
    const target = operation.payload.target
    if (!policy.containers.includes(target)) throw new Error('Container is not allowed')
    if (operation.targetProof !== target) throw new Error('Target changed after preview')
    await command('/usr/bin/podman', ['restart', '--time', '10', target], policy)
    const state = await command('/usr/bin/podman', ['inspect', '--format', '{{.State.Running}}', target], policy)
    if (state.trim() !== 'true') throw new Error('Container did not become running')
    return { target, running: true, note: 'Process running; application readiness requires its health checks' }
  }
  if (operation.kind === 'restart_service') {
    const service = unit(policy, operation.payload.target)
    if (operation.targetProof !== service) throw new Error('Service binding changed after preview')
    const before = await command('/usr/bin/systemctl', ['show', service, '-p', 'ActiveState', '-p', 'SubState'], policy)
    await command('/usr/bin/systemctl', ['restart', service], policy)
    const after = await command('/usr/bin/systemctl', ['show', service, '-p', 'ActiveState', '-p', 'SubState', '-p', 'Result'], policy)
    if (!after.includes('ActiveState=active')) throw new Error(`Service did not become active: ${after}`)
    return { target: operation.payload.target, before, after }
  }
  if (!QUEUED_WECHAT_KINDS.has(operation.kind)) throw new Error('Unsupported executor operation')
  const registry = resolveWechatRegistry(policy)
  if (operation.kind === 'wechat_publish_moment') {
    const adhoc = registry.adhocMoments
    if (!adhoc.enabled) throw new Error('WeChat ad hoc Moments is disabled')
    if (!adhoc.bound || !registry.accountId) throw new Error('WeChat ad hoc Moments binding is missing')
    const visibility = operation.payload.visibility
    if (visibility !== 'private' && visibility !== 'friends') throw new Error('Invalid ad hoc Moments visibility')
    validateAdhocMomentText(operation.payload.text, adhoc.viewport)
    const expectedProof = computeAdhocMomentsBindingSha256({
      accountId: registry.accountId,
      accountAnchor: adhoc.accountAnchor,
      visibility,
      textSha256: digest(operation.payload.text),
      viewport: adhoc.viewport,
    })
    if (operation.targetProof !== expectedProof) {
      throw new Error('WeChat ad hoc Moments binding changed after preview')
    }
    writeAdhocOutboxJob(policy, operation.id, {
      schema_version: 2,
      channel: 'adhoc_moments',
      kind: 'publish',
      job_id: operation.id,
      account_sha256: digest(registry.accountId),
      visibility,
      binding_sha256: expectedProof,
      text: operation.payload.text,
      text_sha256: digest(operation.payload.text),
      trade_date: shanghaiDate(operation.createdAt),
      created_at: nowMs,
      expires_at: operation.expiresAt,
      confirmation_request_id: operation.confirmationRequestId,
    })
    return { queued: true, operation: operation.id, channel: 'adhoc_moments', visibility }
  }

  if (operation.kind === 'wechat_reply_notice') {
    if (!operation.confirmationRequestId || typeof operation.confirmationRequestId !== 'string') {
      throw new Error('Missing owner confirmation authority for notice reply')
    }
    const resolved = resolveActionableInboundNotice(policy, operation.noticeId, nowMs)
    const { notice, contact } = resolved
    if (
      notice.notice_id !== operation.noticeId
      || notice.notice_key !== operation.noticeKey
      || notice.account_sha256 !== operation.accountSha256
      || notice.contact_id !== operation.contactId
      || notice.conversation_kind !== operation.contactType
      || notice.target_proof !== operation.targetProof
      || contact.id !== operation.contactId
      || contact.type !== operation.contactType
      || contact.targetProof !== operation.targetProof
    ) {
      throw new Error('WeChat notice identity binding changed after preview')
    }
    const effectiveExpiresAt = Math.min(operation.expiresAt, resolved.noticeDeadline)
    if (effectiveExpiresAt <= nowMs) {
      throw new Error('WeChat notice expired before outbox queuing')
    }
    const noticeAuth = {
      noticeId: operation.noticeId,
      noticeKey: operation.noticeKey,
      noticeObservedAt: notice.source_observed_at,
      noticeCreatedAt: notice.created_at,
    }
    const out = contact.type === 'group'
      ? enqueueGroupOutboxJob({
        policy,
        registry: resolved.registry,
        contact,
        operationId: operation.id,
        text: operation.payload.text,
        nowMs,
        expiresAt: effectiveExpiresAt,
        confirmationRequestId: operation.confirmationRequestId,
        noticeAuth,
      })
      : enqueueFriendOutboxJob({
        policy,
        registry: resolved.registry,
        contact,
        operationId: operation.id,
        text: operation.payload.text,
        nowMs,
        expiresAt: effectiveExpiresAt,
        confirmationRequestId: operation.confirmationRequestId,
        noticeAuth,
      })
    return { ...out, noticeId: operation.noticeId, expiresAt: effectiveExpiresAt }
  }

  if (operation.kind === 'wechat_send_group' || operation.kind === 'wechat_probe_group') {
    const isProbe = operation.kind === 'wechat_probe_group'
    const contact = operation.contactId ? registry.contacts.find(item => item.id === operation.contactId) : null
    if (!contact || contact.type !== 'group' || !contact.bound || operation.targetProof !== contact.targetProof) {
      throw new Error('WeChat group binding changed after preview')
    }
    return enqueueGroupOutboxJob({
      policy,
      registry,
      contact,
      operationId: operation.id,
      text: operation.payload.text,
      nowMs,
      expiresAt: operation.expiresAt,
      confirmationRequestId: operation.confirmationRequestId,
      isProbe,
    })
  }

  let contact
  if (operation.contactId) {
    contact = registry.contacts.find(item => item.id === operation.contactId)
    if (!contact || !contact.bound || operation.targetProof !== contact.targetProof) {
      throw new Error('WeChat identity binding changed after preview')
    }
  } else {
    // Legacy pre-migration prepared operations lack contactId: match strictly by legacy binding and exact account+recipient proof, never by alias alone.
    contact = registry.contacts.find(item =>
      item.legacy && typeof registry.config.recipient_id === 'string' && item.recipientId === registry.config.recipient_id)
    if (!contact || !contact.bound || operation.targetProof !== digest(JSON.stringify([registry.accountId, contact.recipientId]))) {
      throw new Error('WeChat identity binding changed after preview')
    }
  }
  return enqueueFriendOutboxJob({
    policy,
    registry,
    contact,
    operationId: operation.id,
    text: operation.payload.text,
    nowMs,
    expiresAt: operation.expiresAt,
    confirmationRequestId: operation.confirmationRequestId,
  })
}

function parseWechatInboxRequest(prompt) {
  if (typeof prompt !== 'string' || Buffer.byteLength(prompt) > 512
      || /[\x00-\x1f\x7f-\x9f\u200e\u200f\u202a-\u202e\u2066-\u2069]/.test(prompt)) {
    throw new Error('An exact WeChat inbox request is required')
  }
  const text = prompt.trim()
  if (/^(?:看|看看|查看)\s*(?:刚发的|最新的)\s*朋友圈(?:的)?评论[。？?]?$/.test(text)) {
    return { kind: 'moments' }
  }
  const chat = /^(?:看看|查看|看)\s*([^“”「」"'<>，。！？?：:;/\\]{1,64}?)\s*(?:的)?(?:最近(?:的)?(?:微信|群聊|聊天|群)?消息|刚回了什么)[。？?]?$/.exec(text)
  if (chat && chat[1].trim() && Buffer.byteLength(chat[1].trim()) <= 128) {
    return { kind: 'chat', target: chat[1].trim() }
  }
  throw new Error('An exact WeChat inbox request is required')
}

/** Handle a strict operation request from the dedicated Agent Unix-socket group. */
export async function dispatch(request, journal) {
  fields(request, ['action'], ['params'])
  const params = request.params ?? {}
  const policy = journal.policy
  switch (request.action) {
    case 'host_status': {
      fields(params, [])
      const memory = /^MemAvailable:\s+(\d+) kB$/m.exec(readFileSync('/proc/meminfo', 'utf8'))
      return { cpuCount: cpus().length, load: loadavg(), memoryTotal: totalmem(), memoryAvailable: memory ? Number(memory[1]) * 1024 : freemem(), uptimeSeconds: uptime(),
        disk: await command('/usr/bin/df', ['-h', '/'], policy),
        containers: await command('/usr/bin/podman', ['ps', '--format', '{{.Names}}\t{{.Status}}', '--filter', 'status=running'], policy),
        services: Object.keys(policy.services), limits: policy.limits }
    }
    case 'service_status': {
      fields(params, ['target'])
      return { target: params.target, status: await command('/usr/bin/systemctl', ['show', unit(policy, params.target),
        '-p', 'Id', '-p', 'ActiveState', '-p', 'SubState', '-p', 'Result', '-p', 'MemoryCurrent', '-p', 'NRestarts'], policy) }
    }
    case 'service_logs': {
      fields(params, ['target'])
      return { target: params.target, logs: await command('/usr/bin/journalctl', ['-u', unit(policy, params.target),
        '-n', String(policy.limits.logLines), '--no-pager', '-o', 'cat'], policy) }
    }
    case 'wechat_status': {
      fields(params, [])
      const registry = resolveWechatRegistry(policy)
      const heartbeat = JSON.parse(readFileSync(join(policy.wechatRoot, 'receipts', 'worker.json'), 'utf8'))
      const contacts = registry.contacts.map(contact => ({
        id: contact.id,
        type: contact.type,
        alias: contact.alias,
        displayName: contact.displayName,
        enabled: contact.enabled,
        ...(contact.type === 'friend'
          ? { directEnabled: contact.directEnabled }
          : { groupEnabled: contact.groupEnabled, groupNameReadMode: contact.groupNameReadMode }),
        bound: contact.bound,
        ...(contact.bindingError ? { bindingError: contact.bindingError } : {}),
      }))
      const manualReplyState = resolveEffectiveManualReplyState(policy)
      const base = {
        directEnabled: registry.mode === 'legacy'
          ? contacts[0].directEnabled
          : Boolean(registry.globalEnabled && registry.globalDirectEnabled && contacts.some(contact => contact.type === 'friend' && contact.directEnabled && contact.bound)),
        groupEnabled: Boolean(registry.globalEnabled && registry.globalGroupEnabled && contacts.some(contact => contact.type === 'group' && contact.groupEnabled && contact.bound)),
        manualReplyEnabled: manualReplyState.effectiveEnabled,
        manualReplyPaused: manualReplyState.paused,
        manualReplyMaxAgeSeconds: manualReplyState.maxAgeSeconds,
        manualReplyLeaseExpiresAt: manualReplyState.leaseExpiresAt,
        adhocMoments: {
          enabled: registry.adhocMoments.enabled,
          bound: registry.adhocMoments.bound,
          ...(registry.adhocMoments.bindingError ? { bindingError: registry.adhocMoments.bindingError } : {}),
          defaultVisibility: registry.adhocMoments.defaultVisibility,
          limits: {
            maxChars: registry.adhocMoments.viewport.maxChars,
            maxLogicalLines: registry.adhocMoments.viewport.maxLogicalLines,
            minWindowWidth: registry.adhocMoments.viewport.minWindowWidth,
            minWindowHeight: registry.adhocMoments.viewport.minWindowHeight,
          },
        },
        contacts,
        heartbeat,
        fresh: Date.now() - Date.parse(heartbeat.checked_at) < 120000,
        note: 'ready reflects the worker UI check, not an end-to-end network delivery or recipient read receipt',
      }
      if (registry.mode === 'legacy') {
        return { alias: contacts[0].alias, displayName: contacts[0].displayName, ...base }
      }
      return base
    }
    case 'wechat_inbox_read': {
      fields(params, [])
      const current = journal.policyPath ? readPolicy(journal.policyPath) : policy
      const now = journal.clock()
      const actor = authority(current, now)
      return readCapturedWechatInbox(current, parseWechatInboxRequest(actor.prompt), now)
    }
    case 'wechat_screenshot': {
      fields(params, [])
      const result = await exec('/usr/bin/podman', ['exec', 'gamma-wechat-login', '/usr/local/bin/python3', '-c',
        'from PIL import ImageGrab; import sys; ImageGrab.grab(xdisplay=":99").save(sys.stdout.buffer,format="PNG")'],
      { timeout: policy.limits.commandSeconds * 1000, maxBuffer: 2 * 1024 * 1024, encoding: 'buffer', env: { PATH: '/usr/bin:/bin', HOME: '/root' } })
      return { image: result.stdout.toString('base64'), mimeType: 'image/png', note: 'Read-only current WeChat desktop; do not infer recipient delivery from this image' }
    }
    case 'prepare': return journal.prepare(params)
    case 'execute': fields(params, ['id']); return journal.run(params.id)
    case 'receipt': {
      fields(params, ['id'])
      const operation = journal.get(params.id)
      if (QUEUED_WECHAT_KINDS.has(operation.kind) && operation.state === 'queued') {
        const path = join(policy.wechatRoot, 'adhoc', 'receipts', `${operation.id}.json`)
        const deadline = Date.now() + policy.limits.commandSeconds * 1000
        while (Date.now() < deadline) {
          if (existsSync(path)) {
            operation.workerReceipt = receiptJson(path)
            if (!['queued', 'preparing', 'prepared', 'publishing'].includes(operation.workerReceipt.state)) break
          }
          await new Promise(resolve => setTimeout(resolve, policy.limits.workerPollSeconds * 1000))
        }
        if (['sent_verified', 'published_verified', 'identity_verified'].includes(operation.workerReceipt?.state)) {
          operation.state = 'completed'
        } else if (operation.workerReceipt?.state === 'needs_review') {
          operation.state = 'needs_review'
        } else if (['refused', 'auth_required', 'failed_before_submit', 'expired'].includes(operation.workerReceipt?.state)) {
          operation.state = 'failed'
        }
        journal.save(operation)
      }
      return operation
    }
    default: throw new Error('Unknown broker action')
  }
}

/** Create one non-overlapping exchange projection schedule when `wechatNotifications.enabled` is true. */
export function createBrokerProjectionLoop(policy, projectorOptions = {}) {
  const notifCfg = wechatNotificationsPolicy(policy)
  if (!notifCfg.enabled) return null
  const {
    policyPath,
    autoReplyAdapterFactory,
    autoReplyAdapter,
    autoReplyStore,
    resolveAutoReplyInputs,
    dispatchAutomaticReply,
    autoReplyOptions,
    ...inboxOptions
  } = projectorOptions
  const autoReplyCfg = wechatAutoReplyPolicy(policy)
  const receiptOnlyStore = !autoReplyCfg.enabled && existsSync(join(policy.state, 'wechat-autoreply.sqlite'))
    ? new WechatAutoReplyStore(policy, null, { clock: inboxOptions.clock, statUid: inboxOptions.statUid })
    : null
  const projector = new WechatExchangeProjector(policy, {
    ...inboxOptions,
    onVerifiedAutomaticReceipt: receipt => (autoReplyPump?.store ?? receiptOnlyStore)?.settleAutomaticReceipt(receipt),
  })
  let stopped = false
  let timer = null
  const reportPublicationProvider = autoReplyCfg.cnPublishedReportDir
    ? createCnPublishedReportProvider(autoReplyCfg.cnPublishedReportDir, { trustedUid: process.getuid?.() ?? 0 })
    : null
  const autoReplyPump = autoReplyCfg.enabled
    ? createWechatAutoReplyPump({
      policy,
      policyPath,
      projector,
      store: autoReplyStore,
      adapter: autoReplyAdapter,
      adapterFactory: autoReplyAdapterFactory,
      resolveEvaluationInputs: reportPublicationProvider
        ? async input => ({ reportPublicationProvider, ...await resolveAutoReplyInputs?.(input) })
        : resolveAutoReplyInputs,
      dispatchUnattended: dispatchAutomaticReply,
      clock: inboxOptions.clock,
      statUid: inboxOptions.statUid,
      ...(autoReplyOptions ?? {}),
      onError: (msg) => {
        console.error('WeChat auto-reply pump error:', redact(String(msg)).slice(0, 240))
      },
    })
    : null
  const syncLivePolicyView = () => {
    const activePolicy = policyPath ? readPolicy(policyPath) : policy
    reconcileManualReplyConfigView(activePolicy)
    if (typeof dispatchAutomaticReply === 'function') writeAutoReplyPermit(activePolicy, inboxOptions.clock?.() ?? Date.now())
  }
  const runTick = () => {
    try {
      syncLivePolicyView()
      projector.projectOnce()
    } catch (error) {
      console.error('WeChat exchange projection error:', redact(error instanceof Error ? error.message : String(error)).slice(0, 400))
    }
    if (autoReplyPump && !stopped) {
      autoReplyPump.trigger().catch((error) => {
        console.error('WeChat auto-reply pump error:', redact(error instanceof Error ? error.message : String(error)).slice(0, 240))
      })
    }
  }
  const schedule = () => {
    if (stopped) return
    timer = setTimeout(() => {
      timer = null
      if (stopped) return
      runTick()
      schedule()
    }, notifCfg.projectionIntervalSeconds * 1000)
    timer.unref()
  }
  return {
    start() {
      if (stopped) return
      runTick()
      schedule()
    },
    stop() {
      if (stopped) return
      stopped = true
      if (timer) { clearTimeout(timer); timer = null }
      autoReplyPump?.stop()
      receiptOnlyStore?.close()
      projector.close()
    },
    projector,
    autoReplyPump,
  }
}

/** Start the broker with a protected, pre-created socket directory and deterministic shutdown. */
export function startBroker(policyPath, projectorOptions = {}) {
  const policy = readPolicy(policyPath)
  const directory = statSync(dirname(policy.socket))
  const allowedUids = process.getuid?.() === 0
    ? new Set([0])
    : new Set([0, process.getuid?.()].filter(v => v !== undefined))
  if (!allowedUids.has(directory.uid) || (directory.mode & 0o022) || realpathSync(dirname(policy.socket)) !== dirname(policy.socket)) throw new Error('Unsafe socket directory')
  reconcileManualReplyConfigView(policy)
  const journal = new Operations(policy, policyPath, perform)
  let projectionLoop
  try {
    projectionLoop = createBrokerProjectionLoop(policy, {
      ...projectorOptions, policyPath, dispatchAutomaticReply: request => enqueueAutomaticReply(journal, request),
    })
  } catch (error) {
    journal.close()
    throw error
  }
  if (existsSync(policy.socket)) {
    projectionLoop?.stop()
    journal.close()
    throw new Error('Socket already exists; refuse a second broker')
  }
  const server = createServer(socket => {
    let input = Buffer.alloc(0)
    let accepted = false
    socket.setTimeout(policy.limits.commandSeconds * 1000 + 5000, () => socket.destroy())
    socket.on('error', () => { /* The request still settles and journals independently after client disconnect. */ })
    socket.on('data', chunk => {
      if (accepted) return
      input = Buffer.concat([input, chunk])
      if (input.length > MAX_FRAME) { socket.destroy(); return }
      const end = input.indexOf(10)
      if (end === -1) return
      accepted = true
      void (async () => {
        try { const result = await dispatch(JSON.parse(input.subarray(0, end).toString()), journal); socket.end(JSON.stringify({ ok: true, result }) + '\n') }
        catch (error) { socket.end(JSON.stringify({ ok: false, error: redact(String(error.message)).slice(0, 600) }) + '\n') }
      })()
    })
  })
  server.maxConnections = 8
  server.listen(policy.socket, () => {
    chmodSync(policy.socket, 0o660)
    if (process.getuid?.() === 0) {
      // systemd's RuntimeDirectory is root:octopus-agent, so its GID is the socket's authority.
      chownSync(policy.socket, 0, statSync(dirname(policy.socket)).gid)
    }
    projectionLoop?.start()
    console.log('Octopus operations broker ready')
  })
  let stopping = false
  const stop = () => {
    if (stopping) return
    stopping = true
    projectionLoop?.stop()
    try {
      writeAutoReplyPermit(readPolicy(policyPath), Date.now(), { revoke: true })
    } catch {
      // The already-issued permit expires if its trusted configuration is unavailable.
    }
    server.close()
  }
  server.once('close', () => {
    stopping = true
    projectionLoop?.stop()
    journal.close()
    if (existsSync(policy.socket)) unlinkSync(policy.socket)
    process.off('SIGTERM', stop)
    process.off('SIGINT', stop)
  })
  process.once('SIGTERM', stop)
  process.once('SIGINT', stop)
  return server
}

function runManualReplyCli(policyPath, argv) {
  const sub = argv[0]
  const opts = { action: sub }
  for (let i = 1; i < argv.length; i += 1) {
    if (argv[i] === '--lease-seconds') {
      opts.leaseSeconds = Number(argv[i + 1])
      i += 1
    } else if (argv[i] === '--max-age-seconds') {
      opts.maxAgeSeconds = Number(argv[i + 1])
      i += 1
    } else {
      throw new Error(`Unknown manual-reply option: ${argv[i]}`)
    }
  }
  const result = setManualReplyControl(readPolicy(policyPath), opts)
  console.log(JSON.stringify(result))
}

if (process.argv[1] && import.meta.url === pathToFileURL(realpathSync(process.argv[1])).href) {
  if (process.argv[3] === 'manual-reply') {
    runManualReplyCli(process.argv[2], process.argv.slice(4))
  } else {
    startBroker(process.argv[2])
  }
}
