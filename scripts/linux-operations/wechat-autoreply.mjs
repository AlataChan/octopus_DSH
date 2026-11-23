/**
 * Offline durable eligibility, restricted profile I/O, and one-claim decision bookkeeping
 * for WeChat auto-reply assessment (`scripts/linux-operations`).
 *
 * Production integration gates deferred to later phases:
 * - Live real-model qualification using the isolated `@deepseek-ai/dsh-app-boot` adapter
 *   (`createIsolatedDshAutoReplyAdapter` in `./wechat-autoreply-dsh.mjs`) with dedicated API credentials.
 * - Verified Gamma WeChat 4.1.13.26 desktop evidence and group @self/quote-self/self-echo
 *   proof extractors (including measured phone-to-desktop sync delay).
 * - Worker-side pre-send live chat tail recheck (`superseded_by_new_activity`) inside the
 *   single account lock immediately before UI click, plus Feishu receipt emission.
 */
import { randomBytes } from 'node:crypto'
import {
  chmodSync,
  existsSync,
  lstatSync,
  mkdirSync,
  openSync,
  closeSync,
  constants,
  fstatSync,
  readFileSync,
  realpathSync,
} from 'node:fs'
import { dirname, join, resolve, sep } from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import {
  AUTOREPLY_CONVERSATION_MODES,
  DEFAULT_BANNED_EXPRESSIONS,
  DEFAULT_WECHAT_AUTOREPLY_CORE_POLICY,
  fields,
  readPolicy,
  resolveAutoReplyCorePolicy,
  validateAutoReplyCorePolicy,
  wechatAutoReplyPolicy,
  wechatNotificationsPolicy,
} from './policy.mjs'
import { atomicJson, digest, resolveWechatRegistry } from './operations.mjs'
import {
  compareInboundWorkerEvents,
  formatNoticeId,
  readTrustedManualReplyView,
  resolveManualReplyViewPath,
  sanitizeUntrustedNoticeText,
  writeTrustedHandoffEnvelope,
} from './wechat-inbox.mjs'
import {
  DEFAULT_AUTOREPLY_API_KEY_ENV,
  ISOLATED_DSH_ENTRY_PACKAGE,
  ISOLATED_DSH_FORBIDDEN_BUNDLES,
  ISOLATED_DSH_FORBIDDEN_SERVICES,
  ISOLATED_DSH_OPTIONAL_TRANSPORT_PLUGIN,
  ISOLATED_DSH_PLUGINS,
  RESTRICTED_AUTOREPLY_PROFILE_SPEC,
  SYSTEM_PROMPT_POLICY,
  WECHAT_AUTOREPLY_OUTPUT_JSON_SCHEMA,
  createIsolatedDshAutoReplyAdapter,
  inspectIsolatedDshContext,
  loadDshRuntimeModules,
  resolveDshInstallAnchor,
} from './wechat-autoreply-dsh.mjs'

export {
  DEFAULT_AUTOREPLY_API_KEY_ENV,
  ISOLATED_DSH_ENTRY_PACKAGE,
  ISOLATED_DSH_FORBIDDEN_BUNDLES,
  ISOLATED_DSH_FORBIDDEN_SERVICES,
  ISOLATED_DSH_OPTIONAL_TRANSPORT_PLUGIN,
  ISOLATED_DSH_PLUGINS,
  RESTRICTED_AUTOREPLY_PROFILE_SPEC,
  WECHAT_AUTOREPLY_OUTPUT_JSON_SCHEMA,
  AUTOREPLY_CONVERSATION_MODES,
  DEFAULT_BANNED_EXPRESSIONS,
  createIsolatedDshAutoReplyAdapter,
  inspectIsolatedDshContext,
  loadDshRuntimeModules,
  resolveDshInstallAnchor,
}

export const WECHAT_AUTOREPLY_SCHEMA_VERSION = 1
export const AUTOREPLY_CHANNELS = Object.freeze(['friend', 'group'])
export const AUTOREPLY_TASK_STATES = Object.freeze([
  'claimed',
  'draft_ready',
  'needs_human',
  'needs_review',
  'notice_only',
  'unsupported',
  'expired',
  'sent_verified',
  'not_sent',
])
export const DEFAULT_WECHAT_AUTOREPLY_POLICY = DEFAULT_WECHAT_AUTOREPLY_CORE_POLICY

/** Validate an explicit WeChat auto-reply configuration object. */
export const validateAutoReplyPolicy = validateAutoReplyCorePolicy

/** Normalize optional auto-reply policy settings, defaulting strictly to disabled everywhere. */
export const resolveAutoReplyPolicy = resolveAutoReplyCorePolicy

const TASK_ID_RE = /^[0-9a-f]{24}$/
const HEX64_RE = /^[0-9a-f]{64}$/
const SAFE_PROVIDER_RUN_RE = /^[a-zA-Z0-9][a-zA-Z0-9._:-]{3,127}$/
const MAX_EVIDENCE_BYTES = 2 * 1024 * 1024
const MIN_OBSERVED_MS = 1_700_000_000_000
const MAX_FUTURE_SKEW_MS = 60_000
const AUTOMATIC_SEND_JOB_MS = 180_000
const AUTOMATIC_DRAFT_LIFETIME_MS = 60_000
const DEFINITELY_NOT_SENT_RECEIPTS = new Set(['failed_before_submit', 'refused', 'expired', 'auth_required'])
const GROUP_LIMIT_REASONS = new Set(['group_cooldown_active', 'group_model_attempt_limit', 'group_send_limit'])
const TRUSTED_AUTHOR_VERIFIERS = new Set(['left_bubble', 'avatar_anchor', 'card_anchor', 'native_copy'])
const TRUSTED_MENTION_VERIFIERS = new Set(['anchor', 'native_copy', 'mention_chip'])
const TRUSTED_QUOTE_VERIFIERS = new Set(['anchor', 'native_copy', 'quoted_card'])
const ALLOWED_REPORT_SCOPES = new Set(['daily_structure', 'implied_volatility', 'divergence_assumptions'])

// Deterministic global hard-routing baselines aligned with plan section 6.2.
const FINANCIAL_TRANSFER_RE = /(?:转账|汇款|收款|付款|打款|打钱|发红包|抢红包|红包|银行卡号?|支付宝|微信支付|借钱|垫付|transfer\s+money|wire\s+transfer|red\s+packet)/i
const SENSITIVE_DISCLOSURE_RE = /(?:微信号|手机号|手机号码|电话号码|身份证|银行账号|验证码|登录密码|密码|密钥|私钥|内网|服务器|ssh|api[_-]?key|access[_-]?token|operations\.json|operations\.sqlite|wechat-inbox|wechatroot|ip地址)/i
const LIVE_TRADE_OR_PROMISE_RE = /(?:买入|卖出|加仓|减仓|平仓|建仓|重仓|满仓|清仓|止损|止盈|抄底|逃顶|喊单|带单|具体点位|目标价|保证收益|承诺收益|稳赚|保本|必涨|必跌|保底收益|年化\s*\d)/i
const UNPUBLISHED_RESEARCH_RE = /(?:未发布|还没发布|尚未发布|未公开|内幕消息|内部研报|提前发我|盘中实时|实时净值|预测涨跌|明天怎么走)/i
const RESEARCH_TOPIC_RE = /(?:日报|市场|大盘|盘面|收盘|涨跌|指数|中证|创业板|科创|波动率|期权|gamma|分歧假设|回撤|净值|估值|策略|标的|持仓|行情|仓位|研究报告|研报|隐含波动|\b(?:iv|pcr|skew|vix)\b)/i
const IDENTITY_QUESTION_RE = /^(?:请)?(?:介绍(?:一下)?(?:你)?自己|你是谁|你是做什么的|你能做什么|你是什么(?:助手|机器人)|你是(?:观察者\s*)?Gamma(?:吗)?)\s*[。！!?？]*$/iu
const MARKET_DEFINITION_RE = /^(?:请(?:问|解释一下)?\s*)?(?:什么是\s*(?:期权|隐含波动率|波动率|IV|PCR|VIX|skew|Gamma)|(?:期权|隐含波动率|波动率|IV|PCR|VIX|skew|Gamma)\s*(?:是什么|是什么意思|怎么算|如何计算))\s*[。！!?？]*$/iu
const PROMPT_INJECTION_RE = /(?:ignore\s+(?:all\s+)?(?:previous|prior)\s+instructions|忽略(?:以上|之前|所有)(?:规则|指令|限制)|改发给|转发送给|调用\s*(?:wechat_|restart_|set_limits|shell|bash)|<\|im_start\|>)/i
const DRAFT_SECRET_RE = /(?:Bearer\s+[A-Za-z0-9._-]{6,}|(?:sk|ghp)_[A-Za-z0-9_-]{12,}|(?:api[_-]?key|access[_-]?token|password)\s*[:=])/i

function assertOptionalBoundedInt(value, defaultValue, min, max, fieldName) {
  if (value === undefined) return defaultValue
  if (!Number.isSafeInteger(value) || value < min || value > max) {
    throw new Error(`Invalid autoReply store bound ${fieldName}`)
  }
  return value
}

function defaultStatUid(stat) {
  return stat.uid
}

function assertCleanBoundedString(value, fieldName, maxBytes) {
  if (typeof value !== 'string' || !value.trim() || value.trim() !== value || value.includes('\0')) {
    throw new Error(`Invalid ${fieldName}`)
  }
  if (Buffer.byteLength(value, 'utf8') > maxBytes) {
    throw new Error(`${fieldName} exceeds ${maxBytes} byte limit`)
  }
  return value
}

function expectedBrokerUids() {
  const uid = process.getuid?.()
  return uid === 0 ? new Set([0]) : new Set([0, uid].filter(v => v !== undefined))
}

function openPrivateDirAndSqlite(stateDir, fileName, { readOnly = false, statUid = defaultStatUid } = {}) {
  if (!stateDir || typeof stateDir !== 'string' || !stateDir.startsWith('/')) {
    throw new Error('Absolute private state directory is required')
  }
  if (!existsSync(stateDir)) {
    if (readOnly) throw new Error('Private state directory is missing')
    mkdirSync(stateDir, { recursive: true, mode: 0o700 })
    chmodSync(stateDir, 0o700)
  }
  const dirStat = lstatSync(stateDir)
  if (dirStat.isSymbolicLink() || !dirStat.isDirectory() || (dirStat.mode & 0o077) !== 0) {
    throw new Error('Private state directory has unsafe path or permissions')
  }
  const allowed = expectedBrokerUids()
  if (!allowed.has(statUid(dirStat, stateDir))) {
    throw new Error('Private state directory owner uid is untrusted')
  }
  const dbPath = join(stateDir, fileName)
  if (existsSync(dbPath)) {
    const dbStat = lstatSync(dbPath)
    if (dbStat.isSymbolicLink() || !dbStat.isFile() || (dbStat.mode & 0o077) !== 0) {
      throw new Error(`${fileName} must be a private regular non-symlinked file`)
    }
    if (!allowed.has(statUid(dbStat, dbPath))) {
      throw new Error(`${fileName} owner uid is untrusted`)
    }
  } else if (readOnly) {
    throw new Error(`${fileName} is unavailable`)
  }
  const db = new DatabaseSync(dbPath, readOnly ? { readOnly: true } : {})
  if (!readOnly) {
    chmodSync(dbPath, 0o600)
    db.exec('PRAGMA journal_mode=WAL')
    db.exec('PRAGMA synchronous=FULL')
    db.exec('PRAGMA busy_timeout=5000')
  }
  return db
}

function readVerifiedInboxBatch(policy, batchId, { contextLimit, statUid = defaultStatUid }) {
  if (typeof batchId !== 'string' || !batchId.trim() || batchId.length > 64) {
    throw new Error('Invalid sealed batch identifier')
  }
  const db = openPrivateDirAndSqlite(policy.state, 'wechat-inbox.sqlite', { readOnly: true, statUid })
  try {
    const noticeCols = new Set(db.prepare('PRAGMA table_info(projected_notices)').all().map(col => col.name))
    const batchCols = new Set(db.prepare('PRAGMA table_info(inbound_batches)').all().map(col => col.name))
    for (const reqCol of ['conversation_kind', 'account_sha256', 'contact_id', 'target_proof', 'reply_eligible', 'source_observed_at']) {
      if (!noticeCols.has(reqCol)) throw new Error('Inbox ledger lacks verified notice binding columns')
    }
    for (const reqCol of ['account_sha256', 'contact_id', 'target_proof', 'reply_eligible']) {
      if (!batchCols.has(reqCol)) throw new Error('Inbox ledger lacks verified batch binding columns')
    }

    const batch = db.prepare('SELECT * FROM inbound_batches WHERE batch_id=?').get(batchId)
    if (!batch) {
      throw new Error(`Unknown inbound batch ${batchId}`)
    }
    if (batch.status !== 'sealed' || !Number.isSafeInteger(batch.sealed_at)) {
      throw new Error(`Inbound batch ${batchId} is not sealed`)
    }
    const notices = db.prepare('SELECT * FROM projected_notices WHERE source_ref=? AND kind=?').all(batchId, 'inbound_alert')
    if (notices.length !== 1) {
      throw new Error(`Expected single projected inbound notice for sealed batch ${batchId}`)
    }
    const notice = notices[0]
    if (
      notice.notice_id !== batch.notice_id
      || notice.notice_key !== `inbound:${batch.batch_id}`
      || notice.conversation_key !== batch.conversation_key
      || notice.conversation_kind !== batch.conversation_kind
      || notice.account_sha256 !== batch.account_sha256
      || notice.contact_id !== batch.contact_id
      || notice.target_proof !== batch.target_proof
    ) {
      throw new Error('Sealed batch metadata does not match projected notice')
    }

    const eventRows = db.prepare(
      'SELECT event_id, observed_at, payload FROM ingested_events WHERE batch_id=? ORDER BY observed_at ASC, event_id ASC',
    ).all(batch.batch_id)
    if (eventRows.length === 0 || eventRows.length !== batch.event_count) {
      throw new Error('Sealed batch event set is incomplete')
    }
    const events = eventRows.map(row => JSON.parse(row.payload)).sort(compareInboundWorkerEvents)

    // Strictly isolate context queries to the single authorized conversation_key.
    const contextRows = db.prepare(
      'SELECT event_id, observed_at, summary FROM conversation_context WHERE conversation_key=? ORDER BY observed_at DESC, id DESC LIMIT ?',
    ).all(batch.conversation_key, contextLimit).reverse()

    return { batch, notice, events, contextRows }
  } finally {
    db.close()
  }
}

function verifyEvidenceFileOnDisk(policy, evidencePath, expectedSha256, { statUid = defaultStatUid }) {
  if (typeof evidencePath !== 'string' || !evidencePath.trim() || evidencePath.includes('\0')) {
    return { ok: false, reason: 'missing_evidence_path' }
  }
  const trimmed = evidencePath.trim()
  const prefix = '/home/wechat/.gamma-moments/'
  const mapped = trimmed.startsWith(prefix)
    ? join(policy.wechatRoot, trimmed.slice(prefix.length))
    : (trimmed.startsWith('/') ? trimmed : join(policy.wechatRoot, trimmed))
  const rootReal = realpathSync(policy.wechatRoot)
  const targetAbs = resolve(mapped)
  if (targetAbs !== resolve(policy.wechatRoot) && !targetAbs.startsWith(resolve(policy.wechatRoot) + sep)) {
    return { ok: false, reason: 'evidence_escapes_wechat_root' }
  }
  if (!existsSync(targetAbs)) {
    return { ok: false, reason: 'missing_evidence_file' }
  }
  const ls = lstatSync(targetAbs)
  if (ls.isSymbolicLink() || !ls.isFile() || realpathSync(dirname(targetAbs)) !== rootReal && !realpathSync(dirname(targetAbs)).startsWith(rootReal + sep)) {
    return { ok: false, reason: 'untrusted_evidence_file' }
  }
  const notifCfg = wechatNotificationsPolicy(policy)
  const expectedUids = notifCfg.inboundProducerUid !== null
    ? new Set([notifCfg.inboundProducerUid])
    : expectedBrokerUids()
  const fd = openSync(targetAbs, constants.O_RDONLY | constants.O_NOFOLLOW)
  try {
    const st = fstatSync(fd)
    if (!st.isFile() || (st.mode & 0o027) !== 0 || st.size === 0 || st.size > MAX_EVIDENCE_BYTES) {
      return { ok: false, reason: 'untrusted_evidence_descriptor' }
    }
    if (!expectedUids.has(statUid(st, targetAbs))) {
      return { ok: false, reason: 'evidence_uid_mismatch' }
    }
    const buf = readFileSync(fd)
    if (digest(buf) !== expectedSha256) {
      return { ok: false, reason: 'evidence_sha256_mismatch' }
    }
    return { ok: true, reason: null }
  } finally {
    closeSync(fd)
  }
}

export function checkOperatorBindingActive(policy) {
  if (!policy.ingressDatabase || !existsSync(policy.ingressDatabase)) {
    return { ok: false, reason: 'operator_binding_missing' }
  }
  const dbStat = lstatSync(policy.ingressDatabase)
  if (dbStat.isSymbolicLink() || !dbStat.isFile()) {
    return { ok: false, reason: 'operator_binding_untrusted' }
  }
  const db = new DatabaseSync(policy.ingressDatabase, { readOnly: true })
  try {
    const row = db.prepare('SELECT value FROM u_im_ingress_operators WHERE key=?').get(policy.bindingKey)
    if (!row) return { ok: false, reason: 'operator_binding_missing' }
    const binding = JSON.parse(row.value)
    if (
      binding.revokedAt !== null
      || binding.operatorId !== policy.operatorId
      || binding.targetTenantId !== policy.tenantId
    ) {
      return { ok: false, reason: 'operator_binding_revoked' }
    }
    return { ok: true, reason: null }
  } catch {
    return { ok: false, reason: 'operator_binding_unreadable' }
  } finally {
    db.close()
  }
}

function verifyRuntimeManualReplyViewGate(policy, expectedAccountSha, nowMs, statUid) {
  const viewPath = resolveManualReplyViewPath(policy)
  if (!viewPath || !existsSync(viewPath)) {
    return { ok: true, pauseMaxAge: null, reason: null }
  }
  let view
  try {
    view = readTrustedManualReplyView(policy, { statUid })
  } catch {
    return { ok: false, state: 'needs_review', reason: 'pause_view_untrusted' }
  }
  if (!view) {
    return { ok: false, state: 'needs_review', reason: 'pause_view_untrusted' }
  }
  if (view.account_sha256 !== expectedAccountSha) {
    return { ok: false, state: 'needs_review', reason: 'account_binding_changed' }
  }
  if (view.paused === true) {
    return { ok: false, state: 'notice_only', reason: 'global_paused' }
  }
  if (view.lease_expires_at !== null && nowMs >= view.lease_expires_at) {
    return { ok: false, state: 'notice_only', reason: 'global_pause_lease_expired' }
  }
  return { ok: true, pauseMaxAge: view.max_age_seconds, reason: null }
}

function validateSignalProofForEvent(event, signal, expectedChannel, allowRegisteredSenderMentions) {
  if (!signal || typeof signal !== 'object' || Array.isArray(signal)) {
    return { ok: false, state: 'needs_review', reason: 'unverified_source_proof' }
  }
  try {
    fields(
      signal,
      ['accountSha256', 'targetProof', 'evidenceSha256', 'senderRole'],
      ['authorProof', 'uncertain', 'groupTrigger', 'mentionSelfProof', 'quoteSelfProof'],
    )
  } catch {
    return { ok: false, state: 'needs_review', reason: 'invalid_source_proof_fields' }
  }
  if (
    signal.accountSha256 !== event.accountSha256
    || signal.targetProof !== event.targetProof
    || signal.evidenceSha256 !== event.evidenceSha256
  ) {
    return { ok: false, state: 'needs_review', reason: 'source_proof_binding_mismatch' }
  }
  if (signal.uncertain === true) {
    return { ok: false, state: 'needs_review', reason: 'uncertain_desktop_observation' }
  }
  if (signal.senderRole === 'self' || signal.senderRole === 'robot') {
    return { ok: false, state: 'notice_only', reason: 'self_or_robot_echo' }
  }
  if (signal.senderRole === 'unknown') {
    return { ok: false, state: 'needs_review', reason: 'unknown_sender_proof' }
  }
  if (signal.senderRole !== 'peer') {
    return { ok: false, state: 'needs_review', reason: 'invalid_sender_role' }
  }

  // Require an explicit desktop author proof tied to account, target contact, and intake evidence for every peer bubble.
  const authorProof = signal.authorProof
  if (!authorProof || typeof authorProof !== 'object' || Array.isArray(authorProof)) {
    return { ok: false, state: 'needs_review', reason: 'missing_or_invalid_author_proof' }
  }
  try {
    fields(authorProof, ['accountSha256', 'targetProof', 'evidenceSha256', 'verifiedBy'])
  } catch {
    return { ok: false, state: 'needs_review', reason: 'missing_or_invalid_author_proof' }
  }
  if (
    authorProof.accountSha256 !== event.accountSha256
    || authorProof.targetProof !== event.targetProof
    || authorProof.evidenceSha256 !== event.evidenceSha256
    || !TRUSTED_AUTHOR_VERIFIERS.has(authorProof.verifiedBy)
  ) {
    return { ok: false, state: 'needs_review', reason: 'missing_or_invalid_author_proof' }
  }

  if (expectedChannel !== 'group') {
    return { ok: true, state: 'eligible', reason: null }
  }
  if (signal.groupTrigger === 'at_all') {
    return { ok: false, state: 'notice_only', reason: 'group_at_all_notice_only' }
  }
  if (signal.groupTrigger === 'ordinary' || signal.groupTrigger === 'mention_other') {
    return { ok: false, state: 'notice_only', reason: 'group_not_targeted' }
  }
  if (signal.groupTrigger === 'mention_self') {
    const proof = signal.mentionSelfProof
    if (!proof || typeof proof !== 'object' || Array.isArray(proof)) {
      return { ok: false, state: 'needs_review', reason: 'group_unverified_mention_proof' }
    }
    try {
      fields(proof, ['accountSha256', 'evidenceSha256', 'verifiedBy'])
    } catch {
      return { ok: false, state: 'needs_review', reason: 'group_unverified_mention_proof' }
    }
    if (
      proof.accountSha256 !== event.accountSha256
      || proof.evidenceSha256 !== event.evidenceSha256
      || (event.sourceProof?.triggerVerifier === 'registered_sender' && proof.verifiedBy !== 'registered_sender')
      || !(TRUSTED_MENTION_VERIFIERS.has(proof.verifiedBy) || (
        proof.verifiedBy === 'registered_sender' && allowRegisteredSenderMentions
        && authorProof.verifiedBy === 'native_copy'
        && event.sourceProof?.author === 'native_copy'
        && event.sourceProof?.trigger === 'mention_self'
        && event.sourceProof?.triggerVerifier === 'registered_sender'
        && event.sourceProof?.latest === true
      ))
    ) {
      return { ok: false, state: 'needs_review', reason: 'group_unverified_mention_proof' }
    }
    return { ok: true, state: 'eligible', reason: null }
  }
  if (signal.groupTrigger === 'quote_self') {
    const proof = signal.quoteSelfProof
    if (!proof || typeof proof !== 'object' || Array.isArray(proof)) {
      return { ok: false, state: 'needs_review', reason: 'group_unverified_quote_proof' }
    }
    try {
      fields(proof, ['accountSha256', 'evidenceSha256', 'quotedSourceRef', 'verifiedBy'])
    } catch {
      return { ok: false, state: 'needs_review', reason: 'group_unverified_quote_proof' }
    }
    if (
      proof.accountSha256 !== event.accountSha256
      || proof.evidenceSha256 !== event.evidenceSha256
      || typeof proof.quotedSourceRef !== 'string'
      || !proof.quotedSourceRef.trim()
      || !TRUSTED_QUOTE_VERIFIERS.has(proof.verifiedBy)
    ) {
      return { ok: false, state: 'needs_review', reason: 'group_unverified_quote_proof' }
    }
    return { ok: true, state: 'eligible', reason: null }
  }
  return { ok: false, state: 'needs_review', reason: 'group_unverified_trigger_proof' }
}

function evaluateBatchSignalProofs(events, signalMap, channel, allowRegisteredSenderMentions) {
  if (!signalMap || typeof signalMap !== 'object' || Array.isArray(signalMap)) {
    return {
      ok: false,
      state: 'needs_review',
      reason: channel === 'group' ? 'group_unverified_trigger_proof' : 'friend_unverified_source_proof',
    }
  }
  let sawTargetedGroup = channel !== 'group'
  let sawAtAll = false
  for (const ev of events) {
    const sig = Object.hasOwn(signalMap, ev.eventId) ? signalMap[ev.eventId] : null
    if (sig === null) {
      return {
        ok: false,
        state: 'needs_review',
        reason: channel === 'group' ? 'group_unverified_trigger_proof' : 'friend_unverified_source_proof',
      }
    }
    const checked = validateSignalProofForEvent(ev, sig, channel, allowRegisteredSenderMentions)
    if (checked.ok) {
      if (channel === 'group') sawTargetedGroup = true
      continue
    }
    // In a multi-event group batch, ordinary/at_all chatter can coexist with a targeted @self/quote-self message,
    // whereas echo, uncertain, or invalid/unknown proof always fails closed.
    if (channel === 'group' && (checked.reason === 'group_at_all_notice_only' || checked.reason === 'group_not_targeted')) {
      if (checked.reason === 'group_at_all_notice_only') sawAtAll = true
      continue
    }
    return checked
  }
  if (!sawTargetedGroup) {
    return {
      ok: false,
      state: 'notice_only',
      reason: sawAtAll ? 'group_at_all_notice_only' : 'group_not_targeted',
    }
  }
  return { ok: true, state: 'eligible', reason: null }
}

function desktopReplyOrigin(event) {
  const proof = event?.sourceProof
  if (!event?.actionable || !proof?.latest || !proof.exactText || !HEX64_RE.test(proof.textSha256 ?? '')) return null
  return {
    source_ref: event.sourceRef,
    text_sha256: proof.textSha256,
    evidence_sha256: event.evidenceSha256,
    observed_at: event.observedAt,
  }
}

function desktopEvaluationInputs(batch, events) {
  const eventSignals = Object.fromEntries(events.flatMap(event => {
    const proof = event.sourceProof
    if (!event.actionable || !proof) return []
    const evidence = { accountSha256: event.accountSha256, evidenceSha256: event.evidenceSha256 }
    return [[event.eventId, {
      ...evidence,
      targetProof: event.targetProof,
      senderRole: 'peer',
      authorProof: { ...evidence, targetProof: event.targetProof, verifiedBy: proof.author },
      ...(event.conversationKind === 'group' ? { groupTrigger: proof.trigger ?? 'ordinary' } : {}),
      ...(proof.trigger === 'mention_self' ? { mentionSelfProof: { ...evidence, verifiedBy: proof.triggerVerifier } } : {}),
      ...(proof.trigger === 'quote_self' ? { quoteSelfProof: {
        ...evidence, verifiedBy: proof.triggerVerifier, quotedSourceRef: proof.quotedSourceRef,
      } } : {}),
    }]]
  }))
  const last = events.at(-1)
  const origin = desktopReplyOrigin(last)
  if (!origin) return { eventSignals }
  const tailItems = [{
    senderRole: 'peer',
    textSha256: digest(last.text),
    sourceRef: last.sourceRef,
    sourceTime: last.sourceTime,
  }]
  return { eventSignals, tailState: {
    accountSha256: batch.account_sha256,
    conversationKey: batch.conversation_key,
    targetProof: batch.target_proof,
    verified: true,
    evidenceSha256: last.evidenceSha256,
    evidencePath: last.evidencePath,
    tailObservedAt: last.observedAt,
    lastSourceRef: last.sourceRef,
    tailItems,
    tailDigest: digest(JSON.stringify(tailItems)),
    ownReplyObservedAfterTrigger: false,
    peerFollowUpObservedAfterTrigger: false,
  } }
}

function validateTailObservation({
  policy,
  batch,
  events,
  tailState,
  requestedMode,
  maxTailAgeSeconds,
  nowMs,
  verifyEvidenceFiles,
  statUid,
}) {
  if (tailState === null || tailState === undefined) {
    if (requestedMode === 'unattended') {
      return { ok: false, state: 'needs_review', reason: 'tail_snapshot_required_for_unattended', marker: 'missing_tail' }
    }
    return { ok: true, tailMarker: 'draft_only_no_tail', reason: null }
  }
  if (typeof tailState !== 'object' || Array.isArray(tailState)) {
    return { ok: false, state: 'needs_review', reason: 'tail_uncertain', marker: 'tail_uncertain' }
  }
  try {
    fields(
      tailState,
      [
        'accountSha256',
        'conversationKey',
        'targetProof',
        'verified',
        'evidenceSha256',
        'tailObservedAt',
        'lastSourceRef',
        'tailItems',
        'tailDigest',
        'ownReplyObservedAfterTrigger',
        'peerFollowUpObservedAfterTrigger',
      ],
      ['uncertain', 'evidencePath'],
    )
  } catch {
    return { ok: false, state: 'needs_review', reason: 'tail_proof_invalid', marker: 'tail_proof_invalid' }
  }
  if (
    tailState.accountSha256 !== batch.account_sha256
    || tailState.conversationKey !== batch.conversation_key
    || tailState.targetProof !== batch.target_proof
  ) {
    return { ok: false, state: 'needs_review', reason: 'tail_proof_mismatch', marker: 'tail_proof_mismatch' }
  }
  if (tailState.verified !== true || tailState.uncertain === true) {
    return { ok: false, state: 'needs_review', reason: 'tail_uncertain', marker: 'tail_uncertain' }
  }
  if (tailState.ownReplyObservedAfterTrigger !== false) {
    return { ok: false, state: 'needs_review', reason: 'superseded_by_new_activity:own_reply_observed', marker: 'superseded_own' }
  }
  if (tailState.peerFollowUpObservedAfterTrigger !== false) {
    return { ok: false, state: 'needs_review', reason: 'superseded_by_new_activity:peer_followup_observed', marker: 'superseded_peer' }
  }
  if (typeof tailState.evidenceSha256 !== 'string' || !HEX64_RE.test(tailState.evidenceSha256)) {
    return { ok: false, state: 'needs_review', reason: 'tail_proof_invalid', marker: 'tail_bad_evidence' }
  }
  if (Object.hasOwn(tailState, 'evidencePath') && tailState.evidencePath !== null && verifyEvidenceFiles) {
    const evCheck = verifyEvidenceFileOnDisk(policy, tailState.evidencePath, tailState.evidenceSha256, { statUid })
    if (!evCheck.ok) {
      return { ok: false, state: 'needs_review', reason: `tail_evidence_untrusted:${evCheck.reason}`, marker: 'tail_bad_disk_evidence' }
    }
  }
  const latestEventObservedAt = Math.max(...events.map(ev => Number(ev.observedAt)))
  if (
    !Number.isSafeInteger(tailState.tailObservedAt)
    || tailState.tailObservedAt < MIN_OBSERVED_MS
    || tailState.tailObservedAt > nowMs + MAX_FUTURE_SKEW_MS
    || tailState.tailObservedAt < latestEventObservedAt
    || nowMs - tailState.tailObservedAt > maxTailAgeSeconds * 1000
  ) {
    return { ok: false, state: 'needs_review', reason: 'tail_snapshot_stale', marker: 'tail_stale' }
  }
  if (
    typeof tailState.lastSourceRef !== 'string'
    || !tailState.lastSourceRef.trim()
    || !Array.isArray(tailState.tailItems)
    || tailState.tailItems.length === 0
    || tailState.tailItems.length > 20
  ) {
    return { ok: false, state: 'needs_review', reason: 'tail_proof_invalid', marker: 'tail_empty' }
  }
  for (const item of tailState.tailItems) {
    try {
      fields(item, ['senderRole', 'textSha256', 'sourceRef', 'sourceTime'])
    } catch {
      return { ok: false, state: 'needs_review', reason: 'tail_proof_invalid', marker: 'tail_bad_item' }
    }
    if (!['peer', 'self', 'unknown'].includes(item.senderRole)) {
      return { ok: false, state: 'needs_review', reason: 'tail_proof_invalid', marker: 'tail_bad_role' }
    }
    if (item.senderRole === 'unknown') {
      return { ok: false, state: 'needs_review', reason: 'tail_contains_unknown_sender', marker: 'tail_unknown_sender' }
    }
    if (
      typeof item.textSha256 !== 'string'
      || !HEX64_RE.test(item.textSha256)
      || typeof item.sourceRef !== 'string'
      || !item.sourceRef.trim()
      || typeof item.sourceTime !== 'string'
      || !item.sourceTime.trim()
    ) {
      return { ok: false, state: 'needs_review', reason: 'tail_proof_invalid', marker: 'tail_bad_item_fields' }
    }
  }
  if (typeof tailState.tailDigest !== 'string' || !HEX64_RE.test(tailState.tailDigest) || digest(JSON.stringify(tailState.tailItems)) !== tailState.tailDigest) {
    return { ok: false, state: 'needs_review', reason: 'tail_digest_mismatch', marker: 'tail_digest_mismatch' }
  }
  const bottomItem = tailState.tailItems[tailState.tailItems.length - 1]
  const lastTriggerEvent = events[events.length - 1]
  if (
    bottomItem.senderRole !== 'peer'
    || bottomItem.sourceRef !== tailState.lastSourceRef
    || tailState.lastSourceRef !== lastTriggerEvent.sourceRef
    || bottomItem.textSha256 !== digest(lastTriggerEvent.text)
  ) {
    return { ok: false, state: 'needs_review', reason: 'tail_does_not_match_trigger_event', marker: 'tail_trigger_mismatch' }
  }
  return {
    ok: true,
    tailMarker: `${tailState.tailObservedAt}:${tailState.tailDigest}:${tailState.evidenceSha256}`,
    reason: null,
  }
}

function checkReportBundleTimestamps(bundle, { maxReportAgeSeconds, nowMs }) {
  if (
    !Number.isSafeInteger(bundle.publishedAtMs)
    || bundle.publishedAtMs < MIN_OBSERVED_MS
    || bundle.publishedAtMs > nowMs
    || nowMs - bundle.publishedAtMs > maxReportAgeSeconds * 1000
  ) {
    return { ok: false, reason: 'published_report_outside_publication_window' }
  }
  if (!Number.isSafeInteger(bundle.validUntilMs) || bundle.validUntilMs <= bundle.publishedAtMs || nowMs >= bundle.validUntilMs) {
    return { ok: false, reason: 'published_report_cutoff_expired' }
  }
  return { ok: true }
}

async function resolveVerifiedPublishedReport({
  provider,
  batch,
  channel,
  bannedExpressions,
  maxReportAgeSeconds,
  policyVersion,
  nowMs,
  signal,
}) {
  if (!provider || typeof provider.getVerifiedReportFacts !== 'function') {
    return { ok: false, reason: 'no_published_report_provider', bundle: null }
  }
  if (signal?.aborted) {
    return { ok: false, reason: 'published_report_aborted', bundle: null }
  }
  let rawBundle
  try {
    rawBundle = await provider.getVerifiedReportFacts({
      conversationKey: batch.conversation_key,
      channel,
      accountSha256: batch.account_sha256,
      targetProof: batch.target_proof,
      policyVersion,
      nowMs,
      signal,
    })
  } catch (error) {
    const msg = sanitizeUntrustedNoticeText(error instanceof Error ? error.message : String(error), { singleLine: true, maxBytes: 120 })
    return { ok: false, reason: `published_report_provider_error:${msg || 'failed'}`, bundle: null }
  }

  try {
    fields(rawBundle, ['schemaVersion', 'providerId', 'publishedOnly', 'report'])
    if (rawBundle.schemaVersion !== WECHAT_AUTOREPLY_SCHEMA_VERSION || rawBundle.publishedOnly !== true) {
      return { ok: false, reason: 'published_report_not_published_only', bundle: null }
    }
    if (typeof rawBundle.providerId !== 'string' || !SAFE_PROVIDER_RUN_RE.test(rawBundle.providerId)) {
      return { ok: false, reason: 'published_report_invalid_provider_id', bundle: null }
    }
    const r = rawBundle.report
    fields(
      r,
      ['reportId', 'reportVersion', 'reportDigest', 'publishedAtMs', 'validUntilMs', 'authorizedConversations', 'allowedScopes', 'facts'],
      ['accountSha256'],
    )
    const reportId = assertCleanBoundedString(r.reportId, 'reportId', 64)
    if (!Number.isSafeInteger(r.reportVersion) || r.reportVersion < 1) {
      return { ok: false, reason: 'published_report_invalid_version', bundle: null }
    }
    if (typeof r.reportDigest !== 'string' || !HEX64_RE.test(r.reportDigest)) {
      return { ok: false, reason: 'published_report_invalid_digest', bundle: null }
    }
    if (Object.hasOwn(r, 'accountSha256') && r.accountSha256 !== null && r.accountSha256 !== batch.account_sha256) {
      return { ok: false, reason: 'published_report_account_mismatch', bundle: null }
    }
    if (
      !Number.isSafeInteger(r.publishedAtMs)
      || r.publishedAtMs < MIN_OBSERVED_MS
      || r.publishedAtMs > nowMs
      || nowMs - r.publishedAtMs > maxReportAgeSeconds * 1000
    ) {
      return { ok: false, reason: 'published_report_outside_publication_window', bundle: null }
    }
    if (!Number.isSafeInteger(r.validUntilMs) || r.validUntilMs <= r.publishedAtMs || nowMs >= r.validUntilMs) {
      return { ok: false, reason: 'published_report_cutoff_expired', bundle: null }
    }
    if (!Array.isArray(r.authorizedConversations) || !r.authorizedConversations.includes(batch.conversation_key)) {
      return { ok: false, reason: 'published_report_conversation_not_authorized', bundle: null }
    }
    if (!Array.isArray(r.allowedScopes) || r.allowedScopes.length === 0 || r.allowedScopes.some(s => !ALLOWED_REPORT_SCOPES.has(s))) {
      return { ok: false, reason: 'published_report_disallowed_scope', bundle: null }
    }
    if (!Array.isArray(r.facts) || r.facts.length === 0 || r.facts.length > 10) {
      return { ok: false, reason: 'published_report_empty_or_oversized', bundle: null }
    }
    const allowedSet = new Set(r.allowedScopes)
    const verifiedFacts = []
    for (const fact of r.facts) {
      fields(fact, ['factId', 'scope', 'digest', 'text'])
      const factId = assertCleanBoundedString(fact.factId, 'factId', 64)
      if (typeof fact.scope !== 'string' || !allowedSet.has(fact.scope)) {
        return { ok: false, reason: 'published_report_fact_scope_mismatch', bundle: null }
      }
      if (typeof fact.digest !== 'string' || !HEX64_RE.test(fact.digest)) {
        return { ok: false, reason: 'published_report_fact_invalid_digest', bundle: null }
      }
      const cleanText = sanitizeUntrustedNoticeText(fact.text, { singleLine: true, maxBytes: 400 })
      if (!cleanText || digest(cleanText) !== fact.digest) {
        return { ok: false, reason: 'published_report_fact_digest_mismatch', bundle: null }
      }
      if (
        FINANCIAL_TRANSFER_RE.test(cleanText)
        || SENSITIVE_DISCLOSURE_RE.test(cleanText)
        || LIVE_TRADE_OR_PROMISE_RE.test(cleanText)
        || UNPUBLISHED_RESEARCH_RE.test(cleanText)
        || bannedExpressions.some(expr => expr && cleanText.includes(expr))
      ) {
        return { ok: false, reason: 'published_report_fact_policy_violation', bundle: null }
      }
      verifiedFacts.push(Object.freeze({
        providerId: rawBundle.providerId,
        reportId,
        reportVersion: r.reportVersion,
        reportDigest: r.reportDigest,
        publishedAtMs: r.publishedAtMs,
        validUntilMs: r.validUntilMs,
        factId,
        scope: fact.scope,
        factDigest: fact.digest,
        text: cleanText,
      }))
    }
    return {
      ok: true,
      reason: null,
      bundle: Object.freeze({
        providerId: rawBundle.providerId,
        reportId,
        reportVersion: r.reportVersion,
        reportDigest: r.reportDigest,
        publishedAtMs: r.publishedAtMs,
        validUntilMs: r.validUntilMs,
        facts: Object.freeze(verifiedFacts),
      }),
    }
  } catch (error) {
    const msg = sanitizeUntrustedNoticeText(error instanceof Error ? error.message : String(error), { singleLine: true, maxBytes: 120 })
    return { ok: false, reason: `published_report_invalid_schema:${msg || 'malformed'}`, bundle: null }
  }
}

/**
 * Deterministic hard-routing classifier aligned with plan section 6.2.
 * Routes financial transfers, disclosure requests, live trade/return promises, banned expressions,
 * and unverified/unconfigured research conservatively to `needs_human`.
 * @param {object} params
 * @returns {{ route: 'allow' | 'needs_human', category: string | null, reason: string | null }}
 */
function requiresPublishedReport(text) {
  return RESEARCH_TOPIC_RE.test(text) && !IDENTITY_QUESTION_RE.test(text.trim()) && !MARKET_DEFINITION_RE.test(text.trim())
}

export function classifyInboundRiskAndFacts({
  events,
  conversationRule,
  verifiedReportBundle = null,
  bannedExpressions = DEFAULT_BANNED_EXPRESSIONS,
}) {
  const combined = events
    .map(ev => [ev.text ?? '', ev.quotedText ?? ''].filter(Boolean).join('\n'))
    .join('\n')
  if (FINANCIAL_TRANSFER_RE.test(combined)) {
    return { route: 'needs_human', category: 'financial_transaction', reason: 'Inbound involves transfer, red packet, or financial transaction' }
  }
  if (SENSITIVE_DISCLOSURE_RE.test(combined)) {
    return { route: 'needs_human', category: 'sensitive_disclosure', reason: 'Inbound requests WeChat ID, phone, credentials, or internal server information' }
  }
  if (LIVE_TRADE_OR_PROMISE_RE.test(combined)) {
    return { route: 'needs_human', category: 'trade_instruction_or_promise', reason: 'Inbound asks for live trade execution, target price call, or return promise' }
  }
  for (const expr of bannedExpressions) {
    if (expr && combined.includes(expr)) {
      return { route: 'needs_human', category: 'banned_expression', reason: `Inbound involves prohibited strategy phrase (${expr})` }
    }
  }
  if (UNPUBLISHED_RESEARCH_RE.test(combined)) {
    return { route: 'needs_human', category: 'unpublished_research', reason: 'Inbound requests unpublished or unverified research' }
  }
  if (PROMPT_INJECTION_RE.test(combined)) {
    return { route: 'needs_human', category: 'untrusted_injection_or_unknown', reason: 'Inbound contains untrusted instruction or target-override attempt' }
  }
  if (events.some(ev => requiresPublishedReport(ev.text ?? '') || requiresPublishedReport(ev.quotedText ?? ''))) {
    if (!conversationRule?.allowPublishedReportFacts) {
      return { route: 'needs_human', category: 'research_requires_report_opt_in', reason: 'Research topic requires published-report opt-in for this conversation' }
    }
    if (!verifiedReportBundle || !Array.isArray(verifiedReportBundle.facts) || verifiedReportBundle.facts.length === 0) {
      return { route: 'needs_human', category: 'no_published_report_provider', reason: 'No verified published-report bundle available from publication provider' }
    }
  }
  return { route: 'allow', category: null, reason: null }
}

/**
 * Build the restricted profile payload without leaking other chats or placing untrusted text in systemPrompt.
 * @param {object} params
 * @returns {object}
 */
export function buildRestrictedProfilePromptPayload({
  taskId,
  policyVersion,
  channel,
  requestedMode,
  maxReplyChars,
  conversationRule,
  contextRows,
  events,
  publishedReportFacts = [],
  bannedExpressions = DEFAULT_BANNED_EXPRESSIONS,
}) {
  const envelope = Object.freeze({
    schemaVersion: WECHAT_AUTOREPLY_SCHEMA_VERSION,
    taskId,
    policyVersion,
    channel,
    requestedMode,
    maxReplyChars,
    styleConstraints: Object.freeze({
      tone: conversationRule.tone ?? (channel === 'group' ? 'restrained_research' : 'natural_concise'),
      allowedTopics: conversationRule.allowedTopics ?? Object.freeze([]),
      bannedExpressions: Object.freeze([...bannedExpressions]),
    }),
    publishedReportFacts: conversationRule.allowPublishedReportFacts ? Object.freeze([...publishedReportFacts]) : Object.freeze([]),
    untrustedConversationContext: Object.freeze(contextRows.map((row, idx) => Object.freeze({
      seq: idx + 1,
      role: 'inbound_summary',
      observedAt: row.observed_at,
      summary: sanitizeUntrustedNoticeText(row.summary, { singleLine: true, maxBytes: 256 }),
      untrusted: true,
      authority: 'none',
    }))),
    untrustedInboundMessages: Object.freeze(events.map(ev => Object.freeze({
      eventId: ev.eventId,
      observedAt: ev.observedAt,
      sourceTime: sanitizeUntrustedNoticeText(ev.sourceTime ?? 'unknown', { singleLine: true, maxBytes: 64 }),
      senderDisplayLabel: sanitizeUntrustedNoticeText(ev.senderName ?? '', { singleLine: true, maxBytes: 128 }),
      text: sanitizeUntrustedNoticeText(ev.text ?? '', { singleLine: true, maxBytes: 1024 }),
      quotedSenderDisplayLabel: ev.quotedSender ? sanitizeUntrustedNoticeText(ev.quotedSender, { singleLine: true, maxBytes: 128 }) : null,
      quotedText: ev.quotedText ? sanitizeUntrustedNoticeText(ev.quotedText, { singleLine: true, maxBytes: 256 }) : null,
      untrusted: true,
      authority: 'none',
    }))),
  })
  return Object.freeze({
    profileId: RESTRICTED_AUTOREPLY_PROFILE_SPEC.profileId,
    baseBundle: RESTRICTED_AUTOREPLY_PROFILE_SPEC.baseBundle,
    toolsAllowed: RESTRICTED_AUTOREPLY_PROFILE_SPEC.toolsAllowed,
    systemPrompt: RESTRICTED_AUTOREPLY_PROFILE_SPEC.systemPromptPolicy,
    userInputJson: JSON.stringify(envelope),
    envelope,
  })
}

/**
 * Parse either a typed adapter object or DSH `--json` NDJSON output into `{ providerRunId, sessionId, parsedOutput, toolCalls }`.
 * @param {unknown} response
 * @returns {{ providerRunId: string, sessionId: string | null, parsedOutput: unknown, toolCalls: readonly unknown[] }}
 */
export function parseRestrictedAdapterResponse(response) {
  if (typeof response === 'string') {
    const lines = response.split('\n').map(line => line.trim()).filter(Boolean)
    let sessionId = null
    let finalText = null
    const toolCalls = []
    for (const line of lines) {
      const ev = JSON.parse(line)
      if (!ev || typeof ev !== 'object' || Array.isArray(ev)) continue
      if (ev.type === 'session' && typeof ev.sessionId === 'string') sessionId = ev.sessionId
      if (ev.type === 'tool_call') toolCalls.push(ev)
      if (ev.type === 'final' && typeof ev.text === 'string') finalText = ev.text
    }
    if (!sessionId || !SAFE_PROVIDER_RUN_RE.test(sessionId) || finalText === null) {
      throw new Error('Malformed headless NDJSON stream from restricted profile')
    }
    return {
      providerRunId: sessionId,
      sessionId,
      parsedOutput: JSON.parse(finalText),
      toolCalls,
    }
  }
  if (!response || typeof response !== 'object' || Array.isArray(response)) {
    throw new Error('Restricted adapter response must be an object or NDJSON stream')
  }
  if (typeof response.providerRunId !== 'string' || !SAFE_PROVIDER_RUN_RE.test(response.providerRunId)) {
    throw new Error('Missing or invalid providerRunId from restricted profile')
  }
  if (Object.hasOwn(response, 'finishReason') && response.finishReason !== undefined && response.finishReason !== null) {
    const finishKind = typeof response.finishReason === 'object' ? response.finishReason.kind : null
    if (finishKind !== 'stop' && finishKind !== 'tool-calls') {
      throw new Error(`Restricted adapter response ended with non-terminal finish reason: ${String(finishKind)}`)
    }
  }
  const toolCalls = Array.isArray(response.toolCalls) ? response.toolCalls : []
  const raw = Object.hasOwn(response, 'rawOutput') ? response.rawOutput : response.output
  const parsedOutput = typeof raw === 'string'
    ? (toolCalls.length > 0 && !raw.trim() ? null : JSON.parse(raw))
    : raw
  return {
    providerRunId: response.providerRunId,
    sessionId: typeof response.sessionId === 'string' ? response.sessionId : null,
    parsedOutput,
    toolCalls,
  }
}

/**
 * Strictly validate model JSON output (`answer` or `needs_human` only; rejects targets, tools, extra keys,
 * banned expressions, secrets, and length caps).
 * @param {object} params
 * @returns {{ ok: true, decision: 'answer', answer: string } | { ok: true, decision: 'needs_human', reason: string } | { ok: false, reason: string }}
 */
export function validateRestrictedProfileOutput({
  parsedOutput,
  toolCalls = [],
  maxReplyChars,
  bannedExpressions = DEFAULT_BANNED_EXPRESSIONS,
}) {
  if (Array.isArray(toolCalls) && toolCalls.length > 0) {
    return { ok: false, reason: 'disallowed_model_tool_call' }
  }
  if (!parsedOutput || typeof parsedOutput !== 'object' || Array.isArray(parsedOutput)) {
    return { ok: false, reason: 'invalid_model_output_type' }
  }
  try {
    fields(parsedOutput, ['decision'], ['answer', 'reason'])
  } catch (error) {
    return { ok: false, reason: `invalid_model_output_schema:${error instanceof Error ? error.message : String(error)}` }
  }
  if (parsedOutput.decision === 'needs_human') {
    if (Object.hasOwn(parsedOutput, 'answer') && parsedOutput.answer !== null && parsedOutput.answer !== undefined) {
      return { ok: false, reason: 'invalid_needs_human_with_answer' }
    }
    const reason = sanitizeUntrustedNoticeText(parsedOutput.reason ?? '', { singleLine: true, maxBytes: 240 })
    if (!reason) {
      return { ok: false, reason: 'missing_needs_human_reason' }
    }
    return { ok: true, decision: 'needs_human', reason }
  }
  if (parsedOutput.decision !== 'answer') {
    return { ok: false, reason: 'unsupported_model_decision' }
  }
  if (Object.hasOwn(parsedOutput, 'reason') && parsedOutput.reason !== null && parsedOutput.reason !== undefined) {
    if (typeof parsedOutput.reason !== 'string') return { ok: false, reason: 'invalid_answer_reason' }
  }
  if (typeof parsedOutput.answer !== 'string' || parsedOutput.answer.includes('\0')) {
    return { ok: false, reason: 'invalid_model_answer' }
  }
  const cleanAnswer = sanitizeUntrustedNoticeText(parsedOutput.answer, { singleLine: false, maxBytes: 2048 })
  if (!cleanAnswer) {
    return { ok: false, reason: 'empty_model_answer' }
  }
  if ([...cleanAnswer].length > maxReplyChars) {
    return { ok: false, reason: `answer_exceeds_char_cap:${maxReplyChars}` }
  }
  for (const expr of bannedExpressions) {
    if (expr && cleanAnswer.includes(expr)) {
      return { ok: false, reason: `answer_contains_banned_expression:${expr}` }
    }
  }
  if (DRAFT_SECRET_RE.test(cleanAnswer) || SENSITIVE_DISCLOSURE_RE.test(cleanAnswer)) {
    return { ok: false, reason: 'answer_contains_sensitive_disclosure' }
  }
  if (FINANCIAL_TRANSFER_RE.test(cleanAnswer) || LIVE_TRADE_OR_PROMISE_RE.test(cleanAnswer) || UNPUBLISHED_RESEARCH_RE.test(cleanAnswer)) {
    return { ok: false, reason: 'answer_violates_hard_routing_policy' }
  }
  return { ok: true, decision: 'answer', answer: cleanAnswer }
}

/**
 * Durable SQLite state store and restricted evaluation coordinator for offline WeChat auto-reply
 * eligibility, one-claim concurrency/crash recovery, and non-sending draft/human decisions.
 */
export class WechatAutoReplyStore {
  /**
   * @param {object} policy - Validated deployment policy object.
   * @param {object} [autoReplyPolicy] - Optional auto-reply policy (defaults strictly disabled everywhere).
   * @param {object} [options] - Optional deterministic clock and UID seam for non-root tests.
   */
  constructor(policy, autoReplyPolicy = null, options = {}) {
    this.policy = policy
    this.autoReplyPolicy = resolveAutoReplyPolicy(autoReplyPolicy)
    this.policyPath = typeof options.policyPath === 'string' && options.policyPath.trim()
      ? options.policyPath.trim()
      : null
    this.expectedTransportKey = typeof options.expectedTransportKey === 'string' && options.expectedTransportKey.trim()
      ? options.expectedTransportKey.trim()
      : null
    this.defaultInstallAnchor = typeof options.defaultInstallAnchor === 'string' && options.defaultInstallAnchor.trim()
      ? options.defaultInstallAnchor.trim()
      : null
    this.clock = options.clock ?? (() => Date.now())
    this.statUid = options.statUid ?? defaultStatUid
    this.verifyOperatorBinding = options.verifyOperatorBinding ?? true
    this.verifyEvidenceFiles = options.verifyEvidenceFiles ?? true
    this.dispatchEnabled = options.dispatchEnabled === true
    this.cooperativeShutdownMs = assertOptionalBoundedInt(
      options.cooperativeShutdownMs,
      80,
      0,
      10_000,
      'cooperativeShutdownMs',
    )
    this.maxOrphanedAdapterTasks = assertOptionalBoundedInt(
      options.maxOrphanedAdapterTasks,
      1,
      1,
      32,
      'maxOrphanedAdapterTasks',
    )
    this.maxOrphanedBackgroundTasks = assertOptionalBoundedInt(
      options.maxOrphanedBackgroundTasks,
      4,
      1,
      64,
      'maxOrphanedBackgroundTasks',
    )
    this.maxRetiredOrphanedTasks = assertOptionalBoundedInt(
      options.maxRetiredOrphanedTasks,
      8,
      1,
      64,
      'maxRetiredOrphanedTasks',
    )
    this._closed = false
    this._inFlight = new Set()
    this._backgroundWork = new Set()
    this._orphanedTasks = new Set()
    this._retiredOrphanedTasks = new Set()
    this._adapterExecutionFailed = false
    this._adapterFailureReason = null
    this._backgroundIdleWaiters = new Set()
    this.db = openPrivateDirAndSqlite(policy.state, 'wechat-autoreply.sqlite', {
      readOnly: false,
      statUid: this.statUid,
    })
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS autoreply_tasks (
        batch_id TEXT PRIMARY KEY,
        task_id TEXT NOT NULL UNIQUE,
        notice_id TEXT NOT NULL,
        conversation_key TEXT NOT NULL,
        channel TEXT NOT NULL,
        requested_mode TEXT NOT NULL,
        state TEXT NOT NULL,
        eligibility_decision TEXT NOT NULL,
        send_action TEXT NOT NULL,
        policy_version INTEGER NOT NULL,
        attempts INTEGER NOT NULL,
        max_attempts INTEGER NOT NULL,
        event_ids TEXT NOT NULL,
        frozen_target TEXT,
        context_tail_fingerprint TEXT NOT NULL,
        provider_run_id TEXT,
        draft_text TEXT,
        reason TEXT,
        claimed_at INTEGER,
        lease_expires_at INTEGER,
        batch_expires_at INTEGER,
        decided_at INTEGER,
        send_deadline_at INTEGER,
        send_evidence_at INTEGER
      );
      CREATE INDEX IF NOT EXISTS idx_autoreply_tasks_state ON autoreply_tasks(state, lease_expires_at);
      CREATE INDEX IF NOT EXISTS idx_autoreply_task_claims ON autoreply_tasks(conversation_key, claimed_at);
      CREATE TABLE IF NOT EXISTS autoreply_runtime_flags (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS autoreply_audit (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        batch_id TEXT NOT NULL,
        task_id TEXT NOT NULL,
        from_state TEXT,
        to_state TEXT NOT NULL,
        reason TEXT,
        provider_run_id TEXT,
        recorded_at INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS autoreply_owner_notices (
        task_id TEXT PRIMARY KEY,
        batch_id TEXT NOT NULL UNIQUE,
        notice_key TEXT NOT NULL UNIQUE,
        owner_notice_id TEXT NOT NULL,
        origin_notice_id TEXT NOT NULL,
        conversation_key TEXT,
        body TEXT,
        handoff_file TEXT NOT NULL,
        state TEXT NOT NULL,
        projected_at INTEGER NOT NULL,
        handoff_status TEXT NOT NULL DEFAULT 'written',
        handoff_written_at INTEGER,
        last_handoff_error TEXT
      );
      CREATE INDEX IF NOT EXISTS idx_autoreply_owner_notices_handoff ON autoreply_owner_notices(handoff_status, projected_at, task_id);
    `)
    const auditCols = new Set(this.db.prepare('PRAGMA table_info(autoreply_audit)').all().map(col => col.name))
    if (!auditCols.has('provider_run_id')) {
      this.db.exec('ALTER TABLE autoreply_audit ADD COLUMN provider_run_id TEXT')
    }
    const taskCols = new Set(this.db.prepare('PRAGMA table_info(autoreply_tasks)').all().map(col => col.name))
    for (const column of ['send_deadline_at', 'send_evidence_at']) {
      if (!taskCols.has(column)) this.db.exec(`ALTER TABLE autoreply_tasks ADD COLUMN ${column} INTEGER`)
    }
    // Legacy dispatch outcomes did not record the frozen job deadline. Bound
    // their exposure by the old draft lifetime plus the maximum outbox lease.
    this.db.prepare(`
      UPDATE autoreply_tasks SET send_deadline_at=MIN(COALESCE(batch_expires_at, decided_at + ?), decided_at + ?)
      WHERE send_action IN ('queued', 'needs_review') AND send_deadline_at IS NULL
    `).run(AUTOMATIC_DRAFT_LIFETIME_MS + AUTOMATIC_SEND_JOB_MS, AUTOMATIC_DRAFT_LIFETIME_MS + AUTOMATIC_SEND_JOB_MS)
    this.db.exec('CREATE INDEX IF NOT EXISTS idx_autoreply_send_quota ON autoreply_tasks(conversation_key, send_action)')
    this.recoverInterruptedClaims(this.clock())
  }

  /** Refresh live deployment/auto-reply policy configuration without reopening SQLite or dropping active tracking. */
  syncPolicy(nextPolicy, nextAutoReplyPolicy = null) {
    if (this._closed || !this.db.isOpen) {
      throw new Error('Cannot sync auto-reply policy after store close')
    }
    this.policy = nextPolicy
    this.autoReplyPolicy = resolveAutoReplyPolicy(nextAutoReplyPolicy)
    return this.autoReplyPolicy
  }

  /** Set or clear persistent runtime pause independent of policy file reloads. */
  setRuntimeGlobalPause(paused, nowMs = this.clock()) {
    if (typeof paused !== 'boolean') throw new Error('Paused flag must be boolean')
    this.db.prepare(`
      INSERT INTO autoreply_runtime_flags(key, value) VALUES('global_pause', ?)
      ON CONFLICT(key) DO UPDATE SET value=excluded.value
    `).run(JSON.stringify({ paused, updatedAt: nowMs }))
    return paused
  }

  isRuntimeGlobalPaused() {
    const row = this.db.prepare("SELECT value FROM autoreply_runtime_flags WHERE key='global_pause'").get()
    if (!row) return false
    try { return JSON.parse(row.value)?.paused === true } catch { return true }
  }

  /** Transition any interrupted in-flight `'claimed'` tasks cleanly on startup or lease expiry; never double-claims. */
  recoverInterruptedClaims(nowMs = this.clock()) {
    const claimedRows = this.db.prepare("SELECT * FROM autoreply_tasks WHERE state='claimed'").all()
    if (claimedRows.length === 0) return []
    const recovered = []
    this.db.exec('BEGIN IMMEDIATE')
    try {
      const updateStmt = this.db.prepare(`
        UPDATE autoreply_tasks
        SET state=?, eligibility_decision=?, reason=?, decided_at=?
        WHERE batch_id=? AND state='claimed'
      `)
      const auditStmt = this.db.prepare(`
        INSERT INTO autoreply_audit(batch_id, task_id, from_state, to_state, reason, provider_run_id, recorded_at)
        VALUES(?, ?, 'claimed', ?, ?, ?, ?)
      `)
      for (const row of claimedRows) {
        const leaseExpired = Number.isSafeInteger(row.lease_expires_at) && nowMs >= row.lease_expires_at
        const nextState = leaseExpired ? 'expired' : 'needs_review'
        const nextDecision = leaseExpired ? 'blocked' : 'needs_review'
        const nextReason = leaseExpired
          ? 'claim_lease_expired'
          : 'interrupted_after_claim'
        const res = updateStmt.run(nextState, nextDecision, nextReason, nowMs, row.batch_id)
        if (res.changes === 1) {
          auditStmt.run(row.batch_id, row.task_id, nextState, nextReason, row.provider_run_id ?? null, nowMs)
          recovered.push({ batchId: row.batch_id, taskId: row.task_id, state: nextState, reason: nextReason })
        }
      }
      this.db.exec('COMMIT')
      return recovered
    } catch (error) {
      try { this.db.exec('ROLLBACK') } catch { /* Already rolled back. */ }
      throw error
    }
  }

  /** Read one persisted task outcome by sealed batch identifier. */
  getTaskByBatchId(batchId) {
    const row = this.db.prepare('SELECT * FROM autoreply_tasks WHERE batch_id=?').get(batchId)
    return row ? hydrateTaskRow(row, false) : null
  }

  _notifyBackgroundIdleIfReady() {
    if (this._backgroundWork.size === 0 && this._backgroundIdleWaiters.size > 0) {
      const snap = this.getBackgroundWorkState()
      for (const resolveIdle of [...this._backgroundIdleWaiters]) {
        this._backgroundIdleWaiters.delete(resolveIdle)
        resolveIdle(snap)
      }
    }
  }

  /** Return current active and orphaned background task counts without claiming unproven cleanup. */
  getBackgroundWorkState() {
    let orphanedAdapterCount = 0
    for (const task of this._orphanedTasks) {
      if (task.kind === 'adapter') orphanedAdapterCount += 1
    }
    const derivedState = this._closed
      ? 'closed'
      : (this._adapterExecutionFailed || orphanedAdapterCount >= this.maxOrphanedAdapterTasks || this._orphanedTasks.size >= this.maxOrphanedBackgroundTasks)
        ? 'failed'
        : (this._orphanedTasks.size > 0 || this._retiredOrphanedTasks.size > 0)
          ? 'degraded'
          : 'ready'
    return Object.freeze({
      state: derivedState,
      inFlightCount: this._inFlight.size,
      backgroundWorkCount: this._backgroundWork.size,
      orphanedCount: this._orphanedTasks.size,
      orphanedAdapterCount,
      retiredOrphanedCount: this._retiredOrphanedTasks.size,
      maxOrphanedAdapterTasks: this.maxOrphanedAdapterTasks,
      maxOrphanedBackgroundTasks: this.maxOrphanedBackgroundTasks,
      maxRetiredOrphanedTasks: this.maxRetiredOrphanedTasks,
      lastFailureReason: this._adapterFailureReason,
    })
  }

  /** Explicitly fail-close future adapter executions on this store until replaced or closed. */
  failAdapterExecution(reason = 'operator_marked_failed') {
    const cleanReason = sanitizeUntrustedNoticeText(String(reason), { singleLine: true, maxBytes: 120 }) || 'operator_marked_failed'
    this._adapterExecutionFailed = true
    this._adapterFailureReason = cleanReason
    for (const entry of [...this._inFlight]) {
      try {
        entry.controller.abort(new Error(cleanReason))
      } catch {
        // Ignore abort dispatch errors.
      }
    }
    return this.getBackgroundWorkState()
  }

  /** Acknowledge/retire prior orphaned adapter state so a replacement healthy adapter can proceed within bounded ceilings. */
  replaceAdapterState() {
    if (this._closed || !this.db.isOpen) {
      throw new Error('Cannot replace adapter state after store close')
    }
    if (this._retiredOrphanedTasks.size + this._backgroundWork.size > this.maxRetiredOrphanedTasks) {
      throw new Error(`Cannot replace adapter state while retired/orphaned tasks exceed maxRetiredOrphanedTasks (${this.maxRetiredOrphanedTasks})`)
    }
    for (const task of [...this._orphanedTasks]) {
      if (task.kind === 'adapter') {
        task.retired = true
        this._orphanedTasks.delete(task)
        this._retiredOrphanedTasks.add(task)
      }
    }
    this._adapterExecutionFailed = false
    this._adapterFailureReason = null
    return this.getBackgroundWorkState()
  }

  /** Resolve once all foreground and orphaned background adapter/report/tail promises have settled. */
  whenBackgroundIdle() {
    if (this._backgroundWork.size === 0) {
      return Promise.resolve(this.getBackgroundWorkState())
    }
    return new Promise((resolveIdle) => {
      this._backgroundIdleWaiters.add(resolveIdle)
    })
  }

  _checkBackgroundOwnerHealth(kind, ownerRef) {
    if (!ownerRef) return { ok: true, reason: null }
    for (const task of this._orphanedTasks) {
      if (task.kind === kind && task.ownerRef === ownerRef) {
        return {
          ok: false,
          reason: kind === 'report'
            ? 'published_report_provider_orphaned'
            : kind === 'tail'
              ? 'tail_verifier_orphaned'
              : 'restricted_adapter_orphaned_session_active',
        }
      }
    }
    return { ok: true, reason: null }
  }

  _checkAdapterExecutionHealth(adapter, verifyTailFn) {
    if (this._adapterExecutionFailed) {
      return { ok: false, reason: `restricted_adapter_unhealthy:${this._adapterFailureReason || 'store_failed'}` }
    }
    if (this._retiredOrphanedTasks.size >= this.maxRetiredOrphanedTasks) {
      return { ok: false, reason: 'restricted_adapter_retired_orphan_limit_exceeded' }
    }
    const ownerCheck = this._checkBackgroundOwnerHealth('adapter', adapter)
    if (!ownerCheck.ok) return ownerCheck
    const tailOwnerCheck = this._checkBackgroundOwnerHealth('tail', verifyTailFn)
    if (!tailOwnerCheck.ok) return tailOwnerCheck
    let activeAdapterOrphans = 0
    for (const task of this._orphanedTasks) {
      if (task.kind === 'adapter') activeAdapterOrphans += 1
    }
    if (activeAdapterOrphans >= this.maxOrphanedAdapterTasks) {
      return { ok: false, reason: 'restricted_adapter_orphan_limit_exceeded' }
    }
    if (this._orphanedTasks.size >= this.maxOrphanedBackgroundTasks) {
      return { ok: false, reason: 'restricted_background_orphan_limit_exceeded' }
    }
    if (adapter && typeof adapter.getLifecycleState === 'function') {
      const lifecycle = adapter.getLifecycleState()
      if (
        !lifecycle
        || lifecycle.state !== 'ready'
        || (Number.isSafeInteger(lifecycle.orphanedRuns) && Number.isSafeInteger(lifecycle.maxOrphanedRuns) && lifecycle.orphanedRuns >= lifecycle.maxOrphanedRuns)
      ) {
        const stateLabel = lifecycle?.state ? String(lifecycle.state) : 'unknown'
        return { ok: false, reason: `restricted_adapter_unhealthy:${stateLabel}` }
      }
    }
    return { ok: true, reason: null }
  }

  async _runBoundedInFlight({ timeoutMs, timeoutReason = 'bounded_callback_timeout', kind = 'adapter', ownerRef = null, execute }) {
    const waitMs = Math.max(1, Number.isSafeInteger(timeoutMs) ? timeoutMs : this.autoReplyPolicy.adapterTimeoutMs)
    const controller = new AbortController()
    let timeoutHandle = null
    let closedAbort = false
    let timedOut = false
    let rejectCloseWaiter = null
    const bgEntry = {
      kind,
      ownerRef,
      retired: false,
      settled: false,
      onSettled: null,
    }
    this._backgroundWork.add(bgEntry)

    const inFlightEntry = {
      controller,
      onClose: () => {
        closedAbort = true
        controller.abort(new Error('store_closed_before_settlement'))
        bgEntry.onSettled?.()
        rejectCloseWaiter?.(new Error('store_closed_before_settlement'))
      },
    }
    this._inFlight.add(inFlightEntry)
    let workSettled = false
    let workOk = false
    let workValue
    let workError = null

    try {
      const updateStage = (nextKind, nextOwnerRef) => {
        bgEntry.kind = nextKind
        bgEntry.ownerRef = nextOwnerRef ?? null
      }
      const workPromise = Promise.resolve().then(() => execute(controller.signal, updateStage))
      // Observe both fulfillment and rejection so late background tasks never trigger unhandled rejections.
      const observedWork = workPromise.then(
        (val) => {
          workSettled = true
          workOk = true
          workValue = val
        },
        (err) => {
          workSettled = true
          workOk = false
          workError = err
        },
      ).finally(() => {
        bgEntry.settled = true
        bgEntry.onSettled?.()
        bgEntry.onSettled = null
        this._backgroundWork.delete(bgEntry)
        this._orphanedTasks.delete(bgEntry)
        this._retiredOrphanedTasks.delete(bgEntry)
        this._notifyBackgroundIdleIfReady()
      })

      const timeoutPromise = new Promise((_, rejectTimeout) => {
        timeoutHandle = setTimeout(() => {
          timedOut = true
          controller.abort(new Error(timeoutReason))
          rejectTimeout(new Error(timeoutReason))
        }, waitMs)
      })
      const closePromise = new Promise((_, rejectClose) => {
        rejectCloseWaiter = rejectClose
        if (this._closed || !this.db.isOpen) {
          closedAbort = true
          controller.abort(new Error('store_closed_before_settlement'))
          rejectClose(new Error('store_closed_before_settlement'))
        }
      })
      // Catch closePromise if observedWork/timeoutPromise win first or during grace wait.
      closePromise.catch(() => {})

      await Promise.race([observedWork, timeoutPromise, closePromise])
      if (workSettled) {
        if (workOk) {
          return {
            ok: true,
            abortedOnClose: closedAbort || this._closed || !this.db.isOpen,
            timedOut: false,
            backgroundSettled: true,
            value: workValue,
            error: null,
          }
        }
        return {
          ok: false,
          abortedOnClose: closedAbort || this._closed || !this.db.isOpen,
          timedOut: false,
          backgroundSettled: true,
          value: undefined,
          error: workError,
        }
      }
      return {
        ok: false,
        abortedOnClose: closedAbort || this._closed || !this.db.isOpen,
        timedOut,
        backgroundSettled: false,
        value: undefined,
        error: new Error(timeoutReason),
      }
    } catch (error) {
      if (timedOut && !closedAbort && !this._closed && this.db.isOpen && !workSettled && this.cooperativeShutdownMs > 0) {
        let graceHandle = null
        const gracePromise = new Promise((resolveGrace) => {
          graceHandle = setTimeout(resolveGrace, this.cooperativeShutdownMs)
        })
        await Promise.race([
          new Promise((resolveDone) => {
            if (workSettled || closedAbort || this._closed || !this.db.isOpen) {
              resolveDone()
              return
            }
            bgEntry.onSettled = resolveDone
          }),
          gracePromise,
        ])
        bgEntry.onSettled = null
        if (graceHandle !== null) clearTimeout(graceHandle)
      }
      if (!workSettled) {
        this._orphanedTasks.add(bgEntry)
      }
      return {
        ok: false,
        abortedOnClose: closedAbort || this._closed || !this.db.isOpen,
        timedOut,
        backgroundSettled: workSettled,
        value: undefined,
        error: workSettled && !timedOut ? workError : error,
      }
    } finally {
      if (timeoutHandle !== null) clearTimeout(timeoutHandle)
      this._inFlight.delete(inFlightEntry)
    }
  }

  _abortedPreClaimRecord({ batch, notice, channel, requestedMode, eventIds, frozenTarget, contextTailFingerprint, batchExpiresAt }) {
    return Object.freeze({
      batchId: batch.batch_id,
      taskId: null,
      noticeId: notice.notice_id,
      conversationKey: batch.conversation_key,
      channel,
      requestedMode,
      state: 'needs_review',
      eligibilityDecision: 'blocked',
      sendAction: 'none',
      policyVersion: this.autoReplyPolicy.policyVersion,
      attempts: 0,
      maxAttempts: 1,
      eventIds,
      frozenTarget,
      contextTailFingerprint,
      providerRunId: null,
      draftText: null,
      reason: 'store_closed_before_settlement',
      claimedAt: null,
      leaseExpiresAt: null,
      batchExpiresAt,
      decidedAt: null,
      abortedOnClose: true,
      duplicate: false,
    })
  }

  _evaluatePreClaimGates({ batch, notice, rawChannel, events, contextRows, eventIds, options, nowMs }) {
    const cfg = this.autoReplyPolicy
    const channel = rawChannel === 'friend' || rawChannel === 'group' ? rawChannel : 'unsupported'
    const convRule = cfg.conversations[batch.conversation_key] ?? null
    const requestedMode = convRule ? convRule.mode : 'disabled'
    const tailCheck = validateTailObservation({
      policy: this.policy,
      batch,
      events,
      tailState: options.tailState ?? null,
      requestedMode,
      maxTailAgeSeconds: cfg.maxTailAgeSeconds,
      nowMs,
      verifyEvidenceFiles: this.verifyEvidenceFiles,
      statUid: this.statUid,
    })
    const contextTailFingerprint = digest(JSON.stringify({
      accountSha256: batch.account_sha256,
      conversationKey: batch.conversation_key,
      contactId: batch.contact_id,
      targetProof: batch.target_proof,
      batchId: batch.batch_id,
      noticeId: notice.notice_id,
      eventIds,
      context: contextRows.map(row => [row.event_id, row.observed_at, digest(row.summary)]),
      tailMarker: tailCheck.ok ? tailCheck.tailMarker : tailCheck.marker,
      policyVersion: cfg.policyVersion,
    }))

    if (channel === 'unsupported') {
      return {
        ok: false,
        block: {
          channel: 'unsupported',
          requestedMode: 'disabled',
          state: 'unsupported',
          eligibilityDecision: 'unsupported',
          frozenTarget: null,
          contextTailFingerprint,
          reason: `unsupported_channel:${rawChannel}`,
          batchExpiresAt: null,
        },
      }
    }

    const registry = resolveWechatRegistry(this.policy)
    const accountMatched = Boolean(registry.accountId && digest(registry.accountId) === batch.account_sha256 && notice.account_sha256 === batch.account_sha256)
    const contact = registry.contacts.find(item => item.id === batch.contact_id && item.type === channel) ?? null
    const contactProofValid = Boolean(contact && contact.bound && contact.targetProof && contact.targetProof === batch.target_proof && notice.target_proof === batch.target_proof)
    const contactSendEnabled = Boolean(
      contact
      && registry.globalEnabled
      && contact.enabled
      && (channel === 'friend' ? (registry.globalDirectEnabled && contact.directEnabled) : (registry.globalGroupEnabled && contact.groupEnabled)),
    )

    const frozenTarget = accountMatched && contact && contactProofValid
      ? Object.freeze({
        accountSha256: batch.account_sha256,
        contactId: contact.id,
        contactType: contact.type,
        alias: contact.alias,
        displayName: contact.displayName,
        targetProof: contact.targetProof,
        ...(contact.type === 'friend'
          ? { recipientId: contact.recipientId }
          : { groupId: contact.groupId, groupName: contact.groupName }),
      })
      : null

    if (!accountMatched) {
      return {
        ok: false,
        block: {
          channel,
          requestedMode,
          state: 'needs_review',
          eligibilityDecision: 'needs_review',
          frozenTarget: null,
          contextTailFingerprint,
          reason: 'account_binding_changed',
          batchExpiresAt: null,
        },
      }
    }
    const reviewEvent = events.find(event => event.needsReview || event.triggerType === 'needs_human')
    if (reviewEvent) {
      return {
        ok: false,
        block: {
          channel,
          requestedMode,
          state: 'needs_review',
          eligibilityDecision: 'needs_review',
          frozenTarget,
          contextTailFingerprint,
          reason: /^[a-z][a-z0-9_]{0,79}$/.test(reviewEvent.reason ?? '')
            ? `desktop_review:${reviewEvent.reason}` : 'desktop_review',
          batchExpiresAt: null,
        },
      }
    }
    if (!contactProofValid) {
      return {
        ok: false,
        block: {
          channel,
          requestedMode,
          state: 'needs_review',
          eligibilityDecision: 'needs_review',
          frozenTarget: null,
          contextTailFingerprint,
          reason: 'target_binding_changed_or_missing',
          batchExpiresAt: null,
        },
      }
    }
    if (!contactSendEnabled) {
      return {
        ok: false,
        block: {
          channel,
          requestedMode,
          state: 'notice_only',
          eligibilityDecision: 'blocked',
          frozenTarget,
          contextTailFingerprint,
          reason: 'target_or_channel_disabled',
          batchExpiresAt: null,
        },
      }
    }
    if (batch.reply_eligible !== 1 || notice.reply_eligible !== 1) {
      return {
        ok: false,
        block: {
          channel,
          requestedMode,
          state: 'needs_review',
          eligibilityDecision: 'needs_review',
          frozenTarget,
          contextTailFingerprint,
          reason: 'unverified_source_proof',
          batchExpiresAt: null,
        },
      }
    }
    for (const ev of events) {
      if (
        ev.actionable !== true
        || ev.needsReview === true
        || ev.accountSha256 !== batch.account_sha256
        || ev.contactId !== batch.contact_id
        || !HEX64_RE.test(String(ev.bindingSha256 ?? ''))
        || ev.bindingSha256 !== batch.target_proof
        || typeof ev.sourceRef !== 'string'
        || !ev.sourceRef.trim()
        || !HEX64_RE.test(String(ev.evidenceSha256 ?? ''))
      ) {
        return {
          ok: false,
          block: {
            channel,
            requestedMode,
            state: 'needs_review',
            eligibilityDecision: 'needs_review',
            frozenTarget,
            contextTailFingerprint,
            reason: 'unverified_source_proof',
            batchExpiresAt: null,
          },
        }
      }
      if (this.verifyEvidenceFiles) {
        const evCheck = verifyEvidenceFileOnDisk(this.policy, ev.evidencePath, ev.evidenceSha256, { statUid: this.statUid })
        if (!evCheck.ok) {
          return {
            ok: false,
            block: {
              channel,
              requestedMode,
              state: 'needs_review',
              eligibilityDecision: 'needs_review',
              frozenTarget,
              contextTailFingerprint,
              reason: `unverified_source_proof:${evCheck.reason}`,
              batchExpiresAt: null,
            },
          }
        }
      }
    }

    const manualViewGate = verifyRuntimeManualReplyViewGate(this.policy, batch.account_sha256, nowMs, this.statUid)
    const oldestObserved = Math.min(...events.map(ev => Number(ev.observedAt)))
    const maxAgeSeconds = manualViewGate.pauseMaxAge !== null
      ? Math.min(cfg.maxInboundAgeSeconds, manualViewGate.pauseMaxAge)
      : cfg.maxInboundAgeSeconds
    const batchExpiresAt = Math.min(oldestObserved, batch.sealed_at) + maxAgeSeconds * 1000
    if (oldestObserved > nowMs + MAX_FUTURE_SKEW_MS) {
      return {
        ok: false,
        block: {
          channel,
          requestedMode,
          state: 'needs_review',
          eligibilityDecision: 'needs_review',
          frozenTarget,
          contextTailFingerprint,
          reason: 'inbound_clock_skew_exceeded',
          batchExpiresAt,
        },
      }
    }
    if (nowMs >= batchExpiresAt) {
      return {
        ok: false,
        block: {
          channel,
          requestedMode,
          state: 'expired',
          eligibilityDecision: 'blocked',
          frozenTarget,
          contextTailFingerprint,
          reason: 'inbound_batch_stale',
          batchExpiresAt,
        },
      }
    }

    if (!cfg.enabled) {
      return {
        ok: false,
        block: {
          channel,
          requestedMode,
          state: 'notice_only',
          eligibilityDecision: 'notice_only',
          frozenTarget,
          contextTailFingerprint,
          reason: 'autoreply_disabled',
          batchExpiresAt,
        },
      }
    }
    if (cfg.globalPaused || this.isRuntimeGlobalPaused()) {
      return {
        ok: false,
        block: {
          channel,
          requestedMode,
          state: 'notice_only',
          eligibilityDecision: 'blocked',
          frozenTarget,
          contextTailFingerprint,
          reason: 'global_paused',
          batchExpiresAt,
        },
      }
    }
    if (!manualViewGate.ok) {
      return {
        ok: false,
        block: {
          channel,
          requestedMode,
          state: manualViewGate.state,
          eligibilityDecision: manualViewGate.state === 'needs_review' ? 'needs_review' : 'blocked',
          frozenTarget,
          contextTailFingerprint,
          reason: manualViewGate.reason,
          batchExpiresAt,
        },
      }
    }
    if (this.verifyOperatorBinding) {
      const opCheck = checkOperatorBindingActive(this.policy)
      if (!opCheck.ok) {
        return {
          ok: false,
          block: {
            channel,
            requestedMode,
            state: 'needs_review',
            eligibilityDecision: 'needs_review',
            frozenTarget,
            contextTailFingerprint,
            reason: opCheck.reason,
            batchExpiresAt,
          },
        }
      }
    }
    if (!convRule || convRule.mode === 'disabled') {
      return {
        ok: false,
        block: {
          channel,
          requestedMode: 'disabled',
          state: 'notice_only',
          eligibilityDecision: 'notice_only',
          frozenTarget,
          contextTailFingerprint,
          reason: 'conversation_disabled',
          batchExpiresAt,
        },
      }
    }

    const signalCheck = evaluateBatchSignalProofs(events, options.eventSignals ?? null, channel, convRule.allowRegisteredSenderMentions)
    if (!signalCheck.ok) {
      return {
        ok: false,
        block: {
          channel,
          requestedMode,
          state: signalCheck.state,
          eligibilityDecision: signalCheck.state === 'needs_review' ? 'needs_review' : 'notice_only',
          frozenTarget,
          contextTailFingerprint,
          reason: signalCheck.reason,
          batchExpiresAt,
        },
      }
    }

    if (!tailCheck.ok) {
      return {
        ok: false,
        block: {
          channel,
          requestedMode,
          state: tailCheck.state,
          eligibilityDecision: tailCheck.state,
          frozenTarget,
          contextTailFingerprint,
          reason: tailCheck.reason,
          batchExpiresAt,
        },
      }
    }

    return {
      ok: true,
      channel,
      convRule,
      requestedMode,
      frozenTarget,
      contextTailFingerprint,
      batchExpiresAt,
    }
  }

  /**
   * Evaluate one sealed batch from `wechat-inbox.sqlite`, enforce all security/eligibility gates
   * and group cooldowns atomically, invoke `adapter` at most once per batch under one-claim state,
   * and persist a non-sending draft/human/review decision.
   * @param {string} batchId - Sealed batch identifier in `wechat-inbox.sqlite`.
   * @param {object} [options] - Optional desktop signals, tail snapshot, report facts, and restricted adapter.
   * @returns {Promise<object>} Frozen non-sending assessment record.
   */
  async evaluateSealedBatch(batchId, options = {}) {
    if (this._closed || !this.db.isOpen) {
      throw new Error('Cannot evaluate auto-reply batch after store close')
    }
    let nowMs = this.clock()
    const cfg = this.autoReplyPolicy
    const existingBefore = this.db.prepare('SELECT * FROM autoreply_tasks WHERE batch_id=?').get(batchId)
    if (existingBefore) {
      if (existingBefore.state === 'claimed' && Number.isSafeInteger(existingBefore.lease_expires_at) && nowMs >= existingBefore.lease_expires_at) {
        this.recoverInterruptedClaims(nowMs)
        const updated = this.db.prepare('SELECT * FROM autoreply_tasks WHERE batch_id=?').get(batchId)
        return hydrateTaskRow(updated, true)
      }
      return hydrateTaskRow(existingBefore, true)
    }

    const { batch, notice, events, contextRows } = readVerifiedInboxBatch(this.policy, batchId, {
      contextLimit: cfg.maxContextMessages,
      statUid: this.statUid,
    })
    options = { ...desktopEvaluationInputs(batch, events), ...options }
    const eventIds = Object.freeze(events.map(ev => ev.eventId))
    const rawChannel = batch.conversation_kind

    const initialGate = this._evaluatePreClaimGates({
      batch, notice, rawChannel, events, contextRows, eventIds, options, nowMs,
    })
    if (!initialGate.ok) {
      return this.persistImmediateDecision({
        batch, notice, eventIds, nowMs, ...initialGate.block,
      })
    }

    let {
      channel,
      convRule,
      requestedMode,
      frozenTarget,
      contextTailFingerprint,
      batchExpiresAt,
    } = initialGate

    const promptEvents = events.map(event => event.sourceProof?.question
      ? { ...event, text: event.sourceProof.question } : event)
    let verifiedReportBundle = null
    if (convRule.allowPublishedReportFacts && options.reportPublicationProvider
      && promptEvents.some(ev => requiresPublishedReport(ev.text ?? '') || requiresPublishedReport(ev.quotedText ?? ''))) {
      const reportHealth = this._checkBackgroundOwnerHealth('report', options.reportPublicationProvider)
      if (!reportHealth.ok) {
        return this.persistImmediateDecision({
          batch, notice, channel, requestedMode, state: 'needs_human', eligibilityDecision: 'needs_human',
          eventIds, frozenTarget, contextTailFingerprint, reason: reportHealth.reason, nowMs, batchExpiresAt,
        })
      }
      const reportWaitMs = Math.max(1, Math.min(cfg.adapterTimeoutMs, Math.max(1, batchExpiresAt - nowMs)))
      const boundedReport = await this._runBoundedInFlight({
        timeoutMs: reportWaitMs,
        timeoutReason: 'published_report_lookup_timeout',
        kind: 'report',
        ownerRef: options.reportPublicationProvider,
        execute: signal => resolveVerifiedPublishedReport({
          provider: options.reportPublicationProvider,
          batch,
          channel,
          bannedExpressions: cfg.bannedExpressions,
          maxReportAgeSeconds: cfg.maxReportAgeSeconds,
          policyVersion: cfg.policyVersion,
          nowMs,
          signal,
        }),
      })
      if (boundedReport.abortedOnClose || this._closed || !this.db.isOpen) {
        return this._abortedPreClaimRecord({
          batch, notice, channel, requestedMode, eventIds, frozenTarget, contextTailFingerprint, batchExpiresAt,
        })
      }

      // Re-sample clock and re-verify batch expiry and live authority BEFORE acquiring any generation claim.
      nowMs = this.clock()
      const recheckGate = this._evaluatePreClaimGates({
        batch, notice, rawChannel, events, contextRows, eventIds, options, nowMs,
      })
      if (!recheckGate.ok) {
        return this.persistImmediateDecision({
          batch, notice, eventIds, nowMs, ...recheckGate.block,
        })
      }
      ({
        channel,
        convRule,
        requestedMode,
        frozenTarget,
        contextTailFingerprint,
        batchExpiresAt,
      } = recheckGate)

      const reportCheck = boundedReport.timedOut
        ? { ok: false, reason: 'published_report_lookup_timeout', bundle: null }
        : boundedReport.ok
          ? boundedReport.value
          : {
            ok: false,
            reason: `published_report_provider_error:${sanitizeUntrustedNoticeText(boundedReport.error instanceof Error ? boundedReport.error.message : String(boundedReport.error), { singleLine: true, maxBytes: 120 }) || 'failed'}`,
            bundle: null,
          }
      if (!reportCheck.ok) {
        return this.persistImmediateDecision({
          batch, notice, channel, requestedMode, state: 'needs_human', eligibilityDecision: 'needs_human',
          eventIds, frozenTarget, contextTailFingerprint, reason: reportCheck.reason, nowMs, batchExpiresAt,
        })
      }
      if (!convRule.allowPublishedReportFacts) {
        return this.persistImmediateDecision({
          batch, notice, channel, requestedMode, state: 'needs_human', eligibilityDecision: 'needs_human',
          eventIds, frozenTarget, contextTailFingerprint, reason: 'published_report_not_allowed_for_conversation', nowMs, batchExpiresAt,
        })
      }
      const postWindowCheck = checkReportBundleTimestamps(reportCheck.bundle, {
        maxReportAgeSeconds: cfg.maxReportAgeSeconds,
        nowMs,
      })
      if (!postWindowCheck.ok) {
        return this.persistImmediateDecision({
          batch, notice, channel, requestedMode, state: 'needs_human', eligibilityDecision: 'needs_human',
          eventIds, frozenTarget, contextTailFingerprint, reason: postWindowCheck.reason, nowMs, batchExpiresAt,
        })
      }
      verifiedReportBundle = reportCheck.bundle
    }

    const risk = classifyInboundRiskAndFacts({
      events: promptEvents,
      conversationRule: convRule,
      verifiedReportBundle,
      bannedExpressions: cfg.bannedExpressions,
    })
    if (risk.route === 'needs_human') {
      return this.persistImmediateDecision({
        batch, notice, channel, requestedMode, state: 'needs_human', eligibilityDecision: 'needs_human',
        eventIds, frozenTarget, contextTailFingerprint,
        reason: `${risk.category}:${risk.reason}`, nowMs, batchExpiresAt,
      })
    }

    if (options.adapter && typeof options.adapter.generate === 'function') {
      const adapterHealth = this._checkAdapterExecutionHealth(options.adapter, options.verifyTailAfterGeneration)
      if (!adapterHealth.ok) {
        return this.persistImmediateDecision({
          batch, notice, channel, requestedMode, state: 'needs_review', eligibilityDecision: 'needs_review',
          eventIds, frozenTarget, contextTailFingerprint, reason: adapterHealth.reason, nowMs, batchExpiresAt,
        })
      }
    }

    // Atomic one-claim acquisition (+ persistent atomic group cooldown check).
    const claimResult = this.tryAcquireGenerationClaim({
      batch, notice, channel, requestedMode, convRule, eventIds, frozenTarget,
      contextTailFingerprint, batchExpiresAt, nowMs,
    })
    if (!claimResult.acquired) {
      return claimResult.record
    }

    const maxReplyChars = convRule.maxReplyChars ?? (channel === 'group' ? cfg.maxGroupReplyChars : cfg.maxFriendReplyChars)
    const promptPayload = buildRestrictedProfilePromptPayload({
      taskId: claimResult.record.taskId,
      policyVersion: cfg.policyVersion,
      channel,
      requestedMode,
      maxReplyChars,
      conversationRule: convRule,
      contextRows,
      events: promptEvents,
      publishedReportFacts: verifiedReportBundle?.facts ?? [],
      bannedExpressions: cfg.bannedExpressions,
    })

    if (!options.adapter || typeof options.adapter.generate !== 'function') {
      return this.settleClaim(batch.batch_id, claimResult.record.taskId, {
        state: 'needs_review',
        eligibilityDecision: 'needs_review',
        reason: 'restricted_adapter_missing',
        providerRunId: null,
        draftText: null,
      })
    }

    let providerRunId = null
    let rawAdapterResult
    let parsedAdapter = null
    let parseError = null
    let tailStillValid = true
    let tailRejectReason = 'superseded_by_new_activity:post_generation_tail_changed'
    let executionStage = 'generation'
    const genStartMs = this.clock()
    const remainingLeaseMs = Math.max(1, claimResult.record.leaseExpiresAt - genStartMs)
    const remainingBatchMs = batchExpiresAt ? Math.max(1, batchExpiresAt - genStartMs) : remainingLeaseMs
    const waitMs = Math.max(1, Math.min(cfg.adapterTimeoutMs, remainingLeaseMs, remainingBatchMs))

    const boundedExec = await this._runBoundedInFlight({
      timeoutMs: waitMs,
      timeoutReason: 'claim_lease_expired_timeout',
      kind: 'adapter',
      ownerRef: options.adapter,
      execute: async (signal, updateStage) => {
        executionStage = 'generation'
        try {
          rawAdapterResult = await options.adapter.generate(promptPayload, { signal })
        } catch (err) {
          if (err && typeof err === 'object' && typeof err.providerRunId === 'string' && SAFE_PROVIDER_RUN_RE.test(err.providerRunId)) {
            providerRunId = err.providerRunId
          }
          throw err
        }
        try {
          parsedAdapter = parseRestrictedAdapterResponse(rawAdapterResult)
          providerRunId = parsedAdapter.providerRunId
        } catch (err) {
          parseError = err
        }

        const postGenNowMs = this.clock()
        if (
          signal.aborted
          || (Number.isSafeInteger(claimResult.record.leaseExpiresAt) && postGenNowMs >= claimResult.record.leaseExpiresAt)
          || (Number.isSafeInteger(batchExpiresAt) && postGenNowMs >= batchExpiresAt)
        ) {
          throw new Error('claim_lease_expired_timeout')
        }

        if (typeof options.verifyTailAfterGeneration === 'function') {
          executionStage = 'tail_verification'
          updateStage('tail', options.verifyTailAfterGeneration)
          const tailResult = await options.verifyTailAfterGeneration({
            conversationKey: batch.conversation_key,
            channel,
            accountSha256: batch.account_sha256,
            targetProof: batch.target_proof,
            batchId: batch.batch_id,
            taskId: claimResult.record.taskId,
            signal,
          })
          if (tailResult && typeof tailResult === 'object') {
            tailStillValid = tailResult.ok !== false
            if (!tailStillValid && typeof tailResult.reason === 'string' && tailResult.reason.trim()) {
              const sanitized = sanitizeUntrustedNoticeText(tailResult.reason, { singleLine: true, maxBytes: 120 })
              if (sanitized) tailRejectReason = `superseded_by_new_activity:${sanitized}`
            }
          } else {
            tailStillValid = Boolean(tailResult)
          }
          const postTailNowMs = this.clock()
          if (
            signal.aborted
            || (Number.isSafeInteger(claimResult.record.leaseExpiresAt) && postTailNowMs >= claimResult.record.leaseExpiresAt)
            || (Number.isSafeInteger(batchExpiresAt) && postTailNowMs >= batchExpiresAt)
          ) {
            throw new Error('claim_lease_expired_timeout')
          }
        }
      },
    })

    if (boundedExec.abortedOnClose || this._closed || !this.db.isOpen) {
      return Object.freeze({
        ...claimResult.record,
        providerRunId,
        state: 'needs_review',
        eligibilityDecision: 'blocked',
        reason: 'store_closed_before_settlement',
        abortedOnClose: true,
        duplicate: false,
      })
    }

    if (!boundedExec.ok) {
      const error = boundedExec.error
      const leaseExpiredNow = Number.isSafeInteger(claimResult.record.leaseExpiresAt) && this.clock() >= claimResult.record.leaseExpiresAt
      const batchExpiredNow = Number.isSafeInteger(batchExpiresAt) && this.clock() >= batchExpiresAt
      const isTimeout = boundedExec.timedOut || leaseExpiredNow || batchExpiredNow || (error instanceof Error && error.message === 'claim_lease_expired_timeout')
      if (isTimeout) {
        return this.settleClaim(batch.batch_id, claimResult.record.taskId, {
          state: 'expired',
          eligibilityDecision: 'blocked',
          reason: executionStage === 'tail_verification'
            ? 'tail_verification_lease_expired'
            : (batchExpiredNow && !leaseExpiredNow ? 'inbound_batch_stale' : 'claim_lease_expired'),
          providerRunId,
          draftText: null,
        })
      }
      const msg = sanitizeUntrustedNoticeText(error instanceof Error ? error.message : String(error), { singleLine: true, maxBytes: 180 })
      return this.settleClaim(batch.batch_id, claimResult.record.taskId, {
        state: 'needs_review',
        eligibilityDecision: 'needs_review',
        reason: executionStage === 'tail_verification'
          ? `tail_verification_failed:${msg || 'tail_error'}`
          : `generation_failed:${msg || 'adapter_error'}`,
        providerRunId,
        draftText: null,
      })
    }

    if (parseError || !parsedAdapter) {
      const msg = sanitizeUntrustedNoticeText(parseError instanceof Error ? parseError.message : String(parseError), { singleLine: true, maxBytes: 180 })
      return this.settleClaim(batch.batch_id, claimResult.record.taskId, {
        state: 'needs_human',
        eligibilityDecision: 'needs_human',
        reason: `invalid_model_envelope:${msg}`,
        providerRunId,
        draftText: null,
      })
    }

    const validated = validateRestrictedProfileOutput({
      parsedOutput: parsedAdapter.parsedOutput,
      toolCalls: parsedAdapter.toolCalls,
      maxReplyChars,
      bannedExpressions: cfg.bannedExpressions,
    })
    if (!validated.ok) {
      return this.settleClaim(batch.batch_id, claimResult.record.taskId, {
        state: 'needs_human',
        eligibilityDecision: 'needs_human',
        reason: validated.reason,
        providerRunId,
        draftText: null,
      })
    }
    if (validated.decision === 'needs_human') {
      return this.settleClaim(batch.batch_id, claimResult.record.taskId, {
        state: 'needs_human',
        eligibilityDecision: 'needs_human',
        reason: validated.reason,
        providerRunId,
        draftText: null,
      })
    }
    if (!tailStillValid) {
      return this.settleClaim(batch.batch_id, claimResult.record.taskId, {
        state: 'needs_review',
        eligibilityDecision: 'needs_review',
        reason: tailRejectReason,
        providerRunId,
        draftText: null,
      })
    }

    const eligibilityDecision = requestedMode === 'unattended' ? 'unattended_candidate' : 'draft_only'
    return this.settleClaim(batch.batch_id, claimResult.record.taskId, {
      state: 'draft_ready',
      eligibilityDecision,
      reason: eligibilityDecision === 'unattended_candidate'
        ? 'eligible_unattended_decision_no_send'
        : 'eligible_draft_only',
      providerRunId,
      draftText: validated.answer,
    })
  }

  /**
   * Simulate or perform the pre-generation claim step only (exposed cleanly so tests can exercise
   * mid-claim crash/restart recovery without leaving timers open).
   * @private
   */
  tryAcquireGenerationClaim({
    batch, notice, channel, requestedMode, convRule, eventIds, frozenTarget,
    contextTailFingerprint, batchExpiresAt, nowMs,
  }) {
    const cfg = this.autoReplyPolicy
    this.db.exec('BEGIN IMMEDIATE')
    try {
      const existing = this.db.prepare('SELECT * FROM autoreply_tasks WHERE batch_id=?').get(batch.batch_id)
      if (existing) {
        this.db.exec('COMMIT')
        return { acquired: false, record: hydrateTaskRow(existing, true) }
      }
      const taskId = randomBytes(12).toString('hex')
      if (channel === 'group') {
        const windowMs = (convRule.cooldownSeconds ?? cfg.groupCooldownSeconds) * 1000
        const sendCap = convRule.maxRepliesPerWindow ?? 1
        const sending = this.dispatchEnabled && requestedMode === 'unattended'
        const attemptCap = sending ? 2 * sendCap : sendCap
        const attemptsExhausted = this.db.prepare(`
          SELECT 1 FROM autoreply_tasks WHERE conversation_key=? AND claimed_at>? LIMIT 1 OFFSET ?
        `).get(batch.conversation_key, nowMs - windowMs, attemptCap - 1)
        const sendsExhausted = sending && this.groupSendQuotaFull(batch.conversation_key, convRule, nowMs)
        if (attemptsExhausted || sendsExhausted) {
          const rec = this.insertTaskRowLocked({
            batchId: batch.batch_id,
            taskId,
            noticeId: notice.notice_id,
            conversationKey: batch.conversation_key,
            channel,
            requestedMode,
            state: 'notice_only',
            eligibilityDecision: 'notice_only',
            attempts: 0,
            maxAttempts: cfg.maxAttempts,
            eventIds,
            frozenTarget,
            contextTailFingerprint,
            providerRunId: null,
            draftText: null,
            reason: sendsExhausted ? 'group_send_limit' : 'group_model_attempt_limit',
            claimedAt: null,
            leaseExpiresAt: null,
            batchExpiresAt,
            decidedAt: nowMs,
          })
          this.db.exec('COMMIT')
          return { acquired: false, record: rec }
        }
      }

      const leaseExpiresAt = nowMs + cfg.claimLeaseSeconds * 1000
      const rec = this.insertTaskRowLocked({
        batchId: batch.batch_id,
        taskId,
        noticeId: notice.notice_id,
        conversationKey: batch.conversation_key,
        channel,
        requestedMode,
        state: 'claimed',
        eligibilityDecision: 'claimed',
        attempts: 1,
        maxAttempts: cfg.maxAttempts,
        eventIds,
        frozenTarget,
        contextTailFingerprint,
        providerRunId: null,
        draftText: null,
        reason: 'generation_claimed',
        claimedAt: nowMs,
        leaseExpiresAt,
        batchExpiresAt,
        decidedAt: null,
      })
      this.db.exec('COMMIT')
      return { acquired: true, record: rec }
    } catch (error) {
      try { this.db.exec('ROLLBACK') } catch { /* Already rolled back. */ }
      throw error
    }
  }

  /** @private */
  groupSendQuotaFull(conversationKey, rule, nowMs) {
    const cutoff = nowMs - (rule.cooldownSeconds ?? this.autoReplyPolicy.groupCooldownSeconds) * 1000
    const cap = rule.maxRepliesPerWindow ?? 1
    return Boolean(this.db.prepare(`
      SELECT 1 FROM autoreply_tasks WHERE conversation_key=? AND (
        (send_action='sent_verified' AND send_evidence_at>?)
        OR (send_action IN ('reserved', 'queued', 'needs_review')
          AND MAX(COALESCE(send_deadline_at, 0), COALESCE(send_evidence_at, 0))>?)
      ) LIMIT 1 OFFSET ?
    `).get(conversationKey, cutoff, cutoff, cap - 1))
  }

  /** @private */
  recordSendOutcomeLocked(row, state, action, reason, nowMs, evidenceAt = null) {
    this.db.prepare(`
      UPDATE autoreply_tasks SET state=?, send_action=?, reason=?, decided_at=?, send_evidence_at=? WHERE task_id=?
    `).run(state, action, reason, nowMs, evidenceAt, row.task_id)
    this.db.prepare(`
      INSERT INTO autoreply_audit(batch_id, task_id, from_state, to_state, reason, provider_run_id, recorded_at)
      VALUES(?, ?, ?, ?, ?, ?, ?)
    `).run(row.batch_id, row.task_id, row.state, state, reason, row.provider_run_id, nowMs)
    return hydrateTaskRow(this.db.prepare('SELECT * FROM autoreply_tasks WHERE task_id=?').get(row.task_id), false)
  }

  /** @private */
  reserveAutomaticSend(taskId, nowMs, rejection = null) {
    this.db.exec('BEGIN IMMEDIATE')
    try {
      const row = this.db.prepare(`
        SELECT * FROM autoreply_tasks WHERE task_id=? AND requested_mode='unattended'
          AND state='draft_ready' AND send_action='none'
      `).get(taskId)
      let outcome = null
      if (row) {
        const rule = this.autoReplyPolicy.conversations[row.conversation_key]
        const dispatchBy = row.decided_at + AUTOMATIC_DRAFT_LIFETIME_MS
        const deadline = Math.min(row.batch_expires_at ?? 0, dispatchBy + AUTOMATIC_SEND_JOB_MS)
        const reason = rejection ?? (rule?.mode !== 'unattended' ? 'conversation_mode_changed' : null)
          ?? (Math.min(deadline, dispatchBy) <= nowMs ? 'automatic_draft_expired' : null)
        if (reason) {
          outcome = this.recordSendOutcomeLocked(row, 'needs_review', 'not_sent', reason, nowMs)
        } else if (row.channel === 'group' && this.groupSendQuotaFull(row.conversation_key, rule, nowMs)) {
          outcome = this.recordSendOutcomeLocked(row, 'notice_only', 'not_sent', 'group_send_limit', nowMs)
        } else {
          this.db.prepare("UPDATE autoreply_tasks SET send_action='reserved', send_deadline_at=? WHERE task_id=?")
            .run(deadline, taskId)
          outcome = hydrateTaskRow(this.db.prepare('SELECT * FROM autoreply_tasks WHERE task_id=?').get(taskId), false)
        }
      }
      this.db.exec('COMMIT')
      return outcome
    } catch (error) {
      this.db.exec('ROLLBACK')
      throw error
    }
  }

  /** @private */
  finishAutomaticDispatch(taskId, action, nowMs, deadlineAt = null) {
    this.db.exec('BEGIN IMMEDIATE')
    try {
      const row = this.db.prepare("SELECT * FROM autoreply_tasks WHERE task_id=? AND send_action='reserved'").get(taskId)
      let outcome = null
      if (row && action === 'queued') {
        this.db.prepare("UPDATE autoreply_tasks SET send_action='queued', send_deadline_at=COALESCE(?, send_deadline_at) WHERE task_id=?")
          .run(Number.isSafeInteger(deadlineAt) && deadlineAt > row.decided_at ? deadlineAt : null, taskId)
      } else if (row) {
        const reason = action === 'not_sent' ? 'automatic_dispatch_refused' : 'automatic_dispatch_result_unknown'
        outcome = this.recordSendOutcomeLocked(row, 'needs_review', action, reason, nowMs,
          action === 'needs_review' ? nowMs : null)
      }
      this.db.exec('COMMIT')
      return outcome
    } catch (error) {
      this.db.exec('ROLLBACK')
      throw error
    }
  }

  /** Settle a terminal result verified against the pinned worker receipt, outbox and operation. */
  settleAutomaticReceipt(receipt) {
    if (this._closed || !this.db.isOpen) return null
    this.db.exec('BEGIN IMMEDIATE')
    try {
      const row = this.db.prepare('SELECT * FROM autoreply_tasks WHERE task_id=?').get(receipt.taskId)
      const target = row?.frozen_target ? JSON.parse(row.frozen_target) : null
      let outcome = null
      if (row && row.requested_mode === 'unattended' && row.draft_text && target
        && row.batch_id === receipt.batchId && row.policy_version === receipt.policyVersion
        && row.conversation_key === receipt.conversationKey && target.accountSha256 === receipt.accountSha256
        && target.targetProof === receipt.targetProof && digest(row.draft_text) === receipt.textSha256
        && row.send_action !== 'sent_verified') {
        const receiptReason = ['worker_receipt', receipt.state, receipt.reason].filter(Boolean).join(':')
        if (receipt.state === 'sent_verified') {
          outcome = this.recordSendOutcomeLocked(row, 'sent_verified', 'sent_verified', receiptReason,
            receipt.observedAt, receipt.verifiedAt ?? receipt.observedAt)
        } else if (DEFINITELY_NOT_SENT_RECEIPTS.has(receipt.state) && row.reason !== receiptReason) {
          outcome = this.recordSendOutcomeLocked(row, 'not_sent', 'not_sent', receiptReason, receipt.observedAt)
        } else if (receipt.state === 'needs_review') {
          if (receipt.submissionProvenAbsent && (row.send_action !== 'not_sent' || row.reason !== receiptReason)) {
            outcome = this.recordSendOutcomeLocked(row, 'needs_review', 'not_sent', receiptReason, receipt.observedAt)
          } else if (!receipt.submissionProvenAbsent && !row.reason?.startsWith('worker_receipt:')) {
            // Start an unknown hold only on its first authoritative observation.
            this.db.prepare('UPDATE autoreply_tasks SET send_deadline_at=? WHERE task_id=?').run(receipt.deadlineAt, row.task_id)
            outcome = this.recordSendOutcomeLocked(row, 'needs_review', 'needs_review', receiptReason,
              receipt.observedAt, receipt.observedAt)
          }
        }
      }
      this.db.exec('COMMIT')
      return outcome
    } catch (error) {
      this.db.exec('ROLLBACK')
      throw error
    }
  }

  /** @private */
  persistImmediateDecision({
    batch, notice, channel, requestedMode, state, eligibilityDecision, eventIds,
    frozenTarget, contextTailFingerprint, reason, nowMs, batchExpiresAt = null,
  }) {
    this.db.exec('BEGIN IMMEDIATE')
    try {
      const existing = this.db.prepare('SELECT * FROM autoreply_tasks WHERE batch_id=?').get(batch.batch_id)
      if (existing) {
        this.db.exec('COMMIT')
        return hydrateTaskRow(existing, true)
      }
      const rec = this.insertTaskRowLocked({
        batchId: batch.batch_id,
        taskId: randomBytes(12).toString('hex'),
        noticeId: notice.notice_id,
        conversationKey: batch.conversation_key,
        channel,
        requestedMode,
        state,
        eligibilityDecision,
        attempts: 0,
        maxAttempts: this.autoReplyPolicy.maxAttempts,
        eventIds,
        frozenTarget,
        contextTailFingerprint,
        providerRunId: null,
        draftText: null,
        reason,
        claimedAt: null,
        leaseExpiresAt: null,
        batchExpiresAt,
        decidedAt: nowMs,
      })
      this.db.exec('COMMIT')
      return rec
    } catch (error) {
      try { this.db.exec('ROLLBACK') } catch { /* Already rolled back. */ }
      throw error
    }
  }

  /** @private */
  insertTaskRowLocked(entry) {
    this.db.prepare(`
      INSERT INTO autoreply_tasks(
        batch_id, task_id, notice_id, conversation_key, channel, requested_mode, state,
        eligibility_decision, send_action, policy_version, attempts, max_attempts,
        event_ids, frozen_target, context_tail_fingerprint, provider_run_id, draft_text,
        reason, claimed_at, lease_expires_at, batch_expires_at, decided_at
      ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'none', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      entry.batchId,
      entry.taskId,
      entry.noticeId,
      entry.conversationKey,
      entry.channel,
      entry.requestedMode,
      entry.state,
      entry.eligibilityDecision,
      this.autoReplyPolicy.policyVersion,
      entry.attempts,
      entry.maxAttempts,
      JSON.stringify(entry.eventIds),
      entry.frozenTarget ? JSON.stringify(entry.frozenTarget) : null,
      entry.contextTailFingerprint,
      entry.providerRunId,
      entry.draftText,
      entry.reason,
      entry.claimedAt,
      entry.leaseExpiresAt,
      entry.batchExpiresAt,
      entry.decidedAt,
    )
    this.db.prepare(`
      INSERT INTO autoreply_audit(batch_id, task_id, from_state, to_state, reason, provider_run_id, recorded_at)
      VALUES(?, ?, NULL, ?, ?, ?, ?)
    `).run(entry.batchId, entry.taskId, entry.state, entry.reason, entry.providerRunId ?? null, entry.decidedAt ?? entry.claimedAt ?? this.clock())
    return hydrateTaskRow(this.db.prepare('SELECT * FROM autoreply_tasks WHERE batch_id=?').get(entry.batchId), false)
  }

  /** @private */
  settleClaim(batchId, taskId, { state, eligibilityDecision, reason, providerRunId, draftText }) {
    if (this._closed || !this.db.isOpen) {
      throw new Error('Cannot settle auto-reply claim after store close')
    }
    this.db.exec('BEGIN IMMEDIATE')
    try {
      const current = this.db.prepare('SELECT * FROM autoreply_tasks WHERE batch_id=?').get(batchId)
      if (!current || current.task_id !== taskId || current.state !== 'claimed') {
        this.db.exec('COMMIT')
        return hydrateTaskRow(current, true)
      }
      const settleNow = this.clock()
      const frozenTarget = current.frozen_target ? JSON.parse(current.frozen_target) : null

      let finalState = state
      let finalDecision = eligibilityDecision
      let finalReason = reason
      let finalDraft = draftText

      const forceNonDraft = (nextState, nextDecision, nextReason) => {
        finalState = nextState
        finalDecision = nextDecision
        finalReason = nextReason
        finalDraft = null
      }

      // 1. Atomic lease & batch maturity check MUST precede any draft settlement.
      if (Number.isSafeInteger(current.lease_expires_at) && settleNow >= current.lease_expires_at) {
        const keepLeaseReason = state === 'expired'
          && (reason === 'tail_verification_lease_expired' || reason === 'claim_lease_expired')
        forceNonDraft('expired', 'blocked', keepLeaseReason ? reason : 'claim_lease_expired')
      } else if (Number.isSafeInteger(current.batch_expires_at) && settleNow >= current.batch_expires_at) {
        const keepBatchReason = state === 'expired'
          && (reason === 'tail_verification_lease_expired' || reason === 'inbound_batch_stale')
        forceNonDraft('expired', 'blocked', keepBatchReason ? reason : 'inbound_batch_stale')
      } else {
        // 2. Re-verify live policy file, global pause, operator binding, and account/contact authority at settlement time.
        let effectivePolicy = this.policy
        let effectiveAutoReply = this.autoReplyPolicy
        const needsDraftApproval = finalDraft !== null || finalState === 'draft_ready'
        if (this.policyPath && needsDraftApproval) {
          if (!existsSync(this.policyPath)) {
            forceNonDraft('needs_review', 'needs_review', 'live_policy_missing')
          } else {
            let reloadedPolicy = null
            try {
              reloadedPolicy = readPolicy(this.policyPath)
            } catch {
              forceNonDraft('needs_review', 'needs_review', 'live_policy_untrusted')
            }
            if (reloadedPolicy) {
              const liveNotifCfg = wechatNotificationsPolicy(reloadedPolicy)
              const liveRuntimeCfg = wechatAutoReplyPolicy(reloadedPolicy)
              effectivePolicy = reloadedPolicy
              effectiveAutoReply = resolveAutoReplyPolicy(toStoreAutoReplyPolicy(liveRuntimeCfg))
              if (!liveNotifCfg.enabled || !liveRuntimeCfg.enabled) {
                forceNonDraft('notice_only', 'blocked', 'autoreply_disabled')
              } else if (
                this.expectedTransportKey
                && canonicalTransportKey(liveRuntimeCfg.transport, this.defaultInstallAnchor) !== this.expectedTransportKey
              ) {
                forceNonDraft('needs_review', 'needs_review', 'transport_changed_requires_restart')
              } else {
                const liveConvRule = effectiveAutoReply.conversations?.[current.conversation_key] ?? null
                if (!liveConvRule || liveConvRule.mode === 'disabled') {
                  forceNonDraft('notice_only', 'blocked', 'conversation_disabled')
                } else if (current.requested_mode === 'unattended' && liveConvRule.mode !== 'unattended') {
                  forceNonDraft('needs_review', 'needs_review', 'conversation_mode_changed')
                }
              }
            }
          }
        }
        if ((finalDraft !== null || finalState === 'draft_ready' || !this.policyPath) && !effectiveAutoReply.enabled) {
          forceNonDraft('notice_only', 'blocked', 'autoreply_disabled')
        } else if (effectiveAutoReply.globalPaused || this.isRuntimeGlobalPaused()) {
          forceNonDraft('notice_only', 'blocked', 'global_paused')
        } else if (finalDraft !== null || finalState === 'draft_ready') {
          const expectedSha = frozenTarget?.accountSha256 ?? ''
          const manualViewGate = verifyRuntimeManualReplyViewGate(effectivePolicy, expectedSha, settleNow, this.statUid)
          if (!manualViewGate.ok) {
            forceNonDraft(
              manualViewGate.state,
              manualViewGate.state === 'needs_review' ? 'needs_review' : 'blocked',
              manualViewGate.reason,
            )
          } else if (this.verifyOperatorBinding) {
            const opCheck = checkOperatorBindingActive(effectivePolicy)
            if (!opCheck.ok) {
              forceNonDraft('needs_review', 'needs_review', opCheck.reason)
            }
          }
          if (finalDraft !== null || finalState === 'draft_ready') {
            const registry = resolveWechatRegistry(effectivePolicy)
            const accountMatched = Boolean(registry.accountId && frozenTarget && digest(registry.accountId) === frozenTarget.accountSha256)
            if (!accountMatched) {
              forceNonDraft('needs_review', 'needs_review', 'account_binding_changed')
            } else {
              const contact = registry.contacts.find(
                item => item.id === frozenTarget.contactId && item.type === frozenTarget.contactType,
              ) ?? null
              const proofValid = Boolean(contact && contact.bound && contact.targetProof === frozenTarget.targetProof)
              if (!proofValid) {
                forceNonDraft('needs_review', 'needs_review', 'target_binding_changed_or_missing')
              } else {
                const sendEnabled = Boolean(
                  registry.globalEnabled
                  && contact.enabled
                  && (frozenTarget.contactType === 'friend'
                    ? (registry.globalDirectEnabled && contact.directEnabled)
                    : (registry.globalGroupEnabled && contact.groupEnabled)),
                )
                if (!sendEnabled) {
                  forceNonDraft('notice_only', 'blocked', 'target_or_channel_disabled')
                }
              }
            }
          }
        }
      }

      let updated
      if (finalState === 'draft_ready') {
        updated = this.db.prepare(`
          UPDATE autoreply_tasks
          SET state=?, eligibility_decision=?, reason=?, provider_run_id=?, draft_text=?, decided_at=?
          WHERE batch_id=? AND task_id=? AND state='claimed'
            AND (lease_expires_at IS NULL OR lease_expires_at > ?)
            AND (batch_expires_at IS NULL OR batch_expires_at > ?)
        `).run(
          finalState,
          finalDecision,
          finalReason,
          providerRunId,
          finalDraft,
          settleNow,
          batchId,
          taskId,
          settleNow,
          settleNow,
        )
      } else {
        updated = this.db.prepare(`
          UPDATE autoreply_tasks
          SET state=?, eligibility_decision=?, reason=?, provider_run_id=?, draft_text=?, decided_at=?
          WHERE batch_id=? AND task_id=? AND state='claimed'
        `).run(finalState, finalDecision, finalReason, providerRunId, null, settleNow, batchId, taskId)
      }

      if (updated.changes === 1) {
        this.db.prepare(`
          INSERT INTO autoreply_audit(batch_id, task_id, from_state, to_state, reason, provider_run_id, recorded_at)
          VALUES(?, ?, 'claimed', ?, ?, ?, ?)
        `).run(batchId, taskId, finalState, finalReason, providerRunId ?? null, settleNow)
      }
      const row = this.db.prepare('SELECT * FROM autoreply_tasks WHERE batch_id=?').get(batchId)
      this.db.exec('COMMIT')
      return hydrateTaskRow(row, updated.changes !== 1)
    } catch (error) {
      try { this.db.exec('ROLLBACK') } catch { /* Already rolled back. */ }
      throw error
    }
  }

  /** List durable auto-reply audit transitions ordered chronologically. */
  listAuditTransitions() {
    return this.db.prepare('SELECT * FROM autoreply_audit ORDER BY id ASC').all()
  }

  /** Export a single non-sending decision artifact for inspection without touching `adhoc/outbox`. */
  writeDecisionArtifact(targetPath, record) {
    if (!targetPath || !targetPath.startsWith('/')) {
      throw new Error('Absolute decision artifact path is required')
    }
    atomicJson(targetPath, {
      schemaVersion: WECHAT_AUTOREPLY_SCHEMA_VERSION,
      ...record,
    }, 0o600)
    return targetPath
  }

  /**
   * Project one non-sending human/review/draft assessment into the existing trusted `handoffDir`
   * owner-notice pipeline at most once per task/batch when explicitly opted in.
   * Commits a crash-recoverable `'pending'` row before writing `handoffDir`, marks `'written'`
   * upon atomic delivery and never replays `'unknown'` notices.
   */
  projectOutcomeOwnerNotice(record, options = {}) {
    if (this._closed || !this.db.isOpen) {
      throw new Error('Cannot project owner notice after store close')
    }
    const notifCfg = wechatNotificationsPolicy(this.policy)
    if (!notifCfg.enabled || options.projectOwnerNotices !== true) {
      return null
    }
    const rateLimited = record?.state === 'notice_only' && GROUP_LIMIT_REASONS.has(record.reason)
    if (!record || (!rateLimited && record.state !== 'draft_ready' && record.state !== 'needs_human' && record.state !== 'needs_review')) {
      return null
    }
    const noticeKey = `autoreply:${record.taskId}`
    const ownerNoticeId = formatNoticeId(noticeKey)
    const createdAt = Number.isSafeInteger(record.decidedAt) ? record.decidedAt : this.clock()

    let row
    let alreadyExisted = false
    this.db.exec('BEGIN IMMEDIATE')
    try {
      const existing = this.db.prepare('SELECT * FROM autoreply_owner_notices WHERE task_id=? OR batch_id=?').get(record.taskId, record.batchId)
      if (existing) {
        this.db.exec('COMMIT')
        row = existing
        alreadyExisted = true
      } else {
        const body = renderAutoReplyOwnerNoticeBody(record, notifCfg.maxNoticeBytes)
        const handoffFile = `${digest(noticeKey).slice(0, 32)}.json`
        this.db.prepare(`
          INSERT INTO autoreply_owner_notices(
            task_id, batch_id, notice_key, owner_notice_id, origin_notice_id,
            conversation_key, body, handoff_file, state, projected_at, handoff_status
          ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
        `).run(
          record.taskId,
          record.batchId,
          noticeKey,
          ownerNoticeId,
          record.noticeId,
          record.conversationKey,
          body,
          handoffFile,
          record.state,
          createdAt,
        )
        row = this.db.prepare('SELECT * FROM autoreply_owner_notices WHERE task_id=?').get(record.taskId)
        this.db.exec('COMMIT')
      }
    } catch (error) {
      try { this.db.exec('ROLLBACK') } catch { /* Already rolled back. */ }
      throw error
    }

    if (alreadyExisted && row.handoff_status !== 'pending') {
      return Object.freeze({
        duplicate: true,
        noticeKey: row.notice_key,
        ownerNoticeId: row.owner_notice_id,
        originNoticeId: row.origin_notice_id,
        handoffFile: row.handoff_file,
        state: row.state,
        projectedAt: row.projected_at,
      })
    }

    this._deliverOwnerNoticeRow(row)
    return Object.freeze({
      duplicate: alreadyExisted,
      noticeKey: row.notice_key,
      ownerNoticeId: row.owner_notice_id,
      originNoticeId: row.origin_notice_id,
      handoffFile: row.handoff_file,
      state: row.state,
      projectedAt: row.projected_at,
    })
  }

  _deliverOwnerNoticeRow(row) {
    if (!row || !row.body) {
      throw new Error('Cannot flush owner notice row without durable body')
    }
    const envelope = {
      schemaVersion: 1,
      noticeKey: row.notice_key,
      noticeId: row.owner_notice_id,
      kind: 'inbound_alert',
      conversationKey: row.conversation_key ?? null,
      sourceRef: row.task_id,
      body: row.body,
      createdAt: row.projected_at,
      untrusted: true,
      authority: 'notice_only',
    }
    try {
      const writtenFile = writeTrustedHandoffEnvelope(this.policy, envelope, { statUid: this.statUid })
      const nowMs = this.clock()
      this.db.prepare(
        "UPDATE autoreply_owner_notices SET handoff_file=?, handoff_status='written', handoff_written_at=?, last_handoff_error=NULL WHERE task_id=? AND handoff_status='pending'",
      ).run(writtenFile, nowMs, row.task_id)
      return writtenFile
    } catch (error) {
      const reason = sanitizeUntrustedNoticeText(error instanceof Error ? error.message : String(error), {
        singleLine: true,
        maxBytes: 240,
      }) || 'Handoff write failed'
      this.db.prepare(
        "UPDATE autoreply_owner_notices SET last_handoff_error=? WHERE task_id=? AND handoff_status='pending'",
      ).run(reason, row.task_id)
      throw error
    }
  }

  /**
   * Flush bounded pending owner notices whose durable DB commit preceded filesystem completion,
   * preserving deterministic `noticeId` without regenerating model tasks or duplicating delivered rows.
   */
  flushPendingOwnerNotices(options = {}) {
    if (this._closed || !this.db.isOpen) return 0
    const notifCfg = wechatNotificationsPolicy(this.policy)
    if (!notifCfg.enabled) return 0
    const maxCount = options.maxCount ?? 50
    if (!Number.isSafeInteger(maxCount) || maxCount < 1 || maxCount > 200) {
      throw new Error('flushPendingOwnerNotices maxCount must be an integer between 1 and 200')
    }
    const nowMs = this.clock()
    const cutoff = nowMs - notifCfg.retentionDays * 86_400_000
    const rows = this.db.prepare(
      "SELECT * FROM autoreply_owner_notices WHERE handoff_status='pending' AND projected_at >= ? ORDER BY projected_at ASC, task_id ASC LIMIT ?",
    ).all(cutoff, maxCount)
    let recovered = 0
    for (const row of rows) {
      this._deliverOwnerNoticeRow(row)
      recovered += 1
    }
    return recovered
  }

  /** Read the projected owner-notice metadata for a sealed batch when projected. */
  getOwnerNoticeProjection(batchId) {
    const row = this.db.prepare('SELECT * FROM autoreply_owner_notices WHERE batch_id=?').get(batchId)
    if (!row) return null
    return Object.freeze({
      duplicate: false,
      noticeKey: row.notice_key,
      ownerNoticeId: row.owner_notice_id,
      originNoticeId: row.origin_notice_id,
      handoffFile: row.handoff_file,
      state: row.state,
      projectedAt: row.projected_at,
    })
  }

  /** List all persisted auto-reply owner notice projections ordered chronologically. */
  listOwnerNoticeProjections() {
    return this.db.prepare('SELECT * FROM autoreply_owner_notices ORDER BY projected_at ASC, task_id ASC').all()
  }

  /** Abort any active generation waiters and close the private SQLite handle without late DB writes. */
  close() {
    this._closed = true
    for (const entry of [...this._inFlight]) {
      try { entry.onClose() } catch { /* Ignore close notification failure. */ }
    }
    this._inFlight.clear()
    if (this.db.isOpen) this.db.close()
  }
}

const OWNER_REASON_LABELS = Object.freeze({
  tail_snapshot_required_for_unattended: '无人值守模式缺少实时会话尾部核验快照，已阻断发送并转人工核查',
  friend_unverified_source_proof: '好友来信缺少可信桌面端作者核验依据，已阻断自动拟稿',
  group_unverified_trigger_proof: '群聊来信缺少可信 @本人 或引用本人核验依据，已阻断自动拟稿',
  operator_binding_missing: '飞书操作者绑定缺失或未生效，已阻断自动回复',
  operator_binding_revoked: '飞书操作者绑定已撤销，已阻断自动回复',
  operator_binding_unreadable: '飞书操作者绑定状态不可读，已阻断自动回复',
  pause_view_untrusted: '人工回复暂停控制视图不可信或已损坏，已阻断自动回复',
  account_binding_changed: '当前微信账号绑定发生变化，已阻断自动回复',
  target_binding_changed_or_missing: '目标联系人身份证明已变更或缺失，已阻断自动回复',
  unverified_source_proof: '入站事件来源证明不完整，已阻断自动回复',
  desktop_review: '微信桌面端标记了需人工核查的消息',
  'desktop_review:group_cursor_scroll_gap_ambiguous': '群聊历史有断层，需核对消息的新旧',
  'desktop_review:plain_space_mention_not_triggered': '已读到提及文字，尚无法确认微信原生 @',
  automatic_source_missing: '找不到可核验的原消息，已停止发送',
  automatic_draft_expired: '拟稿已过期，已停止发送',
  automatic_dispatch_refused: '入队时授权或发送条件未通过，未提交微信桌面端',
  automatic_dispatch_result_unknown: '入队结果不确定，请核查微信发送记录',
  tail_snapshot_stale: '会话尾部核验快照已过期，请刷新后人工确认',
  tail_uncertain: '会话尾部状态不确定，已转人工核查',
  tail_contains_unknown_sender: '会话尾部存在未识别发言人，已转人工核查',
  tail_does_not_match_trigger_event: '会话尾部最新消息已变化，已阻断自动拟稿',
  tail_digest_mismatch: '会话尾部摘要校验失败，已转人工核查',
  tail_proof_mismatch: '会话尾部身份证明与原批次不一致',
  research_requires_report_opt_in: '涉及投研问题且该会话未开启已发布报告引用授权，已转人工处理',
  no_published_report_provider: '涉及投研问题但当前未接入已验证的已发布报告来源，已转人工处理',
})

function summarizeOutcomeReasonForOwner(reason) {
  if (typeof reason !== 'string' || !reason.trim()) {
    return '需要人工核查后决定是否回复'
  }
  const clean = reason.trim()
  if (Object.hasOwn(OWNER_REASON_LABELS, clean)) {
    return OWNER_REASON_LABELS[clean]
  }
  if (clean.startsWith('desktop_review:')) {
    return `微信桌面端标记了需人工核查的消息（${clean.slice('desktop_review:'.length)}）`
  }
  if (clean.startsWith('restricted_adapter_')) {
    return '受限模型运行通道处于故障或未恢复状态，已阻断自动评估'
  }
  if (clean.startsWith('superseded_by_new_activity:')) {
    return '检测到会话新增回复或人工介入，已停止本次拟稿'
  }
  if (clean.startsWith('financial_transaction:')) {
    return '涉及转账、红包或资金往来请求，按规则转人工处理'
  }
  if (clean.startsWith('sensitive_disclosure:')) {
    return '涉及微信号、密钥或服务器内部信息询问，按规则转人工处理'
  }
  if (clean.startsWith('trade_instruction_or_promise:')) {
    return '涉及交易指令或收益承诺询问，按规则转人工处理'
  }
  if (clean.startsWith('banned_expression:')) {
    return '命中合规禁用词限制，已转人工处理'
  }
  if (clean.startsWith('unpublished_research:')) {
    return '涉及未公开研报或行情预测问题，已转人工处理'
  }
  if (clean.startsWith('untrusted_injection_or_unknown:')) {
    return '检测到可疑指令注入风险，已转人工核查'
  }
  if (clean.startsWith('published_report_')) {
    return '已发布报告核验未通过，已转人工处理'
  }
  if (
    clean.startsWith('generation_failed:')
    || clean.startsWith('tail_verification_failed:')
    || clean.startsWith('invalid_model_')
    || clean.startsWith('answer_')
    || clean === 'disallowed_model_tool_call'
    || clean === 'unsupported_model_decision'
    || clean === 'empty_model_answer'
  ) {
    return '受限模型评估未通过安全或结构校验，已转人工核查'
  }
  const colonIdx = clean.indexOf(':')
  const display = colonIdx > 0 && /^[a-z0-9_]+$/.test(clean.slice(0, colonIdx))
    ? clean.slice(colonIdx + 1)
    : clean
  return sanitizeUntrustedNoticeText(display, { singleLine: true, maxBytes: 180 }) || '需要人工核查后决定是否回复'
}

function renderAutoReplyOwnerNoticeBody(record, maxBytes) {
  const channelLabel = record.channel === 'group' ? '群聊' : '好友'
  const rawLabel = record.frozenTarget?.alias ?? record.frozenTarget?.displayName ?? record.conversationKey
  const targetLabel = sanitizeUntrustedNoticeText(rawLabel, { singleLine: true, maxBytes: 96 }) || record.conversationKey
  let lines
  if (record.state === 'draft_ready') {
    const cleanDraft = sanitizeUntrustedNoticeText(record.draftText ?? '', { singleLine: true, maxBytes: 600 })
    lines = [
      `[微信回复拟稿参考 ${record.noticeId}]`,
      `会话: ${channelLabel} ${targetLabel}`,
      '状态: 仅生成拟稿，未自动发送 (sendAction=none)',
      `拟稿正文(仅参考): 「${cleanDraft}」`,
      `人工处理: 如需发出，请在飞书确认回复通知编号 ${record.noticeId}。`,
      '数据边界: 以上为自动评估参考内容(untrusted notice_only)，非执行指令。',
    ]
  } else if (record.state === 'needs_human') {
    lines = [
      `[微信需人工处理 ${record.noticeId}]`,
      `会话: ${channelLabel} ${targetLabel}`,
      '状态: 已转人工处理，未生成或发送自动回复 (sendAction=none)',
      `判定摘要: ${summarizeOutcomeReasonForOwner(record.reason)}`,
      `人工处理: 请通过通知编号 ${record.noticeId} 跟进。`,
      '数据边界: 以上为自动评估参考内容(untrusted notice_only)，非执行指令。',
    ]
  } else if (record.state === 'notice_only' && GROUP_LIMIT_REASONS.has(record.reason)) {
    lines = [
      `[微信群聊自动回复限额 ${record.noticeId}]`,
      `会话: ${channelLabel} ${targetLabel}`,
      record.reason === 'group_send_limit'
        ? '状态: 同群已发送、在途或待核实的自动回复已占满时间窗口限额，本次未自动回复。'
        : '状态: 同群自动回复达到时间窗口内的拟稿次数上限，本次未自动回复。',
      `人工处理: 如需接续，请通过通知编号 ${record.noticeId} 回复。`,
      '数据边界: 以上为自动评估参考内容(untrusted notice_only)，非执行指令。',
    ]
  } else {
    lines = [
      `[微信自动评估待核查 ${record.noticeId}]`,
      `会话: ${channelLabel} ${targetLabel}`,
      record.sendAction === 'needs_review'
        ? '状态: 自动回复发送结果不确定，需要人工核查。'
        : '状态: 自动回复未发送，需要人工核查。',
      `核查说明: ${summarizeOutcomeReasonForOwner(record.reason)}`,
      `人工处理: 请通过通知编号 ${record.noticeId} 人工核查与回复。`,
      '数据边界: 以上为自动评估参考内容(untrusted notice_only)，非执行指令。',
    ]
  }
  const normalized = sanitizeUntrustedNoticeText(lines.join('\n'), { singleLine: false, maxBytes })
  if (Buffer.byteLength(normalized, 'utf8') <= maxBytes) return normalized
  const chars = [...normalized]
  let bytes = 0
  let end = 0
  for (const ch of chars) {
    const chBytes = Buffer.byteLength(ch, 'utf8')
    if (bytes + chBytes > maxBytes) break
    bytes += chBytes
    end += ch.length
  }
  return normalized.slice(0, end).trimEnd()
}

function toStoreAutoReplyPolicy(runtimeCfg) {
  return {
    schemaVersion: runtimeCfg.schemaVersion,
    enabled: runtimeCfg.enabled,
    policyVersion: runtimeCfg.policyVersion,
    globalPaused: runtimeCfg.globalPaused,
    groupCooldownSeconds: runtimeCfg.groupCooldownSeconds,
    maxInboundAgeSeconds: runtimeCfg.maxInboundAgeSeconds,
    maxTailAgeSeconds: runtimeCfg.maxTailAgeSeconds,
    maxReportAgeSeconds: runtimeCfg.maxReportAgeSeconds,
    claimLeaseSeconds: runtimeCfg.claimLeaseSeconds,
    adapterTimeoutMs: runtimeCfg.adapterTimeoutMs,
    maxAttempts: runtimeCfg.maxAttempts,
    maxContextMessages: runtimeCfg.maxContextMessages,
    maxFriendReplyChars: runtimeCfg.maxFriendReplyChars,
    maxGroupReplyChars: runtimeCfg.maxGroupReplyChars,
    bannedExpressions: [...runtimeCfg.bannedExpressions],
    conversations: Object.fromEntries(
      Object.entries(runtimeCfg.conversations).map(([convKey, rule]) => [
        convKey,
        {
          mode: rule.mode,
          allowPublishedReportFacts: rule.allowPublishedReportFacts,
          ...(rule.allowRegisteredSenderMentions ? { allowRegisteredSenderMentions: true } : {}),
          allowedTopics: [...rule.allowedTopics],
          ...(rule.tone === null ? {} : { tone: rule.tone }),
          ...(rule.cooldownSeconds === null ? {} : { cooldownSeconds: rule.cooldownSeconds }),
          ...(rule.maxRepliesPerWindow === undefined ? {} : { maxRepliesPerWindow: rule.maxRepliesPerWindow }),
          ...(rule.maxReplyChars === null ? {} : { maxReplyChars: rule.maxReplyChars }),
        },
      ]),
    ),
  }
}

function normalizeTransportBaseUrlKey(baseURL) {
  if (typeof baseURL !== 'string' || !baseURL.trim()) return null
  try {
    return new URL(baseURL.trim()).toString().replace(/\/+$/, '')
  } catch {
    return baseURL.trim()
  }
}

/** Build a canonical fingerprint across all adapter-material transport settings to detect live drift. */
export function canonicalTransportKey(transport, defaultInstallAnchor = null) {
  if (!transport || typeof transport !== 'object') return null
  const mode = transport.mode ?? null
  return JSON.stringify({
    mode,
    installAnchor: transport.installAnchor ?? defaultInstallAnchor ?? null,
    provider: transport.provider ?? (mode === 'deepseek-api-key' ? 'deepseek-official' : 'isolated-autoreply'),
    model: transport.model ?? (mode === 'deepseek-api-key' ? 'deepseek-flash' : 'isolated-autoreply-model'),
    maxTokens: transport.maxTokens ?? 512,
    maxRawOutputBytes: transport.maxRawOutputBytes ?? 8192,
    maxUserInputBytes: transport.maxUserInputBytes ?? 131_072,
    reasoningEffort: transport.reasoningEffort ?? null,
    shutdownTimeoutMs: transport.shutdownTimeoutMs ?? 50,
    maxOrphanedRuns: transport.maxOrphanedRuns ?? 1,
    maxTotalOrphanedRuns: transport.maxTotalOrphanedRuns ?? 4,
    apiKeyEnv: transport.apiKeyEnv ?? DEFAULT_AUTOREPLY_API_KEY_ENV,
    allowSharedDeepSeekApiKey: transport.allowSharedDeepSeekApiKey === true,
    baseURL: normalizeTransportBaseUrlKey(transport.baseURL),
    allowLoopbackHttp: transport.allowLoopbackHttp === true,
  })
}

/**
 * Create a bounded, non-overlapping DSH auto-reply draft-evaluation pump connected to
 * `WechatExchangeProjector`'s durable sealed friend/group batches and `WechatAutoReplyStore`.
 *
 * Strictly default-off (`returns null` when `policy.wechatAutoReply.enabled !== true`), requires
 * explicit transport configuration and DSH installation anchor, never blocks synchronous exchange
 * projection ticks, never regenerates terminal claims after restart, fails closed on missing or
 * drifted policy/transport state, and never sends to WeChat.
 */
export function createWechatAutoReplyPump(options = {}) {
  const {
    policy,
    policyPath = null,
    projector,
    store: providedStore = null,
    adapter: providedAdapter = null,
    adapterFactory = null,
    resolveEvaluationInputs = null,
    dispatchUnattended = null,
    clock = () => Date.now(),
    statUid = defaultStatUid,
    verifyOperatorBinding = true,
    verifyEvidenceFiles = true,
    cooperativeShutdownMs,
    maxOrphanedAdapterTasks,
    maxOrphanedBackgroundTasks,
    maxRetiredOrphanedTasks,
    onError = null,
  } = options

  const runtimeCfg = wechatAutoReplyPolicy(policy)
  if (!runtimeCfg.enabled) return null
  if (!projector || typeof projector.listSealedBatches !== 'function') {
    throw new Error('createWechatAutoReplyPump requires a WechatExchangeProjector instance')
  }
  const notifCfg = wechatNotificationsPolicy(policy)
  if (!notifCfg.enabled) {
    throw new Error('WeChat auto-reply pump requires wechatNotifications enabled: true')
  }

  let adapter = providedAdapter
  let ownsAdapter = false
  let resolvedAnchor = runtimeCfg.transport?.installAnchor ?? null
  if (!adapter) {
    const tCfg = runtimeCfg.transport
    if (!tCfg) {
      throw new Error('WeChat auto-reply pump requires explicit transport configuration')
    }
    resolvedAnchor = resolveDshInstallAnchor(tCfg.installAnchor)
    if (tCfg.mode === 'adapter') {
      if (typeof adapterFactory !== 'function') {
        throw new Error('WeChat auto-reply transport="adapter" requires an explicit adapterFactory')
      }
      adapter = adapterFactory({
        policy,
        autoReplyPolicy: runtimeCfg,
        transport: tCfg,
        installAnchor: resolvedAnchor,
      })
      ownsAdapter = true
    } else if (tCfg.mode === 'deepseek-api-key') {
      adapter = typeof adapterFactory === 'function'
        ? adapterFactory({
          policy,
          autoReplyPolicy: runtimeCfg,
          transport: tCfg,
          installAnchor: resolvedAnchor,
        })
        : createIsolatedDshAutoReplyAdapter({
          ...tCfg,
          transport: 'deepseek-api-key',
          installAnchor: resolvedAnchor,
        })
      ownsAdapter = true
    } else {
      throw new Error(`Unsupported wechatAutoReply transport mode: ${String(tCfg.mode)}`)
    }
  }
  if (!adapter || typeof adapter.generate !== 'function') {
    throw new Error('WeChat auto-reply adapter factory must return an adapter with generate()')
  }

  const initialTransportKey = canonicalTransportKey(runtimeCfg.transport, resolvedAnchor)
  const ownsStore = !providedStore
  const store = providedStore ?? new WechatAutoReplyStore(policy, toStoreAutoReplyPolicy(runtimeCfg), {
    policyPath,
    expectedTransportKey: initialTransportKey,
    defaultInstallAnchor: resolvedAnchor,
    clock,
    statUid,
    verifyOperatorBinding,
    verifyEvidenceFiles,
    ...(cooperativeShutdownMs !== undefined ? { cooperativeShutdownMs } : {}),
    ...(maxOrphanedAdapterTasks !== undefined ? { maxOrphanedAdapterTasks } : {}),
    ...(maxOrphanedBackgroundTasks !== undefined ? { maxOrphanedBackgroundTasks } : {}),
    ...(maxRetiredOrphanedTasks !== undefined ? { maxRetiredOrphanedTasks } : {}),
  })
  store.dispatchEnabled = typeof dispatchUnattended === 'function'

  let activePolicy = policy
  let activeCfg = runtimeCfg
  let activeNotifCfg = notifCfg
  let contiguousScanCursor = null
  let stopped = false
  let inFlight = null
  let lastTriggerAtMs = null

  const dispatchReadyDrafts = async () => {
    if (typeof dispatchUnattended !== 'function') return
    const drafts = store.db.prepare(`
      SELECT * FROM autoreply_tasks
      WHERE requested_mode='unattended' AND state='draft_ready' AND send_action='none'
      ORDER BY decided_at, task_id LIMIT ?
    `).all(activeCfg.maxBatchesPerCycle)
    for (const row of drafts) {
      if (stopped || !store.db.isOpen) return
      let event = null
      let source = null
      try {
        const { events } = readVerifiedInboxBatch(activePolicy, row.batch_id, {
          contextLimit: 0, statUid,
        })
        event = events.at(-1)
        source = desktopReplyOrigin(event)
      } catch (err) {
        reportSafeError('Automatic reply source requires review', err)
      }
      if (!store.db.isOpen) return
      const nowMs = clock()
      let rejection = null
      if (!source) rejection = 'automatic_source_missing'
      else if (row.policy_version !== activeCfg.policyVersion) rejection = 'policy_version_changed'
      else if (store.isRuntimeGlobalPaused()) rejection = 'global_paused'
      const reserved = store.reserveAutomaticSend(row.task_id, nowMs, rejection)
      if (!reserved) continue
      let outcome = reserved
      if (reserved.sendAction === 'reserved') {
        let action = 'needs_review'
        let deadlineAt = null
        try {
          const result = await dispatchUnattended({
            decision: reserved, source, sourceText: event.text, policy: activePolicy, clock,
            dispatchBy: row.decided_at + AUTOMATIC_DRAFT_LIFETIME_MS,
          })
          if (result === false) action = 'not_sent'
          else if (result === true || Number.isSafeInteger(result?.deadlineAt)) action = 'queued'
          deadlineAt = result?.deadlineAt ?? null
        } catch (err) {
          reportSafeError('Automatic reply dispatch requires review', err)
        }
        if (!store.db.isOpen) return
        outcome = store.finishAutomaticDispatch(row.task_id, action, clock(), deadlineAt)
      }
      if (outcome && activeCfg.projectOwnerNotices === true) {
        store.projectOutcomeOwnerNotice(outcome, { projectOwnerNotices: true })
      }
    }
  }

  const reportSafeError = (contextLabel, error) => {
    if (typeof onError !== 'function') return
    const rawMsg = error instanceof Error ? error.message : String(error)
    const clean = sanitizeUntrustedNoticeText(rawMsg, { singleLine: true, maxBytes: 180 })
    try {
      onError(`${contextLabel}: ${clean || 'failed'}`)
    } catch {
      // Ignore error observer failures.
    }
  }

  const runCycle = async () => {
    if (stopped || store._closed || !store.db.isOpen) {
      return Object.freeze({ skipped: true, reason: 'stopped', evaluated: Object.freeze([]), ownerNoticesProjected: 0 })
    }
    if (policyPath) {
      if (!existsSync(policyPath)) {
        store.syncPolicy(activePolicy, { ...toStoreAutoReplyPolicy(activeCfg), enabled: false })
        reportSafeError('Live policy file missing', new Error('Configured policy file is missing'))
        return Object.freeze({ skipped: true, reason: 'policy_missing', evaluated: Object.freeze([]), ownerNoticesProjected: 0 })
      }
      try {
        activePolicy = readPolicy(policyPath)
      } catch (err) {
        store.syncPolicy(activePolicy, { ...toStoreAutoReplyPolicy(activeCfg), enabled: false })
        reportSafeError('Live policy reload refused', err)
        return Object.freeze({ skipped: true, reason: 'policy_untrusted', evaluated: Object.freeze([]), ownerNoticesProjected: 0 })
      }
      activeNotifCfg = wechatNotificationsPolicy(activePolicy)
      activeCfg = wechatAutoReplyPolicy(activePolicy)
    }
    store.syncPolicy(activePolicy, toStoreAutoReplyPolicy(activeCfg))

    if (!activeNotifCfg.enabled || !activeCfg.enabled) {
      return Object.freeze({ skipped: true, reason: 'disabled', evaluated: Object.freeze([]), ownerNoticesProjected: 0 })
    }
    if (canonicalTransportKey(activeCfg.transport, resolvedAnchor) !== initialTransportKey) {
      store.syncPolicy(activePolicy, { ...toStoreAutoReplyPolicy(activeCfg), enabled: false })
      reportSafeError('Live transport configuration changed', new Error('Restart required after transport change'))
      return Object.freeze({ skipped: true, reason: 'transport_changed_requires_restart', evaluated: Object.freeze([]), ownerNoticesProjected: 0 })
    }

    let ownerNoticesProjected = 0
    if (activeCfg.projectOwnerNotices === true) {
      try {
        ownerNoticesProjected += store.flushPendingOwnerNotices({ maxCount: 50 })
      } catch (err) {
        reportSafeError('Pending owner notice flush failed', err)
      }
    }

    const PAGE_LIMIT = 200
    const candidates = []
    const seenBatchIds = new Set()
    const forwardPage = projector.listSealedBatches({
      kinds: ['friend', 'group'],
      limit: PAGE_LIMIT,
      order: 'asc',
      ...(contiguousScanCursor ? { after: contiguousScanCursor } : {}),
    })
    let contiguousBlocked = false
    for (const batch of forwardPage) {
      if (store.getTaskByBatchId(batch.batchId)) {
        if (!contiguousBlocked) {
          contiguousScanCursor = { sealedAt: batch.sealedAt, batchId: batch.batchId }
        }
        continue
      }
      contiguousBlocked = true
      seenBatchIds.add(batch.batchId)
      if (candidates.length < activeCfg.maxBatchesPerCycle) {
        candidates.push(batch)
      }
    }
    if (forwardPage.length === PAGE_LIMIT && candidates.length < activeCfg.maxBatchesPerCycle) {
      const tailLimit = Math.min(PAGE_LIMIT, Math.max(activeCfg.maxBatchesPerCycle * 10, 50))
      const tailDesc = projector.listSealedBatches({
        kinds: ['friend', 'group'],
        limit: tailLimit,
        order: 'desc',
      })
      const tailAsc = [...tailDesc].sort((a, b) => (
        (a.sealedAt - b.sealedAt)
        || (a.batchId < b.batchId ? -1 : a.batchId > b.batchId ? 1 : 0)
      ))
      for (const batch of tailAsc) {
        if (seenBatchIds.has(batch.batchId)) continue
        seenBatchIds.add(batch.batchId)
        if (!store.getTaskByBatchId(batch.batchId)) {
          candidates.push(batch)
          if (candidates.length >= activeCfg.maxBatchesPerCycle) break
        }
      }
    }
    if (candidates.length === 0) {
      await dispatchReadyDrafts()
      return Object.freeze({ skipped: false, reason: null, evaluated: Object.freeze([]), ownerNoticesProjected })
    }

    const evaluated = []
    for (const batch of candidates) {
      if (stopped || store._closed || !store.db.isOpen || !activeCfg.enabled || !activeNotifCfg.enabled) {
        break
      }
      let evalInputs = {}
      if (typeof resolveEvaluationInputs === 'function') {
        try {
          evalInputs = (await resolveEvaluationInputs({
            batch,
            policy: activePolicy,
            autoReplyPolicy: activeCfg,
            nowMs: clock(),
          })) ?? {}
        } catch (err) {
          reportSafeError('Failed resolving auto-reply evaluation inputs', err)
          evalInputs = {}
        }
      }
      if (stopped || store._closed || !store.db.isOpen) {
        break
      }
      const {
        adapter: _ignoredAdapter,
        ...safeEvalInputs
      } = evalInputs && typeof evalInputs === 'object' && !Array.isArray(evalInputs) ? evalInputs : {}

      const outcome = await store.evaluateSealedBatch(batch.batchId, {
        ...safeEvalInputs,
        adapter,
      })
      if (stopped || store._closed || !store.db.isOpen || outcome.abortedOnClose) {
        break
      }

      let ownerNoticeProjected = false
      const candidateForDispatch = typeof dispatchUnattended === 'function'
        && outcome.state === 'draft_ready' && outcome.requestedMode === 'unattended'
      if (!candidateForDispatch && !outcome.duplicate && activeCfg.projectOwnerNotices === true) {
        try {
          const projected = store.projectOutcomeOwnerNotice(outcome, { projectOwnerNotices: true })
          ownerNoticeProjected = Boolean(projected && !projected.duplicate)
          if (ownerNoticeProjected) ownerNoticesProjected += 1
        } catch (err) {
          reportSafeError('Owner notice handoff projection failed', err)
        }
      }
      evaluated.push(Object.freeze({
        batchId: outcome.batchId,
        noticeId: outcome.noticeId,
        state: outcome.state,
        eligibilityDecision: outcome.eligibilityDecision,
        sendAction: outcome.sendAction,
        duplicate: outcome.duplicate,
        ownerNoticeProjected,
      }))
    }
    await dispatchReadyDrafts()

    return Object.freeze({
      skipped: false,
      reason: null,
      evaluated: Object.freeze(evaluated),
      ownerNoticesProjected,
    })
  }

  const pumpOnce = ({ force = true } = {}) => {
    if (inFlight) return inFlight
    if (stopped || store._closed || !store.db.isOpen) {
      return Promise.resolve(Object.freeze({ skipped: true, reason: 'stopped', evaluated: Object.freeze([]), ownerNoticesProjected: 0 }))
    }
    const nowMs = clock()
    if (
      !force
      && lastTriggerAtMs !== null
      && activeCfg.pumpIntervalSeconds > activeNotifCfg.projectionIntervalSeconds
      && nowMs - lastTriggerAtMs < activeCfg.pumpIntervalSeconds * 1000
    ) {
      return Promise.resolve(Object.freeze({ skipped: true, reason: 'interval_not_elapsed', evaluated: Object.freeze([]), ownerNoticesProjected: 0 }))
    }
    lastTriggerAtMs = nowMs
    const run = Promise.resolve()
      .then(() => runCycle())
      .catch((err) => {
        reportSafeError('WeChat auto-reply pump cycle failed', err)
        return Object.freeze({ skipped: true, reason: 'cycle_failed', evaluated: Object.freeze([]), ownerNoticesProjected: 0 })
      })
      .finally(() => {
        if (inFlight === run) inFlight = null
      })
    inFlight = run
    return run
  }

  return Object.freeze({
    store,
    adapter,
    trigger() {
      return pumpOnce({ force: false })
    },
    pumpOnce() {
      return pumpOnce({ force: true })
    },
    isRunning() {
      return inFlight !== null
    },
    async whenIdle() {
      if (inFlight) {
        await inFlight.catch(() => {})
      }
      if (store.db.isOpen) {
        await store.whenBackgroundIdle()
      }
      if (typeof adapter.whenIdle === 'function') {
        await adapter.whenIdle().catch(() => {})
      }
    },
    stop() {
      if (stopped) return
      stopped = true
      if (ownsStore) {
        try {
          store.close()
        } catch {
          // Best-effort shutdown close.
        }
      }
      if (ownsAdapter && typeof adapter.dispose === 'function') {
        void Promise.resolve(adapter.dispose()).catch(() => {})
      }
    },
  })
}

function hydrateTaskRow(row, duplicate) {
  return Object.freeze({
    batchId: row.batch_id,
    taskId: row.task_id,
    noticeId: row.notice_id,
    conversationKey: row.conversation_key,
    channel: row.channel,
    requestedMode: row.requested_mode,
    state: row.state,
    eligibilityDecision: row.eligibility_decision,
    sendAction: row.send_action,
    policyVersion: row.policy_version,
    attempts: row.attempts,
    maxAttempts: row.max_attempts,
    eventIds: Object.freeze(JSON.parse(row.event_ids)),
    frozenTarget: row.frozen_target ? Object.freeze(JSON.parse(row.frozen_target)) : null,
    contextTailFingerprint: row.context_tail_fingerprint,
    providerRunId: row.provider_run_id,
    draftText: row.draft_text,
    reason: row.reason,
    claimedAt: row.claimed_at,
    leaseExpiresAt: row.lease_expires_at,
    batchExpiresAt: row.batch_expires_at,
    decidedAt: row.decided_at,
    duplicate,
  })
}
