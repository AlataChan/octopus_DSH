/** Root-owned deployment policy and strict requests for the Linux operations broker. */
import { readFileSync, realpathSync, statSync } from 'node:fs'
import { isAbsolute } from 'node:path'

const CONTACT_ID_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$/
const AUTOREPLY_CONVERSATION_KEY_RE = /^(friend|group):[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$/
const SAFE_ENV_NAME_RE = /^[A-Z][A-Z0-9_]*$/
const SHARED_DEEPSEEK_API_KEY_ENV = 'DEEPSEEK_API_KEY'

export const AUTOREPLY_CONVERSATION_MODES = Object.freeze(['disabled', 'draft_only', 'unattended'])
export const AUTO_REPLY_TRANSPORT_MODES = Object.freeze(['deepseek-api-key', 'adapter'])
export const DEFAULT_BANNED_EXPRESSIONS = Object.freeze(['卖策略', '开仓状态', '禁止追单'])

export const AUTO_REPLY_POLICY_BOUNDS = Object.freeze({
  groupCooldownSeconds: [60, 86_400],
  maxInboundAgeSeconds: [60, 86_400],
  maxTailAgeSeconds: [5, 300],
  maxReportAgeSeconds: [60, 86_400],
  claimLeaseSeconds: [5, 600],
  adapterTimeoutMs: [20, 120_000],
  maxAttempts: [1, 3],
  maxContextMessages: [1, 50],
  maxFriendReplyChars: [20, 500],
  maxGroupReplyChars: [20, 120],
})

const AUTO_REPLY_PUMP_BOUNDS = Object.freeze({
  maxBatchesPerCycle: [1, 20],
  pumpIntervalSeconds: [1, 300],
})

const AUTO_REPLY_TRANSPORT_INT_BOUNDS = Object.freeze({
  maxTokens: [32, 4096],
  maxRawOutputBytes: [256, 32_768],
  maxUserInputBytes: [512, 262_144],
  shutdownTimeoutMs: [1, 10_000],
  maxOrphanedRuns: [1, 32],
  maxTotalOrphanedRuns: [1, 64],
})

export const DEFAULT_WECHAT_AUTOREPLY_CORE_POLICY = Object.freeze({
  schemaVersion: 1,
  enabled: false,
  policyVersion: 1,
  globalPaused: false,
  groupCooldownSeconds: 300,
  maxInboundAgeSeconds: 900,
  maxTailAgeSeconds: 60,
  maxReportAgeSeconds: 86_400,
  claimLeaseSeconds: 60,
  adapterTimeoutMs: 30_000,
  maxAttempts: 1,
  maxContextMessages: 12,
  maxFriendReplyChars: 220,
  maxGroupReplyChars: 120,
  bannedExpressions: DEFAULT_BANNED_EXPRESSIONS,
  conversations: Object.freeze({}),
})

export const DEFAULT_WECHAT_AUTOREPLY_RUNTIME_POLICY = Object.freeze({
  ...DEFAULT_WECHAT_AUTOREPLY_CORE_POLICY,
  projectOwnerNotices: false,
  maxBatchesPerCycle: 2,
  pumpIntervalSeconds: 2,
  transport: null,
})

export const DEFAULT_ADHOC_VIEWPORT = Object.freeze({
  maxChars: 220,
  maxLogicalLines: 4,
  minWindowWidth: 620,
  minWindowHeight: 800,
  colsPerVisualLine: 48,
  maxVisualLines: 12,
  maxUnbrokenAlnumCols: 48,
  maxUnbrokenTokenCols: 96,
  maxComposerHeightPx: 380,
})

const ADHOC_VIEWPORT_BOUNDS = {
  maxChars: [1, 220],
  maxLogicalLines: [1, 4],
  minWindowWidth: [620, 4096],
  minWindowHeight: [800, 4096],
  colsPerVisualLine: [20, 120],
  maxVisualLines: [1, 24],
  maxUnbrokenAlnumCols: [16, 120],
  maxUnbrokenTokenCols: [24, 240],
  maxComposerHeightPx: [120, 1000],
}

/** Reject unrecognized fields at the JSON request boundary. */
export function fields(value, required, optional = []) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Expected an object')
  if (Object.keys(value).some(key => ![...required, ...optional].includes(key))) throw new Error('Unknown field')
  if (required.some(key => !Object.hasOwn(value, key))) throw new Error('Missing field')
  return value
}

function assertTrimmedText(value, message) {
  if (typeof value !== 'string' || !value.trim() || value.trim() !== value) throw new Error(message)
}

/** Validate administrator-configured friends and enrolled groups without exposing private IDs or anchors. */
export function validateWechatContactsPolicy(contacts) {
  if (!Array.isArray(contacts) || contacts.length < 1 || contacts.length > 50) {
    throw new Error('Invalid wechatContacts registry')
  }
  const ids = new Set()
  const aliases = new Set()
  let legacyCount = 0
  for (const contact of contacts) {
    fields(contact, ['id', 'type', 'alias', 'displayName', 'enabled', 'capabilities'], ['legacy'])
    if (typeof contact.id !== 'string' || !CONTACT_ID_PATTERN.test(contact.id)) {
      throw new Error('Invalid contact id')
    }
    assertTrimmedText(contact.alias, 'Invalid contact alias')
    assertTrimmedText(contact.displayName, 'Invalid contact displayName')
    if (typeof contact.enabled !== 'boolean') throw new Error('Invalid contact enabled flag')
    if (contact.type === 'friend') {
      fields(contact.capabilities, ['directSend'])
      if (typeof contact.capabilities.directSend !== 'boolean') throw new Error('Invalid contact directSend capability')
      if (Object.hasOwn(contact, 'legacy')) {
        if (typeof contact.legacy !== 'boolean') throw new Error('Invalid contact legacy flag')
        if (contact.legacy) legacyCount++
      }
    } else if (contact.type === 'group') {
      if (Object.hasOwn(contact, 'legacy')) throw new Error('Group contacts cannot be legacy')
      fields(contact.capabilities, ['groupSend'])
      if (typeof contact.capabilities.groupSend !== 'boolean') throw new Error('Invalid contact groupSend capability')
    } else {
      throw new Error('Unsupported contact type')
    }
    if (ids.has(contact.id)) throw new Error('Duplicate contact id')
    if (aliases.has(contact.alias)) throw new Error('Duplicate contact alias')
    ids.add(contact.id)
    aliases.add(contact.alias)
  }
  if (legacyCount > 1) throw new Error('At most one legacy contact is allowed')
  for (const contact of contacts) {
    if (contacts.some(other => other !== contact && other.id === contact.alias)) {
      throw new Error('Contact id must not collide with another contact alias')
    }
  }
  return contacts
}

/** Validate optional ad hoc Moments policy configuration (disabled by default when omitted). */
export function validateAdhocMomentsPolicy(value) {
  fields(value, ['enabled'], ['defaultVisibility', 'viewport'])
  if (typeof value.enabled !== 'boolean') throw new Error('Invalid wechatAdhocMoments enabled flag')
  if (Object.hasOwn(value, 'defaultVisibility') && value.defaultVisibility !== null) {
    if (value.defaultVisibility !== 'private' && value.defaultVisibility !== 'friends') {
      throw new Error('Invalid wechatAdhocMoments defaultVisibility')
    }
  }
  if (Object.hasOwn(value, 'viewport')) {
    fields(value.viewport, [], Object.keys(ADHOC_VIEWPORT_BOUNDS))
    for (const [key, val] of Object.entries(value.viewport)) {
      const [low, high] = ADHOC_VIEWPORT_BOUNDS[key]
      if (!Number.isSafeInteger(val) || val < low || val > high) throw new Error(`Invalid viewport bound ${key}`)
    }
  }
  return value
}

export const WECHAT_RECEIPT_NOTIFICATION_STATES = Object.freeze([
  'sent_verified',
  'published_verified',
  'identity_verified',
  'needs_review',
  'failed_before_submit',
  'refused',
  'expired',
  'auth_required',
])

const WECHAT_NOTIFICATION_BOUNDS = {
  projectionIntervalSeconds: [1, 30],
  inboundAggregationSeconds: [15, 30],
  inboundMaxBatchWaitSeconds: [20, 120],
  inboundMaxEventsPerBatch: [1, 20],
  maxContextMessages: [1, 50],
  retentionDays: [1, 90],
  maxNoticeBytes: [256, 4096],
  manualReplyMaxAgeSeconds: [60, 86400],
}

export const DEFAULT_WECHAT_NOTIFICATIONS_POLICY = Object.freeze({
  enabled: false,
  manualReplyEnabled: false,
  manualReplyMaxAgeSeconds: 3600,
  handoffDir: null,
  handoffGid: null,
  receiptProducerUid: null,
  inboundProducerUid: null,
  outboxProducerUid: null,
  projectionIntervalSeconds: 2,
  inboundAggregationSeconds: 20,
  inboundMaxBatchWaitSeconds: 60,
  inboundMaxEventsPerBatch: 10,
  maxContextMessages: 20,
  retentionDays: 14,
  maxNoticeBytes: 2048,
  notifyOnReceiptStates: WECHAT_RECEIPT_NOTIFICATION_STATES,
})

/** Resolve normalized ad hoc Moments policy settings, defaulting strictly to disabled. */
export function adhocMomentsPolicy(policy) {
  const cfg = policy?.wechatAdhocMoments
  return {
    enabled: cfg?.enabled === true,
    defaultVisibility: cfg?.defaultVisibility ?? null,
    viewport: { ...DEFAULT_ADHOC_VIEWPORT, ...(cfg?.viewport ?? {}) },
  }
}

/** Validate optional WeChat->Feishu exchange projection policy (disabled by default when omitted). */
export function validateWechatNotificationsPolicy(value) {
  fields(
    value,
    ['enabled'],
    [
      'manualReplyEnabled',
      'manualReplyMaxAgeSeconds',
      'handoffDir',
      'handoffGid',
      'receiptProducerUid',
      'inboundProducerUid',
      'outboxProducerUid',
      'projectionIntervalSeconds',
      'inboundAggregationSeconds',
      'inboundMaxBatchWaitSeconds',
      'inboundMaxEventsPerBatch',
      'maxContextMessages',
      'retentionDays',
      'maxNoticeBytes',
      'notifyOnReceiptStates',
    ],
  )
  if (typeof value.enabled !== 'boolean') throw new Error('Invalid wechatNotifications enabled flag')
  if (Object.hasOwn(value, 'manualReplyEnabled') && typeof value.manualReplyEnabled !== 'boolean') {
    throw new Error('Invalid wechatNotifications manualReplyEnabled flag')
  }
  if (value.manualReplyEnabled === true && value.enabled !== true) {
    throw new Error('wechatNotifications manualReplyEnabled requires wechatNotifications enabled: true')
  }
  if (Object.hasOwn(value, 'handoffDir') && value.handoffDir !== null) {
    if (typeof value.handoffDir !== 'string' || !value.handoffDir.startsWith('/')) {
      throw new Error('Expected absolute wechatNotifications handoffDir path')
    }
  }
  if (value.enabled && (typeof value.handoffDir !== 'string' || !value.handoffDir.startsWith('/'))) {
    throw new Error('wechatNotifications handoffDir is required when enabled')
  }
  for (const key of ['handoffGid', 'receiptProducerUid', 'inboundProducerUid', 'outboxProducerUid']) {
    if (Object.hasOwn(value, key) && value[key] !== null) {
      if (!Number.isSafeInteger(value[key]) || value[key] < 0) {
        throw new Error(`Invalid wechatNotifications ${key}`)
      }
    }
  }
  for (const [key, [low, high]] of Object.entries(WECHAT_NOTIFICATION_BOUNDS)) {
    if (Object.hasOwn(value, key)) {
      if (!Number.isSafeInteger(value[key]) || value[key] < low || value[key] > high) {
        throw new Error(`Invalid wechatNotifications ${key}`)
      }
    }
  }
  const aggSeconds = value.inboundAggregationSeconds ?? DEFAULT_WECHAT_NOTIFICATIONS_POLICY.inboundAggregationSeconds
  const maxWaitSeconds = value.inboundMaxBatchWaitSeconds ?? DEFAULT_WECHAT_NOTIFICATIONS_POLICY.inboundMaxBatchWaitSeconds
  if (maxWaitSeconds < aggSeconds) {
    throw new Error('wechatNotifications inboundMaxBatchWaitSeconds must be >= inboundAggregationSeconds')
  }
  if (Object.hasOwn(value, 'notifyOnReceiptStates')) {
    if (!Array.isArray(value.notifyOnReceiptStates) || value.notifyOnReceiptStates.length === 0) {
      throw new Error('Invalid wechatNotifications notifyOnReceiptStates')
    }
    const seen = new Set()
    for (const state of value.notifyOnReceiptStates) {
      if (typeof state !== 'string' || !WECHAT_RECEIPT_NOTIFICATION_STATES.includes(state) || seen.has(state)) {
        throw new Error('Invalid wechatNotifications receipt notification state')
      }
      seen.add(state)
    }
  }
  return value
}

/** Resolve normalized WeChat->Feishu exchange projection settings, defaulting strictly to disabled. */
export function wechatNotificationsPolicy(policy) {
  const cfg = policy?.wechatNotifications
  const enabled = cfg?.enabled === true
  return {
    enabled,
    manualReplyEnabled: enabled && cfg?.manualReplyEnabled === true,
    manualReplyMaxAgeSeconds: cfg?.manualReplyMaxAgeSeconds ?? DEFAULT_WECHAT_NOTIFICATIONS_POLICY.manualReplyMaxAgeSeconds,
    handoffDir: cfg?.handoffDir ?? null,
    handoffGid: cfg?.handoffGid ?? null,
    receiptProducerUid: cfg?.receiptProducerUid ?? null,
    inboundProducerUid: cfg?.inboundProducerUid ?? null,
    outboxProducerUid: cfg?.outboxProducerUid ?? null,
    projectionIntervalSeconds: cfg?.projectionIntervalSeconds ?? DEFAULT_WECHAT_NOTIFICATIONS_POLICY.projectionIntervalSeconds,
    inboundAggregationSeconds: cfg?.inboundAggregationSeconds ?? DEFAULT_WECHAT_NOTIFICATIONS_POLICY.inboundAggregationSeconds,
    inboundMaxBatchWaitSeconds: cfg?.inboundMaxBatchWaitSeconds ?? DEFAULT_WECHAT_NOTIFICATIONS_POLICY.inboundMaxBatchWaitSeconds,
    inboundMaxEventsPerBatch: cfg?.inboundMaxEventsPerBatch ?? DEFAULT_WECHAT_NOTIFICATIONS_POLICY.inboundMaxEventsPerBatch,
    maxContextMessages: cfg?.maxContextMessages ?? DEFAULT_WECHAT_NOTIFICATIONS_POLICY.maxContextMessages,
    retentionDays: cfg?.retentionDays ?? DEFAULT_WECHAT_NOTIFICATIONS_POLICY.retentionDays,
    maxNoticeBytes: cfg?.maxNoticeBytes ?? DEFAULT_WECHAT_NOTIFICATIONS_POLICY.maxNoticeBytes,
    notifyOnReceiptStates: cfg?.notifyOnReceiptStates
      ? Object.freeze([...cfg.notifyOnReceiptStates])
      : WECHAT_RECEIPT_NOTIFICATION_STATES,
  }
}

const AUTO_REPLY_CORE_OPTIONAL_FIELDS = Object.freeze([
  'schemaVersion',
  'globalPaused',
  'groupCooldownSeconds',
  'maxInboundAgeSeconds',
  'maxTailAgeSeconds',
  'maxReportAgeSeconds',
  'claimLeaseSeconds',
  'adapterTimeoutMs',
  'maxAttempts',
  'maxContextMessages',
  'maxFriendReplyChars',
  'maxGroupReplyChars',
  'bannedExpressions',
  'conversations',
])

const AUTO_REPLY_POLICY_OPTIONAL_FIELDS = Object.freeze([
  ...AUTO_REPLY_CORE_OPTIONAL_FIELDS,
  'projectOwnerNotices',
  'maxBatchesPerCycle',
  'pumpIntervalSeconds',
  'transport',
])

function assertCleanAutoReplyString(value, fieldName, maxBytes) {
  if (typeof value !== 'string' || !value.trim() || value.trim() !== value || value.includes('\0')) {
    throw new Error(`Invalid ${fieldName}`)
  }
  if (Buffer.byteLength(value, 'utf8') > maxBytes) {
    throw new Error(`${fieldName} exceeds ${maxBytes} byte limit`)
  }
  return value
}

function validateAutoReplyConversationRule(key, rule, defaults) {
  if (!AUTOREPLY_CONVERSATION_KEY_RE.test(key)) {
    throw new Error(`Invalid autoReply conversation key: ${key}`)
  }
  const channel = key.split(':')[0]
  fields(rule, ['mode'], ['allowPublishedReportFacts', 'allowedTopics', 'tone', 'cooldownSeconds', 'maxReplyChars'])
  if (!AUTOREPLY_CONVERSATION_MODES.includes(rule.mode)) {
    throw new Error(`Invalid autoReply conversation mode: ${rule.mode}`)
  }
  if (Object.hasOwn(rule, 'allowPublishedReportFacts') && typeof rule.allowPublishedReportFacts !== 'boolean') {
    throw new Error('Invalid autoReply allowPublishedReportFacts flag')
  }
  const allowPublishedReportFacts = rule.allowPublishedReportFacts === true
  let allowedTopics = Object.freeze([])
  if (Object.hasOwn(rule, 'allowedTopics')) {
    if (!Array.isArray(rule.allowedTopics) || rule.allowedTopics.length > 20) {
      throw new Error('Invalid autoReply allowedTopics')
    }
    allowedTopics = Object.freeze(rule.allowedTopics.map(topic => assertCleanAutoReplyString(topic, 'allowedTopic', 64)))
  }
  let tone = null
  if (Object.hasOwn(rule, 'tone') && rule.tone !== null) {
    tone = assertCleanAutoReplyString(rule.tone, 'tone', 64)
  }
  let cooldownSeconds = channel === 'group' ? defaults.groupCooldownSeconds : null
  if (Object.hasOwn(rule, 'cooldownSeconds') && rule.cooldownSeconds !== null) {
    if (channel !== 'group') {
      throw new Error('AutoReply cooldownSeconds is only supported on group conversations')
    }
    const [low, high] = AUTO_REPLY_POLICY_BOUNDS.groupCooldownSeconds
    if (!Number.isSafeInteger(rule.cooldownSeconds) || rule.cooldownSeconds < low || rule.cooldownSeconds > high) {
      throw new Error('Invalid autoReply group cooldownSeconds')
    }
    cooldownSeconds = rule.cooldownSeconds
  }
  const maxCeiling = channel === 'group' ? defaults.maxGroupReplyChars : defaults.maxFriendReplyChars
  let maxReplyChars = maxCeiling
  if (Object.hasOwn(rule, 'maxReplyChars') && rule.maxReplyChars !== null) {
    const low = channel === 'group' ? AUTO_REPLY_POLICY_BOUNDS.maxGroupReplyChars[0] : AUTO_REPLY_POLICY_BOUNDS.maxFriendReplyChars[0]
    const hardHigh = channel === 'group' ? AUTO_REPLY_POLICY_BOUNDS.maxGroupReplyChars[1] : AUTO_REPLY_POLICY_BOUNDS.maxFriendReplyChars[1]
    if (!Number.isSafeInteger(rule.maxReplyChars) || rule.maxReplyChars < low || rule.maxReplyChars > hardHigh) {
      throw new Error(`Invalid autoReply maxReplyChars for ${channel}`)
    }
    maxReplyChars = Math.min(rule.maxReplyChars, maxCeiling)
  }
  return Object.freeze({
    conversationKey: key,
    channel,
    mode: rule.mode,
    allowPublishedReportFacts,
    allowedTopics,
    tone,
    cooldownSeconds,
    maxReplyChars,
  })
}

function validateAutoReplyCoreFields(value) {
  if (Object.hasOwn(value, 'schemaVersion') && value.schemaVersion !== 1) {
    throw new Error('Unsupported autoReply schemaVersion')
  }
  if (typeof value.enabled !== 'boolean') {
    throw new Error('Invalid autoReply enabled flag')
  }
  if (Object.hasOwn(value, 'policyVersion') && (!Number.isSafeInteger(value.policyVersion) || value.policyVersion < 1)) {
    throw new Error('Invalid autoReply policyVersion')
  }
  if (Object.hasOwn(value, 'globalPaused') && typeof value.globalPaused !== 'boolean') {
    throw new Error('Invalid autoReply globalPaused flag')
  }
  for (const [key, [low, high]] of Object.entries(AUTO_REPLY_POLICY_BOUNDS)) {
    if (Object.hasOwn(value, key)) {
      if (!Number.isSafeInteger(value[key]) || value[key] < low || value[key] > high) {
        throw new Error(`Invalid autoReply bound ${key}`)
      }
    }
  }
  if (Object.hasOwn(value, 'bannedExpressions')) {
    if (!Array.isArray(value.bannedExpressions) || value.bannedExpressions.length > 50) {
      throw new Error('Invalid autoReply bannedExpressions')
    }
    for (const expr of value.bannedExpressions) {
      assertCleanAutoReplyString(expr, 'bannedExpression', 64)
    }
  }
  if (Object.hasOwn(value, 'conversations')) {
    if (!value.conversations || typeof value.conversations !== 'object' || Array.isArray(value.conversations)) {
      throw new Error('Invalid autoReply conversations map')
    }
    const defaults = {
      groupCooldownSeconds: value.groupCooldownSeconds ?? DEFAULT_WECHAT_AUTOREPLY_CORE_POLICY.groupCooldownSeconds,
      maxFriendReplyChars: value.maxFriendReplyChars ?? DEFAULT_WECHAT_AUTOREPLY_CORE_POLICY.maxFriendReplyChars,
      maxGroupReplyChars: value.maxGroupReplyChars ?? DEFAULT_WECHAT_AUTOREPLY_CORE_POLICY.maxGroupReplyChars,
    }
    for (const [convKey, rule] of Object.entries(value.conversations)) {
      validateAutoReplyConversationRule(convKey, rule, defaults)
    }
  }
}

/** Validate core standalone auto-reply store policy without broker transport fields. */
export function validateAutoReplyCorePolicy(value) {
  fields(value, ['enabled', 'policyVersion'], AUTO_REPLY_CORE_OPTIONAL_FIELDS)
  validateAutoReplyCoreFields(value)
  return value
}

/** Normalize core standalone auto-reply store policy, defaulting strictly to disabled everywhere. */
export function resolveAutoReplyCorePolicy(rawPolicy = null) {
  if (rawPolicy === null || rawPolicy === undefined) {
    return DEFAULT_WECHAT_AUTOREPLY_CORE_POLICY
  }
  validateAutoReplyCorePolicy(rawPolicy)
  const banned = new Set(DEFAULT_BANNED_EXPRESSIONS)
  for (const expr of rawPolicy.bannedExpressions ?? []) {
    banned.add(expr)
  }
  const normalizedBase = {
    schemaVersion: 1,
    enabled: rawPolicy.enabled === true,
    policyVersion: rawPolicy.policyVersion,
    globalPaused: rawPolicy.globalPaused === true,
    groupCooldownSeconds: rawPolicy.groupCooldownSeconds ?? DEFAULT_WECHAT_AUTOREPLY_CORE_POLICY.groupCooldownSeconds,
    maxInboundAgeSeconds: rawPolicy.maxInboundAgeSeconds ?? DEFAULT_WECHAT_AUTOREPLY_CORE_POLICY.maxInboundAgeSeconds,
    maxTailAgeSeconds: rawPolicy.maxTailAgeSeconds ?? DEFAULT_WECHAT_AUTOREPLY_CORE_POLICY.maxTailAgeSeconds,
    maxReportAgeSeconds: rawPolicy.maxReportAgeSeconds ?? DEFAULT_WECHAT_AUTOREPLY_CORE_POLICY.maxReportAgeSeconds,
    claimLeaseSeconds: rawPolicy.claimLeaseSeconds ?? DEFAULT_WECHAT_AUTOREPLY_CORE_POLICY.claimLeaseSeconds,
    adapterTimeoutMs: rawPolicy.adapterTimeoutMs ?? DEFAULT_WECHAT_AUTOREPLY_CORE_POLICY.adapterTimeoutMs,
    maxAttempts: rawPolicy.maxAttempts ?? DEFAULT_WECHAT_AUTOREPLY_CORE_POLICY.maxAttempts,
    maxContextMessages: rawPolicy.maxContextMessages ?? DEFAULT_WECHAT_AUTOREPLY_CORE_POLICY.maxContextMessages,
    maxFriendReplyChars: rawPolicy.maxFriendReplyChars ?? DEFAULT_WECHAT_AUTOREPLY_CORE_POLICY.maxFriendReplyChars,
    maxGroupReplyChars: rawPolicy.maxGroupReplyChars ?? DEFAULT_WECHAT_AUTOREPLY_CORE_POLICY.maxGroupReplyChars,
    bannedExpressions: Object.freeze([...banned]),
  }
  const conversations = {}
  for (const [convKey, rule] of Object.entries(rawPolicy.conversations ?? {})) {
    conversations[convKey] = validateAutoReplyConversationRule(convKey, rule, normalizedBase)
  }
  return Object.freeze({
    ...normalizedBase,
    conversations: Object.freeze(conversations),
  })
}

function validateAutoReplyTransportConfig(transport) {
  fields(
    transport,
    ['mode'],
    [
      'installAnchor',
      'provider',
      'model',
      'maxTokens',
      'maxRawOutputBytes',
      'maxUserInputBytes',
      'reasoningEffort',
      'shutdownTimeoutMs',
      'maxOrphanedRuns',
      'maxTotalOrphanedRuns',
      'apiKeyEnv',
      'allowSharedDeepSeekApiKey',
      'baseURL',
      'allowLoopbackHttp',
    ],
  )
  if (!AUTO_REPLY_TRANSPORT_MODES.includes(transport.mode)) {
    throw new Error('Invalid wechatAutoReply transport mode')
  }
  if (Object.hasOwn(transport, 'installAnchor') && transport.installAnchor !== null) {
    if (typeof transport.installAnchor !== 'string' || !transport.installAnchor.trim() || !isAbsolute(transport.installAnchor)) {
      throw new Error('Invalid wechatAutoReply transport installAnchor')
    }
  }
  for (const strKey of ['provider', 'model', 'reasoningEffort']) {
    if (Object.hasOwn(transport, strKey) && transport[strKey] !== undefined && transport[strKey] !== null) {
      assertCleanAutoReplyString(transport[strKey], `wechatAutoReply transport ${strKey}`, 128)
    }
  }
  if (transport.mode === 'deepseek-api-key' && Object.hasOwn(transport, 'provider') && transport.provider !== undefined && transport.provider !== 'deepseek-official') {
    throw new Error('wechatAutoReply deepseek-api-key transport only supports provider deepseek-official')
  }
  for (const [key, [low, high]] of Object.entries(AUTO_REPLY_TRANSPORT_INT_BOUNDS)) {
    if (Object.hasOwn(transport, key)) {
      if (!Number.isSafeInteger(transport[key]) || transport[key] < low || transport[key] > high) {
        throw new Error(`Invalid wechatAutoReply transport ${key}`)
      }
    }
  }
  const maxOrphaned = transport.maxOrphanedRuns ?? 1
  const maxTotal = transport.maxTotalOrphanedRuns ?? 4
  if (maxTotal < maxOrphaned) {
    throw new Error('wechatAutoReply transport maxTotalOrphanedRuns must be >= maxOrphanedRuns')
  }
  for (const boolKey of ['allowSharedDeepSeekApiKey', 'allowLoopbackHttp']) {
    if (Object.hasOwn(transport, boolKey) && typeof transport[boolKey] !== 'boolean') {
      throw new Error(`Invalid wechatAutoReply transport ${boolKey}`)
    }
  }
  if (Object.hasOwn(transport, 'apiKeyEnv') && transport.apiKeyEnv !== undefined) {
    if (typeof transport.apiKeyEnv !== 'string' || !SAFE_ENV_NAME_RE.test(transport.apiKeyEnv.trim())) {
      throw new Error('Invalid wechatAutoReply transport apiKeyEnv')
    }
    if (transport.apiKeyEnv.trim() === SHARED_DEEPSEEK_API_KEY_ENV && transport.allowSharedDeepSeekApiKey !== true) {
      throw new Error('wechatAutoReply transport refuses shared DEEPSEEK_API_KEY without allowSharedDeepSeekApiKey: true')
    }
  }
  if (Object.hasOwn(transport, 'baseURL') && transport.baseURL !== null && transport.baseURL !== undefined) {
    if (typeof transport.baseURL !== 'string' || !transport.baseURL.trim()) {
      throw new Error('Invalid wechatAutoReply transport baseURL')
    }
    const parsed = new URL(transport.baseURL.trim())
    const isLoopback = parsed.hostname === '127.0.0.1' || parsed.hostname === 'localhost' || parsed.hostname === '::1' || parsed.hostname === '[::1]'
    if (parsed.protocol !== 'https:' && !(parsed.protocol === 'http:' && transport.allowLoopbackHttp === true && isLoopback)) {
      throw new Error('wechatAutoReply transport baseURL must use https: or explicit loopback http:')
    }
  }
  return transport
}

/** Validate optional broker-integrated DSH WeChat auto-reply configuration (default-off). */
export function validateWechatAutoReplyPolicy(value) {
  if (value?.enabled === true) {
    fields(value, ['enabled', 'policyVersion', 'transport'], AUTO_REPLY_POLICY_OPTIONAL_FIELDS)
  } else {
    fields(value, ['enabled'], AUTO_REPLY_POLICY_OPTIONAL_FIELDS)
  }
  validateAutoReplyCoreFields(value)
  if (Object.hasOwn(value, 'projectOwnerNotices') && typeof value.projectOwnerNotices !== 'boolean') {
    throw new Error('Invalid wechatAutoReply projectOwnerNotices flag')
  }
  if (value.projectOwnerNotices === true && value.enabled !== true) {
    throw new Error('wechatAutoReply projectOwnerNotices requires wechatAutoReply enabled: true')
  }
  for (const [key, [low, high]] of Object.entries(AUTO_REPLY_PUMP_BOUNDS)) {
    if (Object.hasOwn(value, key)) {
      if (!Number.isSafeInteger(value[key]) || value[key] < low || value[key] > high) {
        throw new Error(`Invalid wechatAutoReply ${key}`)
      }
    }
  }
  if (Object.hasOwn(value, 'transport') && value.transport !== null) {
    validateAutoReplyTransportConfig(value.transport)
  }
  return value
}

/** Resolve normalized broker WeChat auto-reply settings, defaulting strictly to disabled. */
export function wechatAutoReplyPolicy(policy) {
  const cfg = policy?.wechatAutoReply
  if (!cfg || cfg.enabled !== true) {
    return DEFAULT_WECHAT_AUTOREPLY_RUNTIME_POLICY
  }
  validateWechatAutoReplyPolicy(cfg)
  const core = resolveAutoReplyCorePolicy({
    ...(Object.hasOwn(cfg, 'schemaVersion') ? { schemaVersion: cfg.schemaVersion } : {}),
    enabled: true,
    policyVersion: cfg.policyVersion,
    ...(Object.hasOwn(cfg, 'globalPaused') ? { globalPaused: cfg.globalPaused } : {}),
    ...(Object.hasOwn(cfg, 'groupCooldownSeconds') ? { groupCooldownSeconds: cfg.groupCooldownSeconds } : {}),
    ...(Object.hasOwn(cfg, 'maxInboundAgeSeconds') ? { maxInboundAgeSeconds: cfg.maxInboundAgeSeconds } : {}),
    ...(Object.hasOwn(cfg, 'maxTailAgeSeconds') ? { maxTailAgeSeconds: cfg.maxTailAgeSeconds } : {}),
    ...(Object.hasOwn(cfg, 'maxReportAgeSeconds') ? { maxReportAgeSeconds: cfg.maxReportAgeSeconds } : {}),
    ...(Object.hasOwn(cfg, 'claimLeaseSeconds') ? { claimLeaseSeconds: cfg.claimLeaseSeconds } : {}),
    ...(Object.hasOwn(cfg, 'adapterTimeoutMs') ? { adapterTimeoutMs: cfg.adapterTimeoutMs } : {}),
    ...(Object.hasOwn(cfg, 'maxAttempts') ? { maxAttempts: cfg.maxAttempts } : {}),
    ...(Object.hasOwn(cfg, 'maxContextMessages') ? { maxContextMessages: cfg.maxContextMessages } : {}),
    ...(Object.hasOwn(cfg, 'maxFriendReplyChars') ? { maxFriendReplyChars: cfg.maxFriendReplyChars } : {}),
    ...(Object.hasOwn(cfg, 'maxGroupReplyChars') ? { maxGroupReplyChars: cfg.maxGroupReplyChars } : {}),
    ...(Object.hasOwn(cfg, 'bannedExpressions') ? { bannedExpressions: cfg.bannedExpressions } : {}),
    ...(Object.hasOwn(cfg, 'conversations') ? { conversations: cfg.conversations } : {}),
  })
  return Object.freeze({
    ...core,
    projectOwnerNotices: cfg.projectOwnerNotices === true,
    maxBatchesPerCycle: cfg.maxBatchesPerCycle ?? DEFAULT_WECHAT_AUTOREPLY_RUNTIME_POLICY.maxBatchesPerCycle,
    pumpIntervalSeconds: cfg.pumpIntervalSeconds ?? DEFAULT_WECHAT_AUTOREPLY_RUNTIME_POLICY.pumpIntervalSeconds,
    transport: Object.freeze({ ...cfg.transport }),
  })
}

/** Validate finite deployment limits and fixed administrator-owned target maps. */
export function validatePolicy(value) {
  fields(value, ['socket', 'state', 'ingressDatabase', 'bindingKey', 'operatorId', 'tenantId', 'services',
    'containers', 'wechatRoot', 'wechatConfig', 'limits'], ['wechatRecipientAlias', 'wechatContacts', 'wechatContactsConfig', 'wechatAdhocMoments', 'wechatNotifications', 'wechatAutoReply'])
  for (const key of ['socket', 'state', 'ingressDatabase', 'wechatRoot', 'wechatConfig']) {
    if (typeof value[key] !== 'string' || !value[key].startsWith('/')) throw new Error('Expected absolute deployment path')
  }
  if (Object.hasOwn(value, 'wechatContactsConfig')) {
    if (typeof value.wechatContactsConfig !== 'string' || !value.wechatContactsConfig.startsWith('/')) {
      throw new Error('Expected absolute deployment path')
    }
  }
  const binding = JSON.parse(value.bindingKey)
  if (!Array.isArray(binding) || binding.length !== 5 || binding[0] !== 'feishu') throw new Error('Invalid binding')
  for (const key of ['operatorId', 'tenantId']) {
    if (typeof value[key] !== 'string' || !value[key].trim()) throw new Error('Missing identity')
  }
  const hasAlias = Object.hasOwn(value, 'wechatRecipientAlias')
  const hasContacts = Object.hasOwn(value, 'wechatContacts')
  if (hasAlias === hasContacts) throw new Error('Specify either wechatRecipientAlias or wechatContacts')
  if (hasAlias && (typeof value.wechatRecipientAlias !== 'string' || !value.wechatRecipientAlias.trim())) {
    throw new Error('Missing identity')
  }
  if (hasContacts) validateWechatContactsPolicy(value.wechatContacts)
  if (Object.hasOwn(value, 'wechatAdhocMoments')) validateAdhocMomentsPolicy(value.wechatAdhocMoments)
  if (Object.hasOwn(value, 'wechatNotifications')) validateWechatNotificationsPolicy(value.wechatNotifications)
  if (Object.hasOwn(value, 'wechatAutoReply')) {
    validateWechatAutoReplyPolicy(value.wechatAutoReply)
    if (value.wechatAutoReply?.enabled === true && value.wechatNotifications?.enabled !== true) {
      throw new Error('wechatAutoReply requires wechatNotifications enabled: true')
    }
  }
  for (const unit of Object.values(value.services)) {
    if (typeof unit !== 'string' || !/^[a-zA-Z0-9_.@-]+\.service$/.test(unit)) throw new Error('Invalid unit')
  }
  if (!Array.isArray(value.containers) || value.containers.some(name => !/^[a-zA-Z0-9_-]+$/.test(name))) throw new Error('Invalid container')
  fields(value.limits, ['logLines', 'previewSeconds', 'workerPollSeconds', 'maxTextBytes', 'commandSeconds'])
  const bounds = { logLines: [10, 200], previewSeconds: [60, 1800], workerPollSeconds: [2, 30], maxTextBytes: [64, 4096], commandSeconds: [5, 60] }
  for (const [key, [low, high]] of Object.entries(bounds)) {
    if (!Number.isSafeInteger(value.limits[key]) || value.limits[key] < low || value.limits[key] > high) throw new Error(`Invalid limit ${key}`)
  }
  return value
}

/** Load policy only from its root-owned (or current non-root test owner), non-symlinked private directory. */
export function readPolicy(path) {
  if (realpathSync(path) !== path) throw new Error('Policy symlink refused')
  const metadata = statSync(path)
  const allowedUids = process.getuid?.() === 0
    ? new Set([0])
    : new Set([0, process.getuid?.()].filter(v => v !== undefined))
  if (!allowedUids.has(metadata.uid) || (metadata.mode & 0o022)) throw new Error('Policy must be root owned and not writable by others')
  return validatePolicy(JSON.parse(readFileSync(path, 'utf8')))
}
