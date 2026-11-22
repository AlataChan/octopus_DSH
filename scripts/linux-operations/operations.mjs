/** Frozen operations, source-authenticated confirmation and durable single-use execution. */
import { DatabaseSync } from 'node:sqlite'
import { randomBytes, createHash } from 'node:crypto'
import { mkdirSync, readFileSync, writeFileSync, renameSync, openSync, closeSync, fsyncSync, fchmodSync, chmodSync, unlinkSync, lstatSync, realpathSync, existsSync, constants } from 'node:fs'
import { join, dirname, resolve, sep } from 'node:path'
import { DEFAULT_ADHOC_VIEWPORT, adhocMomentsPolicy, fields, validatePolicy } from './policy.mjs'
import { resolveActionableInboundNotice } from './wechat-inbox.mjs'

const CONTAINER_MOMENTS_PREFIX = '/home/wechat/.gamma-moments/'
const ANCHOR_ROLES = ['recipient_anchor', 'recipient_sidebar_anchor', 'recipient_card_anchor']
const GROUP_NAME_READ_MODES = new Set(['auto', 'native_copy', 'anchor'])
export const QUEUED_WECHAT_KINDS = new Set(['wechat_send', 'wechat_send_group', 'wechat_probe_group', 'wechat_publish_moment', 'wechat_reply_notice'])
const RESERVED_MOMENTS_TEXTS = ['文字未发送', '发送失败', '发表失败', '正在发送', '谁可以看', '这一刻的想法']
const RESERVED_EXACT_LINES = new Set(['发表', '全文', '复制'])
const EXOTIC_LINE_BREAKS = ['\r', '\v', '\f', '\x85', '\u2028', '\u2029']
const ASCII_BREAK_CHARS = new Set(['/', '?', '-', '_', '&', '=', '.', ':', ',', ';'])
const OUTBOUND_WECHAT_KINDS = new Set(['wechat_send', 'wechat_send_group', 'wechat_publish_moment', 'wechat_reply_notice'])
const DIRECT_POLICY_VERSION = 2

const NON_SEND_MODIFIER_RE = /(?:不要|别|勿|莫|未|非|起草|草稿|拟稿|试写|设想|假设|假如|如果|要是|能否|是否|可以吗|怎么|如何|准备一下|先准备|先看看|仅预览|别真发|不要发|状态|查询|查看|检查|引用|原话|刚才说|\?|？)/
const TRAILING_DRAFT_OR_NEGATION_RE = /(?:(?:\r?\n|\s*[，,。（(【\[;；"”'’」』]\s*)(?:先别发|别发了|别发出去|别真发|不要发|不要发送|先不发|暂不发|仅草稿|先草稿|先预览|只看下|只写草稿|别提交|不要发布|暂不发布|等待确认|等我确认|可以发吗|能发吗|这样发吗|行吗|合适吗)[）)】\]!！。?？\s]*)$/
const VISIBILITY_LABELS = new Map([
  ['仅自己可见', 'private'],
  ['私密', 'private'],
  ['好友可见', 'friends'],
  ['朋友可见', 'friends'],
  ['全部好友可见', 'friends'],
  ['所有好友可见', 'friends'],
  ['全部朋友可见', 'friends'],
  ['所有朋友可见', 'friends'],
  ['公开', 'friends'],
])
const MOMENTS_VIS_RE = '(仅自己可见|私密|(?:全部|所有)?(?:好友|朋友)可见|公开)'

/** Parse only explicit unambiguous Feishu owner send/publish commands binding target, text and visibility. */
export function parseDirectWechatPrompt(prompt) {
  if (typeof prompt !== 'string') return null
  const trimmed = prompt.trim()
  if (!trimmed || /^['"“‘「『【（(\[>]/.test(trimmed)) return null

  const acceptMatch = (match, build) => {
    if (!match) return null
    const [, head, middle, tailOrBody, maybeBody] = match
    if (maybeBody !== undefined) {
      const target = middle.trim()
      const connector = tailOrBody
      const text = maybeBody
      if (!target || /[，,。!！?？“”"‘'「『（(\[\n]/.test(target)) return null
      if (NON_SEND_MODIFIER_RE.test(head) || NON_SEND_MODIFIER_RE.test(connector)) return null
      if (!text.trim() || TRAILING_DRAFT_OR_NEGATION_RE.test(text)) return null
      return build(target, text)
    }
    return null
  }

  // Notice-ID manual replies require explicit reply-by-wn_<id> syntax without free-form target overrides.
  const replyPatterns = [
    /^((?:请\s*)?(?:直接\s*)?回(?:复)?(?:\s*(?:微信|通知|消息))?\s*)(wn_[0-9a-f]{12})(\s*)(?:\r?\n|[：:][ \t]*)([\s\S]+)$/,
    /^((?:[Pp]lease\s+)?[Rr]eply(?:\s+(?:to\s+)?(?:notice|wechat|message))?\s+)(wn_[0-9a-f]{12})(\s*)(?:\r?\n|[：:][ \t]*)([\s\S]+)$/,
  ]
  for (const pattern of replyPatterns) {
    const match = pattern.exec(trimmed)
    if (!match) continue
    const [, head, noticeId, connector, text] = match
    if (NON_SEND_MODIFIER_RE.test(head) || NON_SEND_MODIFIER_RE.test(connector)) return null
    if (!text.trim() || TRAILING_DRAFT_OR_NEGATION_RE.test(text)) return null
    return { kind: 'wechat_reply_notice', noticeId, text }
  }

  // Group sends require explicit group terminology so friend DMs and group sends stay strictly separated.
  const groupPatterns = [
    /^((?:请\s*)?(?:直接\s*)?群聊发送\s+)([^\n：:]+?)(\s*)(?:\r?\n|[：:][ \t]*)([\s\S]+)$/,
    /^((?:请\s*)?(?:直接\s*)?(?:发(?:送)?(?:一条|条)?群(?:聊)?消息|发群聊)给\s*)([^\n：:]+?)(\s*(?:说)?)(?:\r?\n|[：:][ \t]*)([\s\S]+)$/,
    /^((?:请\s*)?(?:直接\s*)?(?:给|向|往|在)\s*)([^\n：:]+?)(\s*(?:这个群(?:聊)?(?:里|中|内)?|[里中内])?\s*发(?:送)?(?:一条|条)?(?:群(?:聊)?消息|群聊|群微信)(?:说)?)(?:\r?\n|[：:][ \t]*)([\s\S]+)$/,
    /^((?:请\s*)?(?:直接\s*)?(?:给|向|往|在)\s*)([^\n：:]+?群)(\s*(?:这个群(?:聊)?|聊)?(?:里|中|内)\s*发(?:送)?(?:一条|条)?(?:消息|微信)(?:说)?)(?:\r?\n|[：:][ \t]*)([\s\S]+)$/,
  ]
  for (const pattern of groupPatterns) {
    const parsed = acceptMatch(pattern.exec(trimmed), (target, text) => ({ kind: 'wechat_send_group', target, text }))
    if (parsed) return parsed
  }

  // Friend direct sends require explicit friend/wechat DM phrasing and delimited verbatim text.
  const friendPatterns = [
    /^((?:请\s*)?(?:直接\s*)?微信发送\s+)([^\n：:]+?)(\s*)(?:\r?\n|[：:][ \t]*)([\s\S]+)$/,
    /^((?:请\s*)?(?:直接\s*)?(?:给|向)\s*)([^\n：:]+?)(\s*发(?:送)?(?:一条|条)?微信(?:消息|私信)?(?:说)?)(?:\r?\n|[：:][ \t]*)([\s\S]+)$/,
    /^((?:请\s*)?(?:直接\s*)?(?:(?:微信|私信)发(?:送)?(?:一条|条)?给|(?:微信)?发(?:送)?(?:一条|条)?(?:微信(?:消息)?|私信)给)\s*)([^\n：:]+?)(\s*(?:说)?)(?:\r?\n|[：:][ \t]*)([\s\S]+)$/,
  ]
  for (const pattern of friendPatterns) {
    const parsed = acceptMatch(pattern.exec(trimmed), (target, text) => ({ kind: 'wechat_send', target, text }))
    if (parsed) return parsed
  }

  // Ad hoc Moments publish commands bind exact body text and any explicitly stated audience.
  const momentBefore = new RegExp(`^((?:请\\s*)?(?:直接\\s*)?发(?:布)?(?:一条|条)?\\s*)(?:${MOMENTS_VIS_RE}(?:的)?)?(\\s*(?:按需)?朋友圈)(?:\\r?\\n|[：:][ \\t]*)([\\s\\S]+)$`)
  const momentAfter = new RegExp(`^((?:请\\s*)?(?:直接\\s*)?发(?:布)?(?:一条|条)?\\s*(?:按需)?朋友圈\\s*[（(\\[【]\\s*)${MOMENTS_VIS_RE}(\\s*[）)\\]】])(?:\\r?\\n|[：:][ \\t]*)([\\s\\S]+)$`)
  for (const pattern of [momentBefore, momentAfter]) {
    const m = pattern.exec(trimmed)
    if (!m) continue
    const [, head, visLabel, connector, text] = m
    if (NON_SEND_MODIFIER_RE.test(head) || NON_SEND_MODIFIER_RE.test(connector)) return null
    if (!text.trim() || TRAILING_DRAFT_OR_NEGATION_RE.test(text)) return null
    const visibility = visLabel ? (VISIBILITY_LABELS.get(visLabel) ?? null) : null
    if (visLabel && !visibility) return null
    return { kind: 'wechat_publish_moment', visibility, text }
  }

  return null
}

function computeDirectProof({ kind, requestId, key, text }) {
  return digest(JSON.stringify(['direct_v2', kind, requestId, key, text]))
}

function authorizeDirectWechat({ kind, payload, contact, visibility, noticeId, registry, actor }) {
  const parsed = parseDirectWechatPrompt(actor.prompt)
  if (!parsed || parsed.kind !== kind) return { ok: false, key: null }
  if (parsed.text !== payload.text) return { ok: false, key: null }
  if (kind === 'wechat_reply_notice') {
    if (!noticeId || parsed.noticeId !== noticeId || !contact?.id || !contact?.targetProof) {
      return { ok: false, key: null }
    }
    return { ok: true, key: `${noticeId}:${contact.id}:${contact.targetProof}` }
  }
  if (kind === 'wechat_send' || kind === 'wechat_send_group') {
    let promptContact
    try {
      promptContact = resolveUniqueTarget(registry, parsed.target, contact.type, { requireSendEnabled: true })
    } catch {
      return { ok: false, key: null }
    }
    if (promptContact.id !== contact.id) return { ok: false, key: null }
    return { ok: true, key: contact.id }
  }
  if (kind === 'wechat_publish_moment') {
    if (parsed.visibility !== null) {
      return parsed.visibility === visibility
        ? { ok: true, key: visibility, visibilitySource: 'owner_prompt' }
        : { ok: false, key: null }
    }
    const cfgDefault = registry.adhocMoments.defaultVisibility
    if (cfgDefault !== null && visibility === cfgDefault) {
      return { ok: true, key: visibility, visibilitySource: 'configured_default' }
    }
    return { ok: false, key: null }
  }
  return { ok: false, key: null }
}

/** SHA-256 over UTF-8 text or raw bytes for frozen messages, bindings and configuration backups. */
export function digest(value) { return createHash('sha256').update(value).digest('hex') }

function isWideDisplayCodePoint(code) {
  return (
    code === 0x00A1 || code === 0x00A4 || code === 0x00A7 || code === 0x00A8 || code === 0x00AA
    || code === 0x00AD || code === 0x00AE || (code >= 0x00B0 && code <= 0x00B4) || (code >= 0x00B6 && code <= 0x00BA)
    || (code >= 0x00BC && code <= 0x00BF) || code === 0x00D7 || code === 0x00F7
    || (code >= 0x0391 && code <= 0x03A9) || (code >= 0x03B1 && code <= 0x03C9)
    || code === 0x0401 || (code >= 0x0410 && code <= 0x044F) || code === 0x0451
    || (code >= 0x1100 && code <= 0x115F)
    || code === 0x2010 || (code >= 0x2013 && code <= 0x2016) || code === 0x2018 || code === 0x2019
    || code === 0x201C || code === 0x201D || (code >= 0x2020 && code <= 0x2022) || (code >= 0x2024 && code <= 0x2027)
    || code === 0x2030 || code === 0x2032 || code === 0x2033 || code === 0x2035 || code === 0x203B
    || code === 0x203E || code === 0x20AC
    || (code >= 0x2100 && code <= 0x27FF) || (code >= 0x2B00 && code <= 0x2BFF)
    || (code >= 0x2E80 && code <= 0x303F) || (code >= 0x3040 && code <= 0x33FF)
    || (code >= 0x3400 && code <= 0x4DBF) || (code >= 0x4E00 && code <= 0xA4CF)
    || (code >= 0xA960 && code <= 0xA97F) || (code >= 0xAC00 && code <= 0xD7AF)
    || (code >= 0xF900 && code <= 0xFAFF) || (code >= 0xFE10 && code <= 0xFE6F)
    || (code >= 0xFF01 && code <= 0xFF60) || (code >= 0xFFE0 && code <= 0xFFE6)
    || (code >= 0x1F000 && code <= 0x1FAFF) || (code >= 0x20000 && code <= 0x3FFFD)
  )
}

function charDisplayCols(ch) {
  return isWideDisplayCodePoint(ch.codePointAt(0)) ? 2 : 1
}

function estimateLineVisualLines(line, viewport) {
  if (!line) return 1
  for (const wsToken of line.trim().split(/\s+/).filter(Boolean)) {
    const chars = [...wsToken]
    let tokenCols = 0
    let allAscii = true
    for (const ch of chars) {
      if (ch.codePointAt(0) >= 128) allAscii = false
      tokenCols += charDisplayCols(ch)
    }
    if (allAscii && tokenCols > viewport.maxUnbrokenTokenCols) {
      throw new Error('Ad hoc Moments unbroken token exceeds viewport width')
    }
    for (const run of wsToken.match(/[A-Za-z0-9]+/g) ?? []) {
      if (run.length > viewport.maxUnbrokenAlnumCols) {
        throw new Error('Ad hoc Moments unbroken identifier exceeds viewport width')
      }
    }
  }
  const units = []
  let buf = ''
  for (const ch of line) {
    if (/\s/.test(ch)) {
      if (buf) { units.push(buf); buf = '' }
      units.push(ch)
    } else if (charDisplayCols(ch) === 2) {
      if (buf) { units.push(buf); buf = '' }
      units.push(ch)
    } else {
      buf += ch
      if (ASCII_BREAK_CHARS.has(ch)) {
        units.push(buf)
        buf = ''
      }
    }
  }
  if (buf) units.push(buf)

  let visualLines = 1
  let currentCols = 0
  for (const unit of units) {
    let ucols = 0
    for (const ch of unit) ucols += charDisplayCols(ch)
    if (!unit.trim()) {
      if (currentCols + ucols <= viewport.colsPerVisualLine) currentCols += ucols
      else { visualLines += 1; currentCols = 0 }
      continue
    }
    if (currentCols > 0 && currentCols + ucols > viewport.colsPerVisualLine) {
      visualLines += 1
      currentCols = 0
    }
    while (ucols > viewport.colsPerVisualLine) {
      visualLines += 1
      ucols -= viewport.colsPerVisualLine
    }
    currentCols += ucols
  }
  return visualLines
}

/** Validate ad hoc Moments text strictly against character, line, and 620x800 viewport bounds without mutating text. */
export function validateAdhocMomentText(text, viewport = DEFAULT_ADHOC_VIEWPORT) {
  if (typeof text !== 'string') throw new Error('Invalid ad hoc Moments message text')
  if (text.includes('\0') || EXOTIC_LINE_BREAKS.some(ch => text.includes(ch))) {
    throw new Error('Unsupported line break or NUL in ad hoc Moments text')
  }
  if (!text.trim()) throw new Error('Ad hoc Moments text cannot be empty')
  if (text !== text.trim()) throw new Error('Ad hoc Moments text cannot have leading or trailing whitespace')
  const nonWsCount = [...text.replace(/\s+/g, '')].length
  if (nonWsCount > viewport.maxChars) throw new Error('Ad hoc Moments text exceeds 220 non-whitespace characters')
  const lines = text.split('\n')
  if (lines.length > viewport.maxLogicalLines) throw new Error('Ad hoc Moments text exceeds 4 lines')
  if (RESERVED_MOMENTS_TEXTS.some(token => text.includes(token)) || lines.some(line => RESERVED_EXACT_LINES.has(line.trim()))) {
    throw new Error('Ad hoc Moments text cannot contain reserved Moments UI text')
  }
  const visualLines = lines.reduce((sum, line) => sum + estimateLineVisualLines(line, viewport), 0)
  if (visualLines > viewport.maxVisualLines) {
    throw new Error('Ad hoc Moments text exceeds 620x800 composer viewport height')
  }
  return {
    charCount: nonWsCount,
    logicalLines: lines.length,
    estimatedVisualLines: visualLines,
    colsPerVisualLine: viewport.colsPerVisualLine,
    textSha256: digest(text),
  }
}

/** Replace a JSON document durably without following a destination symlink. */
export function atomicJson(path, value, mode = 0o600) {
  const temporary = `${path}.${randomBytes(12).toString('hex')}.tmp`
  const fd = openSync(temporary, constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY, mode)
  try { fchmodSync(fd, mode); writeFileSync(fd, JSON.stringify(value, null, 2) + '\n'); fsyncSync(fd) } finally { closeSync(fd) }
  try { renameSync(temporary, path) } catch (error) {
    try { unlinkSync(temporary) } catch (cleanupError) {
      if (cleanupError.code !== 'ENOENT') throw new AggregateError([error, cleanupError], 'JSON replacement and cleanup failed')
    }
    throw error
  }
  const directory = openSync(dirname(path), constants.O_RDONLY)
  try { fsyncSync(directory) } finally { closeSync(directory) }
}

function resolveContainedFile(wechatRoot, rawPath) {
  if (typeof rawPath !== 'string' || !rawPath.startsWith('/')) {
    return { ok: false, error: 'Invalid path' }
  }
  const mapped = rawPath.startsWith(CONTAINER_MOMENTS_PREFIX)
    ? join(wechatRoot, rawPath.slice(CONTAINER_MOMENTS_PREFIX.length))
    : rawPath
  const rootAbs = resolve(wechatRoot)
  const targetAbs = resolve(mapped)
  if (targetAbs !== rootAbs && !targetAbs.startsWith(rootAbs + sep)) {
    return { ok: false, error: 'Path escapes wechatRoot' }
  }
  try {
    const stat = lstatSync(targetAbs)
    if (stat.isSymbolicLink() || !stat.isFile()) return { ok: false, error: 'Unsafe file type' }
    const rootReal = realpathSync(rootAbs)
    const dirReal = realpathSync(dirname(targetAbs))
    if (dirReal !== rootReal && !dirReal.startsWith(rootReal + sep)) {
      return { ok: false, error: 'Directory escapes wechatRoot' }
    }
    return { ok: true, path: rawPath, hostPath: targetAbs, stat }
  } catch (error) {
    return { ok: false, error: `Missing file: ${error.code ?? error.message}` }
  }
}

function resolveAnchorBinding(wechatRoot, rawPath) {
  const checked = resolveContainedFile(wechatRoot, rawPath)
  if (!checked.ok) return checked
  return { ok: true, path: rawPath, sha256: digest(readFileSync(checked.hostPath)) }
}

/** Format epoch milliseconds into an Asia/Shanghai YYYY-MM-DD date string. */
export function shanghaiDate(epochMs) {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Shanghai',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(new Date(epochMs))
}

/** Compute cross-runtime canonical binding digest shared with the Python worker. */
export function computeBindingSha256({
  accountId,
  contactId,
  contactType,
  alias,
  displayName,
  recipientId,
  recipientQuery,
  accountAnchor,
  ownChatAnchor,
  recipientAnchor,
  recipientSidebarAnchor,
  recipientCardAnchor,
}) {
  const hashPart = value => (value === null || value === undefined ? '-' : digest(String(value)))
  const parts = [
    accountId,
    contactId,
    contactType,
    alias,
    displayName,
    recipientId,
    recipientQuery,
    accountAnchor?.path ?? null,
    accountAnchor?.sha256 ?? null,
    ownChatAnchor?.path ?? null,
    ownChatAnchor?.sha256 ?? null,
    recipientAnchor?.path ?? null,
    recipientAnchor?.sha256 ?? null,
    recipientSidebarAnchor?.path ?? null,
    recipientSidebarAnchor?.sha256 ?? null,
    recipientCardAnchor?.path ?? null,
    recipientCardAnchor?.sha256 ?? null,
  ]
  return digest(parts.map(hashPart).join(':'))
}

/** Compute cross-runtime canonical group binding digest shared with the Python worker. */
export function computeGroupBindingSha256({
  accountId,
  contactId,
  contactType,
  alias,
  displayName,
  groupId,
  groupName,
  groupQuery,
  groupNameReadMode,
  groupSidebarWidth,
  accountAnchor,
  ownChatAnchor,
  groupAnchor,
  groupNameAnchor,
  groupMemberAnchors,
}) {
  const hashPart = value => (value === null || value === undefined ? '-' : digest(String(value)))
  const parts = [
    'group_v1',
    accountId,
    contactId,
    contactType,
    alias,
    displayName,
    groupId,
    groupName,
    groupQuery,
    groupNameReadMode,
    String(groupSidebarWidth),
    accountAnchor?.path ?? null,
    accountAnchor?.sha256 ?? null,
    ownChatAnchor?.path ?? null,
    ownChatAnchor?.sha256 ?? null,
    groupAnchor?.path ?? null,
    groupAnchor?.sha256 ?? null,
    groupNameAnchor?.path ?? null,
    groupNameAnchor?.sha256 ?? null,
    groupMemberAnchors?.[0]?.path ?? null,
    groupMemberAnchors?.[0]?.sha256 ?? null,
    groupMemberAnchors?.[1]?.path ?? null,
    groupMemberAnchors?.[1]?.sha256 ?? null,
  ]
  return digest(parts.map(hashPart).join(':'))
}

/** Compute cross-runtime canonical binding digest for an ad hoc Moments request. */
export function computeAdhocMomentsBindingSha256({
  accountId,
  accountAnchor,
  visibility,
  textSha256,
  viewport,
}) {
  const hashPart = value => (value === null || value === undefined ? '-' : digest(String(value)))
  const vpKey = [
    viewport.maxChars,
    viewport.maxLogicalLines,
    viewport.minWindowWidth,
    viewport.minWindowHeight,
    viewport.colsPerVisualLine,
    viewport.maxVisualLines,
    viewport.maxUnbrokenAlnumCols,
    viewport.maxUnbrokenTokenCols,
    viewport.maxComposerHeightPx,
  ].join(',')
  const parts = [
    'adhoc_moments_v1',
    accountId,
    accountAnchor?.path ?? null,
    accountAnchor?.sha256 ?? null,
    visibility,
    textSha256,
    vpKey,
  ]
  return digest(parts.map(hashPart).join(':'))
}

function validateClientContactsList(contacts) {
  if (!Array.isArray(contacts) || contacts.length < 1 || contacts.length > 50) {
    throw new Error('Invalid WeChat contacts list in sidecar registry')
  }
  const seenIds = new Set()
  const seenAliases = new Set()
  const seenRecipientIds = new Set()
  const seenGroupIds = new Set()
  const seenFriendAnchorPaths = new Map(ANCHOR_ROLES.map(role => [role, new Set()]))
  const seenGroupAnchorPaths = new Set()
  let legacyCount = 0
  const byId = new Map()
  for (const entry of contacts) {
    if (!entry || typeof entry !== 'object' || Array.isArray(entry)) {
      throw new Error('Invalid WeChat contact record')
    }
    if (typeof entry.id !== 'string' || !entry.id.trim()) throw new Error('Invalid WeChat contact id')
    if (entry.type !== 'friend' && entry.type !== 'group') throw new Error('Unsupported WeChat contact type in sidecar')
    if (typeof entry.alias !== 'string' || !entry.alias.trim()) throw new Error('Invalid WeChat contact alias')
    if (typeof entry.enabled !== 'boolean') throw new Error('Invalid WeChat contact enabled flag')
    if (seenIds.has(entry.id) || seenAliases.has(entry.alias)) {
      throw new Error('Duplicate WeChat contact id or alias in sidecar registry')
    }
    seenIds.add(entry.id)
    seenAliases.add(entry.alias)
    if (entry.type === 'friend') {
      if (typeof entry.direct_enabled !== 'boolean') throw new Error('Invalid WeChat friend direct_enabled flag')
      if (Object.hasOwn(entry, 'legacy') && typeof entry.legacy !== 'boolean') {
        throw new Error('Invalid WeChat contact legacy flag')
      }
      if (entry.legacy) legacyCount++
      if (typeof entry.recipient_id === 'string' && entry.recipient_id.trim()) {
        if (seenRecipientIds.has(entry.recipient_id)) {
          throw new Error('Duplicate WeChat recipient_id across contacts')
        }
        seenRecipientIds.add(entry.recipient_id)
      }
      for (const role of ANCHOR_ROLES) {
        if (typeof entry[role] === 'string' && entry[role].trim()) {
          if (seenFriendAnchorPaths.get(role).has(entry[role])) {
            throw new Error('Duplicate WeChat anchor path across contacts')
          }
          seenFriendAnchorPaths.get(role).add(entry[role])
        }
      }
    } else {
      if (entry.legacy === true) throw new Error('Group contacts cannot be legacy')
      if (typeof entry.group_enabled !== 'boolean') throw new Error('Invalid WeChat group_enabled flag')
      if (typeof entry.group_id === 'string' && entry.group_id.trim()) {
        if (seenGroupIds.has(entry.group_id)) throw new Error('Duplicate WeChat group_id across contacts')
        seenGroupIds.add(entry.group_id)
      }
      if (typeof entry.group_anchor === 'string' && entry.group_anchor.trim()) {
        if (seenGroupAnchorPaths.has(entry.group_anchor)) throw new Error('Duplicate WeChat group_anchor path across groups')
        seenGroupAnchorPaths.add(entry.group_anchor)
      }
      if (Object.hasOwn(entry, 'group_name_read_mode') && !GROUP_NAME_READ_MODES.has(entry.group_name_read_mode)) {
        throw new Error('Invalid WeChat group_name_read_mode')
      }
      if (Object.hasOwn(entry, 'group_member_anchors')) {
        if (!Array.isArray(entry.group_member_anchors) || new Set(entry.group_member_anchors).size !== entry.group_member_anchors.length) {
          throw new Error('Invalid or duplicate WeChat group_member_anchors')
        }
      }
    }
    byId.set(entry.id, entry)
  }
  if (legacyCount > 1) throw new Error('At most one legacy contact is allowed in sidecar registry')
  return byId
}

function validateSidecarAdhocMoments(value) {
  if (value === undefined) return null
  if (!value || typeof value !== 'object' || Array.isArray(value) || typeof value.enabled !== 'boolean') {
    throw new Error('Invalid WeChat adhoc_moments configuration in sidecar registry')
  }
  let defaultVisibility = null
  if (Object.hasOwn(value, 'default_visibility') && value.default_visibility !== null) {
    if (value.default_visibility !== 'private' && value.default_visibility !== 'friends') {
      throw new Error('Invalid WeChat adhoc_moments default_visibility in sidecar registry')
    }
    defaultVisibility = value.default_visibility
  }
  return { enabled: value.enabled, default_visibility: defaultVisibility }
}

function readContactsSidecar(policy, accountId) {
  const configured = policy.wechatContactsConfig
  const sidecarPath = configured ?? join(dirname(policy.wechatConfig), 'octopus-contacts.json')
  if (!existsSync(sidecarPath)) {
    return { present: false, configuredExplicitly: Boolean(configured), byId: null, adhocMoments: null, accountError: null }
  }
  const checked = resolveContainedFile(policy.wechatRoot, sidecarPath)
  if (!checked.ok) throw new Error(`Unsafe WeChat contacts sidecar: ${checked.error}`)
  if ((checked.stat.mode & 0o022) !== 0 || (process.getuid?.() === 0 && checked.stat.uid !== 0)) {
    throw new Error('WeChat contacts sidecar must be root-owned and not writable by others')
  }
  const sidecar = JSON.parse(readFileSync(checked.hostPath, 'utf8'))
  if (!sidecar || typeof sidecar !== 'object' || Array.isArray(sidecar) || sidecar.schema_version !== 1) {
    throw new Error('Invalid WeChat contacts sidecar schema')
  }
  let accountError = null
  if (Object.hasOwn(sidecar, 'account_sha256')) {
    if (typeof sidecar.account_sha256 !== 'string' || !/^[0-9a-f]{64}$/.test(sidecar.account_sha256)) {
      throw new Error('Invalid WeChat contacts sidecar account digest')
    }
    if (!accountId || digest(accountId) !== sidecar.account_sha256) {
      accountError = 'account_binding_changed'
    }
  }
  return {
    present: true,
    configuredExplicitly: Boolean(configured),
    byId: validateClientContactsList(sidecar.contacts),
    adhocMoments: validateSidecarAdhocMoments(sidecar.adhoc_moments),
    accountError,
  }
}

function configViewportOverrides(config) {
  const map = {
    adhoc_cols_per_line: 'colsPerVisualLine',
    adhoc_max_visual_lines: 'maxVisualLines',
    adhoc_max_unbroken_alnum_cols: 'maxUnbrokenAlnumCols',
    adhoc_max_unbroken_token_cols: 'maxUnbrokenTokenCols',
    adhoc_max_composer_height_px: 'maxComposerHeightPx',
  }
  const overrides = {}
  for (const [src, dst] of Object.entries(map)) {
    if (Object.hasOwn(config, src) && config[src] !== null && config[src] !== undefined) {
      const n = Number(config[src])
      if (!Number.isSafeInteger(n) || n <= 0) throw new Error(`Invalid WeChat ${src}`)
      overrides[dst] = n
    }
  }
  return overrides
}

/** Merge broker policy allowlist with Gamma config and DSH root-owned sidecar registry. */
export function resolveWechatRegistry(policy) {
  const config = JSON.parse(readFileSync(policy.wechatConfig, 'utf8'))
  if (!config || typeof config !== 'object' || Array.isArray(config)) {
    throw new Error('Invalid WeChat configuration')
  }
  const globalEnabled = config.enabled === true
  const globalDirectEnabled = config.direct_enabled === true
  const globalGroupEnabled = config.group_enabled !== false
  const accountId = typeof config.account_id === 'string' && config.account_id.trim() ? config.account_id : ''

  let accountAnchor = null
  let ownChatAnchor = null
  let accountAnchorError = null
  let ownChatAnchorError = null
  if (typeof config.account_anchor === 'string' && config.account_anchor.length > 0) {
    const resolved = resolveAnchorBinding(policy.wechatRoot, config.account_anchor)
    if (!resolved.ok) accountAnchorError = resolved.error
    else accountAnchor = resolved
  }
  if (typeof config.own_chat_anchor === 'string' && config.own_chat_anchor.length > 0) {
    const resolved = resolveAnchorBinding(policy.wechatRoot, config.own_chat_anchor)
    if (!resolved.ok) ownChatAnchorError = resolved.error
    else ownChatAnchor = resolved
  }
  const globalAnchorError = accountAnchorError ?? ownChatAnchorError

  const sidecar = readContactsSidecar(policy, accountId)
  const hasSidecar = sidecar.present
  const sidecarById = sidecar.byId
  const seenRoleDigests = new Map(ANCHOR_ROLES.map(role => [role, new Set()]))
  const seenGroupAnchorDigests = new Set()

  const resolveFriendAnchors = (source, requireAnchors) => {
    const resolved = {}
    let error = null
    const anyPresent = ANCHOR_ROLES.some(role => typeof source[role] === 'string' && source[role].length > 0)
    if (!requireAnchors && !anyPresent) {
      return { ok: true, anchors: { recipient_anchor: null, recipient_sidebar_anchor: null, recipient_card_anchor: null } }
    }
    for (const role of ANCHOR_ROLES) {
      if (typeof source[role] !== 'string' || !source[role].trim()) {
        error = error ?? 'missing_anchors'
        resolved[role] = null
        continue
      }
      const checked = resolveAnchorBinding(policy.wechatRoot, source[role])
      if (!checked.ok) {
        error = error ?? 'missing_anchors'
        resolved[role] = null
        continue
      }
      if (seenRoleDigests.get(role).has(checked.sha256)) {
        throw new Error('Duplicate WeChat contact anchor digest across contacts')
      }
      seenRoleDigests.get(role).add(checked.sha256)
      resolved[role] = checked
    }
    return { ok: error === null, error, anchors: resolved }
  }

  const resolveGroupAnchors = source => {
    let error = null
    let groupAnchor = null
    if (typeof source.group_anchor !== 'string' || !source.group_anchor.trim()) {
      error = 'missing_anchors'
    } else {
      const checked = resolveAnchorBinding(policy.wechatRoot, source.group_anchor.trim())
      if (!checked.ok) error = 'missing_anchors'
      else {
        if (seenGroupAnchorDigests.has(checked.sha256)) {
          throw new Error('Duplicate WeChat group_anchor digest across groups')
        }
        seenGroupAnchorDigests.add(checked.sha256)
        groupAnchor = checked
      }
    }

    const rawMembers = Array.isArray(source.group_member_anchors) ? source.group_member_anchors : []
    const groupMemberAnchors = []
    if (rawMembers.length < 1 || rawMembers.length > 2) {
      error = error ?? 'missing_anchors'
    } else {
      const memberDigests = new Set()
      const memberPaths = new Set()
      for (const raw of rawMembers) {
        if (typeof raw !== 'string' || !raw.trim()) {
          error = error ?? 'missing_anchors'
          continue
        }
        const trimmed = raw.trim()
        const checked = resolveAnchorBinding(policy.wechatRoot, trimmed)
        if (!checked.ok) {
          error = error ?? 'missing_anchors'
          continue
        }
        if (memberPaths.has(trimmed) || memberDigests.has(checked.sha256)) {
          throw new Error('Duplicate WeChat group member anchor within group')
        }
        memberPaths.add(trimmed)
        memberDigests.add(checked.sha256)
        groupMemberAnchors.push(checked)
      }
      if (groupMemberAnchors.length !== rawMembers.length) error = error ?? 'missing_anchors'
    }

    const readMode = typeof source.group_name_read_mode === 'string' && source.group_name_read_mode.trim()
      ? source.group_name_read_mode.trim()
      : 'auto'
    if (!GROUP_NAME_READ_MODES.has(readMode)) {
      error = error ?? 'invalid_group_name_read_mode'
    }
    let groupNameAnchor = null
    const rawNameAnchor = typeof source.group_name_anchor === 'string' ? source.group_name_anchor.trim() : ''
    if (readMode === 'anchor' || rawNameAnchor.length > 0) {
      if (!rawNameAnchor) error = error ?? 'missing_anchors'
      else {
        const checked = resolveAnchorBinding(policy.wechatRoot, rawNameAnchor)
        if (!checked.ok) error = error ?? 'missing_anchors'
        else groupNameAnchor = checked
      }
    }

    let sidebarWidth = 280
    if (Object.hasOwn(source, 'group_sidebar_width') && source.group_sidebar_width !== null && source.group_sidebar_width !== undefined) {
      if (!Number.isSafeInteger(source.group_sidebar_width) || source.group_sidebar_width < 200 || source.group_sidebar_width > 600) {
        error = error ?? 'invalid_group_sidebar_width'
      } else {
        sidebarWidth = source.group_sidebar_width
      }
    }

    return { ok: error === null, error, groupAnchor, groupMemberAnchors, groupNameAnchor, readMode, sidebarWidth }
  }

  const adhocPolicy = adhocMomentsPolicy(policy)
  const runtimeAdhocOk = globalEnabled
    && config.adhoc_moments_enabled !== false
    && (sidecar.adhocMoments ? sidecar.adhocMoments.enabled === true : config.adhoc_moments_enabled === true)
  let adhocBindingError = sidecar.accountError ?? accountAnchorError
  if (!adhocBindingError && !accountId) adhocBindingError = 'missing_account_identity'
  if (!adhocBindingError && sidecar.adhocMoments?.default_visibility && adhocPolicy.defaultVisibility
      && sidecar.adhocMoments.default_visibility !== adhocPolicy.defaultVisibility) {
    adhocBindingError = 'binding_policy_mismatch'
  }
  const adhocDefaultVisibility = adhocPolicy.defaultVisibility ?? sidecar.adhocMoments?.default_visibility ?? null
  const adhocViewport = { ...adhocPolicy.viewport, ...configViewportOverrides(config) }
  const adhocMoments = {
    enabled: Boolean(adhocPolicy.enabled && runtimeAdhocOk),
    bound: adhocBindingError === null,
    bindingError: adhocBindingError,
    defaultVisibility: adhocDefaultVisibility,
    viewport: adhocViewport,
    accountAnchor,
  }

  const contacts = []
  if (Array.isArray(policy.wechatContacts)) {
    for (const p of policy.wechatContacts) {
      let c = null
      let bindingError = sidecar.accountError
      if (hasSidecar) {
        c = sidecarById.get(p.id) ?? null
        if (!c) bindingError = bindingError ?? 'missing_client_binding'
      } else if (p.type === 'friend' && !sidecar.configuredExplicitly && p.legacy === true && typeof config.recipient_id === 'string' && config.recipient_id.trim()) {
        const legacyQuery = typeof config.recipient_query === 'string' ? config.recipient_query.trim() : ''
        if (!legacyQuery || legacyQuery === p.displayName) {
          c = {
            id: p.id,
            type: p.type,
            alias: p.alias,
            display_name: p.displayName,
            enabled: config.enabled === true,
            direct_enabled: config.direct_enabled === true,
            legacy: true,
            recipient_id: config.recipient_id,
            recipient_query: legacyQuery || p.displayName,
            recipient_anchor: config.recipient_anchor,
            recipient_sidebar_anchor: config.recipient_sidebar_anchor,
            recipient_card_anchor: config.recipient_card_anchor,
          }
        } else {
          bindingError = bindingError ?? 'binding_policy_mismatch'
        }
      } else {
        bindingError = bindingError ?? 'missing_client_binding'
      }

      if (p.type === 'friend') {
        if (c && !bindingError) {
          const clientDisplayName = typeof c.display_name === 'string' && c.display_name.trim()
            ? c.display_name.trim()
            : (typeof c.recipient_query === 'string' ? c.recipient_query.trim() : '')
          if (c.type !== p.type || c.alias !== p.alias || clientDisplayName !== p.displayName || Boolean(c.legacy) !== Boolean(p.legacy)) {
            bindingError = 'binding_policy_mismatch'
          } else if (p.legacy === true && hasSidecar) {
            // A Gamma rewrite that changes the pinned legacy recipient_id must fail closed on the legacy contact.
            if (typeof config.recipient_id !== 'string' || !config.recipient_id.trim() || config.recipient_id !== c.recipient_id) {
              bindingError = 'legacy_recipient_mismatch'
            }
          }
        }
        const recipientId = c && typeof c.recipient_id === 'string' ? c.recipient_id.trim() : ''
        const recipientQuery = c && typeof c.recipient_query === 'string' ? c.recipient_query.trim() : ''
        const anchorResult = c && !bindingError
          ? resolveFriendAnchors(c, hasSidecar)
          : { ok: false, error: bindingError ?? 'missing_client_binding', anchors: {} }
        if (!bindingError) {
          if (!accountId || !recipientId || !recipientQuery) bindingError = 'missing_recipient_identity'
          else if (globalAnchorError) bindingError = 'missing_anchors'
          else if (!anchorResult.ok) bindingError = anchorResult.error
        }
        const bound = bindingError === null
        const enabled = globalEnabled && p.enabled && Boolean(c?.enabled === true)
        const directEnabled = enabled && globalDirectEnabled && p.capabilities.directSend && Boolean(c?.direct_enabled === true)
        const targetProof = bound
          ? computeBindingSha256({
            accountId,
            contactId: p.id,
            contactType: p.type,
            alias: p.alias,
            displayName: p.displayName,
            recipientId,
            recipientQuery,
            accountAnchor,
            ownChatAnchor,
            recipientAnchor: anchorResult.anchors.recipient_anchor,
            recipientSidebarAnchor: anchorResult.anchors.recipient_sidebar_anchor,
            recipientCardAnchor: anchorResult.anchors.recipient_card_anchor,
          })
          : null
        contacts.push({
          id: p.id,
          type: p.type,
          alias: p.alias,
          displayName: p.displayName,
          legacy: Boolean(p.legacy),
          enabled,
          directEnabled,
          bound,
          bindingError,
          recipientId,
          recipientQuery,
          accountId,
          hasSidecar,
          targetProof,
        })
      } else {
        if (c && !bindingError) {
          const clientDisplayName = typeof c.display_name === 'string' && c.display_name.trim()
            ? c.display_name.trim()
            : (typeof c.group_name === 'string' ? c.group_name.trim() : '')
          const clientGroupName = typeof c.group_name === 'string' && c.group_name.trim() ? c.group_name.trim() : clientDisplayName
          if (c.type !== 'group' || c.alias !== p.alias || clientDisplayName !== p.displayName || clientGroupName !== p.displayName || c.legacy === true) {
            bindingError = 'binding_policy_mismatch'
          }
        }
        const groupId = c && typeof c.group_id === 'string' ? c.group_id.trim() : ''
        const groupName = c && typeof c.group_name === 'string' ? c.group_name.trim() : ''
        const groupQuery = c && typeof c.group_query === 'string' && c.group_query.trim() ? c.group_query.trim() : groupName
        const anchorResult = c && !bindingError
          ? resolveGroupAnchors(c)
          : { ok: false, error: bindingError ?? 'missing_client_binding' }
        if (!bindingError) {
          if (!accountId || !groupId || !groupName) bindingError = 'missing_group_identity'
          else if (accountAnchorError || ownChatAnchorError || !ownChatAnchor) bindingError = 'missing_anchors'
          else if (!anchorResult.ok) bindingError = anchorResult.error
        }
        const bound = bindingError === null
        const enabled = globalEnabled && p.enabled && Boolean(c?.enabled === true)
        const groupEnabled = enabled && globalGroupEnabled && p.capabilities.groupSend && Boolean(c?.group_enabled === true)
        const targetProof = bound
          ? computeGroupBindingSha256({
            accountId,
            contactId: p.id,
            contactType: p.type,
            alias: p.alias,
            displayName: p.displayName,
            groupId,
            groupName,
            groupQuery,
            groupNameReadMode: anchorResult.readMode,
            groupSidebarWidth: anchorResult.sidebarWidth,
            accountAnchor,
            ownChatAnchor,
            groupAnchor: anchorResult.groupAnchor,
            groupNameAnchor: anchorResult.groupNameAnchor,
            groupMemberAnchors: anchorResult.groupMemberAnchors,
          })
          : null
        contacts.push({
          id: p.id,
          type: 'group',
          alias: p.alias,
          displayName: p.displayName,
          legacy: false,
          enabled,
          groupEnabled,
          bound,
          bindingError,
          groupId,
          groupName,
          groupQuery,
          groupNameReadMode: anchorResult.readMode ?? 'auto',
          accountId,
          hasSidecar,
          targetProof,
        })
      }
    }
    return { mode: 'registry', config, accountId, globalEnabled, globalDirectEnabled, globalGroupEnabled, hasSidecar, contacts, adhocMoments }
  }

  // Legacy single-recipient policy mode.
  let c = null
  let bindingError = sidecar.accountError
  if (hasSidecar) {
    const legacyMatch = [...sidecarById.values()].find(item => item.legacy === true && item.alias === policy.wechatRecipientAlias)
    if (legacyMatch) {
      if (typeof config.recipient_id !== 'string' || config.recipient_id !== legacyMatch.recipient_id) {
        bindingError = bindingError ?? 'legacy_recipient_mismatch'
      }
      c = legacyMatch
    } else {
      bindingError = bindingError ?? 'missing_client_binding'
    }
  } else {
    c = {
      id: 'default',
      type: 'friend',
      alias: policy.wechatRecipientAlias,
      display_name: typeof config.recipient_query === 'string' && config.recipient_query.trim()
        ? config.recipient_query.trim()
        : policy.wechatRecipientAlias,
      enabled: config.enabled === true,
      direct_enabled: config.direct_enabled === true,
      legacy: true,
      recipient_id: typeof config.recipient_id === 'string' ? config.recipient_id : '',
      recipient_query: typeof config.recipient_query === 'string' ? config.recipient_query : '',
      recipient_anchor: config.recipient_anchor,
      recipient_sidebar_anchor: config.recipient_sidebar_anchor,
      recipient_card_anchor: config.recipient_card_anchor,
    }
  }

  const recipientId = c && typeof c.recipient_id === 'string' ? c.recipient_id.trim() : ''
  const recipientQuery = c && typeof c.recipient_query === 'string' ? c.recipient_query.trim() : ''
  const displayName = recipientQuery || policy.wechatRecipientAlias
  const anchorResult = c && !bindingError
    ? resolveFriendAnchors(c, hasSidecar)
    : { ok: false, error: bindingError ?? 'missing_client_binding', anchors: {} }
  if (!bindingError) {
    if (!accountId || !recipientId) bindingError = 'missing_recipient_identity'
    else if (globalAnchorError) bindingError = 'missing_anchors'
    else if (!anchorResult.ok) bindingError = anchorResult.error
  }
  const bound = bindingError === null
  const enabled = globalEnabled && Boolean(c?.enabled === true)
  const directEnabled = enabled && globalDirectEnabled && Boolean(c?.direct_enabled === true)
  const targetProof = bound
    ? computeBindingSha256({
      accountId,
      contactId: c.id,
      contactType: 'friend',
      alias: policy.wechatRecipientAlias,
      displayName,
      recipientId,
      recipientQuery,
      accountAnchor,
      ownChatAnchor,
      recipientAnchor: anchorResult.anchors.recipient_anchor,
      recipientSidebarAnchor: anchorResult.anchors.recipient_sidebar_anchor,
      recipientCardAnchor: anchorResult.anchors.recipient_card_anchor,
    })
    : null
  contacts.push({
    id: c?.id ?? 'default',
    type: 'friend',
    alias: policy.wechatRecipientAlias,
    displayName,
    legacy: true,
    enabled,
    directEnabled,
    bound,
    bindingError,
    recipientId,
    recipientQuery,
    accountId,
    hasSidecar,
    targetProof,
  })
  return { mode: 'legacy', config, accountId, globalEnabled, globalDirectEnabled, globalGroupEnabled: false, hasSidecar, contacts, adhocMoments }
}

function resolveUniqueTarget(registry, recipient, expectedType, { requireSendEnabled = true } = {}) {
  const matches = registry.contacts.filter(contact =>
    contact.id === recipient || contact.alias === recipient || contact.displayName === recipient)
  if (matches.length === 0) throw new Error(expectedType === 'group' ? 'Group target is not bound' : 'Recipient is not bound')
  if (matches.length > 1) throw new Error('Ambiguous WeChat recipient; specify unique contact id or alias')
  const contact = matches[0]
  if (contact.type !== expectedType) {
    throw new Error(expectedType === 'friend' ? 'Only friend contacts are supported for wechat_send' : 'Target is a friend, not an enrolled group')
  }
  if (!contact.enabled || (requireSendEnabled && !(expectedType === 'friend' ? contact.directEnabled : contact.groupEnabled))) {
    throw new Error('WeChat target is disabled')
  }
  if (!contact.bound) {
    throw new Error(contact.bindingError === 'binding_policy_mismatch' ? 'WeChat contact policy mismatch' : 'WeChat recipient binding is missing')
  }
  return contact
}

/** Resolve authority from the ingress-owned database; model text is never an authority source. */
export function authority(policy, now) {
  const db = new DatabaseSync(policy.ingressDatabase, { readOnly: true })
  try {
    const operator = db.prepare('SELECT value FROM u_im_ingress_operators WHERE key=?').get(policy.bindingKey)
    if (!operator) throw new Error('No authenticated binding')
    const binding = JSON.parse(operator.value)
    if (binding.revokedAt !== null || binding.operatorId !== policy.operatorId || binding.targetTenantId !== policy.tenantId) throw new Error('Binding revoked or changed')
    const rows = db.prepare("SELECT value FROM u_im_ingress_deliveries WHERE json_extract(value,'$.status')='in_flight' AND json_extract(value,'$.operatorId')=? AND json_extract(value,'$.targetTenantId')=?").all(policy.operatorId, policy.tenantId)
    const live = rows.map(row => JSON.parse(row.value)).filter(row => {
      const identity = JSON.parse(row.deliveryKey)
      return JSON.stringify(identity.slice(0, 5)) === policy.bindingKey && Date.parse(row.receivedAt) <= now && now - Date.parse(row.receivedAt) <= policy.limits.previewSeconds * 1000
    })
    if (live.length !== 1) throw new Error('Exactly one current authenticated human delivery is required')
    return { requestId: live[0].requestId, prompt: live[0].prompt, receivedAt: Date.parse(live[0].receivedAt) }
  } finally { db.close() }
}

/** Durable journal with no automatic replay of claimed operations. */
export class Operations {
  constructor(policy, policyPath, execute, clock = Date.now, resolveAuthority = authority) {
    this.policy = validatePolicy(policy)
    this.policyPath = policyPath
    this.execute = execute
    this.clock = clock
    this.resolveAuthority = resolveAuthority
    mkdirSync(policy.state, { recursive: true, mode: 0o700 })
    const opsDbPath = join(policy.state, 'operations.sqlite')
    this.db = new DatabaseSync(opsDbPath)
    chmodSync(opsDbPath, 0o600)
    this.db.exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; CREATE TABLE IF NOT EXISTS operations(id TEXT PRIMARY KEY, value TEXT NOT NULL)")
    for (const row of this.db.prepare("SELECT id,value FROM operations WHERE json_extract(value,'$.state')='executing'").all()) {
      const operation = JSON.parse(row.value)
      operation.state = 'needs_review'
      operation.error = 'Broker stopped after claiming the operation; inspect before retrying'
      this.save(operation)
    }
  }

  save(operation) {
    this.db.prepare('INSERT INTO operations(id,value) VALUES(?,?) ON CONFLICT(id) DO UPDATE SET value=excluded.value').run(operation.id, JSON.stringify(operation))
  }

  get(id) {
    if (typeof id !== 'string' || !/^[0-9a-f]{24}$/.test(id)) throw new Error('Invalid operation identifier')
    const row = this.db.prepare('SELECT value FROM operations WHERE id=?').get(id)
    if (!row) throw new Error('Unknown operation')
    const operation = JSON.parse(row.value)
    if (operation.state === 'prepared' && this.clock() >= operation.expiresAt) { operation.state = 'expired'; this.save(operation) }
    return operation
  }

  prepare(input) {
    fields(input, ['kind', 'payload'])
    const actor = this.resolveAuthority(this.policy, this.clock())
    const payload = structuredClone(input.payload)
    let direct = false
    let directVersion
    let directSource
    let directProof
    let targetProof
    let recipientDisplayName
    let contactId
    let contactType
    let recipientAlias
    let visibility
    let visibilitySource
    let adhocLayout
    let noticeId
    let noticeKey
    let noticeSourceObservedAt
    let noticeCreatedAt
    let noticeDeadline
    let accountSha256
    if (input.kind === 'restart_service' || input.kind === 'restart_container') {
      fields(payload, ['target'])
      if (input.kind === 'restart_service' ? !Object.hasOwn(this.policy.services, payload.target) : !this.policy.containers.includes(payload.target)) throw new Error('Target is not allowed')
      targetProof = input.kind === 'restart_service' ? this.policy.services[payload.target] : payload.target
      direct = actor.prompt.trim() === `${input.kind === 'restart_service' ? '重启服务' : '重启容器'} ${payload.target}`
    } else if (input.kind === 'wechat_send' || input.kind === 'wechat_send_group') {
      fields(payload, ['recipient', 'text'])
      if (typeof payload.recipient !== 'string' || !payload.recipient.trim()) throw new Error('Invalid WeChat recipient')
      if (typeof payload.text !== 'string' || !payload.text.trim() || Buffer.byteLength(payload.text) > this.policy.limits.maxTextBytes || payload.text.includes('\0')) throw new Error('Invalid or oversized message')
      const registry = resolveWechatRegistry(this.policy)
      const expectedType = input.kind === 'wechat_send' ? 'friend' : 'group'
      const contact = resolveUniqueTarget(registry, payload.recipient, expectedType)
      payload.recipient = contact.alias
      targetProof = contact.targetProof
      recipientDisplayName = contact.displayName
      contactId = contact.id
      contactType = contact.type
      recipientAlias = contact.alias
      const directCheck = authorizeDirectWechat({ kind: input.kind, payload, contact, visibility: null, registry, actor })
      if (directCheck.ok) {
        direct = true
        directVersion = DIRECT_POLICY_VERSION
        directSource = 'ingress_prompt'
        directProof = computeDirectProof({ kind: input.kind, requestId: actor.requestId, key: directCheck.key, text: payload.text })
      }
    } else if (input.kind === 'wechat_reply_notice') {
      fields(payload, ['noticeId', 'text'])
      if (typeof payload.noticeId !== 'string' || !/^wn_[0-9a-f]{12}$/.test(payload.noticeId)) {
        throw new Error('Invalid WeChat notice identifier')
      }
      if (typeof payload.text !== 'string' || !payload.text.trim() || Buffer.byteLength(payload.text) > this.policy.limits.maxTextBytes || payload.text.includes('\0')) {
        throw new Error('Invalid or oversized message')
      }
      const resolved = resolveActionableInboundNotice(this.policy, payload.noticeId, this.clock())
      const { notice, contact, registry } = resolved
      targetProof = contact.targetProof
      recipientDisplayName = contact.displayName
      contactId = contact.id
      contactType = contact.type
      recipientAlias = contact.alias
      noticeId = notice.notice_id
      noticeKey = notice.notice_key
      noticeSourceObservedAt = notice.source_observed_at
      noticeCreatedAt = notice.created_at
      noticeDeadline = resolved.noticeDeadline
      accountSha256 = notice.account_sha256
      const directCheck = authorizeDirectWechat({
        kind: 'wechat_reply_notice',
        payload,
        contact,
        visibility: null,
        noticeId: notice.notice_id,
        registry,
        actor,
      })
      if (directCheck.ok) {
        direct = true
        directVersion = DIRECT_POLICY_VERSION
        directSource = 'ingress_prompt'
        directProof = computeDirectProof({ kind: 'wechat_reply_notice', requestId: actor.requestId, key: directCheck.key, text: payload.text })
      }
    } else if (input.kind === 'wechat_probe_group') {
      fields(payload, ['recipient'])
      if (typeof payload.recipient !== 'string' || !payload.recipient.trim()) throw new Error('Invalid WeChat group target')
      const registry = resolveWechatRegistry(this.policy)
      const contact = resolveUniqueTarget(registry, payload.recipient, 'group', { requireSendEnabled: false })
      payload.recipient = contact.alias
      targetProof = contact.targetProof
      recipientDisplayName = contact.displayName
      contactId = contact.id
      contactType = contact.type
      recipientAlias = contact.alias
      direct = true
    } else if (input.kind === 'wechat_publish_moment') {
      fields(payload, ['text'], ['visibility'])
      const registry = resolveWechatRegistry(this.policy)
      const adhoc = registry.adhocMoments
      if (!adhoc.enabled) throw new Error('WeChat ad hoc Moments is disabled')
      if (!adhoc.bound || !registry.accountId) {
        throw new Error(adhoc.bindingError === 'binding_policy_mismatch'
          ? 'WeChat ad hoc Moments policy mismatch'
          : 'WeChat ad hoc Moments binding is missing')
      }
      let usedConfiguredDefault = false
      if (Object.hasOwn(payload, 'visibility') && payload.visibility !== undefined) {
        if (payload.visibility !== 'private' && payload.visibility !== 'friends') {
          throw new Error('Invalid ad hoc Moments visibility; expected private or friends')
        }
        visibility = payload.visibility
      } else if (adhoc.defaultVisibility) {
        visibility = adhoc.defaultVisibility
        payload.visibility = visibility
        usedConfiguredDefault = true
      } else {
        throw new Error('Ad hoc Moments visibility is required (private or friends)')
      }
      if (typeof payload.text !== 'string' || Buffer.byteLength(payload.text) > Math.min(4096, this.policy.limits.maxTextBytes)) {
        throw new Error('Ad hoc Moments message exceeds maximum byte limit')
      }
      adhocLayout = validateAdhocMomentText(payload.text, adhoc.viewport)
      targetProof = computeAdhocMomentsBindingSha256({
        accountId: registry.accountId,
        accountAnchor: adhoc.accountAnchor,
        visibility,
        textSha256: adhocLayout.textSha256,
        viewport: adhoc.viewport,
      })
      const directCheck = authorizeDirectWechat({ kind: input.kind, payload, contact: null, visibility, registry, actor })
      if (directCheck.ok) {
        direct = true
        directVersion = DIRECT_POLICY_VERSION
        directSource = 'ingress_prompt'
        visibilitySource = directCheck.visibilitySource
        directProof = computeDirectProof({ kind: input.kind, requestId: actor.requestId, key: directCheck.key, text: payload.text })
      } else {
        visibilitySource = usedConfiguredDefault ? 'configured_default' : 'preview_argument'
      }
    } else if (input.kind === 'set_limits') {
      fields(payload, [], ['logLines', 'previewSeconds'])
      if (!Object.keys(payload).length) throw new Error('Empty configuration change')
      validatePolicy({ ...this.policy, limits: { ...this.policy.limits, ...payload } })
      payload.previousSha256 = digest(readFileSync(this.policyPath, 'utf8'))
    } else if (input.kind === 'restore_limits') {
      fields(payload, ['operation'])
      const old = this.get(payload.operation)
      if (old.kind !== 'set_limits' || old.state !== 'completed') throw new Error('No completed configuration backup')
      payload.previousSha256 = digest(readFileSync(this.policyPath, 'utf8'))
    } else throw new Error('Unknown operation kind')
    // A duplicate model call in the same request returns the original frozen preview.
    const canonicalPayload = Object.fromEntries(Object.entries(payload).sort(([left], [right]) => left.localeCompare(right)))
    const fingerprint = digest(JSON.stringify([actor.requestId, input.kind, canonicalPayload]))
    const previous = this.db.prepare("SELECT value FROM operations WHERE json_extract(value,'$.fingerprint')=?").get(fingerprint)
    if (previous) return JSON.parse(previous.value)
    const nowMs = this.clock()
    const defaultExpiresAt = nowMs + this.policy.limits.previewSeconds * 1000
    const expiresAt = input.kind === 'wechat_reply_notice'
      ? Math.min(defaultExpiresAt, noticeDeadline)
      : defaultExpiresAt
    if (expiresAt <= nowMs) throw new Error('WeChat notice expired for manual reply')
    const operation = { id: randomBytes(12).toString('hex'), kind: input.kind, payload, fingerprint, state: 'prepared',
      createdAt: nowMs, expiresAt,
      actorRequestId: actor.requestId, direct: Boolean(direct) }
    if (directVersion !== undefined) operation.directVersion = directVersion
    if (directSource !== undefined) operation.directSource = directSource
    if (directProof !== undefined) operation.directProof = directProof
    if (targetProof !== undefined) operation.targetProof = targetProof
    if (recipientDisplayName !== undefined) operation.recipientDisplayName = recipientDisplayName
    if (contactId !== undefined) operation.contactId = contactId
    if (contactType !== undefined) operation.contactType = contactType
    if (recipientAlias !== undefined) operation.recipientAlias = recipientAlias
    if (visibility !== undefined) operation.visibility = visibility
    if (visibilitySource !== undefined) operation.visibilitySource = visibilitySource
    if (adhocLayout !== undefined) operation.adhocLayout = adhocLayout
    if (noticeId !== undefined) operation.noticeId = noticeId
    if (noticeKey !== undefined) operation.noticeKey = noticeKey
    if (noticeSourceObservedAt !== undefined) operation.noticeSourceObservedAt = noticeSourceObservedAt
    if (noticeCreatedAt !== undefined) operation.noticeCreatedAt = noticeCreatedAt
    if (noticeDeadline !== undefined) operation.noticeDeadline = noticeDeadline
    if (accountSha256 !== undefined) operation.accountSha256 = accountSha256
    this.save(operation)
    return operation
  }

  async run(id) {
    let operation = this.get(id)
    if (['completed', 'queued', 'needs_review', 'expired'].includes(operation.state)) return operation
    if (operation.state !== 'prepared') throw new Error('Operation has already been claimed')
    const actor = this.resolveAuthority(this.policy, this.clock())
    const confirmed = actor.prompt.trim() === `确认执行 ${id}` && actor.receivedAt >= operation.createdAt && actor.requestId !== operation.actorRequestId
    let directAuthorized = false
    if (!confirmed && operation.direct === true && actor.requestId === operation.actorRequestId) {
      if (operation.kind === 'restart_service' || operation.kind === 'restart_container') {
        directAuthorized = actor.prompt.trim() === `${operation.kind === 'restart_service' ? '重启服务' : '重启容器'} ${operation.payload.target}`
      } else if (operation.kind === 'wechat_probe_group') {
        directAuthorized = true
      } else if (OUTBOUND_WECHAT_KINDS.has(operation.kind)
          && operation.directVersion === DIRECT_POLICY_VERSION
          && typeof operation.directProof === 'string') {
        const parsed = parseDirectWechatPrompt(actor.prompt)
        if (parsed && parsed.kind === operation.kind && parsed.text === operation.payload.text) {
          if (operation.kind === 'wechat_send' || operation.kind === 'wechat_send_group') {
            const frozenNames = new Set([operation.contactId, operation.recipientAlias, operation.recipientDisplayName, operation.payload.recipient].filter(Boolean))
            if (frozenNames.has(parsed.target)
                && operation.contactId
                && operation.directProof === computeDirectProof({ kind: operation.kind, requestId: actor.requestId, key: operation.contactId, text: operation.payload.text })) {
              directAuthorized = true
            }
          } else if (operation.kind === 'wechat_reply_notice') {
            if (parsed.noticeId === operation.noticeId
                && operation.contactId
                && operation.targetProof
                && operation.directProof === computeDirectProof({ kind: operation.kind, requestId: actor.requestId, key: `${operation.noticeId}:${operation.contactId}:${operation.targetProof}`, text: operation.payload.text })) {
              directAuthorized = true
            }
          } else if (operation.kind === 'wechat_publish_moment') {
            const visMatches = parsed.visibility !== null
              ? (parsed.visibility === operation.visibility && operation.visibilitySource === 'owner_prompt')
              : (operation.visibilitySource === 'configured_default')
            if (visMatches
                && operation.directProof === computeDirectProof({ kind: operation.kind, requestId: actor.requestId, key: operation.visibility, text: operation.payload.text })) {
              directAuthorized = true
            }
          }
        }
      }
    }
    if (!(confirmed || directAuthorized)) throw new Error(`需要本人在飞书回复：确认执行 ${id}`)

    const replacement = { ...operation, state: 'executing', confirmationRequestId: actor.requestId }
    this.db.exec('BEGIN IMMEDIATE')
    try {
      if (!confirmed && OUTBOUND_WECHAT_KINDS.has(operation.kind)) {
        const priorClaim = this.db.prepare(
          "SELECT id FROM operations WHERE id!=? AND json_extract(value,'$.actorRequestId')=? AND json_extract(value,'$.kind') IN ('wechat_send','wechat_send_group','wechat_publish_moment','wechat_reply_notice') AND json_extract(value,'$.state') IN ('executing','queued','completed','needs_review')"
        ).get(id, actor.requestId)
        if (priorClaim) throw new Error('Only one direct WeChat operation is authorized per owner request')
      }
      const changed = this.db.prepare("UPDATE operations SET value=? WHERE id=? AND json_extract(value,'$.state')='prepared'").run(JSON.stringify(replacement), id)
      if (changed.changes !== 1) throw new Error('Operation concurrently claimed')
      this.db.exec('COMMIT')
    } catch (error) {
      try { this.db.exec('ROLLBACK') } catch { /* Already rolled back before rethrowing. */ }
      throw error
    }
    operation = replacement
    try {
      if (operation.kind === 'set_limits' || operation.kind === 'restore_limits') {
        const original = readFileSync(this.policyPath, 'utf8')
        if (digest(original) !== operation.payload.previousSha256) throw new Error('Configuration changed after preview')
        const backup = join(this.policy.state, `${id}.backup.json`)
        atomicJson(backup, JSON.parse(original))
        const limits = operation.kind === 'set_limits' ? { ...this.policy.limits, ...operation.payload } :
          JSON.parse(readFileSync(join(this.policy.state, `${operation.payload.operation}.backup.json`), 'utf8')).limits
        const next = { ...this.policy, limits: Object.fromEntries(Object.entries(limits).filter(([key]) => key !== 'previousSha256')) }
        validatePolicy(next)
        atomicJson(this.policyPath, next)
        this.policy = next
        operation.result = { backup: id, limits: next.limits }
        operation.state = 'completed'
      } else {
        operation.result = await this.execute(operation, this.policy, { now: this.clock() })
        operation.state = QUEUED_WECHAT_KINDS.has(operation.kind) ? 'queued' : 'completed'
      }
    } catch (error) { operation.state = 'needs_review'; operation.error = String(error.message).slice(0, 500) }
    this.save(operation)
    return operation
  }

  close() { if (this.db.isOpen) this.db.close() }
}
