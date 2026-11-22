/**
 * Durable Feishu owner-notice persistence, restricted handoff ingestion, and
 * uncertainty-safe dispatch separated from human-ingress delivery records.
 *
 * @module @deepseek-ai/dsh-experimental-im-feishu/owner-notice
 */

import { createHash } from 'node:crypto'
import {
  closeSync,
  constants,
  fstatSync,
  lstatSync,
  openSync,
  readdirSync,
  readFileSync,
  realpathSync,
} from 'node:fs'
import { isAbsolute, join, resolve } from 'node:path'
import { z } from 'zod'
import { defineDomain, domainTable, type Domain, type KvTable } from '@deepseek-ai/dsh-storage-domain'
import { sanitizeOutboundText } from './feishu-normalizer.ts'
import {
  FeishuOutboundError,
  type FeishuOutboundClient,
  type FeishuSendTextResult,
} from './feishu-outbound.ts'
import type { OperatorBindingRecord } from './storage.ts'

/** Storage domain name dedicated to WeChat owner notifications. */
export const OWNER_NOTICE_DOMAIN_NAME = 'im_owner_notice'
/** Version of the owner-notice domain table schema. */
export const OWNER_NOTICE_SCHEMA_VERSION = 1
/** Maximum sanitized UTF-16 length accepted for an owner notice body. */
export const OWNER_NOTICE_MAX_LENGTH = 4096
/** Maximum serialized handoff envelope byte size accepted by ingress. */
export const MAX_HANDOFF_ENVELOPE_BYTES = 16_384
/** Explicit capability contract version exported for installer runtime resolution parity checks. */
export const OWNER_NOTICE_CAPABILITY_VERSION = 1

/** Built module capability marker inspected before enabling `ownerNotices` in an ingress profile. */
export const ownerNoticesCapability = Object.freeze({
  supported: true,
  schemaVersion: OWNER_NOTICE_CAPABILITY_VERSION,
  configKey: 'ownerNotices',
  handoffSchemaVersion: 1,
  boundedNoticePump: true,
})

/** Supported categories for owner-directed WeChat notification records. */
export const ownerNoticeKindSchema = z.enum(['inbound_alert', 'outbound_receipt'])
/** Type of owner notification record. */
export type OwnerNoticeKind = z.infer<typeof ownerNoticeKindSchema>

/** Durable send lifecycle distinguishing claimed attempts from uncertain outcomes. */
export const ownerNoticeStatusSchema = z.enum(['pending', 'in_flight', 'sent', 'failed', 'unknown'])
/** Send status of an owner notification record. */
export type OwnerNoticeStatus = z.infer<typeof ownerNoticeStatusSchema>

/** Schema validating one durable owner notice record. */
export const ownerNoticeRecordSchema = z.object({
  noticeKey: z.string().min(1),
  noticeId: z.string().regex(/^wn_[0-9a-f]{12}$/),
  kind: ownerNoticeKindSchema,
  conversationKey: z.string().min(1).nullable().optional(),
  bindingKey: z.string().min(1),
  chatId: z.string().min(1),
  operatorId: z.string().min(1),
  targetTenantId: z.string().min(1),
  body: z.string().min(1),
  replyMessageId: z.string().min(1).nullable(),
  sourceRef: z.string().min(1),
  untrusted: z.literal(true),
  authority: z.literal('notice_only'),
  status: ownerNoticeStatusSchema,
  attempts: z.number().int().nonnegative(),
  messageId: z.string().nullable(),
  error: z.string().nullable(),
  createdAt: z.string(),
  updatedAt: z.string(),
  sentAt: z.string().nullable(),
}).strict()

/** Durable record tracking one Feishu owner notice independent of IM ingress deliveries. */
export type OwnerNoticeRecord = z.infer<typeof ownerNoticeRecordSchema>

/** Schema tracking the latest sent Feishu platform message ID per WeChat conversation key. */
export const conversationThreadSchema = z.object({
  conversationKey: z.string().min(1),
  lastMessageId: z.string().min(1),
  lastNoticeId: z.string().regex(/^wn_[0-9a-f]{12}$/),
  updatedAt: z.string(),
}).strict()

/** Durable thread cursor mapping one WeChat conversation key to its latest Feishu reply ID. */
export type ConversationThreadRecord = z.infer<typeof conversationThreadSchema>

/** Strict sanitized envelope produced into a restricted read-only handoff directory. */
export const handoffNoticeEnvelopeSchema = z.object({
  schemaVersion: z.literal(1),
  noticeKey: z.string().min(1).max(256),
  noticeId: z.string().regex(/^wn_[0-9a-f]{12}$/),
  kind: ownerNoticeKindSchema,
  conversationKey: z.string().min(1).max(256).nullable(),
  sourceRef: z.string().min(1).max(256),
  body: z.string().min(1).max(OWNER_NOTICE_MAX_LENGTH),
  createdAt: z.number().int().nonnegative(),
  untrusted: z.literal(true),
  authority: z.literal('notice_only'),
}).strict()

/** Typed handoff envelope transferred from the trusted exchange reader to IM ingress. */
export type HandoffNoticeEnvelope = z.infer<typeof handoffNoticeEnvelopeSchema>

/** Opt-in, default-off owner notification settings for the IM ingress plugin. */
export const ownerNoticePluginConfigSchema = z.object({
  enabled: z.boolean().default(false),
  handoffDirectory: z.string().min(1).optional(),
  bindingKey: z.string().min(1).optional(),
  operatorId: z.string().min(1).optional(),
  targetTenantId: z.string().min(1).optional(),
  feishuAppId: z.string().min(1).optional(),
  feishuChatId: z.string().min(1).optional(),
  expectedProducerUid: z.number().int().nonnegative().optional(),
  pollIntervalMs: z.number().int().positive().max(300_000).default(5000),
  retentionDays: z.number().int().min(1).max(90).default(14),
  maxNoticeBytes: z.number().int().min(256).max(OWNER_NOTICE_MAX_LENGTH).default(OWNER_NOTICE_MAX_LENGTH),
  maxPendingNotices: z.number().int().positive().max(10_000).default(1000),
  maxIngestPerCycle: z.number().int().positive().max(200).default(20),
  maxSendsPerCycle: z.number().int().positive().max(100).default(5),
}).strict().superRefine((cfg, ctx) => {
  if (!cfg.enabled) return
  if (!cfg.handoffDirectory) {
    ctx.addIssue({ code: 'custom', path: ['handoffDirectory'], message: 'handoffDirectory is required when ownerNotices.enabled is true' })
  }
  if (!cfg.bindingKey) {
    ctx.addIssue({ code: 'custom', path: ['bindingKey'], message: 'bindingKey is required when ownerNotices.enabled is true' })
  } else {
    let validTuple = false
    try {
      const tupleSchema = z.tuple([z.literal('feishu'), z.string().min(1), z.string().min(1), z.string().min(1), z.string().min(1)])
      validTuple = tupleSchema.safeParse(JSON.parse(cfg.bindingKey)).success
    } catch {
      validTuple = false
    }
    if (!validTuple) {
      ctx.addIssue({ code: 'custom', path: ['bindingKey'], message: 'bindingKey must be a canonical 5-tuple Feishu JSON array' })
    }
  }
  if (!cfg.operatorId) {
    ctx.addIssue({ code: 'custom', path: ['operatorId'], message: 'operatorId is required when ownerNotices.enabled is true' })
  }
  if (!cfg.targetTenantId) {
    ctx.addIssue({ code: 'custom', path: ['targetTenantId'], message: 'targetTenantId is required when ownerNotices.enabled is true' })
  }
})

/** Raw opt-in owner notification config accepted in ingress profiles before default resolution. */
export type OwnerNoticePluginRawConfig = z.input<typeof ownerNoticePluginConfigSchema>
/** Validated owner notification config resolved for ingress runtime execution. */
export type OwnerNoticePluginConfig = z.output<typeof ownerNoticePluginConfigSchema>

/** Private storage domain definition for Feishu owner notices and thread cursors. */
export const ownerNoticeDomainSpec = defineDomain({
  name: OWNER_NOTICE_DOMAIN_NAME,
  version: OWNER_NOTICE_SCHEMA_VERSION,
  tables: {
    notices: domainTable<string, OwnerNoticeRecord>(ownerNoticeRecordSchema),
    threads: domainTable<string, ConversationThreadRecord>(conversationThreadSchema),
  },
})

/** Minimal Feishu outbound surface reused by owner notices without touching app secrets. */
export type FeishuNoticeTextSender = Pick<FeishuOutboundClient, 'sendTextMessage'>

/** Expected administrator-configured owner binding assertions checked before dispatching. */
export interface ExpectedOwnerBinding {
  readonly bindingKey: string
  readonly operatorId: string
  readonly targetTenantId: string
  readonly appId?: string | undefined
  readonly chatId?: string | undefined
}

/** Input required to enqueue one idempotent Feishu owner notice. */
export interface EnqueueOwnerNoticeInput {
  readonly noticeKey: string
  readonly noticeId?: string | undefined
  readonly kind: OwnerNoticeKind
  readonly conversationKey?: string | null | undefined
  readonly bindingKey: string
  readonly chatId: string
  readonly operatorId: string
  readonly targetTenantId: string
  readonly body: string
  readonly replyMessageId?: string | undefined
  readonly sourceRef: string
}

/** Definite validation failure for a missing, revoked, or mismatched owner route. */
export class InvalidOwnerRouteError extends Error {
  /** Distinguishes pre-dispatch route rejection from post-send uncertainty. */
  readonly status = 'failed' as const

  /**
   * Construct a definite owner route error.
   *
   * @param message - Failure reason shown without leaking credentials.
   */
  constructor(message: string) {
    super(message)
    this.name = 'InvalidOwnerRouteError'
  }
}

const definiteFailureStatusSchema = z.object({ status: z.literal('failed') })
const sentResultSchema = z.object({ messageId: z.string().trim().min(1) })
const BIDI_AND_CONTROL_RE = /[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f\u200e\u200f\u202a-\u202e\u2066-\u2069]/g

function resolveAllowedProducerUids(expectedProducerUid?: number): ReadonlySet<number> {
  if (expectedProducerUid !== undefined) return new Set([expectedProducerUid])
  const uids = [0]
  const currentUid = process.getuid?.()
  if (currentUid !== undefined) uids.push(currentUid)
  return new Set(uids)
}

/**
 * Verify that the handoff directory is canonical, non-symlinked, non-writable by group/others,
 * has no world permissions (`& 0o027 === 0`), and is owned by a pinned producer UID.
 *
 * @param directory - Restricted handoff directory configured for owner notices.
 * @param expectedProducerUid - Optional pinned UID override for strict caller verification.
 */
export function assertTrustedHandoffDirectory(
  directory: string,
  expectedProducerUid?: number,
): void {
  if (!isAbsolute(directory)) {
    throw new Error('Owner notice handoffDirectory must be an absolute path')
  }
  const lstat = lstatSync(directory)
  if (lstat.isSymbolicLink() || !lstat.isDirectory()) {
    throw new Error('Owner notice handoffDirectory must be a non-symlinked directory')
  }
  if (realpathSync(directory) !== resolve(directory)) {
    throw new Error('Owner notice handoffDirectory must be canonical')
  }
  if ((lstat.mode & 0o027) !== 0) {
    throw new Error('Owner notice handoffDirectory must not be group/world-writable or world-accessible')
  }
  const allowedUids = resolveAllowedProducerUids(expectedProducerUid)
  if (!allowedUids.has(lstat.uid)) {
    throw new Error(`Owner notice handoffDirectory owner uid ${lstat.uid} is not permitted`)
  }
}

/**
 * Derive a deterministic actionable `wn_<12hex>` identifier from a unique notice key.
 *
 * @param noticeKey - Unique idempotency key for the batch or receipt transition.
 * @returns Actionable `wn_<12hex>` identifier shared across storage and Feishu text.
 */
export function formatOwnerNoticeId(noticeKey: string): string {
  const digest = createHash('sha256').update(noticeKey).digest('hex').slice(0, 12)
  return `wn_${digest}`
}

/**
 * Scrub control characters, bidirectional overrides, reasoning blocks, and secret tokens
 * from untrusted WeChat notice text before storing or sending to Feishu.
 *
 * @param text - Untrusted notice text assembled from worker inputs or receipts.
 * @param maxLength - Maximum UTF-16 character length preserved after sanitization.
 * @returns Sanitized bounded display text safe for Feishu delivery.
 */
export function sanitizeNoticeDisplayText(
  text: string,
  maxLength = OWNER_NOTICE_MAX_LENGTH,
): string {
  const stripped = text.replace(BIDI_AND_CONTROL_RE, '')
  return sanitizeOutboundText(stripped, maxLength)
}

/**
 * Verify that an operator binding record is active, matches the configured canonical Feishu tuple,
 * and still targets the authorized operator and tenant.
 *
 * @param binding - Live binding read from `u_im_ingress_operators`.
 * @param expected - Policy assertions for operator, tenant, binding key, and optional app/chat pins.
 * @returns Verified active Feishu binding record.
 */
export function resolveAuthenticatedOwnerRoute(
  binding: OperatorBindingRecord | undefined,
  expected: ExpectedOwnerBinding,
): OperatorBindingRecord {
  if (!binding) {
    throw new InvalidOwnerRouteError('Owner binding record is missing')
  }
  if (binding.revokedAt !== null) {
    throw new InvalidOwnerRouteError('Owner binding has been revoked')
  }
  if (binding.provider !== 'feishu') {
    throw new InvalidOwnerRouteError('Owner notice routing requires a Feishu operator binding')
  }
  const canonicalKey = JSON.stringify([
    binding.provider,
    binding.appId,
    binding.tenantId,
    binding.chatId,
    binding.senderId,
  ])
  if (binding.bindingKey !== expected.bindingKey || canonicalKey !== expected.bindingKey) {
    throw new InvalidOwnerRouteError('Owner binding key mismatch')
  }
  if (binding.operatorId !== expected.operatorId || binding.targetTenantId !== expected.targetTenantId) {
    throw new InvalidOwnerRouteError('Owner operator or target tenant mismatch')
  }
  if (expected.appId !== undefined && binding.appId !== expected.appId) {
    throw new InvalidOwnerRouteError('Owner Feishu appId mismatch')
  }
  if (expected.chatId !== undefined && binding.chatId !== expected.chatId) {
    throw new InvalidOwnerRouteError('Owner Feishu chatId mismatch')
  }
  return binding
}

/** Private durable journal for WeChat owner notices backed by `@deepseek-ai/dsh-storage-domain`. */
export class OwnerNoticeStorage {
  private readonly table: KvTable<string, OwnerNoticeRecord>
  private readonly threadsTable: KvTable<string, ConversationThreadRecord>
  private readonly queues = new Map<string, Promise<unknown>>()

  /**
   * Bind storage to an opened `ownerNoticeDomainSpec` domain.
   *
   * @param domain - Opened domain holding the `notices` and `threads` KV tables.
   */
  constructor(domain: Domain<typeof ownerNoticeDomainSpec>) {
    this.table = domain.table('notices')
    this.threadsTable = domain.table('threads')
  }

  /**
   * Look up a single durable owner notice by its idempotency key.
   *
   * @param noticeKey - Unique key for the batch or receipt state.
   * @returns Stored notice record when present.
   */
  getNotice(noticeKey: string): OwnerNoticeRecord | undefined {
    return this.table.get(noticeKey)
  }

  /**
   * Look up the latest confirmed Feishu message ID for one WeChat conversation key.
   *
   * @param conversationKey - Canonical conversation identifier such as `friend:alice`.
   * @returns Platform message ID when a prior inbound batch succeeded.
   */
  getThreadMessageId(conversationKey: string): string | undefined {
    return this.threadsTable.get(conversationKey)?.lastMessageId
  }

  /**
   * Count notices currently waiting for first-attempt Feishu delivery.
   *
   * @returns Number of `'pending'` notices.
   */
  countPending(): number {
    let count = 0
    for (const [, value] of this.table.entries()) {
      if (value.status === 'pending') count += 1
    }
    return count
  }

  /**
   * List all owner notices ordered deterministically by creation timestamp then notice key.
   *
   * @returns Immutable snapshot of stored owner notices.
   */
  listNotices(): readonly OwnerNoticeRecord[] {
    return Array.from(this.table.entries(), ([, value]) => value).sort((a, b) =>
      a.createdAt.localeCompare(b.createdAt) || a.noticeKey.localeCompare(b.noticeKey))
  }

  /**
   * List owner notices still eligible for first-attempt Feishu dispatch.
   *
   * @returns Pending notice records ordered deterministically.
   */
  listPendingNotices(): readonly OwnerNoticeRecord[] {
    return this.listNotices().filter(record => record.status === 'pending')
  }

  /**
   * Transition any previously claimed `'in_flight'` notice to `'unknown'` on startup/reopen
   * so crashes during Feishu HTTP POST never duplicate a potentially delivered notice.
   *
   * @returns Number of claimed notices moved to `'unknown'`.
   */
  async reconcileInFlightNotices(): Promise<number> {
    let transitioned = 0
    for (const [noticeKey, record] of Array.from(this.table.entries())) {
      if (record.status !== 'in_flight') continue
      await this.serialize(noticeKey, async () => {
        const current = this.table.get(noticeKey)
        if (!current || current.status !== 'in_flight') return
        const now = new Date().toISOString()
        await this.table.update(noticeKey, existing => ({
          ...existing,
          status: 'unknown',
          error: 'Previous owner notice attempt ended without durable Feishu confirmation',
          updatedAt: now,
        }))
        transitioned += 1
      })
    }
    return transitioned
  }

  /**
   * Insert a pending owner notice idempotently by `noticeKey`.
   *
   * @param input - Notice metadata and untrusted display body.
   * @returns Existing record if already stored, otherwise the newly persisted pending notice.
   */
  enqueueNotice(input: EnqueueOwnerNoticeInput): Promise<OwnerNoticeRecord> {
    return this.serialize(input.noticeKey, async () => {
      const existing = this.table.get(input.noticeKey)
      if (existing) return existing

      const sanitizedBody = sanitizeNoticeDisplayText(input.body)
      if (!sanitizedBody) {
        throw new Error('Owner notice body cannot be empty after sanitization')
      }
      const noticeId = input.noticeId ?? formatOwnerNoticeId(input.noticeKey)
      const now = new Date().toISOString()
      const candidate = ownerNoticeRecordSchema.parse({
        noticeKey: input.noticeKey,
        noticeId,
        kind: input.kind,
        conversationKey: input.conversationKey ?? null,
        bindingKey: input.bindingKey,
        chatId: input.chatId,
        operatorId: input.operatorId,
        targetTenantId: input.targetTenantId,
        body: sanitizedBody,
        replyMessageId: input.replyMessageId ?? null,
        sourceRef: input.sourceRef,
        untrusted: true,
        authority: 'notice_only',
        status: 'pending',
        attempts: 0,
        messageId: null,
        error: null,
        createdAt: now,
        updatedAt: now,
        sentAt: null,
      })
      await this.table.put(candidate.noticeKey, candidate)
      return candidate
    })
  }

  /**
   * Atomically claim one `'pending'` owner notice as `'in_flight'` before HTTP dispatch.
   *
   * @param noticeKey - Notice key to claim.
   * @returns Updated `'in_flight'` notice record with incremented attempt counter.
   */
  recordAttempt(noticeKey: string): Promise<OwnerNoticeRecord> {
    return this.serialize(noticeKey, async () => {
      const existing = this.table.get(noticeKey)
      if (!existing) throw new Error(`Unknown owner notice: ${noticeKey}`)
      if (existing.status !== 'pending') {
        throw new Error(`Owner notice ${noticeKey} is ${existing.status} and cannot be dispatched again`)
      }
      const now = new Date().toISOString()
      return this.table.update(noticeKey, current => ({
        ...current,
        status: 'in_flight',
        attempts: current.attempts + 1,
        error: null,
        updatedAt: now,
      }))
    })
  }

  /**
   * Persist confirmed Feishu delivery after receiving a non-empty `messageId`
   * and update the shared conversation reply cursor when `conversationKey` is present.
   *
   * @param noticeKey - Claimed notice key.
   * @param expectedAttempt - Attempt sequence recorded by `recordAttempt`.
   * @param messageId - Platform message ID returned by Feishu.
   * @returns Updated `'sent'` notice record.
   */
  recordSent(noticeKey: string, expectedAttempt: number, messageId: string): Promise<OwnerNoticeRecord> {
    if (!messageId.trim()) {
      throw new Error('Confirmed Feishu owner notice requires a non-empty messageId')
    }
    return this.serialize(noticeKey, async () => {
      const existing = this.table.get(noticeKey)
      if (!existing) throw new Error(`Unknown owner notice: ${noticeKey}`)
      if (existing.status !== 'in_flight' || existing.attempts !== expectedAttempt) {
        throw new Error(`Stale owner notice completion for ${noticeKey}`)
      }
      const now = new Date().toISOString()
      const cleanId = messageId.trim()
      const updated = await this.table.update(noticeKey, current => ({
        ...current,
        status: 'sent',
        messageId: cleanId,
        error: null,
        updatedAt: now,
        sentAt: now,
      }))
      if (updated.conversationKey) {
        await this.threadsTable.put(updated.conversationKey, {
          conversationKey: updated.conversationKey,
          lastMessageId: cleanId,
          lastNoticeId: updated.noticeId,
          updatedAt: now,
        })
      }
      return updated
    })
  }

  /**
   * Record either a definite pre-delivery failure (`'failed'`) or post-dispatch ambiguity (`'unknown'`).
   *
   * @param noticeKey - Claimed notice key.
   * @param expectedAttempt - Attempt sequence recorded by `recordAttempt`.
   * @param errorMessage - Bounded diagnostic summary without secrets.
   * @param isUnknown - True when delivery might already have reached Feishu.
   * @returns Updated `'unknown'` or `'failed'` notice record.
   */
  recordFailure(
    noticeKey: string,
    expectedAttempt: number,
    errorMessage: string,
    isUnknown: boolean,
  ): Promise<OwnerNoticeRecord> {
    return this.serialize(noticeKey, async () => {
      const existing = this.table.get(noticeKey)
      if (!existing) throw new Error(`Unknown owner notice: ${noticeKey}`)
      if (existing.status !== 'in_flight' || existing.attempts !== expectedAttempt) {
        throw new Error(`Stale owner notice failure for ${noticeKey}`)
      }
      const now = new Date().toISOString()
      return this.table.update(noticeKey, current => ({
        ...current,
        status: isUnknown ? 'unknown' : 'failed',
        error: sanitizeNoticeDisplayText(errorMessage, 500) || 'Owner notice dispatch failed',
        updatedAt: now,
      }))
    })
  }

  /**
   * Remove expired terminal (`'sent'` or `'failed'`) notices and stale thread cursors
   * while strictly retaining `'pending'`, `'in_flight'`, and `'unknown'` safety records.
   *
   * @param nowMs - Current epoch timestamp in milliseconds.
   * @param retentionDays - Retention horizon in whole days.
   * @returns Number of expired notice and thread records removed.
   */
  async pruneExpiredRecords(nowMs: number, retentionDays: number): Promise<number> {
    const cutoff = nowMs - retentionDays * 86_400_000
    let removed = 0
    for (const [noticeKey, record] of Array.from(this.table.entries())) {
      if (record.status !== 'sent' && record.status !== 'failed') continue
      if (Date.parse(record.createdAt) >= cutoff) continue
      await this.serialize(noticeKey, async () => {
        const current = this.table.get(noticeKey)
        if (!current || (current.status !== 'sent' && current.status !== 'failed')) return
        if (Date.parse(current.createdAt) >= cutoff) return
        await this.table.delete(noticeKey)
        removed += 1
      })
    }
    for (const [conversationKey, thread] of Array.from(this.threadsTable.entries())) {
      if (Date.parse(thread.updatedAt) < cutoff) {
        await this.threadsTable.delete(conversationKey)
        removed += 1
      }
    }
    return removed
  }

  private async serialize<T>(noticeKey: string, work: () => Promise<T>): Promise<T> {
    const previous = this.queues.get(noticeKey)
    const run = (previous ? previous.catch(() => undefined) : Promise.resolve()).then(work)
    this.queues.set(noticeKey, run)
    try {
      return await run
    } finally {
      if (this.queues.get(noticeKey) === run) {
        this.queues.delete(noticeKey)
      }
    }
  }
}

/**
 * Verifies the live authenticated owner route and dispatches owner notices through
 * `@deepseek-ai/dsh-experimental-im-feishu`'s existing Feishu outbound transport.
 */
export class OwnerNoticeDispatcher {
  private readonly storage: OwnerNoticeStorage
  private readonly sender: FeishuNoticeTextSender
  private readonly resolveBinding: (bindingKey: string) => OperatorBindingRecord | undefined
  private readonly expectedBinding: ExpectedOwnerBinding

  /**
   * @param storage - Durable notice storage isolated from ingress deliveries.
   * @param sender - Reused `FeishuTransport` / `FeishuOutboundClient` exposing `sendTextMessage`.
   * @param resolveBinding - Live lookup against `u_im_ingress_operators`.
   * @param expectedBinding - Validated owner operator/tenant/binding assertions.
   */
  constructor(
    storage: OwnerNoticeStorage,
    sender: FeishuNoticeTextSender,
    resolveBinding: (bindingKey: string) => OperatorBindingRecord | undefined,
    expectedBinding: ExpectedOwnerBinding,
  ) {
    this.storage = storage
    this.sender = sender
    this.resolveBinding = resolveBinding
    this.expectedBinding = expectedBinding
  }

  /**
   * Dispatch a single pending notice once, authenticating the current binding first and classifying
   * ambiguous timeouts/network errors/missing message IDs as `'unknown'` rather than retrying.
   *
   * @param noticeKey - Notice key to inspect and deliver if `'pending'`.
   * @returns Terminal or current notice record (`'sent'`, `'failed'`, `'unknown'`, or pre-existing).
   */
  async dispatchNotice(noticeKey: string): Promise<OwnerNoticeRecord> {
    const existing = this.storage.getNotice(noticeKey)
    if (!existing) throw new Error(`Unknown owner notice: ${noticeKey}`)
    if (existing.status !== 'pending') return existing

    const claimed = await this.storage.recordAttempt(noticeKey)
    let route: OperatorBindingRecord
    try {
      const binding = this.resolveBinding(claimed.bindingKey)
      route = resolveAuthenticatedOwnerRoute(binding, this.expectedBinding)
      if (
        claimed.operatorId !== route.operatorId
        || claimed.targetTenantId !== route.targetTenantId
        || claimed.chatId !== route.chatId
      ) {
        throw new InvalidOwnerRouteError('Stored notice destination diverges from authenticated owner route')
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      return this.storage.recordFailure(noticeKey, claimed.attempts, message, false)
    }

    const threadReplyId = claimed.replyMessageId
      ?? (claimed.conversationKey ? this.storage.getThreadMessageId(claimed.conversationKey) : undefined)

    try {
      const request = {
        chatId: route.chatId,
        text: claimed.body,
        ...(threadReplyId ? { replyMessageId: threadReplyId } : {}),
      }
      const result: FeishuSendTextResult = await this.sender.sendTextMessage(request)
      const parsedResult = sentResultSchema.safeParse(result)
      if (!parsedResult.success) {
        return await this.storage.recordFailure(
          noticeKey,
          claimed.attempts,
          'Feishu send response omitted messageId',
          true,
        )
      }
      return await this.storage.recordSent(noticeKey, claimed.attempts, parsedResult.data.messageId)
    } catch (error) {
      const definite = error instanceof FeishuOutboundError
        || error instanceof InvalidOwnerRouteError
        || definiteFailureStatusSchema.safeParse(error).success
      const message = error instanceof Error ? error.message : String(error)
      return this.storage.recordFailure(noticeKey, claimed.attempts, message, !definite)
    }
  }

  /**
   * Dispatch pending notices in deterministic creation order up to an optional per-cycle cap,
   * stopping before claiming any new notice if shutdown has started.
   *
   * @param options - Optional per-cycle dispatch limit and shutdown predicate.
   * @returns Updated notice records in dispatch order.
   */
  async dispatchPending(options?: {
    readonly maxSends?: number | undefined
    readonly shouldStop?: (() => boolean) | undefined
  }): Promise<readonly OwnerNoticeRecord[]> {
    const results: OwnerNoticeRecord[] = []
    const maxSends = options?.maxSends
    for (const notice of this.storage.listPendingNotices()) {
      if (options?.shouldStop?.()) break
      if (maxSends !== undefined && results.length >= maxSends) break
      results.push(await this.dispatchNotice(notice.noticeKey))
    }
    return results
  }
}

/** Configuration options for `OwnerNoticeHandoffService`. */
export interface OwnerNoticeHandoffServiceOptions {
  readonly handoffDirectory: string
  readonly expectedProducerUid?: number | undefined
  readonly retentionDays: number
  readonly maxNoticeBytes: number
  readonly maxPendingNotices: number
  readonly maxIngestPerCycle?: number | undefined
  readonly maxSendsPerCycle?: number | undefined
  readonly shouldStop?: (() => boolean) | undefined
  readonly expectedBinding: ExpectedOwnerBinding
  readonly storage: OwnerNoticeStorage
  readonly dispatcher: OwnerNoticeDispatcher
  readonly resolveBinding: (bindingKey: string) => OperatorBindingRecord | undefined
  readonly onWarning?: ((message: string, error?: unknown) => void) | undefined
}

/** Summary reported by one non-overlapping `OwnerNoticeHandoffService.pollOnce()` cycle. */
export interface OwnerNoticePollSummary {
  readonly ingested: number
  readonly skippedDuplicates: number
  readonly rejected: number
  readonly dispatched: readonly OwnerNoticeRecord[]
}

/**
 * Bounded, non-overlapping ingress handoff poller that ingests sanitized envelopes from a
 * restricted read-only directory without ever touching `wechatRoot` or WeChat secrets.
 */
export class OwnerNoticeHandoffService {
  private readonly options: OwnerNoticeHandoffServiceOptions
  private inFlight: Promise<OwnerNoticePollSummary> | undefined

  /**
   * Construct a handoff poller bound to one validated owner notice storage and dispatcher.
   *
   * @param options - Validated handoff path, retention, route binding, and dispatcher dependencies.
   */
  constructor(options: OwnerNoticeHandoffServiceOptions) {
    assertTrustedHandoffDirectory(options.handoffDirectory, options.expectedProducerUid)
    this.options = options
  }

  /**
   * Scan `handoffDirectory` once using non-overlapping locking, ingest bounded fresh non-expired
   * envelopes, dispatch up to `maxSendsPerCycle` pending notices without claiming new work during
   * shutdown, and prune expired terminal history.
   *
   * @param nowMs - Optional current epoch milliseconds for deterministic testing.
   * @returns Counts of newly queued, deduplicated, rejected, and dispatched notices.
   */
  pollOnce(nowMs = Date.now()): Promise<OwnerNoticePollSummary> {
    if (this.inFlight) return this.inFlight
    if (this.options.shouldStop?.()) {
      return Promise.resolve({ ingested: 0, skippedDuplicates: 0, rejected: 0, dispatched: [] })
    }
    const run = this.doPollOnce(nowMs).finally(() => {
      if (this.inFlight === run) this.inFlight = undefined
    })
    this.inFlight = run
    return run
  }

  private async doPollOnce(nowMs: number): Promise<OwnerNoticePollSummary> {
    assertTrustedHandoffDirectory(this.options.handoffDirectory, this.options.expectedProducerUid)
    const allowedUids = resolveAllowedProducerUids(this.options.expectedProducerUid)
    const retentionMs = this.options.retentionDays * 86_400_000
    const maxIngest = this.options.maxIngestPerCycle ?? 20
    const maxSends = this.options.maxSendsPerCycle ?? 5
    const entries = readdirSync(this.options.handoffDirectory, { withFileTypes: true })
      .sort((a, b) => a.name.localeCompare(b.name))

    let ingested = 0
    let skippedDuplicates = 0
    let rejected = 0

    const bindingTupleSchema = z.tuple([
      z.literal('feishu'),
      z.string().min(1),
      z.string().min(1),
      z.string().min(1),
      z.string().min(1),
    ])
    const fallbackChatId = bindingTupleSchema.parse(JSON.parse(this.options.expectedBinding.bindingKey))[3]

    for (const entry of entries) {
      if (this.options.shouldStop?.() || ingested >= maxIngest) break
      if (!entry.name.endsWith('.json')) continue
      const fullPath = join(this.options.handoffDirectory, entry.name)
      try {
        const lstat = lstatSync(fullPath)
        if (lstat.isSymbolicLink() || !lstat.isFile()) {
          throw new Error(`Unsafe non-regular or symlinked handoff file ${entry.name}`)
        }
        const fd = openSync(fullPath, constants.O_RDONLY | constants.O_NOFOLLOW)
        let rawText: string
        try {
          const stat = fstatSync(fd)
          if (!stat.isFile()) {
            throw new Error(`Handoff descriptor is not a regular file: ${entry.name}`)
          }
          if ((stat.mode & 0o027) !== 0) {
            throw new Error(`Handoff file ${entry.name} has unsafe group-write or world permissions`)
          }
          if (!allowedUids.has(stat.uid)) {
            throw new Error(`Handoff file ${entry.name} owner uid ${stat.uid} is not permitted`)
          }
          if (stat.size <= 0 || stat.size > MAX_HANDOFF_ENVELOPE_BYTES) {
            throw new Error(`Handoff file ${entry.name} size exceeds limit`)
          }
          rawText = readFileSync(fd, 'utf8')
        } finally {
          closeSync(fd)
        }

        const envelope = handoffNoticeEnvelopeSchema.parse(JSON.parse(rawText))
        if (nowMs - envelope.createdAt > retentionMs) {
          continue
        }
        if (this.options.storage.getNotice(envelope.noticeKey)) {
          skippedDuplicates += 1
          continue
        }
        if (this.options.storage.countPending() >= this.options.maxPendingNotices) {
          this.options.onWarning?.('Pending owner notice queue reached maxPendingNotices; deferring new handoff files')
          break
        }
        const liveBinding = this.options.resolveBinding(this.options.expectedBinding.bindingKey)
        const chatId = liveBinding?.chatId ?? this.options.expectedBinding.chatId ?? fallbackChatId
        const truncatedBody = sanitizeNoticeDisplayText(envelope.body, this.options.maxNoticeBytes)
        if (!truncatedBody) {
          throw new Error(`Handoff file ${entry.name} body is empty after sanitization`)
        }
        await this.options.storage.enqueueNotice({
          noticeKey: envelope.noticeKey,
          noticeId: envelope.noticeId,
          kind: envelope.kind,
          conversationKey: envelope.conversationKey,
          bindingKey: this.options.expectedBinding.bindingKey,
          chatId,
          operatorId: this.options.expectedBinding.operatorId,
          targetTenantId: this.options.expectedBinding.targetTenantId,
          body: truncatedBody,
          sourceRef: envelope.sourceRef,
        })
        ingested += 1
      } catch (error) {
        rejected += 1
        this.options.onWarning?.(`Rejected unsafe or malformed handoff file ${entry.name}`, error)
      }
    }

    const dispatched = this.options.shouldStop?.()
      ? []
      : await this.options.dispatcher.dispatchPending({
        maxSends,
        shouldStop: this.options.shouldStop,
      })
    if (!this.options.shouldStop?.()) {
      await this.options.storage.pruneExpiredRecords(nowMs, this.options.retentionDays)
    }
    return { ingested, skippedDuplicates, rejected, dispatched }
  }
}
