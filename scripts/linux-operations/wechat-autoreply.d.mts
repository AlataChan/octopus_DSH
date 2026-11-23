/** Type declarations exposing narrow minimal contracts for `wechat-autoreply.mjs`. */
import type { IsolatedDshAdapterLifecycleState } from './wechat-autoreply-dsh.mjs'
import type { VerifiedAutomaticWechatReceipt } from './wechat-inbox.mjs'

export type WechatAutoReplyMode = 'disabled' | 'draft_only' | 'unattended'
export type WechatAutoReplyChannel = 'friend' | 'group'
export type WechatAutoReplyState =
  | 'claimed'
  | 'draft_ready'
  | 'needs_human'
  | 'needs_review'
  | 'notice_only'
  | 'unsupported'
  | 'expired'
  | 'sent_verified'
  | 'not_sent'
export type WechatAutoReplySendAction = 'none' | 'reserved' | 'queued' | 'needs_review' | 'sent_verified' | 'not_sent'
export type WechatAutoReplyEligibilityDecision =
  | 'claimed'
  | 'draft_only'
  | 'unattended_candidate'
  | 'needs_human'
  | 'needs_review'
  | 'notice_only'
  | 'unsupported'
  | 'blocked'

export interface WechatAutoReplyConversationRule {
  readonly mode: WechatAutoReplyMode
  readonly allowPublishedReportFacts?: boolean | undefined
  readonly allowRegisteredSenderMentions?: boolean | undefined
  readonly allowedTopics?: readonly string[] | undefined
  readonly tone?: string | null | undefined
  readonly cooldownSeconds?: number | null | undefined
  readonly maxRepliesPerWindow?: 1 | 2 | undefined
  readonly maxReplyChars?: number | null | undefined
}

export interface WechatAutoReplyPolicyInput {
  readonly schemaVersion?: 1 | undefined
  readonly enabled: boolean
  readonly policyVersion: number
  readonly globalPaused?: boolean | undefined
  readonly groupCooldownSeconds?: number | undefined
  readonly maxInboundAgeSeconds?: number | undefined
  readonly maxTailAgeSeconds?: number | undefined
  readonly maxReportAgeSeconds?: number | undefined
  readonly claimLeaseSeconds?: number | undefined
  readonly adapterTimeoutMs?: number | undefined
  readonly maxAttempts?: number | undefined
  readonly maxContextMessages?: number | undefined
  readonly maxFriendReplyChars?: number | undefined
  readonly maxGroupReplyChars?: number | undefined
  readonly bannedExpressions?: readonly string[] | undefined
  readonly conversations?: Readonly<Record<string, WechatAutoReplyConversationRule>> | undefined
}

export interface NormalizedWechatAutoReplyPolicy {
  readonly schemaVersion: 1
  readonly enabled: boolean
  readonly policyVersion: number
  readonly globalPaused: boolean
  readonly groupCooldownSeconds: number
  readonly maxInboundAgeSeconds: number
  readonly maxTailAgeSeconds: number
  readonly maxReportAgeSeconds: number
  readonly claimLeaseSeconds: number
  readonly adapterTimeoutMs: number
  readonly maxAttempts: number
  readonly maxContextMessages: number
  readonly maxFriendReplyChars: number
  readonly maxGroupReplyChars: number
  readonly bannedExpressions: readonly string[]
  readonly conversations: Readonly<Record<string, WechatAutoReplyConversationRule>>
}

export interface WechatAutoReplyAuthorProof {
  readonly accountSha256: string
  readonly targetProof: string
  readonly evidenceSha256: string
  readonly verifiedBy: 'left_bubble' | 'avatar_anchor' | 'card_anchor' | 'native_copy'
}

export interface WechatAutoReplyMentionSelfProof {
  readonly accountSha256: string
  readonly evidenceSha256: string
  readonly verifiedBy: 'anchor' | 'native_copy' | 'mention_chip' | 'registered_sender'
}

export interface WechatAutoReplyQuoteSelfProof {
  readonly accountSha256: string
  readonly evidenceSha256: string
  readonly quotedSourceRef: string
  readonly verifiedBy: 'anchor' | 'native_copy' | 'quoted_card'
}

export interface WechatAutoReplyEventSignal {
  readonly accountSha256: string
  readonly targetProof: string
  readonly evidenceSha256: string
  readonly senderRole: 'peer' | 'self' | 'robot' | 'unknown'
  readonly authorProof?: WechatAutoReplyAuthorProof | undefined
  readonly uncertain?: boolean | undefined
  readonly groupTrigger?: 'mention_self' | 'quote_self' | 'at_all' | 'mention_other' | 'ordinary' | 'unknown' | undefined
  readonly mentionSelfProof?: WechatAutoReplyMentionSelfProof | undefined
  readonly quoteSelfProof?: WechatAutoReplyQuoteSelfProof | undefined
}

export interface WechatAutoReplyTailBubbleItem {
  readonly senderRole: 'peer' | 'self' | 'unknown'
  readonly textSha256: string
  readonly sourceRef: string
  readonly sourceTime: string
}

export interface WechatAutoReplyTailSnapshot {
  readonly accountSha256: string
  readonly conversationKey: string
  readonly targetProof: string
  readonly verified: boolean
  readonly evidenceSha256: string
  readonly evidencePath?: string | null | undefined
  readonly tailObservedAt: number
  readonly lastSourceRef: string
  readonly tailItems: readonly WechatAutoReplyTailBubbleItem[]
  readonly tailDigest: string
  readonly ownReplyObservedAfterTrigger: boolean
  readonly peerFollowUpObservedAfterTrigger: boolean
  readonly uncertain?: boolean | undefined
}

export type WechatReportScope = 'daily_structure' | 'implied_volatility' | 'divergence_assumptions'

export interface WechatAutoReplyPublishedFact {
  readonly factId: string
  readonly scope: WechatReportScope
  readonly digest: string
  readonly text: string
}

export interface WechatAutoReplyPublishedReportBundle {
  readonly schemaVersion: 1
  readonly providerId: string
  readonly publishedOnly: true
  readonly report: {
    readonly reportId: string
    readonly reportVersion: number
    readonly reportDigest: string
    readonly publishedAtMs: number
    readonly validUntilMs: number
    readonly accountSha256?: string | null | undefined
    readonly authorizedConversations: readonly string[]
    readonly allowedScopes: readonly WechatReportScope[]
    readonly facts: readonly WechatAutoReplyPublishedFact[]
  }
}

export {
  DEFAULT_AUTOREPLY_API_KEY_ENV,
  ISOLATED_DSH_ENTRY_PACKAGE,
  ISOLATED_DSH_FORBIDDEN_BUNDLES,
  ISOLATED_DSH_FORBIDDEN_SERVICES,
  ISOLATED_DSH_OPTIONAL_TRANSPORT_PLUGIN,
  ISOLATED_DSH_PLUGINS,
  RESTRICTED_AUTOREPLY_PROFILE_SPEC,
  WECHAT_AUTOREPLY_OUTPUT_JSON_SCHEMA,
  createIsolatedDshAutoReplyAdapter,
  inspectIsolatedDshContext,
  loadDshRuntimeModules,
  resolveDshInstallAnchor,
} from './wechat-autoreply-dsh.mjs'
export type {
  IsolatedDshAdapterLifecycleState,
  IsolatedDshAdapterRegistrationContext,
  IsolatedDshAutoReplyAdapter,
  IsolatedDshAutoReplyAdapterOptions,
  IsolatedDshCleanupAudit,
  IsolatedDshContextAudit,
  IsolatedDshFinishReason,
  IsolatedDshLifecycleStatus,
  IsolatedDshPromptEnvelope,
  IsolatedDshRunAudit,
} from './wechat-autoreply-dsh.mjs'

export interface WechatAutoReplyReportPublicationProvider {
  getVerifiedReportFacts(query: {
    readonly conversationKey: string
    readonly channel: WechatAutoReplyChannel
    readonly accountSha256: string
    readonly targetProof: string
    readonly policyVersion: number
    readonly nowMs: number
    readonly signal?: AbortSignal | undefined
  }): Promise<WechatAutoReplyPublishedReportBundle> | WechatAutoReplyPublishedReportBundle
}

export interface RestrictedAutoReplyPromptPayload {
  readonly profileId: string
  readonly baseBundle: string
  readonly toolsAllowed: readonly string[]
  readonly systemPrompt: string
  readonly userInputJson: string
  readonly envelope: {
    readonly schemaVersion: 1
    readonly taskId: string
    readonly policyVersion: number
    readonly channel: WechatAutoReplyChannel
    readonly requestedMode: 'draft_only' | 'unattended'
    readonly maxReplyChars: number
    readonly styleConstraints: {
      readonly tone: string
      readonly allowedTopics: readonly string[]
      readonly bannedExpressions: readonly string[]
    }
    readonly publishedReportFacts: readonly Record<string, unknown>[]
    readonly untrustedConversationContext: readonly Record<string, unknown>[]
    readonly untrustedInboundMessages: readonly Record<string, unknown>[]
  }
}

export interface WechatAutoReplyProfileAdapter {
  generate(
    request: RestrictedAutoReplyPromptPayload,
    options?: { readonly signal: AbortSignal },
  ): Promise<unknown>
  readonly getLifecycleState?: (() => IsolatedDshAdapterLifecycleState) | undefined
}

export interface WechatAutoReplyBackgroundWorkState {
  readonly state: 'ready' | 'degraded' | 'failed' | 'closed'
  readonly inFlightCount: number
  readonly backgroundWorkCount: number
  readonly orphanedCount: number
  readonly orphanedAdapterCount: number
  readonly retiredOrphanedCount: number
  readonly maxOrphanedAdapterTasks: number
  readonly maxOrphanedBackgroundTasks: number
  readonly maxRetiredOrphanedTasks: number
  readonly lastFailureReason: string | null
}

export interface WechatAutoReplyFrozenTarget {
  readonly accountSha256: string
  readonly contactId: string
  readonly contactType: WechatAutoReplyChannel
  readonly alias: string
  readonly displayName: string
  readonly targetProof: string
  readonly recipientId?: string | undefined
  readonly groupId?: string | undefined
  readonly groupName?: string | undefined
}

export interface WechatAutoReplyTaskRecord {
  readonly batchId: string
  readonly taskId: string
  readonly noticeId: string
  readonly conversationKey: string
  readonly channel: WechatAutoReplyChannel | 'unsupported'
  readonly requestedMode: WechatAutoReplyMode
  readonly state: WechatAutoReplyState
  readonly eligibilityDecision: WechatAutoReplyEligibilityDecision
  readonly sendAction: WechatAutoReplySendAction
  readonly policyVersion: number
  readonly attempts: number
  readonly maxAttempts: number
  readonly eventIds: readonly string[]
  readonly frozenTarget: WechatAutoReplyFrozenTarget | null
  readonly contextTailFingerprint: string
  readonly providerRunId: string | null
  readonly draftText: string | null
  readonly reason: string | null
  readonly claimedAt: number | null
  readonly leaseExpiresAt: number | null
  readonly batchExpiresAt: number | null
  readonly decidedAt: number | null
  readonly duplicate: boolean
  readonly abortedOnClose?: boolean | undefined
}

export interface WechatAutoReplyAuditTransition {
  readonly id: number
  readonly batch_id: string
  readonly task_id: string
  readonly from_state: string | null
  readonly to_state: string
  readonly reason: string | null
  readonly provider_run_id: string | null
  readonly recorded_at: number
}

export interface WechatAutoReplyEvaluateOptions {
  readonly eventSignals?: Readonly<Record<string, WechatAutoReplyEventSignal>> | null | undefined
  readonly tailState?: WechatAutoReplyTailSnapshot | null | undefined
  readonly reportPublicationProvider?: WechatAutoReplyReportPublicationProvider | null | undefined
  readonly adapter?: WechatAutoReplyProfileAdapter | null | undefined
  readonly verifyTailAfterGeneration?:
    | ((context: {
      readonly conversationKey: string
      readonly channel: WechatAutoReplyChannel
      readonly accountSha256: string
      readonly targetProof: string
      readonly batchId: string
      readonly taskId: string
      readonly signal: AbortSignal
    }) =>
      | Promise<boolean | { readonly ok: boolean; readonly reason?: string | undefined }>
      | boolean
      | { readonly ok: boolean; readonly reason?: string | undefined })
    | undefined
}

export const WECHAT_AUTOREPLY_SCHEMA_VERSION: 1
export const AUTOREPLY_CHANNELS: readonly ['friend', 'group']
export const AUTOREPLY_CONVERSATION_MODES: readonly ['disabled', 'draft_only', 'unattended']
export const AUTOREPLY_TASK_STATES: readonly WechatAutoReplyState[]
export const DEFAULT_BANNED_EXPRESSIONS: readonly string[]
export const DEFAULT_WECHAT_AUTOREPLY_POLICY: NormalizedWechatAutoReplyPolicy

export function validateAutoReplyPolicy(value: WechatAutoReplyPolicyInput): WechatAutoReplyPolicyInput
export function resolveAutoReplyPolicy(rawPolicy?: WechatAutoReplyPolicyInput | null): NormalizedWechatAutoReplyPolicy

export function classifyInboundRiskAndFacts(params: {
  readonly events: readonly { readonly text?: string | undefined; readonly quotedText?: string | null | undefined }[]
  readonly conversationRule?: WechatAutoReplyConversationRule | null | undefined
  readonly verifiedReportBundle?: {
    readonly reportId: string
    readonly reportVersion: number
    readonly reportDigest: string
    readonly facts: readonly Record<string, unknown>[]
  } | null | undefined
  readonly bannedExpressions?: readonly string[] | undefined
}): {
  readonly route: 'allow' | 'needs_human'
  readonly category: string | null
  readonly reason: string | null
}

export function buildRestrictedProfilePromptPayload(params: {
  readonly taskId: string
  readonly policyVersion: number
  readonly channel: WechatAutoReplyChannel
  readonly requestedMode: 'draft_only' | 'unattended'
  readonly maxReplyChars: number
  readonly conversationRule: WechatAutoReplyConversationRule
  readonly contextRows: readonly { readonly observed_at: number; readonly summary: string }[]
  readonly events: readonly {
    readonly eventId: string
    readonly observedAt: number
    readonly sourceTime?: string | undefined
    readonly senderName?: string | undefined
    readonly text?: string | undefined
    readonly quotedSender?: string | null | undefined
    readonly quotedText?: string | null | undefined
  }[]
  readonly publishedReportFacts?: readonly Record<string, unknown>[] | undefined
  readonly bannedExpressions?: readonly string[] | undefined
}): RestrictedAutoReplyPromptPayload

export function parseRestrictedAdapterResponse(response: unknown): {
  readonly providerRunId: string
  readonly sessionId: string | null
  readonly parsedOutput: unknown
  readonly toolCalls: readonly unknown[]
}

export function validateRestrictedProfileOutput(params: {
  readonly parsedOutput: unknown
  readonly toolCalls?: readonly unknown[] | undefined
  readonly maxReplyChars: number
  readonly bannedExpressions?: readonly string[] | undefined
}):
  | { readonly ok: true; readonly decision: 'answer'; readonly answer: string }
  | { readonly ok: true; readonly decision: 'needs_human'; readonly reason: string }
  | { readonly ok: false; readonly reason: string }

export interface WechatAutoReplyOwnerNoticeProjection {
  readonly duplicate: boolean
  readonly noticeKey: string
  readonly ownerNoticeId: string
  readonly originNoticeId: string
  readonly handoffFile: string
  readonly state: WechatAutoReplyState
  readonly projectedAt: number
}

export interface WechatAutoReplyPumpEvaluatedItem {
  readonly batchId: string
  readonly noticeId: string
  readonly state: WechatAutoReplyState
  readonly eligibilityDecision: WechatAutoReplyEligibilityDecision
  readonly sendAction: WechatAutoReplySendAction
  readonly duplicate: boolean
  readonly ownerNoticeProjected: boolean
}

export interface WechatAutoReplyPumpCycleSummary {
  readonly skipped: boolean
  readonly reason: string | null
  readonly evaluated: readonly WechatAutoReplyPumpEvaluatedItem[]
  readonly ownerNoticesProjected: number
}

export interface WechatAutoReplyPump {
  readonly store: WechatAutoReplyStore
  readonly adapter: WechatAutoReplyProfileAdapter
  trigger(): Promise<WechatAutoReplyPumpCycleSummary>
  pumpOnce(): Promise<WechatAutoReplyPumpCycleSummary>
  isRunning(): boolean
  whenIdle(): Promise<void>
  stop(): void
}

export class WechatAutoReplyStore {
  constructor(
    policy: Readonly<Record<string, unknown>>,
    autoReplyPolicy?: WechatAutoReplyPolicyInput | null,
    options?: {
      readonly policyPath?: string | null | undefined
      readonly expectedTransportKey?: string | null | undefined
      readonly defaultInstallAnchor?: string | null | undefined
      readonly clock?: (() => number) | undefined
      readonly statUid?: ((stat: { readonly uid: number }, path: string) => number) | undefined
      readonly verifyOperatorBinding?: boolean | undefined
      readonly verifyEvidenceFiles?: boolean | undefined
      readonly dispatchEnabled?: boolean | undefined
      readonly cooperativeShutdownMs?: number | undefined
      readonly maxOrphanedAdapterTasks?: number | undefined
      readonly maxOrphanedBackgroundTasks?: number | undefined
      readonly maxRetiredOrphanedTasks?: number | undefined
    },
  )
  syncPolicy(
    nextPolicy: Readonly<Record<string, unknown>>,
    nextAutoReplyPolicy?: WechatAutoReplyPolicyInput | null,
  ): NormalizedWechatAutoReplyPolicy
  setRuntimeGlobalPause(paused: boolean, nowMs?: number): boolean
  isRuntimeGlobalPaused(): boolean
  recoverInterruptedClaims(nowMs?: number): readonly {
    readonly batchId: string
    readonly taskId: string
    readonly state: WechatAutoReplyState
    readonly reason: string
  }[]
  getBackgroundWorkState(): WechatAutoReplyBackgroundWorkState
  failAdapterExecution(reason?: string): WechatAutoReplyBackgroundWorkState
  replaceAdapterState(): WechatAutoReplyBackgroundWorkState
  whenBackgroundIdle(): Promise<WechatAutoReplyBackgroundWorkState>
  getTaskByBatchId(batchId: string): WechatAutoReplyTaskRecord | null
  settleAutomaticReceipt(receipt: VerifiedAutomaticWechatReceipt): WechatAutoReplyTaskRecord | null
  evaluateSealedBatch(
    batchId: string,
    options?: WechatAutoReplyEvaluateOptions,
  ): Promise<WechatAutoReplyTaskRecord>
  listAuditTransitions(): readonly WechatAutoReplyAuditTransition[]
  writeDecisionArtifact(targetPath: string, record: WechatAutoReplyTaskRecord): string
  projectOutcomeOwnerNotice(
    record: WechatAutoReplyTaskRecord,
    options?: { readonly projectOwnerNotices?: boolean | undefined },
  ): WechatAutoReplyOwnerNoticeProjection | null
  flushPendingOwnerNotices(options?: { readonly maxCount?: number | undefined }): number
  getOwnerNoticeProjection(batchId: string): WechatAutoReplyOwnerNoticeProjection | null
  listOwnerNoticeProjections(): readonly Record<string, string | number>[]
  close(): void
}

export function canonicalTransportKey(
  transport?: Readonly<Record<string, unknown>> | null,
  defaultInstallAnchor?: string | null,
): string | null

export function checkOperatorBindingActive(policy: Readonly<Record<string, unknown>>): {
  readonly ok: boolean
  readonly reason: string | null
}

export function createWechatAutoReplyPump(options: {
  readonly policy: Readonly<Record<string, unknown>>
  readonly policyPath?: string | null | undefined
  readonly projector: {
    listSealedBatches(options?: {
      readonly kinds?: readonly ('friend' | 'group' | 'moments')[] | undefined
      readonly limit?: number | undefined
      readonly after?: { readonly sealedAt: number; readonly batchId: string } | null | undefined
      readonly order?: 'asc' | 'desc' | undefined
    }): readonly Record<string, unknown>[]
  }
  readonly store?: WechatAutoReplyStore | null | undefined
  readonly adapter?: WechatAutoReplyProfileAdapter | null | undefined
  readonly adapterFactory?: ((context: {
    readonly policy: Readonly<Record<string, unknown>>
    readonly autoReplyPolicy: Readonly<Record<string, unknown>>
    readonly transport: Readonly<Record<string, unknown>>
    readonly installAnchor: string
  }) => WechatAutoReplyProfileAdapter) | null | undefined
  readonly resolveEvaluationInputs?: ((context: {
    readonly batch: Record<string, unknown>
    readonly policy: Readonly<Record<string, unknown>>
    readonly autoReplyPolicy: Readonly<Record<string, unknown>>
    readonly nowMs: number
  }) => Promise<WechatAutoReplyEvaluateOptions | null | undefined> | WechatAutoReplyEvaluateOptions | null | undefined) | null | undefined
  readonly dispatchUnattended?: ((context: {
    readonly decision: WechatAutoReplyTaskRecord
    readonly source: {
      readonly source_ref: string
      readonly text_sha256: string
      readonly evidence_sha256: string
      readonly observed_at: number
    }
    readonly sourceText: string
    readonly policy: Readonly<Record<string, unknown>>
    readonly clock: () => number
    readonly dispatchBy: number
  }) => boolean | { readonly deadlineAt: number } | Promise<boolean | { readonly deadlineAt: number }>) | null | undefined
  readonly clock?: (() => number) | undefined
  readonly statUid?: ((stat: { readonly uid: number }, path: string) => number) | undefined
  readonly verifyOperatorBinding?: boolean | undefined
  readonly verifyEvidenceFiles?: boolean | undefined
  readonly cooperativeShutdownMs?: number | undefined
  readonly maxOrphanedAdapterTasks?: number | undefined
  readonly maxOrphanedBackgroundTasks?: number | undefined
  readonly maxRetiredOrphanedTasks?: number | undefined
  readonly onError?: ((message: string) => void) | undefined
}): WechatAutoReplyPump | null
