/** Type declarations for importing `wechat-inbox.mjs` from TypeScript integration suites. */

export interface WechatExchangeProjectResult {
  readonly enabled: boolean
  readonly inbound: {
    readonly enabled: boolean
    readonly accepted: number
    readonly duplicates: number
    readonly rejected: readonly { readonly file: string; readonly reason: string }[]
  } | null
  readonly sealed: readonly {
    readonly noticeKey: string
    readonly noticeId: string
    readonly conversationKey: string
    readonly eventCount: number
    readonly handoffFile: string
  }[]
  readonly receipts: {
    readonly enabled: boolean
    readonly projected: number
    readonly recovered: number
    readonly skippedTransient: number
    readonly duplicates: number
    readonly rejected: readonly { readonly file: string; readonly reason: string }[]
  } | null
  readonly handoffsRecovered?: number | undefined
}

export interface SealedInboundBatchSummary {
  readonly batchId: string
  readonly noticeId: string
  readonly conversationKey: string
  readonly conversationKind: 'friend' | 'group' | 'moments'
  readonly conversationName: string
  readonly eventCount: number
  readonly sealedAt: number
  readonly accountSha256: string | null
  readonly contactId: string | null
  readonly targetProof: string | null
  readonly replyEligible: boolean
}

export class WechatExchangeProjector {
  constructor(
    policy: Record<string, unknown>,
    options?: {
      readonly clock?: (() => number) | undefined
      readonly statUid?: ((stat: { readonly uid: number }, path: string) => number) | undefined
    },
  )
  isEnabled(): boolean
  flushPendingHandoffs(options?: {
    readonly kind?: 'inbound_alert' | 'outbound_receipt' | null | undefined
    readonly maxCount?: number | undefined
    readonly continueOnError?: boolean | undefined
  }): { readonly enabled: boolean; readonly written: number; readonly failed: number }
  projectOnce(): WechatExchangeProjectResult
  listSealedBatches(options?: {
    readonly kinds?: readonly ('friend' | 'group' | 'moments')[] | undefined
    readonly limit?: number | undefined
    readonly after?: { readonly sealedAt: number; readonly batchId: string } | null | undefined
    readonly order?: 'asc' | 'desc' | undefined
  }): readonly SealedInboundBatchSummary[]
  listProjectedNotices(): readonly Record<string, string | number | null>[]
  listRejectedInputs(): readonly Record<string, string | number | null>[]
  close(): void
}

export function formatNoticeId(noticeKey: string): string

export function sanitizeUntrustedNoticeText(
  rawText: string,
  options?: { readonly singleLine?: boolean | undefined; readonly maxBytes?: number | undefined },
): string

export function writeTrustedHandoffEnvelope(
  policy: Record<string, unknown>,
  envelope: Record<string, unknown>,
  options?: { readonly statUid?: ((stat: { readonly uid: number }, path: string) => number) | undefined },
): string

export function resolveManualReplyViewPath(policy: Record<string, unknown>): string | null

export function readTrustedManualReplyView(
  policy: Record<string, unknown>,
  options?: { readonly statUid?: ((stat: { readonly uid: number }, path: string) => number) | undefined },
): {
  readonly schema_version: number
  readonly account_sha256: string
  readonly enabled: boolean
  readonly manual_reply_enabled: boolean
  readonly max_age_seconds: number
  readonly paused: boolean
  readonly lease_expires_at: number | null
} | null

export function resolveActionableInboundNotice(
  policy: Record<string, unknown>,
  noticeId: string,
  nowMs?: number,
  options?: { readonly statUid?: ((stat: { readonly uid: number }, path: string) => number) | undefined },
): {
  readonly notice: Record<string, string | number | null>
  readonly batch: Record<string, string | number | null>
  readonly contact: Record<string, unknown>
  readonly registry: Record<string, unknown>
  readonly noticeDeadline: number
}

export function readCapturedWechatInbox(
  policy: Record<string, unknown>,
  scope: { readonly kind: 'moments' } | { readonly kind: 'chat'; readonly target: string },
  nowMs?: number,
): {
  readonly source: 'captured_wechat_inbox'
  readonly scope: 'latest_captured_moments_activity' | 'friend' | 'group'
  readonly conversationName: string
  readonly note: string
  readonly untrusted: true
  readonly events: readonly {
    readonly sender: string
    readonly text: string
    readonly sourceTime: string
    readonly observedAt: number
    readonly needsReview: boolean
  }[]
}
