/** Pinned dedicated env variable name for isolated WeChat auto-reply transport auth. */
export const DEFAULT_AUTOREPLY_API_KEY_ENV: 'OCTOPUS_WECHAT_AUTOREPLY_API_KEY'

/** Owning DSH boot entry package name. */
export const ISOLATED_DSH_ENTRY_PACKAGE: '@deepseek-ai/dsh-app-boot'

/** Minimal plugins mounted inside the isolated WeChat auto-reply container. */
export const ISOLATED_DSH_PLUGINS: readonly ['@deepseek-ai/dsh-llm', '@deepseek-ai/dsh-system-prompt']

/** Optional transport plugin mounted only when `transport: 'deepseek-api-key'` is selected. */
export const ISOLATED_DSH_OPTIONAL_TRANSPORT_PLUGIN: '@deepseek-ai/dsh-llm-deepseek-api-key'

/** Shared DSH bundles explicitly forbidden in the isolated auto-reply composition. */
export const ISOLATED_DSH_FORBIDDEN_BUNDLES: readonly [
  '@deepseek-ai/dsh-base',
  '@deepseek-ai/dsh-headless',
  '@deepseek-ai/dsh-sdk-minimal',
]

/** Service names verified absent from the isolated Cordis root context. */
export const ISOLATED_DSH_FORBIDDEN_SERVICES: readonly string[]

/** Strict structured JSON output schema enforced for `answer` / `needs_human` decisions. */
export const WECHAT_AUTOREPLY_OUTPUT_JSON_SCHEMA: Readonly<Record<string, unknown>>

/** Fixed system prompt policy string injected into the isolated `SystemPrompt` section. */
export const SYSTEM_PROMPT_POLICY: string

/** Pinned isolated DSH composition contract for WeChat auto-reply draft generation. */
export const RESTRICTED_AUTOREPLY_PROFILE_SPEC: {
  readonly profileId: string
  readonly entryPackage: string
  readonly baseBundle: string
  readonly runtimeMode: 'dsh-app-boot-isolated'
  readonly plugins: readonly string[]
  readonly optionalTransportPlugin: string
  readonly toolsAllowed: readonly string[]
  readonly toolRegistryPresent: false
  readonly credentialsStorePresent: false
  readonly sessionPersistencePresent: false
  readonly forbiddenBundles: readonly string[]
  readonly forbiddenServices: readonly string[]
  readonly forbiddenCapabilities: readonly string[]
  readonly systemPromptPolicy: string
  readonly outputFields: {
    readonly required: readonly string[]
    readonly optional: readonly string[]
    readonly additionalProperties: false
  }
  readonly outputSchema: Readonly<Record<string, unknown>>
}

/** Audit snapshot describing verified service and plugin isolation of a booted DSH container. */
export interface IsolatedDshContextAudit {
  readonly toolRegistryPresent: false
  readonly credentialsStorePresent: false
  readonly sessionPersistencePresent: false
  readonly absentServicesVerified: true
  readonly mountedPlugins: readonly string[]
  readonly presentServices: {
    readonly llm: boolean
    readonly systemPrompt: boolean
    readonly tools: boolean
    readonly credentials: boolean
    readonly sessions: boolean
    readonly agents: boolean
    readonly fs: boolean
    readonly mcp: boolean
  }
}

/** Per-call runtime audit emitted alongside isolated DSH generation output. */
export interface IsolatedDshRunAudit extends IsolatedDshContextAudit {
  readonly bootDurationMs: number
  readonly assembledToolsCount: number
  readonly assembledContextsCount: number
  readonly renderedSystemPromptLength: number
}

/** Cleanup proof snapshot reported for one isolated DSH run without claiming unproven provider disposal. */
export interface IsolatedDshCleanupAudit {
  readonly bootSettled: boolean
  readonly registrationSettled: boolean
  readonly streamCompleted: boolean
  readonly streamReturned: boolean
  readonly streamSettled: boolean
  readonly adapterDisposed: boolean
  readonly contextDisposed: boolean
  readonly providerDisposed: boolean
  readonly timedOut: boolean
}

/** Lifecycle status of an isolated DSH auto-reply adapter instance. */
export type IsolatedDshLifecycleStatus = 'ready' | 'failed' | 'disposed'

/** Snapshot of active/orphaned runs and cleanup state on {@link IsolatedDshAutoReplyAdapter}. */
export interface IsolatedDshAdapterLifecycleState {
  readonly state: IsolatedDshLifecycleStatus
  readonly generation: number
  readonly activeRuns: number
  readonly orphanedRuns: number
  readonly totalOrphanedRuns: number
  readonly maxOrphanedRuns: number
  readonly maxTotalOrphanedRuns: number
  readonly lastFailureReason: string | null
  readonly lastCleanup: IsolatedDshCleanupAudit | null
}

/** Typed finish reason preserved from the isolated LLM stream. */
export interface IsolatedDshFinishReason {
  readonly kind: string
  readonly failure?: {
    readonly message?: string | undefined
    readonly code?: string | undefined
  } | undefined
}

/** Prompt envelope accepted by the isolated DSH adapter. */
export interface IsolatedDshPromptEnvelope {
  readonly profileId: string
  readonly toolsAllowed: readonly string[]
  readonly systemPrompt: string
  readonly userInputJson: string
}

/** Context passed to an explicit adapter registration callback in `'adapter'` transport mode. */
export interface IsolatedDshAdapterRegistrationContext {
  readonly ctx: unknown
  readonly provider: string
  readonly llm: unknown
  readonly LlmAdapter: unknown
  readonly LlmError: unknown
  readonly signal: AbortSignal
}

/** Options for {@link createIsolatedDshAutoReplyAdapter}. */
export interface IsolatedDshAutoReplyAdapterOptions {
  readonly transport?: 'adapter' | 'deepseek-api-key' | undefined
  readonly provider?: string | undefined
  readonly model?: string | undefined
  readonly maxTokens?: number | undefined
  readonly maxRawOutputBytes?: number | undefined
  readonly maxUserInputBytes?: number | undefined
  readonly reasoningEffort?: string | undefined
  readonly shutdownTimeoutMs?: number | undefined
  readonly maxOrphanedRuns?: number | undefined
  readonly maxTotalOrphanedRuns?: number | undefined
  readonly apiKeyEnv?: string | undefined
  readonly allowSharedDeepSeekApiKey?: boolean | undefined
  readonly baseURL?: string | undefined
  readonly allowLoopbackHttp?: boolean | undefined
  readonly installAnchor?: string | null | undefined
  readonly env?: Readonly<Record<string, string | undefined>> | undefined
  readonly registerAdapter?:
    | ((context: IsolatedDshAdapterRegistrationContext) => Promise<(() => void) | void> | (() => void) | void)
    | undefined
  readonly onAudit?: ((audit: IsolatedDshRunAudit) => void) | undefined
  readonly onCleanup?: ((cleanup: IsolatedDshCleanupAudit) => void) | undefined
}

/** Isolated single-turn DSH adapter with bounded cancellation, fail-closed orphan guards, and safe replace/dispose controls. */
export interface IsolatedDshAutoReplyAdapter {
  readonly spec: typeof RESTRICTED_AUTOREPLY_PROFILE_SPEC
  generate(
    request: IsolatedDshPromptEnvelope,
    callOptions?: { readonly signal?: AbortSignal | undefined },
  ): Promise<{
    readonly providerRunId: string
    readonly sessionId: null
    readonly rawOutput: string
    readonly toolCalls: readonly Record<string, unknown>[]
    readonly finishReason: IsolatedDshFinishReason
    readonly runtimeAudit: IsolatedDshRunAudit
  }>
  getLifecycleState(): IsolatedDshAdapterLifecycleState
  fail(reason?: string): IsolatedDshAdapterLifecycleState
  replace(nextOptions?: IsolatedDshAutoReplyAdapterOptions): IsolatedDshAdapterLifecycleState
  dispose(): Promise<IsolatedDshAdapterLifecycleState>
  whenIdle(): Promise<IsolatedDshAdapterLifecycleState>
}

/**
 * Resolve the DSH installation `package.json` anchor used to load built runtime packages.
 * @param installAnchor - optional absolute path to a DSH package.json anchor.
 * @returns verified absolute anchor path.
 */
export function resolveDshInstallAnchor(installAnchor?: string | null): string

/**
 * Resolve and import built DSH runtime modules (`app-boot`, `llm`, `system-prompt`, `launch-environment`).
 * @param options - optional installation anchor and transport selection.
 * @returns resolved module namespaces and bare-module base URL.
 */
export function loadDshRuntimeModules(options?: {
  readonly installAnchor?: string | null | undefined
  readonly transport?: 'adapter' | 'deepseek-api-key' | undefined
}): Promise<{
  readonly installAnchor: string
  readonly bareModuleBaseUrl: string
  readonly appBoot: Record<string, unknown>
  readonly llm: Record<string, unknown>
  readonly systemPrompt: Record<string, unknown>
  readonly launchEnv: Record<string, unknown>
}>

/**
 * Verify that a booted Cordis container exposes `llm` and `systemPrompt` without any forbidden service or plugin.
 * @param ctx - booted Cordis root context.
 * @param options - expected mounted plugin allowlist.
 * @returns immutable audit summary of present and verified-absent services.
 */
export function inspectIsolatedDshContext(
  ctx: unknown,
  options?: { readonly expectedPluginNames?: readonly string[] | undefined },
): IsolatedDshContextAudit

/**
 * Create a single-turn, tool-free DSH generation adapter executing through `@deepseek-ai/dsh-app-boot`.
 * @param options - transport, model, credential-env allowlist, bounded shutdown, and offline adapter registration options.
 * @returns restricted adapter compatible with `WechatAutoReplyStore.evaluateSealedBatch`.
 */
export function createIsolatedDshAutoReplyAdapter(options?: IsolatedDshAutoReplyAdapterOptions): IsolatedDshAutoReplyAdapter
