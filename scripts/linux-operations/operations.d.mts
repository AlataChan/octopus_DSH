/** Minimal TypeScript declarations for importing `operations.mjs` in integration specs. */

export class Operations {
  constructor(
    policy: Record<string, unknown>,
    policyPath: string,
    execute?: (operation: Record<string, unknown>, policy: Record<string, unknown>) => Promise<Record<string, unknown>>,
    clock?: () => number,
  )
  prepare(input: { readonly kind: string; readonly payload: Record<string, unknown> }): { readonly id: string }
  run(id: string): Promise<{ readonly id: string; readonly state: string }>
  close(): void
}
