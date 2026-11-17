/** Render the canonical recorded Team session through the real Web Host and Client bundle. */
import type {} from '@deepseek-ai/dsh-api-session-controller'
import type {} from '@deepseek-ai/dsh-experimental-assistant-registry'
import { SessionId } from '@deepseek-ai/dsh-session'
import { mkdtemp, readFile } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { fileURLToPath } from 'node:url'
import { chromium, type Browser, type Page } from 'playwright'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import {
  assertFixtureInventory, captureStableAria, compareOrRefreshGolden,
  launchWebScaffold, realizeSeedFixture, seedSession, selectedSessionFixture, watchConsole,
  type WebScaffold,
} from './scaffold.ts'
import { newEnglishPage } from './support.ts'

const RECORDING = fileURLToPath(new URL('../../../snapshots/session/team-orchestration', import.meta.url))
const EXPECTED = fileURLToPath(new URL('../../../snapshots/web/team-room', import.meta.url))
const OVERLAY = fileURLToPath(new URL('./agent-team-room.overlay.yml', import.meta.url))
const ANCHOR = fileURLToPath(new URL('../../../packages/experimental/agent-team-profile/package.json', import.meta.url))
const SESSION = 'room-recorded-team'

describe('recorded Team Room', () => {
  let screenshotDir: string
  let scaffold: WebScaffold
  let browser: Browser
  let page: Page
  let tripwire: ReturnType<typeof watchConsole>

  beforeAll(async () => {
    screenshotDir = await mkdtemp(join(tmpdir(), 'dsh-team-room-qa-'))
    scaffold = await launchWebScaffold({ extraOverlayPath: OVERLAY, extraInstallAnchors: [ANCHOR, fileURLToPath(new URL('../../../packages/experimental/client-ui-assistants/package.json', import.meta.url))] })
    const parent = await selectedSessionFixture(`${RECORDING}/session.jsonl`)
    const parentId = await seedSession(scaffold, await readFile(parent, 'utf8'), SESSION)
    for (const ordinal of [1, 2]) {
      const child = await selectedSessionFixture(`${RECORDING}/session.${ordinal}.jsonl`)
      const text = realizeSeedFixture(scaffold, await readFile(child, 'utf8'), SESSION)
      await seedSession(scaffold, text, `${SESSION}-child-${ordinal + 1}`)
    }
    await scaffold.ctx.sessionController.rename({ sessionId: parentId, title: 'Recorded Team Room' })
    browser = await chromium.launch()
    page = await newEnglishPage(browser)
    await page.setViewportSize({ width: 1440, height: 1024 })
    tripwire = watchConsole(page)
    await page.goto(scaffold.authenticatedUrl, { waitUntil: 'load' })
    const group = page.locator('[role="treeitem"]').first()
    await group.waitFor({ timeout: 15_000 })
    if (await group.getAttribute('aria-expanded') !== 'true') await group.click()
  }, 120_000)

  afterAll(async () => {
    try { await browser?.close() } finally { await scaffold?.close() }
    if (screenshotDir) console.log(`Team Room screenshots: ${screenshotDir}`)
  })

  it('keeps all three built-in squads visible with the saved selector in the composed homepage', async () => {
    const squads = page.getByRole('region', { name: 'Standing squads', exact: true })
    await squads.waitFor({ timeout: 15_000 })
    for (const name of ['Document squad', 'Case squad', 'Comms squad']) {
      expect(await squads.getByRole('button', { name, exact: true }).count()).toBe(1)
    }
    expect(await squads.locator('[data-assistant-selector]').count()).toBe(1)
    await squads.getByRole('button', { name: 'Roster seats', exact: true }).first().click()
    expect(await squads.innerText()).toContain('archivist')
    expect(await squads.innerText()).toContain('metric-checker')
    await compareOrRefreshGolden(`${EXPECTED}/home-ui.expected.md`,
      await captureStableAria(page, 'section[aria-label="Standing squads"]', scaffold.workspaceCwd), scaffold.mode)
    await page.screenshot({ path: join(screenshotDir, 'home-light.png') })
    await page.evaluate(() => { document.body.setAttribute('data-ds-dark-theme', '') })
    await page.screenshot({ path: join(screenshotDir, 'home-dark.png') })
    await page.evaluate(() => { document.body.removeAttribute('data-ds-dark-theme') })
    await page.setViewportSize({ width: 600, height: 800 })
    await page.screenshot({ path: join(screenshotDir, 'home-narrow.png') })
    await expect.poll(() => squads.evaluate(node => node.scrollWidth <= node.clientWidth + 1)).toBe(true)
    await page.setViewportSize({ width: 1440, height: 1024 })
    expect((await scaffold.ctx.agentTeams.listInstitutionSquads()).every(row => row.leadSessionId === undefined)).toBe(true)
    expect(await scaffold.ctx.assistants.listTeams()).toHaveLength(0)
    await scaffold.ctx.agentTeams.transactInstitutionCatalog(async file => ({
      next: { ...file, leads: { ...file.leads, document: SESSION } }, result: true,
    }))
    await page.reload({ waitUntil: 'load' })
    await squads.getByRole('button', { name: 'Document squad', exact: true }).waitFor()
    await expect.poll(async () => (await scaffold.ctx.agentTeams.listInstitutionSquads()).find(row => row.id === 'document')?.leadSessionId).toBe(SESSION)
    await squads.getByText('Standing', { exact: true }).waitFor()
    expect(await scaffold.ctx.assistants.listTeams()).toHaveLength(0)
  })

  it('shows recorded members, tasks and exchanges with embedded artwork in both palettes and a narrow dock', async () => {
    await page.locator('[role="treeitem"]').filter({ hasText: 'Recorded Team Room' }).last().click()
    await page.locator('[data-team-action]').getByRole('button', { name: /^Team/ }).click()
    await page.locator('[data-team-room]').waitFor({ timeout: 15_000 })
    const room = page.locator('[data-team-dock] [data-team-room]')
    const seats = room.locator('[data-room-seat]')
    expect(await seats.count()).toBe(3)
    expect(await room.innerText()).toContain('notes-writer')
    expect(await room.innerText()).toContain('summary-writer')
    expect(await room.locator('[data-room-table]').count()).toBe(1)
    expect(await room.locator('[data-edge-kind="message"]').count()).toBe(2)
    expect(await room.locator('animateMotion').count()).toBe(0)
    const images = await room.locator('img').evaluateAll((nodes: HTMLImageElement[]) => nodes.map(node => ({
      embedded: node.src.startsWith('data:image/webp;base64,'),
      loaded: node.complete && node.naturalWidth > 0,
    })))
    expect(images.every(image => image.embedded && image.loaded)).toBe(true)
    await compareOrRefreshGolden(`${EXPECTED}/ui.expected.md`,
      await captureStableAria(page, '[data-team-dock] section', scaffold.workspaceCwd), scaffold.mode)
    const original = await seats.evaluateAll(nodes => nodes.map(node => [node.getAttribute('data-member-id'), node.getAttribute('data-seat-index')]))
    for (const dark of [false, true]) {
      await page.evaluate(dark => document.body.toggleAttribute('data-ds-dark-theme', dark), dark)
      await page.screenshot({ path: join(screenshotDir, `${dark ? 'dark' : 'light'}.png`) })
      expect(await seats.evaluateAll(nodes => nodes.map(node => [node.getAttribute('data-member-id'), node.getAttribute('data-seat-index')]))).toEqual(original)
    }
    await page.evaluate(() => { document.body.removeAttribute('data-ds-dark-theme') })
    await page.setViewportSize({ width: 600, height: 800 })
    await page.screenshot({ path: join(screenshotDir, 'narrow.png') })
    await expect.poll(() => room.evaluate(node => node.scrollWidth <= node.clientWidth + 1)).toBe(true)
    expect(tripwire.pageErrors).toEqual([])
    expect(tripwire.warnings).toEqual([])
  })

  it('opens the shared room from the saved-team collaboration button without starting a task', async () => {
    await page.setViewportSize({ width: 1440, height: 1024 })
    await page.locator('[data-team-dock]').getByRole('button', { name: 'Close', exact: true }).click()
    const coordinator = await scaffold.ctx.assistants.createProfile({ name: 'Room coordinator', responsibility: 'Coordinate the recorded team' })
    const saved = await scaffold.ctx.assistants.createTeam({ name: 'Recorded standing team', coordinatorProfileId: coordinator.id, leadSessionId: SessionId(SESSION) })
    const coordinatorSeat = await scaffold.ctx.assistants.createSeat({
      teamId: saved.id, profileId: coordinator.id, name: 'coordinator', role: 'coordinator', duty: 'Coordinate the recorded team',
    })
    await scaffold.ctx.assistants.bindSeatSession(coordinatorSeat.id, SessionId(SESSION))
    for (const [index, name] of ['notes-writer', 'summary-writer'].entries()) {
      const profile = await scaffold.ctx.assistants.createProfile({ name, responsibility: name })
      const seat = await scaffold.ctx.assistants.createSeat({ teamId: saved.id, profileId: profile.id, name, role: 'teammate', duty: name })
      await scaffold.ctx.assistants.bindSeatSession(seat.id, SessionId(`${SESSION}-child-${index + 2}`))
    }
    await page.getByRole('navigation', { name: 'Global panels' }).getByRole('button', { name: 'Assistants', exact: true }).click()
    const team = page.locator('article').filter({ hasText: 'Recorded standing team' })
    await team.getByRole('button', { name: 'Details', exact: true }).click()
    await page.getByTestId('open-dock-button').click()
    const panel = page.getByTestId('standing-team-dock')
    await expect.poll(() => page.getByRole('dialog').evaluate(node => Number(getComputedStyle(node).opacity))).toBe(1)
    await expect.poll(() => panel.locator('[data-room-seat]').count()).toBe(3)
    await expect.poll(() => panel.locator('[aria-busy="true"]').count()).toBe(0)
    expect(await panel.locator('[data-team-room]').innerText()).toContain('notes-writer')
    expect(await panel.locator('[data-team-room]').innerText()).toContain('summary-writer')
    expect(await panel.getByRole('button', { name: 'Reduce motion', exact: true }).count()).toBe(1)
    await compareOrRefreshGolden(`${EXPECTED}/standing-ui.expected.md`,
      await captureStableAria(page, '[data-testid="standing-team-dock"]', scaffold.workspaceCwd), scaffold.mode)
    await page.screenshot({ path: join(screenshotDir, 'standing-light.png') })
    await page.evaluate(() => { document.body.setAttribute('data-ds-dark-theme', '') })
    await page.screenshot({ path: join(screenshotDir, 'standing-dark.png') })
    expect(tripwire.pageErrors).toEqual([])
    expect(tripwire.warnings).toEqual([])
    await assertFixtureInventory(EXPECTED, ['ui.expected.md', 'standing-ui.expected.md', 'home-ui.expected.md'])
  })


  it('shows newly configured roles before any runtime session exists', async () => {
    const coordinator = await scaffold.ctx.assistants.createProfile({ name: 'Planning coordinator', responsibility: 'Plan the event' })
    const planned = await scaffold.ctx.assistants.createTeam({ name: 'Unstarted planning team', coordinatorProfileId: coordinator.id })
    for (const name of ['budget-checker', 'venue-checker']) {
      const profile = await scaffold.ctx.assistants.createProfile({ name, responsibility: name })
      await scaffold.ctx.assistants.createSeat({ teamId: planned.id, profileId: profile.id, name, role: 'teammate', duty: name })
    }
    await page.evaluate(() => { document.body.removeAttribute('data-ds-dark-theme') })
    await page.getByRole('dialog').getByRole('button', { name: 'Close', exact: true }).click()
    await page.getByRole('button', { name: 'Cancel', exact: true }).click()
    // Reopen the directory so the new public catalog records are fetched.
    await page.getByRole('navigation', { name: 'Global panels' }).getByRole('button', { name: 'Plugins', exact: true }).click()
    await page.getByRole('navigation', { name: 'Global panels' }).getByRole('button', { name: 'Assistants', exact: true }).click()
    await page.locator('article').filter({ hasText: 'Unstarted planning team' }).getByRole('button', { name: 'Details', exact: true }).click()
    await page.getByTestId('open-dock-button').click()
    const panel = page.getByTestId('standing-team-dock')
    expect(await panel.locator('[data-room-seat]').count()).toBe(3)
    expect(await panel.locator('[data-status="unprovisioned"]').count()).toBe(3)
    expect(await panel.locator('[data-status="running"]').count()).toBe(0)
    expect(await panel.locator('animateMotion').count()).toBe(0)
    expect(await panel.innerText()).toContain('budget-checker')
    expect(await panel.innerText()).toContain('venue-checker')
    await expect.poll(() => page.getByRole('dialog').evaluate(node => Number(getComputedStyle(node).opacity))).toBe(1)
    await page.screenshot({ path: join(screenshotDir, 'standing-planned.png') })
    expect(tripwire.pageErrors).toEqual([])
    expect(tripwire.warnings).toEqual([])
  })

})
