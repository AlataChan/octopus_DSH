/** Team Room seats and handoffs projected from the real roster and task board. */

import { useEffect, useId, useRef, useState, type ReactNode } from 'react'
import type {
  TeamInteractionEdge,
  TeamMemberView as TeamRosterMember,
  TeamTaskView,
} from '@deepseek-ai/dsh-experimental-agent-team/client'
import { StateDot } from '@deepseek-ai/dsh-client-ui-primitives'
import { TeamAvatar, memberAvatarSeed } from './TeamAvatar.tsx'
import { ROOM_BACKGROUND_URL, ROOM_TABLE_URL, ROOM_WORKER_URL, ROOM_WORKER_ALT_URL } from './team-room-art.ts'
import { NS, type TeamKey } from './locales.ts'
import type { PropsLocale } from '@deepseek-ai/dsh-client-ui-slots'
import css from './TeamAction.module.css'

const TOPOLOGY_PREF_KEY = 'dsh.client.agent-team.topology'
const MOTION_PREF_KEY = 'dsh.client.agent-team.reduceMotion'

function readTopologyPref(): boolean {
  try {
    const raw = sessionStorage.getItem(TOPOLOGY_PREF_KEY)
    if (raw === '0') return false
    if (raw === '1') return true
  } catch {
    /* sessionStorage may be unavailable in locked-down embeds */
  }
  return true
}

function writeTopologyPref(enabled: boolean): void {
  try {
    sessionStorage.setItem(TOPOLOGY_PREF_KEY, enabled ? '1' : '0')
  } catch {
    /* ignore quota / privacy mode */
  }
}

function readReduceMotion(): boolean {
  try {
    return localStorage.getItem(MOTION_PREF_KEY) === '1'
  } catch {
    return false
  }
}

function writeReduceMotion(enabled: boolean): void {
  try {
    localStorage.setItem(MOTION_PREF_KEY, enabled ? '1' : '0')
  } catch {
    /* ignore */
  }
}

function memberStatusKey(status: TeamRoomMember<string>['status']): TeamKey {
  switch (status) {
    case 'running': return 'memberStatus.running'
    case 'inactive': return 'memberStatus.inactive'
    case 'provisioning': return 'memberStatus.provisioning'
    case 'failed': return 'memberStatus.failed'
    case 'unprovisioned': return 'memberStatus.unprovisioned'
    case 'unobserved': return 'memberStatus.unobserved'
  }
}

function statusDot(status: TeamRoomMember<string>['status']): 'done' | 'warning' | 'ongoing' | 'error' | 'idle' {
  switch (status) {
    case 'running': return 'ongoing'
    case 'provisioning': return 'warning'
    case 'failed': return 'error'
    case 'inactive':
    case 'unprovisioned':
    case 'unobserved':
      return 'idle'
  }
}

/** One real runtime member or saved roster entry; its owner supplies the durable rendering identity. */
export interface TeamRoomMember<Id extends string> extends Pick<TeamRosterMember, 'name' | 'role' | 'description' | 'model'> {
  readonly id: Id
  readonly status: TeamRosterMember['status'] | 'unprovisioned' | 'unobserved'
}

/** Read-only room data; planned seats carry no fabricated Session identity or execution state. */
export interface TeamRoomView<Id extends string> {
  readonly key?: Id
  readonly members: readonly TeamRoomMember<Id>[]
  readonly tasks: readonly TeamTaskView[]
  readonly interactions: readonly TeamInteractionEdge[]
}

/** Locale-backed labels used by the topology strip. */
export interface TeamTopologyCopy {
  title: string
  settledTitle: string
  settledHint: string
  hint: string
  show: string
  hide: string
  empty: string
  edgeMessage: string
  edgeTask: string
  motionReduce: string
  motionFull: string
  memberStatus: (key: TeamKey) => string
  leadRole: string
  workerRole: string
  activityTitle: string
  progress: (done: number, total: number) => string
}

export interface TeamTopologyProps<Id extends string = string> {
  view: TeamRoomView<Id>
  copy: TeamTopologyCopy
  /** Incremented when the Team panel opens so the map is revealed and scrolled into view. */
  revealKey?: number
}

/**
 * Display observed members or saved seats without starting sessions or fetching Host state.
 * Seats retain their member IDs across refreshes and panel reopening in the same browser session.
 * Mount only while the Team panel is open; no second Host API or background polling.
 * @param props - Read-only room data, locale-owned labels and optional reveal request.
 * @returns Spatial roster, task progress and recent interaction feed.
 */
export function TeamTopology<Id extends string>({ view, copy, revealKey = 0 }: TeamTopologyProps<Id>) {
  const [canvas, setCanvas] = useState<HTMLDivElement | null>(null)
  const [canvasWidth, setCanvasWidth] = useState(600)
  const [enabled, setEnabled] = useState(readTopologyPref)
  const [reduceMotion, setReduceMotion] = useState(readReduceMotion)
  const [flashIds, setFlashIds] = useState<ReadonlySet<string>>(() => new Set())
  const prevEdges = useRef<ReadonlySet<string>>(new Set(view.interactions.map(edge => edge.id)))
  const flashTimer = useRef<number | undefined>(undefined)
  const markerId = useId().replaceAll(':', '')

  useEffect(() => {
    if (revealKey === 0) return
    setEnabled(true)
    writeTopologyPref(true)
  }, [revealKey])

  useEffect(() => () => { window.clearTimeout(flashTimer.current) }, [])

  useEffect(() => {
    const nextEdgeIds = new Set(view.interactions.map(edge => edge.id))
    const appeared = [...nextEdgeIds].filter(id => !prevEdges.current.has(id))
    prevEdges.current = nextEdgeIds
    if (reduceMotion) {
      window.clearTimeout(flashTimer.current)
      setFlashIds(new Set())
      return
    }
    if (appeared.length === 0) return
    window.clearTimeout(flashTimer.current)
    setFlashIds(new Set(appeared))
    flashTimer.current = window.setTimeout(() => { setFlashIds(new Set()) }, 2600)
  }, [view.interactions, reduceMotion])

  const toggle = (): void => {
    setEnabled((current) => {
      const next = !current
      writeTopologyPref(next)
      return next
    })
  }

  const toggleMotion = (): void => {
    setReduceMotion((current) => {
      const next = !current
      writeReduceMotion(next)
      return next
    })
  }

  useEffect(() => {
    if (canvas === null || typeof ResizeObserver === 'undefined') return
    const observer = new ResizeObserver(([entry]) => {
      if (entry !== undefined) setCanvasWidth(entry.contentRect.width)
    })
    observer.observe(canvas)
    return () => { observer.disconnect() }
  }, [canvas])

  const teamKey = view.key ?? view.members.find(member => member.role === 'lead')?.id ?? view.members[0]?.id ?? ''
  const seatCache = useRef<{ key: string; ids: string[] }>({ key: '', ids: [] })
  const previousIds = seatCache.current.key === teamKey ? seatCache.current.ids : readSeatIds(teamKey)
  const seatIds = assignSeats(view.members, previousIds)
  const membersById = new Map<string, TeamRoomMember<Id>>(view.members.map(member => [member.id, member]))
  const seats = seatIds.flatMap((id, slot) => {
    const member = membersById.get(id)
    return member === undefined ? [] : [{ member, slot, ...seatPosition(slot) }]
  })
  const positions = new Map(seats.map(seat => [seat.member.name, { x: seat.x * canvasWidth, y: seat.y + 115 }]))
  const tables = [...new Set(seats.map(seat => Math.floor(seat.slot / 5)))]
  const lastTable = Math.max(0, ...tables)
  const lastHasFrontSeats = seats.some(seat => Math.floor(seat.slot / 5) === lastTable && seat.slot % 5 > 2)
  const height = lastTable * 640 + (lastHasFrontSeats ? 640 : 540)
  const recentInteractions = view.interactions.filter(edge => positions.has(edge.from) && positions.has(edge.to)).slice(-3)

  useEffect(() => {
    seatCache.current = { key: teamKey, ids: seatIds }
    if (teamKey === '') return
    try {
      sessionStorage.setItem(`dsh.client.agent-team.seats:${teamKey}`, JSON.stringify(seatIds))
    } catch {
      /* Seat placement still works in memory when storage is unavailable. */
    }
  }, [teamKey, seatIds])
  const tasks = view.tasks.filter(task => task.status !== 'deleted')
  const completed = tasks.filter(task => task.status === 'completed').length
  const settled = tasks.length > 0 && completed === tasks.length && view.members.length > 0
    && view.members.every(member => member.status === 'inactive')

  return (
    <section className={css.topology} aria-label={copy.title} data-reduce-motion={reduceMotion}>
      {settled && <div className={css.teamSettled}>
        <StateDot state="done" appearance="step" size={24} />
        <span className={css.teamSettledText}><strong>{copy.settledTitle}</strong><small>{copy.settledHint}</small></span>
        <span className={css.teamSettledCount}>{completed} / {tasks.length}</span>
      </div>}
      <div className={css.topologyHeader}>
        <h3>{copy.title}</h3>
        <button type="button" className={css.smallButton} onClick={toggleMotion} aria-pressed={reduceMotion}>
          {reduceMotion ? copy.motionFull : copy.motionReduce}
        </button>
        <button type="button" className={css.smallButton} onClick={toggle}>
          {enabled ? copy.hide : copy.show}
        </button>
      </div>
      {!enabled ? null : (
        <>
          <p className={css.topologyHint}>{copy.hint}</p>
          {seats.length === 0 ? (
            <p className={css.topologyEmpty}>{copy.empty}</p>
          ) : (
            <div className={css.topologyCanvas} ref={setCanvas}>
              <div className={css.topologyScene} data-team-room style={{ height }}>
                {tables.map(table => (
                  <div key={table} className={css.roomEnvironment} style={{ top: table * 640 }} aria-hidden="true">
                    <img className={css.roomBackground} src={ROOM_BACKGROUND_URL} alt="" />
                    <img className={css.roomTable} data-room-table={table} src={ROOM_TABLE_URL} alt="" />
                  </div>
                ))}
                <svg className={css.topologySvg} viewBox={`0 0 ${canvasWidth} ${height}`} style={{ height }} preserveAspectRatio="none" aria-hidden>
                  <defs>
                    <marker id={`${markerId}-message`} viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                      <path d="M 0 0 L 8 4 L 0 8 Z" className={css.topologyMessageArrow} />
                    </marker>
                    <marker id={`${markerId}-task`} viewBox="0 0 8 8" refX="7" refY="4" markerWidth="5" markerHeight="5" orient="auto">
                      <path d="M 0 0 L 8 4 L 0 8 Z" className={css.topologyTaskArrow} />
                    </marker>
                  </defs>
                  {recentInteractions.map((edge, lane) => renderEdge(edge, positions, flashIds.has(edge.id), lane, markerId))}
                </svg>
                {seats.map(({ member, slot, x, y }) => {
                  const task = member.status === 'running' ? view.tasks.find(row => row.ownerName === member.name && row.status === 'in_progress') : undefined
                  return (
                    <div
                      key={member.id}
                      className={`${css.topologyNode}${member.status === 'running' ? ` ${css.topologyNodeActive}` : ''}`}
                      data-room-seat
                      data-member-id={member.id}
                      data-seat-index={slot}
                      data-seat-side={slot % 5 === 2 || slot % 5 === 4 ? 'right' : 'left'}
                      data-status={member.status}
                      data-role={member.role}
                      style={{ left: `${x * 100}%`, top: y }}
                    >
                      <img
                        className={css.roomWorker}
                        src={memberAvatarSeed(member.id, member.role) % 2 === 0 ? ROOM_WORKER_URL : ROOM_WORKER_ALT_URL}
                        alt=""
                        aria-hidden="true"
                      />
                      <div className={css.roomNameplate}>
                        <TeamAvatar member={member} />
                        <div className={css.topologyMemberInfo}>
                          <div className={css.topologyMemberHeading}><strong title={member.name}>{member.name}</strong><span className={css.topologyRole}>{member.role === 'lead' ? copy.leadRole : copy.workerRole}</span></div>
                          {member.description !== undefined && (
                            <span className={css.topologyDescription} title={member.description}>{member.description}</span>
                          )}
                          <span className={css.topologyMemberState}>
                            <StateDot state={statusDot(member.status)} />
                            {copy.memberStatus(memberStatusKey(member.status))}
                          </span>
                          {member.model !== undefined && <span className={css.topologyModel} title={member.model}>{member.model}</span>}
                        </div>
                      </div>
                      {task !== undefined && <span className={css.topologyTask} title={task.subject}>{task.subject}</span>}
                    </div>
                  )
                })}
              </div>
              {tasks.length > 0 && (
                <div className={css.topologyProgress}>
                  <span>{copy.progress(completed, tasks.length)}</span>
                  <progress max={tasks.length} value={completed} aria-label={copy.progress(completed, tasks.length)} />
                </div>
              )}
              {recentInteractions.length > 0 && (
                <div className={css.topologyFeed} aria-label={copy.activityTitle}>
                  <span className={css.topologyFeedTitle}>{copy.activityTitle}</span>
                  {recentInteractions.map((edge) => {
                    const source = view.members.find(member => member.name === edge.from)
                    const target = view.members.find(member => member.name === edge.to)
                    return (
                      <div key={edge.id} className={flashIds.has(edge.id) ? css.topologyFeedActive : undefined}>
                        <span className={css.topologyFeedPair}>
                          {source !== undefined && <TeamAvatar member={source} />}
                          <span>{edge.from}</span><span aria-hidden>→</span>
                          {target !== undefined && <TeamAvatar member={target} />}
                          <span>{edge.to}</span>
                        </span>
                        <span>{edge.kind === 'message' ? copy.edgeMessage : copy.edgeTask}</span>
                      </div>
                    )
                  })}
                </div>
              )}
            </div>
          )}
        </>
      )}
    </section>
  )
}

function renderEdge(
  edge: TeamInteractionEdge,
  positions: ReadonlyMap<string, { x: number; y: number }>,
  flash: boolean,
  lane: number,
  markerId: string,
): ReactNode {
  const from = positions.get(edge.from)
  const to = positions.get(edge.to)
  if (from === undefined || to === undefined) return null
  const bend = Math.max(from.y, to.y) + 45 + lane * 16
  const path = `M ${from.x} ${from.y} C ${from.x} ${bend} ${to.x} ${bend} ${to.x} ${to.y}`
  return (
    <g key={edge.id} className={flash ? css.topologyEdgeFlash : undefined} data-edge-kind={edge.kind}>
      <path
        d={path}
        markerEnd={`url(#${markerId}-${edge.kind === 'message' ? 'message' : 'task'})`}
        className={edge.kind === 'message' ? css.topologyEdgeMessage : css.topologyEdgeTask}
      />
      {flash && edge.kind === 'message' && (
        <circle r={5.5} className={css.topologyDot}>
          <animateMotion dur="1.25s" repeatCount="2" path={path} />
        </circle>
      )}
    </g>
  )
}

/** Read only validated local seat identities; the Host roster owns all member content. */
function readSeatIds(teamKey: string): string[] {
  try {
    const value: unknown = JSON.parse(sessionStorage.getItem(`dsh.client.agent-team.seats:${teamKey}`) ?? '[]')
    if (!Array.isArray(value) || !value.every((id: unknown) => typeof id === 'string')) return []
    const occupied = value.filter(id => id !== '')
    return new Set(occupied).size === occupied.length ? value : []
  } catch {
    return [] // Unavailable storage or a malformed preference does not prevent opening the room.
  }
}

/** Keep surviving members in place and reuse vacancies for newly joined members. */
function assignSeats<Id extends string>(members: readonly TeamRoomMember<Id>[], previous: readonly string[]): string[] {
  const live = new Set<string>(members.map(member => member.id))
  const ids = previous.map(id => live.has(id) ? id : '')
  const lead = members.find(member => member.role === 'lead')
  if (lead !== undefined && ids[0] !== lead.id) {
    const displaced = ids[0]
    const oldLead = ids.indexOf(lead.id)
    if (oldLead >= 0) ids[oldLead] = ''
    ids[0] = lead.id
    if (displaced !== undefined && displaced !== '') ids.push(displaced)
  }
  for (const member of members) {
    if (ids.includes(member.id)) continue
    const vacancy = ids.indexOf('')
    if (vacancy < 0) ids.push(member.id)
    else ids[vacancy] = member.id
  }
  return ids
}

/** Each additional group gets its own table rather than shrinking portraits and text. */
function seatPosition(slot: number): { x: number; y: number } {
  const position = slot % 5
  const x = position === 0 ? 0.5 : position % 2 === 1 ? 0.2 : 0.8
  const y = position === 0 ? 20 : position <= 2 ? 175 : 375
  return { x, y: Math.floor(slot / 5) * 640 + y }
}

/**
 * Bind the shared room's copy to the Agent Teams dictionary for any entry point.
 * @param t - Locale-owned Agent Teams translator.
 * @returns Labels and formatters consumed by the room renderer.
 */
export function teamRoomCopy(t: PropsLocale<typeof NS>['t']): TeamTopologyCopy {
  return {
    title: t('topology'), settledTitle: t('teamCompletedTitle'), settledHint: t('teamIdleHint'),
    hint: t('topologyHint'), show: t('topologyShow'), hide: t('topologyHide'), empty: t('topologyEmpty'),
    edgeMessage: t('topologyEdgeMessage'), edgeTask: t('topologyEdgeTask'),
    motionReduce: t('motionReduce'), motionFull: t('motionFull'), memberStatus: key => t(key),
    leadRole: t('topologyLead'), workerRole: t('topologyWorker'), activityTitle: t('topologyActivity'),
    progress: (done, total) => t('topologyProgress', { done, total }),
  }
}
