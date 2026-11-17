/** Local artwork; the Client bundle embeds WebP bytes so installed profiles need no asset server. */
export const ROOM_BACKGROUND_URL = new URL('./assets/team-room-background.webp', import.meta.url).href

/** Empty table, independent of roster size and member identities. */
export const ROOM_TABLE_URL = new URL('./assets/team-room-table.webp', import.meta.url).href

/** Decorative seated body; the adjacent identity portrait and labels belong to the live member. */
export const ROOM_WORKER_URL = new URL('./assets/team-room-worker.webp', import.meta.url).href

/** Alternate decorative body, selected by the same stable identity seed as the member portrait. */
export const ROOM_WORKER_ALT_URL = new URL('./assets/team-room-worker-alt.webp', import.meta.url).href
