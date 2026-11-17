import { clientBundle } from '../../client/tsdown.client.ts'
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'

const preset = clientBundle('@deepseek-ai/dsh-experimental-client-ui-agent-team', ['lib/types/index.js'])

export default (inlineConfig: { env?: Record<string, string> }) => preset(inlineConfig).map(config => {
  if (config.name !== '@deepseek-ai/dsh-experimental-client-ui-agent-team/client') return config
  return {
    ...config,
    plugins: [{
      name: 'inline-team-room-art',
      load(this: { addWatchFile(file: string): void }, id: string) {
        const normalized = id.replaceAll('\\', '/')
        if (!normalized.endsWith('/src/client/team-room-art.ts') && !normalized.endsWith('/lib/types/client/team-room-art.js')) return null
        return ['BACKGROUND', 'TABLE', 'WORKER', 'WORKER_ALT'].map(name => {
          const file = fileURLToPath(new URL(`./src/client/assets/team-room-${name.toLowerCase().replaceAll('_', '-')}.webp`, import.meta.url))
          this.addWatchFile(file)
          return `export const ROOM_${name}_URL = ${JSON.stringify(`data:image/webp;base64,${readFileSync(file).toString('base64')}`)}`
        }).join('\n')
      },
    }, ...config.plugins ?? []],
  }
})
