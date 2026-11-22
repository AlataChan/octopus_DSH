import { closeSync, constants, fstatSync, lstatSync, openSync, readFileSync, realpathSync } from 'node:fs'
import { join } from 'node:path'
import { digest, shanghaiDate } from './operations.mjs'

const dayFormat = /^\d{4}-\d{2}-\d{2}$/u
const symbols = Object.freeze({
  399852: '中证1000',
  159915: '创业板ETF',
  510500: '中证500ETF',
  588000: '科创50ETF',
})
const directions = Object.freeze({ bullish: '偏多', bearish: '偏空', neutral: '中性' })
const scopes = Object.freeze(['daily_structure', 'implied_volatility'])
const maxAgeMs = 4 * 86_400_000

function trustedDirectory(path, uid) {
  const stat = lstatSync(path)
  if (!stat.isDirectory() || stat.uid !== uid || (stat.mode & 0o022) || realpathSync(path) !== path) {
    throw new Error('untrusted_published_report_directory')
  }
}

function trustedText(path, uid, maxBytes) {
  const fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK)
  try {
    const stat = fstatSync(fd)
    if (!stat.isFile() || stat.uid !== uid || (stat.mode & 0o022) || stat.size > maxBytes) {
      throw new Error('untrusted_published_report_file')
    }
    const text = readFileSync(fd, 'utf8')
    if (Buffer.byteLength(text, 'utf8') > maxBytes) throw new Error('published_report_file_too_large')
    return { text, modifiedAt: stat.mtimeMs }
  } finally {
    closeSync(fd)
  }
}

/** Read the structured snapshot linked to a successfully delivered CN closing report. */
export function createCnPublishedReportProvider(root, { trustedUid = 0 } = {}) {
  return {
    async getVerifiedReportFacts({ conversationKey, accountSha256, nowMs, signal }) {
      if (signal?.aborted) throw new Error('published_report_aborted')
      const historyDir = join(root, 'report_history')
      trustedDirectory(root, trustedUid)
      trustedDirectory(historyDir, trustedUid)
      const journal = trustedText(join(root, 'cn_daily_delivery_attempts.jsonl'), trustedUid, 8 * 1024 * 1024).text
      const lines = journal.split('\n')
      lines.pop() // The writer may still be appending the last line.
      const candidates = lines.filter(line => line.trim()).map(line => JSON.parse(line)).filter(row =>
        row.mode === 'daily_close' && row.sent === true && row.channels?.feishu?.returned === true
        && row.history === 'written' && row.state === 'written' && dayFormat.test(row.trade_date)
        && /^[a-f0-9]{64}$/u.test(row.final_body_sha256 ?? ''),
      ).map(row => ({ row, publishedAt: Date.parse(row.finished_at), startedAt: Date.parse(row.started_at) }))
        .filter(({ row, publishedAt, startedAt }) => Number.isFinite(startedAt) && startedAt <= publishedAt
          && publishedAt <= nowMs && nowMs - publishedAt < maxAgeMs
          && row.trade_date === shanghaiDate(publishedAt))
        .sort((a, b) => b.publishedAt - a.publishedAt)
      const latest = candidates[0]
      if (!latest) throw new Error('no_verified_cn_report')
      const { row, publishedAt, startedAt } = latest
      const day = row.trade_date
      const history = trustedText(join(historyDir, day + '.json'), trustedUid, 64 * 1024)
      if (history.modifiedAt < startedAt - 1000 || history.modifiedAt > publishedAt + 1000) {
        throw new Error('published_report_history_timestamp_mismatch')
      }
      const snapshot = JSON.parse(history.text)
      if (snapshot.schema_version !== 1 || snapshot.trade_date !== day) {
        throw new Error('published_report_history_date_mismatch')
      }
      const facts = []
      const addFact = (symbol, scope, text) => facts.push({ factId: symbol + '-' + scope, scope, text, digest: digest(text) })
      for (const [symbol, name] of Object.entries(symbols)) {
        const structure = snapshot.daily_structure?.[symbol]
        const direction = structure && Object.hasOwn(directions, structure.direction) ? directions[structure.direction] : null
        if (direction && structure.status === 'current' && structure.source_bar_date === day
          && ['underlying_rule', 'proxy_reference'].includes(structure.authority)) {
          const qualifier = structure.authority === 'proxy_reference' ? '（期货代理）' : ''
          addFact(symbol, 'daily_structure', day + ' 发布日报：' + name + '日线方向' + direction + qualifier + '。')
        }
        const item = snapshot.items?.[symbol]
        if (item?.iv_date_valid === true && item.measurement_date === day
          && Number.isFinite(item.iv) && item.iv >= 0 && item.iv <= 1000
          && /^\d{4}$/u.test(item.month ?? '')) {
          addFact(symbol, 'implied_volatility',
            day + ' 发布日报：' + name + ' ' + item.month + '合约月平值隐含波动率为' + item.iv.toFixed(2) + '%（日报快照）。')
        }
      }
      if (!facts.length) throw new Error('published_report_has_no_verified_facts')
      return {
        schemaVersion: 1, providerId: 'gamma-cn-published-report-v1', publishedOnly: true,
        report: {
          reportId: 'cn-index-' + day, reportVersion: 1, reportDigest: row.final_body_sha256,
          publishedAtMs: publishedAt, validUntilMs: publishedAt + maxAgeMs,
          authorizedConversations: [conversationKey], accountSha256, allowedScopes: scopes, facts,
        },
      }
    },
  }
}
