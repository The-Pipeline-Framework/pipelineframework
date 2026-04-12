const DEVTO_FEED_URL = 'https://dev.to/feed/tag/tpf'
const DEFAULT_LIMIT = 10
const MIN_LIMIT = 1
const MAX_LIMIT = 30
const EDGE_CACHE_TTL_SECONDS = 300
const SUCCESS_MAX_AGE_SECONDS = 300
const SUCCESS_S_MAX_AGE_SECONDS = 600
const ERROR_MAX_AGE_SECONDS = 60
const ERROR_S_MAX_AGE_SECONDS = 120
const RATE_LIMIT_MAX_AGE_SECONDS = 30
const RATE_LIMIT_S_MAX_AGE_SECONDS = 180
const PRE_FLIGHT_MAX_AGE_SECONDS = 86400
const PRE_FLIGHT_S_MAX_AGE_SECONDS = 86400
const EXCERPT_MAX_LENGTH = 180

const XML_ENTITY_MAP = {
  amp: '&',
  apos: '\'',
  gt: '>',
  lt: '<',
  quot: '"',
  nbsp: ' ',
  mdash: '—',
  ndash: '–',
  hellip: '…',
  rsquo: ''',
  lsquo: ''',
  rdquo: '"',
  ldquo: '"'
}

const parseLimit = (value) => {
  const parsed = Number.parseInt(value ?? '', 10)
  if (Number.isNaN(parsed)) {
    return DEFAULT_LIMIT
  }
  return Math.min(Math.max(parsed, MIN_LIMIT), MAX_LIMIT)
}

const responseHeaders = (cacheControl, corsMaxAgeSeconds) => ({
  'content-type': 'application/json; charset=utf-8',
  'cache-control': cacheControl,
  'access-control-allow-origin': '*',
  'access-control-allow-methods': 'GET, OPTIONS',
  'access-control-allow-headers': 'Content-Type',
  ...(corsMaxAgeSeconds ? {'access-control-max-age': String(corsMaxAgeSeconds)} : {})
})

const escapeRegex = (value) => value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')

const stripCdata = (value) => value
  .replace(/^<!\[CDATA\[/, '')
  .replace(/\]\]>$/, '')

export const decodeHtmlEntities = (value) => value.replace(/&(#x?[0-9a-fA-F]+|[a-zA-Z]+);/g, (entity, token) => {
  if (token[0] === '#') {
    const isHex = token[1]?.toLowerCase() === 'x'
    const rawCode = isHex ? token.slice(2) : token.slice(1)
    const codePoint = Number.parseInt(rawCode, isHex ? 16 : 10)
    return Number.isNaN(codePoint) ? entity : String.fromCodePoint(codePoint)
  }

  const normalized = token.toLowerCase()
  return XML_ENTITY_MAP[normalized] ?? entity
})

export const stripHtml = (value) => value
  .replace(/<[^>]+>/g, ' ')
  .replace(/\s+/g, ' ')
  .trim()

export const truncateText = (value, maxLength = EXCERPT_MAX_LENGTH) => {
  if (value.length <= maxLength) {
    return value
  }

  const truncated = value.slice(0, maxLength).trimEnd()
  const lastSpace = truncated.lastIndexOf(' ')
  return `${(lastSpace > 0 ? truncated.slice(0, lastSpace) : truncated).trimEnd()}...`
}

const readTag = (xml, tagName) => {
  const pattern = new RegExp(`<${escapeRegex(tagName)}(?:\\s[^>]*)?>([\\s\\S]*?)</${escapeRegex(tagName)}>`, 'i')
  const match = xml.match(pattern)
  return match ? stripCdata(match[1].trim()) : ''
}

const normalizeDate = (value) => {
  if (!value) {
    return ''
  }

  const parsed = new Date(value)
  return Number.isNaN(parsed.getTime()) ? '' : parsed.toISOString()
}

export const normalizeDevToFeed = (xml, limit = DEFAULT_LIMIT) => {
  if (typeof xml !== 'string' || !xml.includes('<rss')) {
    return []
  }

  const normalizedLimit = parseLimit(limit)
  const items = [...xml.matchAll(/<item>([\s\S]*?)<\/item>/gi)]

  return items
    .map((match) => {
      const itemXml = match[1]
      const title = decodeHtmlEntities(readTag(itemXml, 'title'))
      const url = decodeHtmlEntities(readTag(itemXml, 'link'))
      const guid = decodeHtmlEntities(readTag(itemXml, 'guid'))
      const author = decodeHtmlEntities(readTag(itemXml, 'dc:creator'))
      const publishedAt = normalizeDate(readTag(itemXml, 'pubDate'))
      const description = decodeHtmlEntities(readTag(itemXml, 'description'))
      const excerpt = truncateText(stripHtml(description))

      if (!title || !(url || guid)) {
        return null
      }

      return {
        id: guid || url,
        title,
        url: url || guid,
        published_at: publishedAt,
        author,
        excerpt
      }
    })
    .filter(Boolean)
    .slice(0, normalizedLimit)
}

export async function onRequestOptions() {
  return new Response(null, {
    status: 204,
    headers: responseHeaders(
      `public, max-age=${PRE_FLIGHT_MAX_AGE_SECONDS}, s-maxage=${PRE_FLIGHT_S_MAX_AGE_SECONDS}`,
      PRE_FLIGHT_MAX_AGE_SECONDS
    )
  })
}

export async function onRequestGet(context) {
  const {request} = context
  const url = new URL(request.url)
  const limit = parseLimit(url.searchParams.get('limit'))

  let upstream
  try {
    upstream = await fetch(DEVTO_FEED_URL, {
      headers: {
        Accept: 'application/rss+xml, application/xml;q=0.9, text/xml;q=0.8'
      },
      cf: {
        cacheEverything: true,
        cacheTtl: EDGE_CACHE_TTL_SECONDS
      }
    })
  } catch (_) {
    return new Response(JSON.stringify([]), {
      status: 200,
      headers: responseHeaders(`public, max-age=${RATE_LIMIT_MAX_AGE_SECONDS}, s-maxage=${RATE_LIMIT_S_MAX_AGE_SECONDS}, stale-while-revalidate=120`)
    })
  }

  if (upstream.status === 403 || upstream.status === 429) {
    return new Response(JSON.stringify([]), {
      status: 200,
      headers: {
        ...responseHeaders(`public, max-age=${RATE_LIMIT_MAX_AGE_SECONDS}, s-maxage=${RATE_LIMIT_S_MAX_AGE_SECONDS}, stale-while-revalidate=120`),
        'x-rate-limited': 'true'
      }
    })
  }

  if (!upstream.ok) {
    return new Response(
      JSON.stringify({error: `DEV.to feed returned ${upstream.status}`}),
      {
        status: upstream.status,
        headers: responseHeaders(`public, max-age=${ERROR_MAX_AGE_SECONDS}, s-maxage=${ERROR_S_MAX_AGE_SECONDS}`)
      }
    )
  }

  const rawXml = await upstream.text()
  const normalized = normalizeDevToFeed(rawXml, limit)

  return new Response(JSON.stringify(normalized), {
    status: 200,
    headers: responseHeaders(`public, max-age=${SUCCESS_MAX_AGE_SECONDS}, s-maxage=${SUCCESS_S_MAX_AGE_SECONDS}, stale-while-revalidate=120`)
  })
}
