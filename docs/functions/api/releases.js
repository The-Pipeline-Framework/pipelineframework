const GITHUB_API_BASE = 'https://api.github.com/repos/The-Pipeline-Framework/pipelineframework/releases'
const DEFAULT_PER_PAGE = 10
const MIN_PER_PAGE = 1
const MAX_PER_PAGE = 30
const EDGE_CACHE_TTL_SECONDS = 300
const SUCCESS_MAX_AGE_SECONDS = 300
const SUCCESS_S_MAX_AGE_SECONDS = 600
const ERROR_MAX_AGE_SECONDS = 60
const ERROR_S_MAX_AGE_SECONDS = 120
const RATE_LIMIT_MAX_AGE_SECONDS = 30
const RATE_LIMIT_S_MAX_AGE_SECONDS = 180
const PRE_FLIGHT_MAX_AGE_SECONDS = 86400
const PRE_FLIGHT_S_MAX_AGE_SECONDS = 86400
const GITHUB_API_VERSION = '2024-11-28'

const parsePerPage = (value) => {
  const parsed = Number.parseInt(value ?? '', 10)
  if (Number.isNaN(parsed)) {
    return DEFAULT_PER_PAGE
  }
  return Math.min(Math.max(parsed, MIN_PER_PAGE), MAX_PER_PAGE)
}

const responseHeaders = (cacheControl, corsMaxAgeSeconds) => ({
  'content-type': 'application/json; charset=utf-8',
  'cache-control': cacheControl,
  'access-control-allow-origin': '*',
  'access-control-allow-methods': 'GET, OPTIONS',
  'access-control-allow-headers': 'Content-Type, Authorization',
  ...(corsMaxAgeSeconds ? {'access-control-max-age': String(corsMaxAgeSeconds)} : {})
})

const safeJson = async (response) => {
  const raw = await response.text()
  try {
    return JSON.parse(raw)
  } catch (_) {
    return []
  }
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
  const {request, env} = context
  const url = new URL(request.url)
  const perPage = parsePerPage(url.searchParams.get('per_page'))

  const headers = {
    Accept: 'application/vnd.github+json',
    'User-Agent': 'tpf-docs-releases-widget',
    'X-GitHub-Api-Version': GITHUB_API_VERSION
  }
  if (env?.GITHUB_TOKEN) {
    headers.Authorization = `Bearer ${env.GITHUB_TOKEN}`
  }

  const githubUrl = `${GITHUB_API_BASE}?per_page=${perPage}`
  const upstream = await fetch(githubUrl, {
    headers,
    cf: {
      cacheEverything: true,
      cacheTtl: EDGE_CACHE_TTL_SECONDS
    }
  })

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
      JSON.stringify({error: `GitHub API returned ${upstream.status}`}),
      {
        status: upstream.status,
        headers: responseHeaders(`public, max-age=${ERROR_MAX_AGE_SECONDS}, s-maxage=${ERROR_S_MAX_AGE_SECONDS}`)
      }
    )
  }

  const raw = await safeJson(upstream)
  const normalized = Array.isArray(raw)
    ? raw.map((release) => ({
        id: release.id,
        tag_name: release.tag_name,
        name: release.name,
        draft: release.draft,
        prerelease: release.prerelease,
        published_at: release.published_at,
        html_url: release.html_url
      }))
    : []

  return new Response(JSON.stringify(normalized), {
    status: 200,
    headers: responseHeaders(`public, max-age=${SUCCESS_MAX_AGE_SECONDS}, s-maxage=${SUCCESS_S_MAX_AGE_SECONDS}, stale-while-revalidate=120`)
  })
}
