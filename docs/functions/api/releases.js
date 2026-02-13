const GITHUB_API_BASE = 'https://api.github.com/repos/The-Pipeline-Framework/pipelineframework/releases'

const parsePerPage = (value) => {
  const parsed = Number.parseInt(value ?? '', 10)
  if (Number.isNaN(parsed)) {
    return 10
  }
  return Math.min(Math.max(parsed, 1), 30)
}

export async function onRequestGet(context) {
  const {request} = context
  const url = new URL(request.url)
  const perPage = parsePerPage(url.searchParams.get('per_page'))

  const githubUrl = `${GITHUB_API_BASE}?per_page=${perPage}`
  const upstream = await fetch(githubUrl, {
    headers: {
      Accept: 'application/vnd.github+json',
      'User-Agent': 'tpf-docs-releases-widget'
    },
    cf: {
      cacheEverything: true,
      cacheTtl: 300
    }
  })

  if (!upstream.ok) {
    return new Response(
      JSON.stringify({error: `GitHub API returned ${upstream.status}`}),
      {
        status: upstream.status,
        headers: {
          'content-type': 'application/json; charset=utf-8',
          'cache-control': 'public, max-age=60, s-maxage=120'
        }
      }
    )
  }

  const raw = await upstream.json()
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
    headers: {
      'content-type': 'application/json; charset=utf-8',
      'cache-control': 'public, max-age=300, s-maxage=600, stale-while-revalidate=120'
    }
  })
}

