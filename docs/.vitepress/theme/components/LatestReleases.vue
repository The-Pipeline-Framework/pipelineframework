<template>
  <section class="releases-panel" aria-labelledby="latest-releases-title">
    <div class="releases-head">
      <h2 id="latest-releases-title">Latest Releases</h2>
      <a class="all-releases" :href="releasesUrl" target="_blank" rel="noopener noreferrer">
        View all
      </a>
    </div>

    <p class="releases-subtitle">
      Fresh from GitHub releases for The Pipeline Framework.
    </p>

    <div v-if="loading" class="release-loading" aria-live="polite">
      Loading releases...
    </div>

    <div v-else-if="error" class="release-error" aria-live="polite">
      Could not load releases right now.
      <a :href="releasesUrl" target="_blank" rel="noopener noreferrer">Open releases</a>.
    </div>

    <ul v-else class="release-list">
      <li v-for="release in releases" :key="release.id" class="release-item">
        <a :href="release.html_url" target="_blank" rel="noopener noreferrer" class="release-link">
          <span class="release-tag">{{ release.tag_name }}</span>
          <span class="release-name">{{ release.name || release.tag_name }}</span>
          <span class="release-meta">
            <time :datetime="release.published_at">{{ formatDate(release.published_at) }}</time>
            <span v-if="release.prerelease" class="release-badge">Pre-release</span>
          </span>
        </a>
      </li>
    </ul>
  </section>
</template>

<script setup>
import {onMounted, ref} from 'vue'

const releases = ref([])
const loading = ref(true)
const error = ref(false)

const releasesUrl = 'https://github.com/The-Pipeline-Framework/pipelineframework/releases'
const releasesApiUrl = '/api/releases?per_page=10'
const fallbackReleasesApiUrl = 'https://api.github.com/repos/The-Pipeline-Framework/pipelineframework/releases?per_page=10'
const cacheTtlMs = 15 * 60 * 1000
const cacheKey = `tpf:releases:${releasesApiUrl}`

const formatDate = (isoDate) => {
  if (!isoDate) return ''
  return new Intl.DateTimeFormat('en-GB', {
    day: '2-digit',
    month: 'short',
    year: 'numeric'
  }).format(new Date(isoDate))
}

onMounted(async () => {
  try {
    try {
      const cached = sessionStorage.getItem(cacheKey)
      if (cached) {
        const parsed = JSON.parse(cached)
        if (parsed?.timestamp && Array.isArray(parsed?.data) && parsed.data.length > 0 && Date.now() - parsed.timestamp < cacheTtlMs) {
          releases.value = parsed.data
          return
        }
      }
    } catch (_) {
      sessionStorage.removeItem(cacheKey)
    }

    const fetchReleases = async (url) => {
      const response = await fetch(url, {
        headers: {
          Accept: 'application/vnd.github+json'
        }
      })
      if (!response.ok) {
        throw new Error(`GitHub API returned ${response.status}`)
      }
      return response.json()
    }

    let data
    try {
      data = await fetchReleases(releasesApiUrl)
    } catch (_) {
      data = await fetchReleases(fallbackReleasesApiUrl)
    }

    releases.value = Array.isArray(data)
      ? data.filter((release) => !release.draft).slice(0, 3)
      : []
    sessionStorage.setItem(cacheKey, JSON.stringify({
      timestamp: Date.now(),
      data: releases.value
    }))
  } catch (err) {
    error.value = true
  } finally {
    loading.value = false
  }
})
</script>

<style scoped>
.releases-panel {
  margin: 1.4rem 0 2rem;
  padding: 1rem 1.1rem;
  border: 1px solid var(--vp-c-border);
  border-radius: 14px;
  background:
    radial-gradient(circle at top left, var(--vp-c-brand-subtle), transparent 52%),
    var(--vp-c-bg);
}

.releases-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 0.8rem;
}

.releases-head h2 {
  margin: 0;
  border: 0;
  padding: 0;
  font-size: 1.2rem;
  color: var(--vp-c-text-1);
}

.all-releases {
  font-weight: 600;
  font-size: 0.95rem;
}

.releases-subtitle {
  margin: 0.45rem 0 0.9rem;
  color: var(--vp-c-text-2);
}

.release-list {
  list-style: none;
  margin: 0;
  padding: 0;
  display: grid;
  gap: 0.55rem;
}

.release-item {
  border: 1px solid var(--vp-c-border);
  border-radius: 10px;
  transition: border-color 180ms ease, transform 180ms ease;
}

.release-item:hover {
  border-color: var(--vp-c-brand-light);
  transform: translateY(-1px);
}

.release-link {
  display: grid;
  grid-template-columns: auto 1fr auto;
  gap: 0.7rem;
  align-items: center;
  text-decoration: none;
  padding: 0.65rem 0.75rem;
}

.release-tag {
  font-family: var(--vp-font-family-mono);
  font-size: 0.82rem;
  color: var(--vp-c-brand);
  background: var(--vp-c-brand-subtle-strong);
  border-radius: 8px;
  padding: 0.18rem 0.4rem;
}

.release-name {
  color: var(--vp-c-text-1);
  font-weight: 500;
}

.release-meta {
  display: inline-flex;
  align-items: center;
  gap: 0.45rem;
  color: var(--vp-c-text-2);
  font-size: 0.84rem;
}

.release-badge {
  font-size: 0.74rem;
  border: 1px solid var(--vp-c-border);
  border-radius: 999px;
  padding: 0.1rem 0.4rem;
}

.release-loading,
.release-error {
  color: var(--vp-c-text-2);
  font-size: 0.95rem;
}

@media (max-width: 640px) {
  .release-link {
    grid-template-columns: 1fr;
    gap: 0.35rem;
  }

  .release-meta {
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    gap: 0.2rem;
  }
}
</style>
