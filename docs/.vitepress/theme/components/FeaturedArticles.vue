<template>
  <section class="articles-panel" aria-labelledby="latest-articles-title">
    <div class="articles-head">
      <h2 id="latest-articles-title">Latest Articles</h2>
      <a class="all-articles" :href="feedUrl" target="_blank" rel="noopener noreferrer">
        View feed
      </a>
    </div>

    <p class="articles-subtitle">
      Recent DEV Community posts tagged <code>tpf</code>.
    </p>

    <div v-if="loading" class="article-loading" aria-live="polite">
      Loading articles...
    </div>

    <div v-else-if="error" class="article-error" aria-live="polite">
      Could not load articles right now.
      <a :href="feedUrl" target="_blank" rel="noopener noreferrer">Open feed</a>.
    </div>

    <div v-else-if="articles.length === 0" class="article-empty" aria-live="polite">
      No tagged articles are available right now.
      <a :href="feedUrl" target="_blank" rel="noopener noreferrer">Open feed</a>.
    </div>

    <ul v-else class="article-list">
      <li v-for="article in articles" :key="article.id" class="article-item">
        <a :href="article.url" target="_blank" rel="noopener noreferrer" class="article-link">
          <span class="article-title">{{ article.title }}</span>
          <span class="article-meta">
            <span v-if="article.author">{{ article.author }}</span>
            <time v-if="article.published_at" :datetime="article.published_at">
              {{ formatDate(article.published_at) }}
            </time>
          </span>
        </a>
      </li>
    </ul>
  </section>
</template>

<script setup>
import {onMounted, ref} from 'vue'

const articles = ref([])
const loading = ref(true)
const error = ref(false)

const feedUrl = 'https://dev.to/feed/tag/tpf'
const articlesApiUrl = '/api/devto-feed?limit=10'
const cacheTtlMs = 15 * 60 * 1000
const cacheKey = `tpf:articles:${articlesApiUrl}`

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
          articles.value = parsed.data
          return
        }
      }
    } catch (_) {
      sessionStorage.removeItem(cacheKey)
    }

    const response = await fetch(articlesApiUrl)
    if (!response.ok) {
      throw new Error(`Feed API returned ${response.status}`)
    }

    const data = await response.json()
    articles.value = Array.isArray(data) ? data.slice(0, 3) : []
    sessionStorage.setItem(cacheKey, JSON.stringify({
      timestamp: Date.now(),
      data: articles.value
    }))
  } catch (_) {
    error.value = true
  } finally {
    loading.value = false
  }
})
</script>

<style scoped>
.articles-panel {
  margin: 1.4rem 0 2rem;
  padding: 1rem 1.1rem;
  border: 1px solid var(--vp-c-border);
  border-radius: 14px;
  background:
    radial-gradient(circle at top left, var(--vp-c-brand-subtle), transparent 52%),
    var(--vp-c-bg);
}

.articles-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 0.8rem;
}

.articles-head h2 {
  margin: 0;
  border: 0;
  padding: 0;
  font-size: 1.2rem;
  color: var(--vp-c-text-1);
}

.all-articles {
  font-weight: 600;
  font-size: 0.95rem;
}

.articles-subtitle {
  margin: 0.45rem 0 0.9rem;
  color: var(--vp-c-text-2);
}

.articles-subtitle code {
  font-size: 0.88em;
}

.article-list {
  list-style: none;
  margin: 0;
  padding: 0;
  display: grid;
  gap: 0.55rem;
}

.article-item {
  border: 1px solid var(--vp-c-border);
  border-radius: 10px;
  transition: border-color 180ms ease, transform 180ms ease;
}

.article-item:hover {
  border-color: var(--vp-c-brand-light);
  transform: translateY(-1px);
}

.article-link {
  display: grid;
  gap: 0.35rem;
  text-decoration: none;
  padding: 0.75rem;
}

.article-title {
  color: var(--vp-c-text-1);
  font-weight: 600;
}

.article-meta {
  display: inline-flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 0.55rem;
  color: var(--vp-c-text-2);
  font-size: 0.84rem;
}

.article-loading,
.article-empty,
.article-error {
  color: var(--vp-c-text-2);
  font-size: 0.95rem;
}

@media (max-width: 640px) {
  .article-meta {
    flex-direction: column;
    align-items: flex-start;
    gap: 0.2rem;
  }
}
</style>
