<script setup>
import { computed, onMounted, ref } from 'vue'
import { data } from '../../../../value/coffee-machine/coffee-machine.data.js'
import { normalizeSearch, searchableFaqText } from '../../coffee-machine/public-utils.js'

const query = ref('')
const topic = ref('')
const persona = ref('')
const personaById = new Map(data.personas.map((entry) => [entry.id, entry]))

const matchesFaq = (faq) => {
  if (topic.value && faq.track !== topic.value) return false
  if (persona.value && !faq.personas.some((entry) => entry.persona === persona.value)) return false
  const needle = normalizeSearch(query.value)
  return !needle || searchableFaqText(faq, personaById).includes(needle)
}

const faqs = computed(() => data.faqs.filter(matchesFaq))
const quotes = computed(() => {
  const needle = normalizeSearch(query.value)
  if (!needle || persona.value) return []
  return data.quotes.filter((quote) =>
    (!topic.value || quote.track === topic.value) &&
    normalizeSearch(`${quote.quote} ${quote.title}`).includes(needle)
  )
})
const personas = computed(() => {
  const needle = normalizeSearch(query.value)
  if (!needle || topic.value || persona.value) return []
  return data.personas.filter((entry) => normalizeSearch(`${entry.name} ${entry.description} ${entry.worldview} ${entry.biography}`).includes(needle))
})

onMounted(() => {
  const params = new URLSearchParams(window.location.search)
  const requestedTopic = params.get('topic') || ''
  const requestedPersona = params.get('persona') || ''
  if (data.topics.some((entry) => entry.id === requestedTopic)) topic.value = requestedTopic
  if (data.personas.some((entry) => entry.id === requestedPersona)) persona.value = requestedPersona
  query.value = params.get('q') || ''
})
</script>

<template>
  <main class="coffee-app coffee-directory">
    <header class="coffee-page-header"><p class="coffee-eyebrow">Wander deliberately</p><h1>Search the Coffee Machine</h1><p>Questions, quotes, personas, and tags—all derived from the FAQ source.</p></header>
    <form class="coffee-search" role="search" @submit.prevent>
      <label>Search<input v-model="query" type="search" placeholder="Try replay, Spring, or governance"></label>
      <label>Topic<select v-model="topic"><option value="">All topics</option><option v-for="entry in data.topics" :key="entry.id" :value="entry.id">{{ entry.name }}</option></select></label>
      <label>Persona<select v-model="persona"><option value="">All personas</option><option v-for="entry in data.personas" :key="entry.id" :value="entry.id">{{ entry.name }}</option></select></label>
    </form>

    <section class="coffee-section" aria-live="polite">
      <h2>Questions <small>{{ faqs.length }}</small></h2>
      <div v-if="faqs.length" class="coffee-faq-grid">
        <a v-for="faq in faqs" :key="faq.id" class="coffee-faq-card" :href="faq.route"><span>{{ data.topics.find((entry) => entry.id === faq.track)?.name }}</span><strong>{{ faq.question }}</strong><small>{{ faq.tags.join(' · ') }}</small></a>
      </div>
      <p v-else class="coffee-empty">No conversation matches those filters yet.</p>
    </section>

    <section v-if="quotes.length" class="coffee-section"><h2>Quotes <small>{{ quotes.length }}</small></h2><div class="coffee-quote-list"><blockquote v-for="quote in quotes" :key="quote.faqId"><p>“{{ quote.quote }}”</p><a :href="quote.route">{{ quote.title }}</a></blockquote></div></section>
    <section v-if="personas.length" class="coffee-section"><h2>Personas <small>{{ personas.length }}</small></h2><div class="coffee-faq-grid"><a v-for="entry in personas" :key="entry.id" class="coffee-faq-card" :href="`/value/coffee-machine/personas#${entry.id}`"><span aria-hidden="true">{{ entry.icon }}</span><strong>{{ entry.name }}</strong><small>{{ entry.description }}</small></a></div></section>
  </main>
</template>
