<script setup>
import { computed, ref } from 'vue'
import { data } from '../../../../value/coffee-machine/coffee-machine.data.js'
import { normalizeSearch } from '../../coffee-machine/public-utils.js'

const query = ref('')
const groups = computed(() => {
  const needle = normalizeSearch(query.value)
  return data.topics.map((topic) => ({
    topic,
    quotes: data.quotes.filter((quote) => quote.track === topic.id && normalizeSearch(`${quote.quote} ${quote.title}`).includes(needle))
  })).filter((group) => group.quotes.length)
})
</script>

<template>
  <main class="coffee-app coffee-directory">
    <header class="coffee-page-header"><p class="coffee-eyebrow">Pull quotes</p><h1>Fortunes from the Coffee Machine</h1><p>The lines worth carrying into the next architecture discussion.</p></header>
    <label class="coffee-search coffee-search--single">Search quotes<input v-model="query" type="search" placeholder="Filter quotes by phrase or FAQ"></label>
    <section v-for="group in groups" :key="group.topic.id" class="coffee-section">
      <h2><span aria-hidden="true">{{ group.topic.icon }}</span> {{ group.topic.name }}</h2>
      <div class="coffee-quote-list"><blockquote v-for="quote in group.quotes" :key="quote.faqId"><p>“{{ quote.quote }}”</p><a :href="quote.route">{{ quote.title }}</a></blockquote></div>
    </section>
    <p v-if="!groups.length" class="coffee-empty">No quote matches that search.</p>
  </main>
</template>
