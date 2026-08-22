<script setup>
import { computed, onMounted, ref } from 'vue'
import { data } from '../../../../value/coffee-machine/coffee-machine.data.js'
import CoffeeFortune from './CoffeeFortune.vue'

const selectedPersona = ref('')
const debateIndex = ref(0)
const debates = data.faqs.flatMap((faq) => faq.personas.map((statement) => ({ ...statement, faq })))
const personaById = new Map(data.personas.map((persona) => [persona.id, persona]))

const recentFaqs = [...data.faqs]
  .sort((left, right) => right.added.localeCompare(left.added) || left.question.localeCompare(right.question, 'en'))
  .slice(0, 6)

const personaFaqs = computed(() => selectedPersona.value
  ? data.faqs.filter((faq) => faq.personas.some((entry) => entry.persona === selectedPersona.value))
  : [])

onMounted(() => {
  debateIndex.value = Math.floor(Math.random() * debates.length)
})
</script>

<template>
  <main class="coffee-app">
    <header class="coffee-hero">
      <p class="coffee-eyebrow">The conference break that keeps going</p>
      <h1>The Coffee Machine</h1>
      <p>Real architecture questions, recognizable engineering instincts, and answers that admit the trade-offs.</p>
      <a class="coffee-button" href="/value/coffee-machine/search">Search the conversations</a>
    </header>

    <section class="coffee-section" aria-labelledby="coffee-topics">
      <div class="coffee-section__heading"><div><p class="coffee-eyebrow">Browse</p><h2 id="coffee-topics">By topic</h2></div></div>
      <div class="coffee-topic-grid">
        <a v-for="topic in data.topics" :key="topic.id" class="coffee-topic-card" :href="`/value/coffee-machine/search?topic=${topic.id}`">
          <span class="coffee-topic-card__icon" aria-hidden="true">{{ topic.icon }}</span>
          <strong>{{ topic.name }}</strong>
          <span>{{ topic.description }}</span>
          <small>{{ topic.count }} questions</small>
        </a>
      </div>
    </section>

    <section class="coffee-section coffee-two-column">
      <div><p class="coffee-eyebrow">Random quote</p><h2>Fortune from the machine</h2><CoffeeFortune :quotes="data.quotes" /></div>
      <div v-if="debates.length" class="coffee-debate">
        <p class="coffee-eyebrow">Today’s debate</p>
        <h2>What trade-off is this missing?</h2>
        <article :style="{ '--coffee-accent': personaById.get(debates[debateIndex].persona)?.accent }">
          <div class="coffee-persona-card__identity"><span aria-hidden="true">{{ personaById.get(debates[debateIndex].persona)?.icon }}</span><strong>{{ personaById.get(debates[debateIndex].persona)?.name }}</strong></div>
          <blockquote>“{{ debates[debateIndex].text }}”</blockquote>
          <a :href="debates[debateIndex].faq.route">Join the conversation</a>
        </article>
      </div>
    </section>

    <section class="coffee-section">
      <div class="coffee-section__heading"><div><p class="coffee-eyebrow">Fresh pot</p><h2>Recently added FAQs</h2></div><a href="/value/coffee-machine/search">Browse all</a></div>
      <div class="coffee-faq-grid">
        <a v-for="faq in recentFaqs" :key="faq.id" class="coffee-faq-card" :href="faq.route"><span>{{ data.topics.find((topic) => topic.id === faq.track)?.name }}</span><strong>{{ faq.question }}</strong></a>
      </div>
    </section>

    <section class="coffee-section" aria-labelledby="coffee-personas">
      <div class="coffee-section__heading"><div><p class="coffee-eyebrow">Choose a chair</p><h2 id="coffee-personas">Browse by persona</h2></div><a href="/value/coffee-machine/personas">Meet everyone</a></div>
      <div class="coffee-persona-picker">
        <button
          v-for="persona in data.personas"
          :key="persona.id"
          type="button"
          :class="{ active: selectedPersona === persona.id }"
          :style="{ '--coffee-accent': persona.accent }"
          @click="selectedPersona = selectedPersona === persona.id ? '' : persona.id"
        ><span aria-hidden="true">{{ persona.icon }}</span>{{ persona.name }}</button>
      </div>
      <div v-if="selectedPersona" class="coffee-faq-grid coffee-persona-results" aria-live="polite">
        <a v-for="faq in personaFaqs" :key="faq.id" class="coffee-faq-card" :href="faq.route"><strong>{{ faq.question }}</strong></a>
      </div>
    </section>
  </main>
</template>
