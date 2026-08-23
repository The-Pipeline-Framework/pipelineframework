<script setup>
import { data } from '../../../../value/coffee-machine/coffee-machine.data.js'
</script>

<template>
  <main class="coffee-app coffee-directory">
    <header class="coffee-page-header"><p class="coffee-eyebrow">The regulars</p><h1>Meet the Coffee Machine personas</h1><p>Familiar engineering worldviews, represented with affection and just enough skepticism.</p></header>
    <section
      v-for="persona in data.personas"
      :id="persona.id"
      :key="persona.id"
      class="coffee-persona-profile"
      :style="{ '--coffee-accent': persona.accent }"
    >
      <header><span aria-hidden="true">{{ persona.icon }}</span><div><h2>{{ persona.name }}</h2><p>{{ persona.description }}</p></div></header>
      <div class="coffee-persona-profile-body">
        <div><h3>Biography</h3><p>{{ persona.biography }}</p><h3>Typical worldview</h3><p>{{ persona.worldview }}</p></div>
        <div><h3>Favorite misconceptions</h3><ul><li v-for="item in persona.favoriteMisconceptions" :key="`${item.faqId}-${item.text}`"><a :href="item.route">“{{ item.text }}”</a></li></ul></div>
      </div>
      <details><summary>FAQs where {{ persona.name }} appears ({{ persona.appearances.length }})</summary><ul><li v-for="item in persona.appearances" :key="`${item.faqId}-${item.text}`"><a :href="item.route">{{ item.title }}</a></li></ul></details>
    </section>
  </main>
</template>
