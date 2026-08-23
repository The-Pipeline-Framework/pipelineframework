<script setup>
import { computed } from 'vue'
import { useData } from 'vitepress'
import { data } from '../../../../value/coffee-machine/coffee-machine.data.js'

const { frontmatter } = useData()
const personaById = new Map(data.personas.map((persona) => [persona.id, persona]))
const statements = computed(() => frontmatter.value.coffeeMachine?.personas || [])
</script>

<template>
  <section v-if="statements.length" class="coffee-misconceptions" aria-labelledby="coffee-misconceptions-title">
    <h2 id="coffee-misconceptions-title">
      {{ statements.length }} coffee-machine misconception{{ statements.length === 1 ? '' : 's' }}
    </h2>
    <div class="coffee-misconceptions-grid">
      <article
        v-for="(statement, index) in statements"
        :key="`${statement.persona}-${index}`"
        class="coffee-persona-card"
        :style="{ '--coffee-accent': personaById.get(statement.persona)?.accent }"
      >
        <div class="coffee-persona-card-identity">
          <span aria-hidden="true">{{ personaById.get(statement.persona)?.icon }}</span>
          <strong>{{ personaById.get(statement.persona)?.name }}</strong>
        </div>
        <p>“{{ statement.text }}”</p>
      </article>
    </div>
  </section>
</template>
