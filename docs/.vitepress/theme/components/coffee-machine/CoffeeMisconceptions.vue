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
    <h2 id="coffee-misconceptions-title">Three coffee-machine misconceptions</h2>
    <div class="coffee-misconceptions__grid">
      <article
        v-for="(statement, index) in statements"
        :key="`${statement.persona}-${index}`"
        class="coffee-persona-card"
        :style="{ '--coffee-accent': personaById.get(statement.persona)?.accent }"
      >
        <div class="coffee-persona-card__identity">
          <span aria-hidden="true">{{ personaById.get(statement.persona)?.icon }}</span>
          <strong>{{ personaById.get(statement.persona)?.name }}</strong>
        </div>
        <p>“{{ statement.text }}”</p>
      </article>
    </div>
  </section>
</template>
