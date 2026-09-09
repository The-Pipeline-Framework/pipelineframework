<script setup>
import glossary from '../../glossary.json'

const entries = Object.entries(glossary)
  .sort(([left], [right]) => left.localeCompare(right, 'en-GB'))

const anchor = (term) => term
  .toLowerCase()
  .replace(/[^a-z0-9]+/g, '-')
  .replace(/^-|-$/g, '')
</script>

<template>
  <dl class="tpf-glossary">
    <template v-for="[term, definition] in entries" :key="term">
      <dt :id="anchor(term)">
        <a class="header-anchor" :href="`#${anchor(term)}`" :aria-label="`Permalink to ${term}`">#</a>
        {{ term }}
      </dt>
      <dd>{{ definition }}</dd>
    </template>
  </dl>
</template>

<style scoped>
.tpf-glossary dt {
  margin-top: 1.4rem;
  color: var(--vp-c-text-1);
  font-size: 1.05rem;
  font-weight: 700;
}

.tpf-glossary dd {
  margin: 0.25rem 0 0;
  color: var(--vp-c-text-2);
}

.header-anchor {
  margin-left: -1rem;
  padding-right: 0.25rem;
  color: var(--vp-c-brand-1);
  opacity: 0;
}

dt:hover .header-anchor {
  opacity: 1;
}
</style>
