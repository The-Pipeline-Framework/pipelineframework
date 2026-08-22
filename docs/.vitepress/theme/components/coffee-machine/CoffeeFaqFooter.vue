<script setup>
import { computed, onMounted, ref, watch } from 'vue'
import { useData } from 'vitepress'
import { data } from '../../../../value/coffee-machine/coffee-machine.data.js'
import { chooseDifferentIndex } from '../../coffee-machine/public-utils.js'

const { frontmatter } = useData()
const quoteIndex = ref(0)
const faqId = computed(() => frontmatter.value.faq?.id)
const currentIndex = computed(() => data.faqs.findIndex((faq) => faq.id === faqId.value))
const current = computed(() => data.faqs[currentIndex.value])
const faqById = new Map(data.faqs.map((faq) => [faq.id, faq]))
const related = computed(() => (current.value?.related || []).map((id) => faqById.get(id)).filter(Boolean))
const previous = computed(() => currentIndex.value > 0 ? data.faqs[currentIndex.value - 1] : undefined)
const next = computed(() => currentIndex.value >= 0 && currentIndex.value < data.faqs.length - 1 ? data.faqs[currentIndex.value + 1] : undefined)
const quote = computed(() => data.quotes[quoteIndex.value])

function randomizeQuote() {
  quoteIndex.value = chooseDifferentIndex(data.quotes.length, quoteIndex.value)
}

onMounted(randomizeQuote)
watch(faqId, randomizeQuote)
</script>

<template>
  <footer v-if="current" class="coffee-faq-footer">
    <section v-if="related.length">
      <h2>Related questions</h2>
      <ul>
        <li v-for="faq in related" :key="faq.id"><a :href="faq.route">{{ faq.question }}</a></li>
      </ul>
    </section>
    <section v-if="quote" class="coffee-faq-footer__quote">
      <h2>Another thought over coffee</h2>
      <blockquote>“{{ quote.quote }}”</blockquote>
      <a :href="quote.route">Open the conversation</a>
    </section>
    <nav class="coffee-faq-nav" aria-label="Coffee Machine FAQ navigation">
      <a v-if="previous" :href="previous.route"><span>Previous</span>{{ previous.question }}</a>
      <span v-else />
      <a v-if="next" :href="next.route"><span>Next</span>{{ next.question }}</a>
    </nav>
  </footer>
</template>
