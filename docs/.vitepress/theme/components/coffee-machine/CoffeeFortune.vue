<script setup>
import { onMounted, ref } from 'vue'
import { chooseDifferentIndex } from '../../coffee-machine/public-utils.js'

const props = defineProps({
  quotes: { type: Array, required: true }
})

const currentIndex = ref(0)

function anotherCoffee() {
  currentIndex.value = chooseDifferentIndex(props.quotes.length, currentIndex.value)
}

onMounted(anotherCoffee)
</script>

<template>
  <section v-if="quotes.length" class="coffee-fortune" aria-live="polite">
    <div class="coffee-terminal"><span aria-hidden="true">$</span> fortune tpf</div>
    <blockquote>“{{ quotes[currentIndex].quote }}”</blockquote>
    <div class="coffee-fortune-actions">
      <a :href="quotes[currentIndex].route">Open the conversation</a>
      <button type="button" class="coffee-button" @click="anotherCoffee">Another coffee.</button>
    </div>
  </section>
</template>
