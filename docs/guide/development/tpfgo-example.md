---
title: Redirecting...
search: false
head:
  - - meta
    - name: robots
      content: noindex
  - - meta
    - http-equiv: refresh
      content: 0;url=/develop/tpfgo-example
---

<script setup>
import {onMounted} from 'vue'
import {withBase} from 'vitepress'

onMounted(() => {
  if (typeof window !== 'undefined') {
    window.location.replace(withBase('/develop/tpfgo-example'))
  }
})
</script>

# Redirecting...

This page moved to [/develop/tpfgo-example](/develop/tpfgo-example).
