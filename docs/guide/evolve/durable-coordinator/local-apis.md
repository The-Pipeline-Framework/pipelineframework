---
title: Redirecting...
search: false
head:
  - - meta
    - name: robots
      content: noindex
  - - meta
    - http-equiv: refresh
      content: 0;url=/evolve/durable-coordinator/local-apis
---

<script setup>
import {onMounted} from 'vue'
import {withBase} from 'vitepress'

onMounted(() => {
  if (typeof window !== 'undefined') {
    window.location.replace(withBase('/evolve/durable-coordinator/local-apis'))
  }
})
</script>

# Redirecting...

This page moved to [/evolve/durable-coordinator/local-apis](/evolve/durable-coordinator/local-apis).
