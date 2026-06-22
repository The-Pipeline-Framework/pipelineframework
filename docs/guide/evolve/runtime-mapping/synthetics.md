---
title: Redirecting...
search: false
head:
  - - meta
    - name: robots
      content: noindex
  - - meta
    - http-equiv: refresh
      content: 0;url=/evolve/runtime-mapping/synthetics
---

<script setup>
import {onMounted} from 'vue'
import {withBase} from 'vitepress'

onMounted(() => {
  if (typeof window !== 'undefined') {
    window.location.replace(withBase('/evolve/runtime-mapping/synthetics'))
  }
})
</script>

# Redirecting...

This page moved to [/evolve/runtime-mapping/synthetics](/evolve/runtime-mapping/synthetics).
