---
title: Redirecting...
search: false
head:
  - - meta
    - name: robots
      content: noindex
  - - meta
    - http-equiv: refresh
      content: 0;url=/architecture/caching/cache-vs-persistence
---

<script setup>
import {onMounted} from 'vue'
import {withBase} from 'vitepress'

onMounted(() => {
  if (typeof window !== 'undefined') {
    window.location.replace(withBase('/architecture/caching/cache-vs-persistence'))
  }
})
</script>

# Redirecting...

This page moved to [/architecture/caching/cache-vs-persistence](/architecture/caching/cache-vs-persistence).
