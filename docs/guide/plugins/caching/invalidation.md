---
title: Redirecting...
search: false
head:
  - - meta
    - name: robots
      content: noindex
  - - meta
    - http-equiv: refresh
      content: 0;url=/architecture/caching/invalidation
---

<script setup>
import {onMounted} from 'vue'
import {withBase} from 'vitepress'

onMounted(() => {
  if (typeof window !== 'undefined') {
    window.location.replace(withBase('/architecture/caching/invalidation'))
  }
})
</script>

# Redirecting...

This page moved to [/architecture/caching/invalidation](/architecture/caching/invalidation).
