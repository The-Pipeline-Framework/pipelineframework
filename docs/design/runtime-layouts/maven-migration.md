---
title: Redirecting...
search: false
head:
  - - meta
    - name: robots
      content: noindex
  - - meta
    - http-equiv: refresh
      content: 0;url=/deploy/runtime-layouts/maven-migration
---

<script setup>
import {onMounted} from 'vue'
import {withBase} from 'vitepress'

onMounted(() => {
  if (typeof window !== 'undefined') {
    window.location.replace(withBase('/deploy/runtime-layouts/maven-migration'))
  }
})
</script>

# Redirecting...

This page moved to [/deploy/runtime-layouts/maven-migration](/deploy/runtime-layouts/maven-migration).
