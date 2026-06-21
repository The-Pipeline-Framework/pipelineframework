---
title: Redirecting...
search: false
head:
  - - meta
    - name: robots
      content: noindex
  - - meta
    - http-equiv: refresh
      content: 0;url=/deploy/runtime-layouts/pom-layout-matrix
---

<script setup>
import {onMounted} from 'vue'
import {withBase} from 'vitepress'

onMounted(() => {
  if (typeof window !== 'undefined') {
    window.location.replace(withBase('/deploy/runtime-layouts/pom-layout-matrix'))
  }
})
</script>

# Redirecting...

This page moved to [/deploy/runtime-layouts/pom-layout-matrix](/deploy/runtime-layouts/pom-layout-matrix).
