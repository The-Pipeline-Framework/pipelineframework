---
title: Redirecting...
search: false
head:
  - - meta
    - name: robots
      content: noindex
  - - meta
    - http-equiv: refresh
      content: 0;url=/deploy/csv-payments-pom-lifecycle
---

<script setup>
import {onMounted} from 'vue'
import {withBase} from 'vitepress'

onMounted(() => {
  if (typeof window !== 'undefined') {
    window.location.replace(withBase('/deploy/csv-payments-pom-lifecycle'))
  }
})
</script>

# Redirecting...

This page moved to [/deploy/csv-payments-pom-lifecycle](/deploy/csv-payments-pom-lifecycle).
