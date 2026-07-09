---
title: Redirecting...
search: false
head:
  - - meta
    - name: robots
      content: noindex
  - - meta
    - http-equiv: refresh
      content: 0;url=/deploy/runtime-layouts/csv-payments-monolith
---

<script setup>
import {onMounted} from 'vue'
import {withBase} from 'vitepress'

onMounted(() => {
  if (typeof window !== 'undefined') {
    window.location.replace(withBase('/deploy/runtime-layouts/csv-payments-monolith'))
  }
})
</script>

# Redirecting...

This page moved to [/deploy/runtime-layouts/csv-payments-monolith](/versions/v26.7.1/deploy/runtime-layouts/csv-payments-monolith).
