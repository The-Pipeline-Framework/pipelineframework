---
title: Redirecting...
search: false
head:
  - - meta
    - name: robots
      content: noindex
  - - meta
    - http-equiv: refresh
      content: 0;url=/design/operator-reuse-strategy
---

<script setup>
import {onMounted} from 'vue'
import {withBase} from 'vitepress'

onMounted(() => {
  if (typeof window !== 'undefined') {
    window.location.replace(withBase('/design/operator-reuse-strategy'))
  }
})
</script>

# Redirecting...

This page moved to [/design/operator-reuse-strategy](/design/operator-reuse-strategy).
