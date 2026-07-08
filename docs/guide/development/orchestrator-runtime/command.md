---
title: Redirecting...
search: false
head:
  - - meta
    - name: robots
      content: noindex
  - - meta
    - http-equiv: refresh
      content: 0;url=/deploy/orchestrator-runtime/command
---

<script setup>
import {onMounted} from 'vue'
import {withBase} from 'vitepress'

onMounted(() => {
  if (typeof window !== 'undefined') {
    window.location.replace(withBase('/deploy/orchestrator-runtime/command'))
  }
})
</script>

# Redirecting...

This page moved to [/deploy/orchestrator-runtime/command](/deploy/orchestrator-runtime/command).
