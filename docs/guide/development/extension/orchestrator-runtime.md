---
title: Redirecting...
search: false
head:
  - - meta
    - name: robots
      content: noindex
  - - meta
    - http-equiv: refresh
      content: 0;url=/develop/extension/orchestrator-runtime
---

<script setup>
import {onMounted} from 'vue'
import {withBase} from 'vitepress'

onMounted(() => {
  if (typeof window !== 'undefined') {
    window.location.replace(withBase('/develop/extension/orchestrator-runtime'))
  }
})
</script>

# Redirecting...

This page moved to [/develop/extension/orchestrator-runtime](/develop/extension/orchestrator-runtime).
