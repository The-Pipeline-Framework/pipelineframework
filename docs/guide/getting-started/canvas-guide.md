---
title: Redirecting...
search: false
head:
  - - meta
    - name: robots
      content: noindex
  - - meta
    - http-equiv: refresh
      content: 0;url=/design/pipeline-studio/canvas-guide
---

<script setup>
import {onMounted} from 'vue'
import {withBase} from 'vitepress'

onMounted(() => {
  if (typeof window !== 'undefined') {
    window.location.replace(withBase('/design/pipeline-studio/canvas-guide'))
  }
})
</script>

# Redirecting...

This page moved to [/design/pipeline-studio/canvas-guide](/design/pipeline-studio/canvas-guide).
