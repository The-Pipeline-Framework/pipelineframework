---
title: Redirecting...
search: false
head:
  - - meta
    - name: robots
      content: noindex
  - - meta
    - http-equiv: refresh
      content: 0;url=/design/caching/replay-walkthrough
---

<script setup>
import {onMounted} from 'vue'
import {withBase} from 'vitepress'

onMounted(() => {
  if (typeof window !== 'undefined') {
    window.location.replace(withBase('/design/caching/replay-walkthrough'))
  }
})
</script>

# Redirecting...

This page moved to [/design/caching/replay-walkthrough](/design/caching/replay-walkthrough).
