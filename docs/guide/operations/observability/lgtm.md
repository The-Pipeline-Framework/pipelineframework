---
title: Redirecting...
search: false
head:
  - - meta
    - name: robots
      content: noindex
  - - meta
    - http-equiv: refresh
      content: 0;url=/operate/observability/lgtm
---

<script setup>
import {onMounted} from 'vue'
import {withBase} from 'vitepress'

onMounted(() => {
  if (typeof window !== 'undefined') {
    window.location.replace(withBase('/operate/observability/lgtm'))
  }
})
</script>

# Redirecting...

This page moved to [/operate/observability/lgtm](/operate/observability/lgtm).
