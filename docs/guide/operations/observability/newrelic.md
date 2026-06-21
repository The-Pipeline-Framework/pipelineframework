---
title: Redirecting...
search: false
head:
  - - meta
    - name: robots
      content: noindex
  - - meta
    - http-equiv: refresh
      content: 0;url=/operate/observability/newrelic
---

<script setup>
import {onMounted} from 'vue'
import {withBase} from 'vitepress'

onMounted(() => {
  if (typeof window !== 'undefined') {
    window.location.replace(withBase('/operate/observability/newrelic'))
  }
})
</script>

# Redirecting...

This page moved to [/operate/observability/newrelic](/operate/observability/newrelic).
