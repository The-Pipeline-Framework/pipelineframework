---
title: Redirecting...
search: false
head:
  - - meta
    - name: robots
      content: noindex
  - - meta
    - http-equiv: refresh
      content: 0;url=/deploy/google-cloud-run-functions
---

<script setup>
import {onMounted} from 'vue'
import {withBase} from 'vitepress'

onMounted(() => {
  if (typeof window !== 'undefined') {
    window.location.replace(withBase('/deploy/google-cloud-run-functions'))
  }
})
</script>

# Redirecting...

This page moved to [/deploy/google-cloud-run-functions](/deploy/google-cloud-run-functions).
