---
title: Redirecting...
search: false
head:
  - - meta
    - name: robots
      content: noindex
  - - meta
    - http-equiv: refresh
      content: 0;url=/architecture/common-module-structure
---

<script setup>
import {onMounted} from 'vue'
import {withBase} from 'vitepress'

onMounted(() => {
  if (typeof window !== 'undefined') {
    window.location.replace(withBase('/architecture/common-module-structure'))
  }
})
</script>

# Redirecting...

This page moved to [/architecture/common-module-structure](/architecture/common-module-structure).
