---
title: Redirecting...
search: false
head:
  - - meta
    - name: robots
      content: noindex
  - - meta
    - http-equiv: refresh
      content: 0;url=/develop/extension/reactive-services
---

<script setup>
import {onMounted} from 'vue'
import {withBase} from 'vitepress'

onMounted(() => {
  if (typeof window !== 'undefined') {
    window.location.replace(withBase('/develop/extension/reactive-services'))
  }
})
</script>

# Redirecting...

This page moved to [/develop/extension/reactive-services](/develop/extension/reactive-services).
