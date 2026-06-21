---
title: Redirecting...
search: false
head:
  - - meta
    - name: robots
      content: noindex
  - - meta
    - http-equiv: refresh
      content: 0;url=/develop/mappers-and-dtos
---

<script setup>
import {onMounted} from 'vue'
import {withBase} from 'vitepress'

onMounted(() => {
  if (typeof window !== 'undefined') {
    window.location.replace(withBase('/develop/mappers-and-dtos'))
  }
})
</script>

# Redirecting...

This page moved to [/develop/mappers-and-dtos](/develop/mappers-and-dtos).
