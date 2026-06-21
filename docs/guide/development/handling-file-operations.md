---
title: Redirecting...
search: false
head:
  - - meta
    - name: robots
      content: noindex
  - - meta
    - http-equiv: refresh
      content: 0;url=/develop/handling-file-operations
---

<script setup>
import {onMounted} from 'vue'
import {withBase} from 'vitepress'

onMounted(() => {
  if (typeof window !== 'undefined') {
    window.location.replace(withBase('/develop/handling-file-operations'))
  }
})
</script>

# Redirecting...

This page moved to [/develop/handling-file-operations](/develop/handling-file-operations).
