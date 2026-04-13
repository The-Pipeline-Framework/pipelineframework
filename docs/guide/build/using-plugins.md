---
title: Redirecting...
head:
  - - meta
    - http-equiv: refresh
      content: 2;url=/guide/development/using-plugins
---

<script setup>
import {onMounted} from 'vue'

onMounted(() => {
  if (typeof window !== 'undefined') {
    window.setTimeout(() => {
      window.location.replace('/guide/development/using-plugins')
    }, 1500)
  }
})
</script>

# Redirecting...

Using Plugins is available at [Development > Using Plugins](/guide/development/using-plugins).
