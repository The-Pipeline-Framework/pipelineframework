---
title: Redirecting...
head:
  - - meta
    - http-equiv: refresh
      content: 2;url=/guide/development/using-plugins
---

<script setup>
import {onMounted, onUnmounted} from 'vue'

let redirectTimer

onMounted(() => {
  if (typeof window !== 'undefined') {
    redirectTimer = window.setTimeout(() => {
      window.location.replace('/guide/development/using-plugins')
    }, 1500)
  }
})

onUnmounted(() => {
  if (redirectTimer) {
    window.clearTimeout(redirectTimer)
  }
})
</script>

# Redirecting...

Using Plugins is available at [Development > Using Plugins](/guide/development/using-plugins).
