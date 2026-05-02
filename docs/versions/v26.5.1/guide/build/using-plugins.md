---
title: Redirecting...
head:
  - - meta
    - http-equiv: refresh
      content: 2;url=/versions/v26.5.1/guide/development/using-plugins
search: false
---

<script setup>
import {onMounted, onUnmounted} from 'vue'

let redirectTimer

onMounted(() => {
  if (typeof window !== 'undefined') {
    redirectTimer = window.setTimeout(() => {
      window.location.replace('/versions/v26.5.1/guide/development/using-plugins')
    }, 1500)
  }
})

onUnmounted(() => {
  if (redirectTimer != null) {
    window.clearTimeout(redirectTimer)
  }
})
</script>

# Redirecting...

Using Plugins is available at [Development > Using Plugins](/versions/v26.5.1/guide/development/using-plugins).
