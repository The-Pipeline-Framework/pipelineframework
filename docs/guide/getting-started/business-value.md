---
title: Redirecting...
---

<script setup>
import {onMounted, onUnmounted} from 'vue'
import {withBase} from 'vitepress'

let redirectTimer

onMounted(() => {
  if (typeof window !== 'undefined') {
    redirectTimer = window.setTimeout(() => {
      try {
        window.location.replace(withBase('/value/business-value'))
      } catch (error) {
        console.error('Redirect to /value/business-value failed', error)
      }
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

<p aria-live="polite">Redirecting to the Business Value page…</p>

Business Value moved to [Value > Business Value](/value/business-value).
