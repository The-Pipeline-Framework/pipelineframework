---
title: Redirecting...
---

<script setup>
import {onMounted} from 'vue'
import {withBase} from 'vitepress'

onMounted(() => {
  if (typeof window !== 'undefined') {
    window.setTimeout(() => {
      try {
        window.location.replace(withBase('/value/business-value'))
      } catch (error) {
        console.error('Redirect to /value/business-value failed', error)
      }
    }, 1500)
  }
})
</script>

# Redirecting...

<p aria-live="polite">Redirecting to the Business Value page…</p>

Business Value moved to [Value > Business Value](/value/business-value).
