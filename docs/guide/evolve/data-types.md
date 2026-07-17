---
title: Redirecting...
search: false
head:
  - - meta
    - name: robots
      content: noindex
  - - meta
    - http-equiv: refresh
      content: 0;url=../../../develop/pipeline-template-dsl
---

<script setup>
import {onMounted} from 'vue'
import {withBase} from 'vitepress'

onMounted(() => {
  if (typeof window !== 'undefined') {
    window.location.replace(withBase('/develop/pipeline-template-dsl'))
  }
})
</script>

# Redirecting...

This page moved to <a href="../../../develop/pipeline-template-dsl">/develop/pipeline-template-dsl</a>.
