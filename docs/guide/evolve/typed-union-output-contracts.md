---
title: Redirecting...
search: false
head:
  - - meta
    - name: robots
      content: noindex
  - - meta
    - http-equiv: refresh
      content: 0;url=../../../develop/pipeline-template/types
---

<script setup>
import {onMounted} from 'vue'
import {withBase} from 'vitepress'

onMounted(() => {
  if (typeof window !== 'undefined') {
    window.location.replace(withBase('/develop/pipeline-template/types'))
  }
})
</script>

# Redirecting...

This page moved to <a href="../../../develop/pipeline-template/types">Canonical Types</a>.
