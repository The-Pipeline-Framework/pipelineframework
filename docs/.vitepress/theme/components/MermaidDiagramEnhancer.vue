<!--
  Copyright (c) 2023-2025 Mariano Barcia

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

<script setup>
import {nextTick, onBeforeUnmount, onMounted, ref, watch} from 'vue'
import {useRoute} from 'vitepress'

const route = useRoute()
const activeSvg = ref('')
const activeTitle = ref('Diagram')
let mutationObserver
let enhanceTimer
let lastFocusedElement

function diagramTitleFor(stage) {
  let cursor = stage.previousElementSibling
  while (cursor) {
    if (/^H[1-6]$/.test(cursor.tagName)) {
      return cursor.textContent?.trim() || 'Diagram'
    }
    cursor = cursor.previousElementSibling
  }
  return 'Diagram'
}

function openDiagram(stage) {
  const svg = stage.querySelector('svg')
  if (!svg) {
    return
  }
  lastFocusedElement = document.activeElement instanceof HTMLElement ? document.activeElement : null
  activeTitle.value = diagramTitleFor(stage)
  activeSvg.value = svg.outerHTML
  document.documentElement.classList.add('tpf-mermaid-lightbox-open')
  nextTick(() => {
    document.querySelector('.tpf-mermaid-lightbox__close')?.focus()
  })
}

function closeDiagram() {
  activeSvg.value = ''
  document.documentElement.classList.remove('tpf-mermaid-lightbox-open')
  lastFocusedElement?.focus?.()
}

function handleStageKeydown(event, stage) {
  if (event.key !== 'Enter' && event.key !== ' ') {
    return
  }
  event.preventDefault()
  openDiagram(stage)
}

function enhanceDiagram(stage) {
  if (!stage.querySelector('svg')) {
    return
  }
  stage.querySelector(':scope > .tpf-mermaid-expand')?.remove()
  if (stage.dataset.tpfMermaidEnhanced !== 'true') {
    stage.addEventListener('click', () => openDiagram(stage))
    stage.addEventListener('keydown', (event) => handleStageKeydown(event, stage))
  }
  stage.dataset.tpfMermaidEnhanced = 'true'
  stage.classList.add('tpf-mermaid-stage')
  stage.tabIndex = 0
  stage.setAttribute('role', 'button')
  stage.setAttribute('aria-label', `Expand ${diagramTitleFor(stage)} diagram`)
  stage.title = 'Expand diagram'
}

function enhanceDiagrams() {
  clearTimeout(enhanceTimer)
  enhanceTimer = window.setTimeout(() => {
    document.querySelectorAll('.vp-doc .mermaid').forEach(enhanceDiagram)
  }, 80)
}

function handleKeydown(event) {
  if (event.key === 'Escape' && activeSvg.value) {
    closeDiagram()
  }
}

onMounted(() => {
  enhanceDiagrams()
  mutationObserver = new MutationObserver(enhanceDiagrams)
  mutationObserver.observe(document.body, {childList: true, subtree: true})
  document.addEventListener('keydown', handleKeydown)
})

onBeforeUnmount(() => {
  clearTimeout(enhanceTimer)
  mutationObserver?.disconnect()
  document.removeEventListener('keydown', handleKeydown)
  document.documentElement.classList.remove('tpf-mermaid-lightbox-open')
})

watch(
  () => route.path,
  () => nextTick(enhanceDiagrams)
)
</script>

<template>
  <Teleport to="body">
    <div
      v-if="activeSvg"
      class="tpf-mermaid-lightbox"
      role="dialog"
      aria-modal="true"
      :aria-label="`${activeTitle} expanded diagram`"
      @click.self="closeDiagram"
    >
      <div class="tpf-mermaid-lightbox__panel">
        <div class="tpf-mermaid-lightbox__header">
          <p>{{ activeTitle }}</p>
          <button
            type="button"
            class="tpf-mermaid-lightbox__close"
            title="Close diagram"
            aria-label="Close diagram"
            @click="closeDiagram"
          >
            Close
          </button>
        </div>
        <div class="tpf-mermaid-lightbox__stage" v-html="activeSvg"></div>
      </div>
    </div>
  </Teleport>
</template>
