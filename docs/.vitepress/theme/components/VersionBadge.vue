<!--
  - Copyright (c) 2023-2025 Mariano Barcia
  -
  - Licensed under the Apache License, Version 2.0 (the "License");
  - you may not use this file except in compliance with the License.
  - You may obtain a copy of the License at
  -
  -     http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing, software
  - distributed under the License is distributed on an "AS IS" BASIS,
  - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  - See the License for the specific language governing permissions and
  - limitations under the License.
  -->

<template>
  <div v-if="isVersioned" class="version-badge" role="status">
    <span class="version-badge__label">Archived Docs</span>
    <span class="version-badge__path">{{ versionLabel }}</span>
  </div>
</template>

<script setup>
import {computed} from 'vue'
import {useRoute} from 'vitepress'

const route = useRoute()
const isVersioned = computed(() => route.path.startsWith('/versions/'))
const versionLabel = computed(() => {
  if (!isVersioned.value) {
    return ''
  }
  const parts = route.path.split('/').filter(Boolean)
  return parts.length > 1 ? parts[1] : 'versioned'
})
</script>

<style scoped>
.version-badge {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin: 0 0 1.5rem;
  padding: 0.75rem 1rem;
  border-radius: 999px;
  border: 1px solid var(--vp-c-divider);
  background: var(--vp-c-bg-soft);
  color: var(--vp-c-text-1);
  font-size: 0.875rem;
}

.version-badge__label {
  font-weight: 600;
  letter-spacing: 0.01em;
  text-transform: uppercase;
}

.version-badge__path {
  font-variant-numeric: tabular-nums;
  color: var(--vp-c-text-2);
}
</style>
