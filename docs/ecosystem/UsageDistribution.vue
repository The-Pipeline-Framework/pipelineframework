<script setup>
import { computed } from 'vue'
import { adoption, displayCount, sortedBins } from './usage-display.js'

const props = defineProps({ title: String, distribution: Object, order: Array, feature: Boolean })
const bins = computed(() => sortedBins(props.distribution, props.order))
const max = computed(() => Math.max(1, ...bins.value.map(([, count]) => count)))
const percent = computed(() => props.feature ? adoption(props.distribution) : null)
</script>

<template>
  <div class="usage-distribution">
    <h3>{{ title }}</h3>
    <template v-if="bins.length">
      <p v-if="feature" class="adoption">
        <template v-if="percent !== null"><strong>≈{{ percent }}%</strong> of known reports use this</template>
        <template v-else>No known feature counts</template>
      </p>
      <table>
        <caption class="visually-hidden">{{ title }} distribution, rounded reporting instances</caption>
        <thead><tr><th scope="col">Value</th><th scope="col">Instances</th></tr></thead>
        <tbody>
          <tr v-for="[value, count] in bins" :key="value">
            <th scope="row">{{ value === 'unknown' ? 'Unknown' : value }}</th>
            <td>
              <span class="bin"><span>{{ displayCount(count) }}</span>
                <span class="track" aria-hidden="true"><span :style="{width: (100 * count / max) + '%'}"></span></span>
              </span>
            </td>
          </tr>
        </tbody>
      </table>
    </template>
    <p v-else class="muted">{{ distribution?.status === 'suppressed' ? 'Withheld to protect small cohorts.' : 'No published distribution for this week.' }}</p>
  </div>
</template>

<style scoped>
.usage-distribution { min-width: 0; padding: 1rem; border: 1px solid var(--vp-c-divider); border-radius: 12px; background: var(--vp-c-bg); }
.usage-distribution h3 { margin: 0 0 .5rem; font-size: 1rem; line-height: 1.35; }
.adoption { margin: 0 0 .5rem; font-size: .84rem; color: var(--vp-c-text-2); }
.adoption strong { font-size: 1.3rem; color: var(--vp-c-text-1); }
.muted { color: var(--vp-c-text-2); font-size: .88rem; margin-bottom: 0; }
table { display: table; width: 100%; margin: 0; font-size: .79rem; }
th, td { border: 0; padding: .23rem .25rem; background: none; text-align: left; overflow-wrap: anywhere; }
thead th { color: var(--vp-c-text-2); }
tbody th { font-weight: 500; }
tr { border-color: var(--vp-c-divider); }
tr:nth-child(2n) { background: none; }
td { width: 55%; }
.bin { display: grid; grid-template-columns: minmax(3.3em, auto) minmax(0, 1fr); align-items: center; gap: .35rem; font-variant-numeric: tabular-nums; }
.track { display: block; height: .35rem; overflow: hidden; border-radius: 4px; background: var(--vp-c-default-soft); }
.track > span { display: block; height: 100%; border-radius: 4px; background: var(--vp-c-brand-1); }
.visually-hidden { position: absolute; width: 1px; height: 1px; clip-path: inset(50%); overflow: hidden; white-space: nowrap; }
</style>
