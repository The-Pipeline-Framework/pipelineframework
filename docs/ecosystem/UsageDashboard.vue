<script setup>
import { computed, onMounted, ref } from 'vue'
import UsageDistribution from './UsageDistribution.vue'
import { bands, describeStatus, displayCount, hasCount, stale, weekEnd } from './usage-display.js'

const props = defineProps({ snapshot: Object, download: String, synthetic: Boolean })
const latest = computed(() => props.snapshot?.weeks.at(-1))
const distributions = computed(() => latest.value?.distributions)
const peak = computed(() => Math.max(1, ...(props.snapshot?.weeks.filter(hasCount).map(w => w.reporting_application_instances) ?? [])))
const isStale = ref(false)
onMounted(() => { isStale.value = Boolean(props.snapshot && stale(props.snapshot, new Date())) })

const groups = [
  { id: 'versions', title: 'Framework versions', intro: 'Packaged TPF component versions and the Java major running the compiler.',
    fields: [['Compiler version', 'compiler_version'], ['Contracts version', 'contracts_version'], ['Java major', 'java_major']] },
  { id: 'features', title: 'Feature adoption', intro: 'Approximate shares among known reports, with count bands where published.', group: 'feature_counts',
    fields: [['Nested pipeline invocations', 'nested_invocation', bands], ['ONE_TO_MANY', 'one_to_many', bands], ['MANY_TO_ONE', 'many_to_one', bands], ['Query', 'query', bands], ['Command', 'command', bands], ['Await', 'await', bands]] },
  { id: 'complexity', title: 'Application complexity', intro: 'Coarse distributions of authored application structure.', group: 'sizes',
    fields: [['Reachable pipeline definitions', 'pipelines', bands], ['Authored logical steps', 'steps', bands], ['Canonical type declarations', 'types', bands], ['Contributing Block packages', 'blocks', bands]] },
  { id: 'execution', title: 'Execution architecture', intro: 'Transport, platform and layout describe different aspects of a reporting instance.',
    fields: [['Transport', 'transport', ['LOCAL', 'REST', 'GRPC']], ['Platform', 'platform', ['COMPUTE', 'FUNCTION']], ['Runtime layout', 'runtime_layout', ['MODULAR', 'PIPELINE_RUNTIME', 'MONOLITH', 'UNSPECIFIED']]] }
]
function dimension(section, key) {
  return (section.group ? distributions.value?.[section.group] : distributions.value)?.[key]
}
</script>

<template>
  <div class="usage">
    <p v-if="synthetic" class="notice">Synthetic preview — these figures do not measure TPF adoption.</p>
    <div class="period">
      <p class="eyebrow">Build census · UTC ISO weeks</p>
      <template v-if="snapshot">
        <p><strong>Latest complete reporting week in this snapshot</strong>
          <br>
          <time :datetime="snapshot.latest_complete_week">{{ snapshot.latest_complete_week }}</time> to
          <time :datetime="weekEnd(snapshot.latest_complete_week)">{{ weekEnd(snapshot.latest_complete_week) }}</time>
        </p>
        <p class="subtle">Snapshot generated <time :datetime="snapshot.generated_at">{{ snapshot.generated_at.replace('T', ' ').replace(/\.\d{3}Z$/, ' UTC') }}</time>.
          <a v-if="download && !synthetic" :href="download">Download aggregate JSON</a>
        </p>
        <p v-if="isStale" class="notice">This snapshot is out of date. Showing the last published data.</p>
        <p v-if="!hasCount(latest)" class="notice">{{ describeStatus(latest) }} for the latest complete week.</p>
      </template>
      <p v-else><strong>No approved snapshot has been published.</strong> Statistics will appear once eligible data has been reviewed for publication.</p>
    </div>

    <section aria-labelledby="usage-overview">
      <h2 id="usage-overview">Overview</h2>
      <dl class="metrics">
        <div><dt>Reporting application instances</dt><dd>{{ displayCount(latest?.reporting_application_instances) }}</dd></div>
        <div><dt>Reporting installations</dt><dd>{{ displayCount(latest?.reporting_installations) }}</dd></div>
      </dl>
      <p class="subtle">Rounded counts for the latest complete reporting week. A dash means unavailable.</p>
    </section>

    <section aria-labelledby="usage-trend">
      <h2 id="usage-trend">Adoption over time</h2>
      <p class="subtle">Reporting application instances across 12 complete UTC weeks. Gaps show unavailable data.</p>
      <template v-if="snapshot">
        <div class="chart" aria-hidden="true">
          <div v-for="(week, i) in snapshot.weeks" :key="week.week_start" class="column">
            <div class="column-track">
              <span v-if="hasCount(week)" class="column-bar" :style="{height: (100 * week.reporting_application_instances / peak) + '%'}"></span>
              <span v-else class="column-gap">·</span>
            </div>
            <span class="tick">{{ i % 4 === 0 || i === 11 ? week.week_start.slice(5) : '' }}</span>
          </div>
        </div>
        <details>
          <summary>Weekly reporting totals (table)</summary>
          <table class="history">
            <thead><tr><th scope="col">Week beginning</th><th scope="col">Instances</th><th scope="col">Installations</th><th scope="col">Status</th></tr></thead>
            <tbody><tr v-for="week in snapshot.weeks" :key="week.week_start">
              <th scope="row"><time :datetime="week.week_start">{{ week.week_start }}</time></th>
              <td>{{ displayCount(week.reporting_application_instances) }}</td>
              <td>{{ displayCount(week.reporting_installations) }}</td>
              <td>{{ describeStatus(week) }}</td>
            </tr></tbody>
          </table>
        </details>
      </template>
      <p v-else>No weekly history is available yet.</p>
    </section>

    <section v-for="section in groups" :key="section.id" :aria-labelledby="'usage-' + section.id">
      <h2 :id="'usage-' + section.id">{{ section.title }}</h2>
      <p class="subtle">{{ section.intro }}</p>
      <div class="distributions">
        <UsageDistribution v-for="[label, key, order] in section.fields" :key="key" :title="label"
          :distribution="dimension(section, key)" :order="order" :feature="section.id === 'features'" />
      </div>
    </section>
  </div>
</template>

<style scoped>
.usage { margin: 1.5rem 0 2rem; }
.usage section { margin-top: 2rem; }
.period { background: var(--vp-c-bg-soft); border: 1px solid var(--vp-c-divider); border-radius: 14px; padding: 1rem 1.25rem; }
.period p { margin: .25rem 0; }
.eyebrow { color: var(--vp-c-brand-1); font-size: .75rem; font-weight: 700; text-transform: uppercase; letter-spacing: .08em; }
.subtle { color: var(--vp-c-text-2); font-size: .88rem; }
.notice { color: var(--vp-c-text-1); border-inline-start: 3px solid var(--vp-c-brand-1); padding-inline-start: .75rem; }
.metrics { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: .75rem; margin: 1rem 0 .5rem; }
.metrics div { background: var(--vp-c-bg-soft); border: 1px solid var(--vp-c-divider); border-radius: 12px; padding: .85rem 1rem; }
.metrics dt { font-weight: 600; font-size: .84rem; }
.metrics dd { font-variant-numeric: tabular-nums; margin: .2rem 0 0; font-size: 1.8rem; font-weight: 700; }
.distributions { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(100%, 230px), 1fr)); gap: .75rem; }
.chart { display: grid; grid-template-columns: repeat(12, minmax(0, 1fr)); gap: .28rem; padding: .5rem; border: 1px solid var(--vp-c-divider); border-radius: 12px; background: var(--vp-c-bg-soft); }
.column { min-width: 0; text-align: center; }
.column-track { height: 100px; border-bottom: 1px solid var(--vp-c-divider); display: flex; align-items: flex-end; justify-content: center; }
.column-bar { width: 68%; border-radius: 4px 4px 0 0; background: var(--vp-c-brand-1); }
.column-gap { color: var(--vp-c-text-2); }
.tick { display: block; font-size: .65rem; color: var(--vp-c-text-2); height: 1.2rem; line-height: 1.2rem; white-space: nowrap; }
details { margin-top: .7rem; }
summary { cursor: pointer; color: var(--vp-c-brand-1); font-weight: 600; }
.history { font-size: .84rem; max-width: 100%; }
.history th, .history td { padding: .3rem .45rem; }
@media (max-width: 390px) {
  .metrics { grid-template-columns: 1fr; }
  .chart { gap: .15rem; }
  .tick { font-size: .56rem; }
}
</style>
