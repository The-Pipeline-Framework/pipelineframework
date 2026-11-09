---
title: TPF Ecosystem Usage
description: Build-derived adoption statistics from reporting TPF installations, with weekly trends, versions, features and application structure.
sidebar: false
aside: false
---

<script setup>
import UsageDashboard from './UsageDashboard.vue'
import { data } from './usage.data.js'
</script>

# TPF Ecosystem Usage

Anonymous, build-derived statistics from reporting TPF installations. The census uses random
pseudonymous identifiers to count reports privately; the dashboard publishes only aggregates.

<UsageDashboard :snapshot="data.snapshot" :download="data.download" :synthetic="data.synthetic" />

## Methodology and privacy

Each reporting application instance is one original configuration path in one retained
installation. The same source project can count separately on different machines or in CI.
Each weekly total counts reporting instances once and installations with at least one accepted
report once. Distributions use the latest accepted observation for each instance that week.

Weeks run Monday to Sunday in UTC and close on Tuesday UTC to allow late Sunday reports.
Only complete eligible weeks are shown. A missing week is not zero, and a machine that
stops reporting may still use TPF. Builds that opt out, run offline or cannot retain
identity may be absent. These figures do not estimate the entire TPF population.

Counts are rounded independently to tens. Weeks and distribution cells require at least
20 contributing installations; a sparse cell suppresses the entire distribution. Feature
percentages use the displayed rounded counts: nonzero bands divided by all known bands.
Unknown values are excluded from that denominator. The rounded bands may not add up to
the headline total. Application structure is shown in bands, without estimated averages.

The census observes eligible compiler builds and collects no runtime activity. Disable it with
`TPF_TELEMETRY_DISABLED=true`. See the [census protocol and privacy runbook](https://github.com/The-Pipeline-Framework/pipelineframework-census/tree/145b54754ca301127491a23519aaa94d5adae939/docs),
the [observation schema](https://github.com/The-Pipeline-Framework/pipelineframework-census/blob/145b54754ca301127491a23519aaa94d5adae939/protocol/v1.schema.json),
and the [public aggregate schema](/ecosystem/schema/public-usage-v1.json).
