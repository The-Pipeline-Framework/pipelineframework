/*
 * Copyright (c) 2023-2025 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// .vitepress/theme/index.js
import DefaultTheme from 'vitepress/theme'
import {h} from 'vue'
import './custom.css'
import Callout from './components/Callout.vue'
import HeroSection from './components/HeroSection.vue'
import VersionBadge from './components/VersionBadge.vue'
import LatestReleases from './components/LatestReleases.vue'

export default {
  ...DefaultTheme,
  Layout() {
    return h(DefaultTheme.Layout, null, {
      'doc-before': () => h(VersionBadge)
    })
  },
  enhanceApp({ app }) {
    // Register custom components
    app.component('Callout', Callout)
    app.component('HeroSection', HeroSection)
    app.component('LatestReleases', LatestReleases)
  }
}
