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
import './mermaid.css'
import Callout from './components/Callout.vue'
import FeaturedArticles from './components/FeaturedArticles.vue'
import HeroSection from './components/HeroSection.vue'
import VersionBadge from './components/VersionBadge.vue'
import LatestReleases from './components/LatestReleases.vue'
import MermaidDiagramEnhancer from './components/MermaidDiagramEnhancer.vue'
import SidebarAccordion from './components/SidebarAccordion.vue'

function installReplayViewerHardNavigation() {
  if (typeof window === 'undefined' || window.__tpfReplayViewerHardNavInstalled) {
    return
  }
  window.__tpfReplayViewerHardNavInstalled = true
  const replayViewerPaths = new Set(['/replay-viewer/', '/replay-viewer/index.html'])
  window.addEventListener('click', (event) => {
    if (event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) {
      return
    }
    const anchor = event.target instanceof Element ? event.target.closest('a[href]') : null
    if (!anchor) {
      return
    }
    const href = anchor.getAttribute('href')
    if (!replayViewerPaths.has(href)) {
      return
    }
    event.preventDefault()
    event.stopImmediatePropagation()
    window.location.assign('/replay-viewer/')
  }, true)

  window.addEventListener('popstate', () => {
    if (window.location.pathname.startsWith('/replay-viewer/')) {
      window.location.replace('/replay-viewer/')
    }
  })
}

export default {
  ...DefaultTheme,
  Layout() {
    return h('div', [
      h(DefaultTheme.Layout, null, {
        'doc-before': () => h(VersionBadge)
      }),
      h(SidebarAccordion),
      h(MermaidDiagramEnhancer)
    ])
  },
  enhanceApp({ app }) {
    // Register custom components
    app.component('Callout', Callout)
    app.component('FeaturedArticles', FeaturedArticles)
    app.component('HeroSection', HeroSection)
    app.component('LatestReleases', LatestReleases)
    installReplayViewerHardNavigation()
  }
}
