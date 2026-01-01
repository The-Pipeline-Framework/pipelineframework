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

import {defineConfig} from 'vitepress'
import {withMermaid} from "vitepress-plugin-mermaid"

// Use withMermaid to wrap the entire configuration - this enables GitHub-style mermaid code blocks
// Note: This adds significant size to the bundle due to Mermaid's dependencies
export default withMermaid(
  defineConfig({
    title: 'The Pipeline Framework',
    description: 'A framework for building reactive pipeline processing systems',
    
    // Disable dead links check since we're only documenting the pipeline framework
    ignoreDeadLinks: true,
    
    // Base URL for the site (can be changed for different deployments)
    base: '/',
    
    // Register custom theme
    themeConfig: {
        nav: [
            {text: 'Home', link: '/'},
            {text: 'Guides', link: '/guide/'},
            {text: 'Evolve', link: '/guide/evolve/'},
            {text: 'Versions', link: '/versions'}
        ],

        sidebar: [
            {
                text: 'Build Fast',
                items: [
                    {text: 'Overview', link: '/guide/'},
                    {text: 'Quick Start', link: '/guide/getting-started/quick-start'},
                    {text: 'Canvas Guide', link: '/guide/getting-started/canvas-guide'},
                    {text: 'Business Value', link: '/guide/getting-started/business-value'}
                ]
            },
            {
                text: 'Functional Architecture',
                items: [
                    {text: 'Application Structure', link: '/guide/application/application-structure'},
                    {text: 'Common Module Structure', link: '/guide/application/common-module-structure'},
                    {text: 'Pipeline Compilation', link: '/guide/build/pipeline-compilation'},
                    {text: 'Best Practices', link: '/guide/operations/best-practices'}
                ]
            },
            {
                text: 'Develop',
                items: [
                    {text: '@PipelineStep Annotation', link: '/guide/development/pipeline-step'},
                    {text: 'Code a Step', link: '/guide/development/code-a-step'},
                    {text: 'Using Plugins', link: '/guide/development/using-plugins'},
                    {text: 'Mappers and DTOs', link: '/guide/development/mappers-and-dtos'},
                    {text: 'Handling File Operations', link: '/guide/development/handling-file-operations'},
                    {text: 'Java-Centered Types', link: '/guide/development/java-centered-types'},
                    {text: 'Dependency Management', link: '/guide/build/dependency-management'},
                    {text: 'Upgrade Guide', link: '/guide/development/upgrade'},
                    {text: 'Orchestrator Runtime', link: '/guide/development/orchestrator-runtime'},
                    {text: 'Configuration', link: '/guide/application/configuration'}
                ]
            },
            {
                text: 'Observe',
                items: [
                    {text: 'Observability Overview', link: '/guide/operations/observability/'},
                    {text: 'Metrics', link: '/guide/operations/observability/metrics'},
                    {text: 'Tracing', link: '/guide/operations/observability/tracing'},
                    {text: 'Logging', link: '/guide/operations/observability/logging'},
                    {text: 'Health Checks', link: '/guide/operations/observability/health-checks'},
                    {text: 'Alerting', link: '/guide/operations/observability/alerting'},
                    {text: 'Security Notes', link: '/guide/operations/observability/security'},
                    {text: 'Error Handling & DLQ', link: '/guide/operations/error-handling'},
                ]
            },
            {
                text: 'Extend',
                items: [
                    {text: 'Writing a Plugin', link: '/guide/plugins/writing-a-plugin'},
                    {text: 'Orchestrator Extensions', link: '/guide/development/extension/orchestrator-runtime'},
                    {text: 'Reactive Service Extensions', link: '/guide/development/extension/reactive-services'},
                    {text: 'Client Step Extensions', link: '/guide/development/extension/client-steps'},
                    {text: 'REST Resource Extensions', link: '/guide/development/extension/rest-resources'}
                ]
            },
            {
                text: 'Evolve',
                items: [
                    {text: 'Architecture', link: '/guide/evolve/architecture'},
                    {text: 'Annotation Processor Architecture', link: '/guide/evolve/annotation-processor-architecture'},
                    {text: 'Plugins Architecture', link: '/guide/evolve/plugins-architecture'},
                    {text: 'Aspect Semantics', link: '/guide/evolve/aspects/semantics'},
                    {text: 'Aspect Ordering', link: '/guide/evolve/aspects/ordering'},
                    {text: 'Aspect Warnings', link: '/guide/evolve/aspects/warnings'},
                    {text: 'Reference Implementation', link: '/guide/evolve/reference-implementation'},
                    {text: 'Template Generator (Reference)', link: '/guide/evolve/template-generator'},
                    {text: 'Publishing', link: '/guide/evolve/publishing'},
                    {text: 'CI Guidelines', link: '/guide/evolve/ci-guidelines'},
                    {text: 'Testing Guidelines', link: '/guide/evolve/testing-guidelines'},
                    {text: 'Gotchas & Pitfalls', link: '/guide/evolve/gotchas-pitfalls'},
                    {text: 'Proto Descriptor Integration', link: '/guide/evolve/protobuf-integration-descriptor-res'}
                ]
            },
            {
                text: 'Additional Resources',
                items: [
                    {text: 'Versions', link: '/versions'}
                ]
            },
            
        ],

      // Add search functionality
      search: {
        provider: 'local'
      },
      
      socialLinks: [
        { icon: 'github', link: 'https://github.com/mbarcia/pipelineframework' }
      ]
    },
    
    vite: {
      optimizeDeps: { 
        include: ['@braintree/sanitize-url'] 
      },
      resolve: {
        alias: {
          dayjs: 'dayjs/',
        },
      },
      server: {
        fs: {
          allow: ['../..']
        }
      }
    },
    
    head: [
      // Add Google Fonts for Quarkus-like typography
      ['link', { rel: 'preconnect', href: 'https://fonts.googleapis.com' }],
      ['link', { rel: 'preconnect', href: 'https://fonts.gstatic.com', crossorigin: '' }],
      ['link', { rel: 'stylesheet', href: 'https://fonts.googleapis.com/css2?family=Red+Hat+Display:wght@400;500;700;900&display=swap' }],
      ['link', { rel: 'stylesheet', href: 'https://fonts.googleapis.com/css2?family=Red+Hat+Text:wght@400;500&display=swap' }]
    ]
  })
)
