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
const ADDITIONAL_RESOURCES_LABEL = 'Additional Resources'

const mainSidebar = [
    {
        text: 'Value',
        collapsed: true,
        items: [
            {text: 'Overview', link: '/value/'},
            {text: 'Business Value', link: '/value/business-value'},
            {text: 'Developer Joy', link: '/value/developer-experience'},
            {text: 'Performance', link: '/value/runtime-efficiency'},
            {text: 'Portable Serverless Functions', link: '/value/integration-flexibility'},
            {text: 'State, Replay, and Queryable Data', link: '/value/state-replay-and-queryable-data'},
            {text: 'Start Monolith, Split Later', link: '/value/deployment-evolution'},
            {text: 'Operational Confidence', link: '/value/operational-confidence'},
            {text: 'Plugins, Not Glue', link: '/value/extensibility-and-platform'}
        ]
    },
    {
        text: 'Build',
        collapsed: true,
        items: [
            {
                text: 'Getting Started',
                collapsed: true,
                items: [
                    {text: 'Quick Start', link: '/guide/getting-started/'},
                    {text: 'Canvas Guide', link: '/guide/getting-started/canvas-guide'}
                ]
            },
            {text: 'Pipeline Compilation', link: '/guide/build/pipeline-compilation'},
            {text: 'Operators', link: '/guide/build/operators'},
            {
                text: 'Runtime Layouts',
                link: '/guide/build/runtime-layouts/',
                collapsed: true,
                items: [
                    {text: 'Using Runtime Mapping', link: '/guide/build/runtime-layouts/using-runtime-mapping'},
                    {text: 'Maven Migration Playbook', link: '/guide/build/runtime-layouts/maven-migration'},
                    {text: 'Search Lambda Verification Lane', link: '/guide/build/runtime-layouts/search-lambda'},
                    {text: 'Search Azure Functions Testing Guide', link: '/guide/build/runtime-layouts/search-azure-functions'},
                    {text: 'Multi-Cloud Function Providers', link: '/guide/build/runtime-layouts/function-providers'},
                    {text: 'POM vs Layout Matrix', link: '/guide/build/runtime-layouts/pom-layout-matrix'},
                    {text: 'CSV Payments Pipeline-Runtime Walkthrough', link: '/guide/build/runtime-layouts/csv-payments-pipeline-runtime'},
                    {text: 'CSV Payments Monolith Walkthrough', link: '/guide/build/runtime-layouts/csv-payments-monolith'}
                ]
            },
            {text: 'Configuration Reference', link: '/guide/build/configuration/'},
            {text: 'Application Configuration', link: '/guide/application/configuration'},
            {text: 'Using Plugins', link: '/guide/development/using-plugins'},
            {text: 'Dependency Management', link: '/guide/build/dependency-management'},
            {text: 'Pipeline Parent POM Lifecycle', link: '/guide/build/pipeline-parent-pom-lifecycle'},
            {text: 'CSV Payments POM Lifecycle', link: '/guide/build/csv-payments-pom-lifecycle'},
            {text: 'Best Practices', link: '/guide/operations/best-practices'}
        ]
    },
    {
        text: 'Design',
        collapsed: true,
        items: [
            {text: 'Application Structure', link: '/guide/design/application-structure'},
            {text: 'Common Module Structure', link: '/guide/design/common-module-structure'},
            {text: 'Operator Reuse Strategy', link: '/guide/design/operator-reuse-strategy'},
            {text: 'Expansion and Reduction', link: '/guide/design/expansion-and-reduction'},
            {text: 'Runtime Topology Strategy', link: '/guide/design/runtime-topology-strategy'}
        ]
    },
    {
        text: 'Develop',
        collapsed: true,
        items: [
            {text: '@PipelineStep Annotation', link: '/guide/development/pipeline-step'},
            {text: 'Code a Step', link: '/guide/development/code-a-step'},
            {text: 'Typed Union Outputs', link: '/guide/development/typed-union-outputs'},
            {text: 'Item Reject Sink', link: '/guide/development/item-reject-sink'},
            {text: 'Operators', link: '/guide/development/operators'},
            {text: 'Operator Build Troubleshooting', link: '/guide/development/operators-build-troubleshooting'},
            {text: 'Extending Operator Libraries', link: '/guide/development/extension/operator-libraries'},
            {text: 'Mappers and DTOs', link: '/guide/development/mappers-and-dtos'},
            {text: 'Handling File Operations', link: '/guide/development/handling-file-operations'},
            {text: 'Testing with Testcontainers', link: '/guide/development/testing'},
            {
                text: 'Orchestrator Runtime',
                link: '/guide/development/orchestrator-runtime/',
                collapsed: false,
                items: [
                    {text: 'Overview', link: '/guide/development/orchestrator-runtime/'},
                    {text: 'Queue-Async Runtime', link: '/guide/development/orchestrator-runtime/queue-async'},
                    {text: 'Checkpoint Handoff', link: '/guide/development/orchestrator-runtime/checkpoint-handoff'},
                    {text: 'Await Boundaries', link: '/guide/development/orchestrator-runtime/await'}
                ]
            },
            {text: 'TPFGo Example', link: '/guide/development/tpfgo-example'},
            {text: 'AWS Lambda Platform', link: '/guide/development/aws-lambda'},
            {text: 'Azure Functions Platform', link: '/guide/development/azure-functions'},
            {text: 'Google Cloud Run Functions Platform', link: '/guide/development/google-cloud-run-functions'},
            {text: 'Performance', link: '/guide/development/performance'},
            {text: 'Customization Points', link: '/guide/development/customization-points'}
        ]
    },
    {
        text: 'Operate',
        collapsed: true,
        items: [
            {text: 'Orchestrator Runtime Modes', link: '/guide/development/orchestrator-runtime/'},
            {text: 'Await Boundaries', link: '/guide/operations/await-boundaries'},
            {text: 'Error Handling & DLQ', link: '/guide/operations/error-handling'},
            {text: 'Queue-Async Crash Semantics', link: '/guide/operations/error-handling#queue-async-crash-matrix'},
            {text: 'In-flight Probe', link: '/guide/operations/in-flight-probe'},
            {text: 'Operators', link: '/guide/operations/operators'},
            {text: 'Operator Playbook', link: '/guide/operations/operators-playbook'},
            {text: 'Operator Troubleshooting', link: '/guide/operations/operators-troubleshooting'},
            {text: 'AWS Lambda SnapStart', link: '/guide/operations/aws-lambda-snapstart'},
            {
                text: 'Observability',
                collapsed: false,
                items: [
                    {text: 'Overview', link: '/guide/operations/observability/'},
                    {text: 'Metrics', link: '/guide/operations/observability/metrics'},
                    {text: 'Tracing', link: '/guide/operations/observability/tracing'},
                    {text: 'Replay & Live Topology', link: '/guide/operations/observability/replay'},
                    {text: 'Replay Viewer', link: '/replay-viewer/'},
                    {text: 'Logging', link: '/guide/operations/observability/logging'},
                    {text: 'Health Checks', link: '/guide/operations/observability/health-checks'},
                    {text: 'Alerting', link: '/guide/operations/observability/alerting'},
                    {text: 'NewRelic OTel', link: '/guide/operations/observability/newrelic'},
                    {text: 'Using Quarkus LGTM', link: '/guide/operations/observability/lgtm'},
                    {text: 'Security Notes', link: '/guide/operations/observability/security'}
                ]
            }
        ]
    },
    {
        text: 'Plugins',
        collapsed: true,
        items: [
            {text: 'Writing a Plugin', link: '/guide/plugins/writing-a-plugin'},
            {text: 'Persistence Plugin', link: '/guide/plugins/persistence'},
            {text: 'Field Materialization', link: '/guide/plugins/materialization'},
            {
                text: 'Caching',
                collapsed: true,
                items: [
                    {text: 'Caching Configuration', link: '/guide/plugins/caching/configuration'},
                    {text: 'Caching Policies', link: '/guide/plugins/caching/policies'},
                    {text: 'Caching Invalidation', link: '/guide/plugins/caching/invalidation'},
                    {text: 'Search Replay Walkthrough', link: '/guide/plugins/caching/replay-walkthrough'},
                    {text: 'Cache Key Strategy', link: '/guide/plugins/caching/key-strategy'},
                    {text: 'Cache vs Persistence', link: '/guide/plugins/caching/cache-vs-persistence'}
                ]
            }
        ]
    },
    {
        text: 'Connectors',
        collapsed: true,
        items: [
            {text: 'Object Ingest', link: '/guide/connectors/object-ingest'}
        ]
    },
    {
        text: 'Evolve',
        collapsed: true,
        items: [
            {text: 'Architecture', link: '/guide/evolve/architecture'},
            {
                text: 'Framework Portability Assessment',
                link: '/guide/evolve/framework-portability-assessment/',
                collapsed: true,
                items: [
                    {text: 'Coupling Inventory', link: '/guide/evolve/framework-portability-assessment/coupling-inventory'},
                    {text: 'Quarkus Coupling', link: '/guide/evolve/framework-portability-assessment/quarkus-coupling'},
                    {text: 'Vert.x Coupling', link: '/guide/evolve/framework-portability-assessment/vertx-coupling'},
                    {text: 'Runtime Split', link: '/guide/evolve/framework-portability-assessment/runtime-split'},
                    {text: 'Reactive Portability', link: '/guide/evolve/framework-portability-assessment/reactive-portability'},
                    {text: 'Persistence Portability', link: '/guide/evolve/framework-portability-assessment/persistence'},
                    {text: 'Annotation Removal', link: '/guide/evolve/framework-portability-assessment/annotation-removal'},
                    {text: 'Code Generation Portability', link: '/guide/evolve/framework-portability-assessment/code-generation'},
                    {text: 'Maven And Scaffolding', link: '/guide/evolve/framework-portability-assessment/maven-and-scaffolding'},
                    {text: 'Roadmap And Guardrails', link: '/guide/evolve/framework-portability-assessment/roadmap-and-guardrails'}
                ]
            },
            {text: 'Orchestrator Control Plane', link: '/guide/evolve/architecture#orchestrator-control-plane-current'},
            {
                text: 'Await Unit Runtime',
                link: '/guide/evolve/await-unit-runtime/',
                collapsed: false,
                items: [
                    {text: 'Model', link: '/guide/evolve/await-unit-runtime/'},
                    {text: 'Sequences', link: '/guide/evolve/await-unit-runtime/sequences'},
                    {text: 'Patterns', link: '/guide/evolve/await-unit-runtime/patterns'},
                    {text: 'Limitations And Debt', link: '/guide/evolve/await-unit-runtime/operations-and-debt'}
                ]
            },
            {
                text: 'Durable Coordinator',
                link: '/guide/evolve/durable-coordinator/',
                collapsed: true,
                items: [
                    {text: 'Overview', link: '/guide/evolve/durable-coordinator/'},
                    {text: 'Coordinator And Worker Topology', link: '/guide/evolve/durable-coordinator/coordinator-worker-topology'},
                    {text: 'Worker Protocols', link: '/guide/evolve/durable-coordinator/worker-protocols'},
                    {text: 'Step-Aware Invocation Runtime', link: '/guide/evolve/durable-coordinator/boundary-invocation-model'},
                    {
                        text: 'Brokered Runtime Boundaries',
                        link: '/guide/evolve/brokered-boundaries/',
                        collapsed: true,
                        items: [
                            {text: 'Overview', link: '/guide/evolve/brokered-boundaries/'},
                            {text: 'Boundary Taxonomy', link: '/guide/evolve/brokered-boundaries/boundary-taxonomy'},
                            {text: 'Dispatch Substrates', link: '/guide/evolve/brokered-boundaries/dispatch-substrates'},
                            {text: 'Envelope And Data Policy', link: '/guide/evolve/brokered-boundaries/envelope-and-data-policy'},
                            {text: 'Adoption And Slices', link: '/guide/evolve/brokered-boundaries/adoption-and-slices'}
                        ]
                    },
                    {text: 'Bundle Contract', link: '/guide/evolve/durable-coordinator/bundle-contract'},
                    {text: 'Pipeline Contract And Release Model', link: '/guide/evolve/durable-coordinator/pipeline-contract-release-model'},
                    {text: 'Runtime Boundaries And Performance', link: '/guide/evolve/durable-coordinator/runtime-boundaries-performance'},
                    {text: 'Local APIs', link: '/guide/evolve/durable-coordinator/local-apis'},
                    {text: 'Self-Hosted Deployment', link: '/guide/evolve/durable-coordinator/self-hosted-deployment'},
                    {text: 'Self-Hosted HA Roadmap', link: '/guide/evolve/durable-coordinator/self-hosted-ha-roadmap'},
                    {text: 'Self-Hosted Milestone', link: '/guide/evolve/durable-coordinator/self-hosted-milestone'}
                ]
            },
            {
                text: 'Annotation Processor Guide',
                link: '/guide/evolve/annotation-processor/',
                collapsed: true,
                items: [
                    {text: 'Overview', link: '/guide/evolve/annotation-processor/'},
                    {text: 'Phases and Flow', link: '/guide/evolve/annotation-processor/phases-and-flow'},
                    {text: 'Models and Bindings', link: '/guide/evolve/annotation-processor/models-and-bindings'},
                    {text: 'Generation and Rendering', link: '/guide/evolve/annotation-processor/generation-and-rendering'},
                    {text: 'Current Architecture', link: '/guide/evolve/annotation-processor/current-architecture'}
                ]
            },
            {text: 'Annotation Processor Architecture (Compat)', link: '/guide/evolve/annotation-processor-architecture'},
            {text: 'Compiler Pipeline Architecture', link: '/guide/evolve/compiler-pipeline-architecture'},
            {text: 'Runtime Core Decoupling', link: '/guide/evolve/runtime-core-decoupling'},
            {text: 'I/O Shell Absorption', link: '/guide/evolve/io-shell-absorption'},
            {text: 'Operators Internals', link: '/guide/evolve/operators-internals'},
            {text: 'Data Types', link: '/guide/evolve/data-types'},
            {text: 'Typed Union Output Contracts', link: '/guide/evolve/typed-union-output-contracts'},
            {
                text: 'Runtime Mapping',
                collapsed: true,
                items: [
                    {text: 'Schema', link: '/guide/evolve/runtime-mapping/schema'},
                    {text: 'Cheat Sheet', link: '/guide/evolve/runtime-mapping/cheat-sheet'},
                    {text: 'Build Topologies', link: '/guide/evolve/runtime-mapping/build-topologies'},
                    {text: 'Annotation Processing', link: '/guide/evolve/runtime-mapping/annotation-processing'},
                    {text: 'Synthetics', link: '/guide/evolve/runtime-mapping/synthetics'},
                    {text: 'Implementation Plan', link: '/guide/evolve/runtime-mapping/implementation-plan'},
                    {text: 'TDD Plan', link: '/guide/evolve/runtime-mapping/tdd-plan'},
                    {text: 'Validation & Migration Examples', link: '/guide/evolve/runtime-mapping/validation-migration-examples'}
                ]
            },
            {text: 'Plugins Architecture', link: '/guide/evolve/plugins-architecture'},
            {text: 'Reference Implementation', link: '/guide/evolve/reference-implementation'},
            {text: 'Template Generator (Reference)', link: '/guide/evolve/template-generator'},
            {text: 'Publishing', link: '/guide/evolve/publishing'},
            {text: 'CI Guidelines', link: '/guide/evolve/ci-guidelines'},
            {text: 'Testing Guidelines', link: '/guide/evolve/testing-guidelines'},
            {text: 'Gotchas & Pitfalls', link: '/guide/evolve/gotchas-pitfalls'},
            {text: 'Proto Descriptor Integration', link: '/guide/evolve/protobuf-integration-descriptor-res'},
            {text: 'Protobuf-over-HTTP Dispatch Design', link: '/guide/evolve/protobuf-over-http-dispatch-design'},
            {
                text: 'Aspects',
                collapsed: true,
                items: [
                    {text: 'Aspect Semantics', link: '/guide/evolve/aspects/semantics'},
                    {text: 'Aspect Ordering', link: '/guide/evolve/aspects/ordering'},
                    {text: 'Aspect Warnings', link: '/guide/evolve/aspects/warnings'}
                ]
            },
            {
                text: 'TPF Go',
                collapsed: true,
                items: [
                    {text: 'TPF Go Overview', link: '/guide/evolve/tpfgo/'},
                    {text: 'TPF Go Design Spectrum', link: '/guide/evolve/tpfgo/design-spectrum'},
                    {text: 'TPF Go DDD Alignment', link: '/guide/evolve/tpfgo/ddd-alignment'},
                    {text: 'TPF Go Roadmap', link: '/guide/evolve/tpfgo/roadmap'}
                ]
            }
        ]
    },
    {
        text: ADDITIONAL_RESOURCES_LABEL,
        collapsed: true,
        items: [
            {text: 'Versions', link: '/versions/'}
        ]
    },
]

const toNavItems = (items = []) =>
    items.map((item) => {
        const navItem = {text: item.text}
        if (item.link) {
            navItem.link = item.link
        }
        if (item.items?.length) {
            navItem.items = toNavItems(item.items)
        }
        return navItem
    })

const topNavSections = mainSidebar
    .filter((section) => section.text !== ADDITIONAL_RESOURCES_LABEL)
    .map((section) => ({
        text: section.text,
        items: toNavItems(section.items ?? []),
    }))

export default withMermaid(
  defineConfig({
    title: 'The Pipeline Framework',
    description: 'A framework for building reactive pipeline processing systems',
    lang: 'en-GB',
    mermaid: {
      fontFamily: 'Red Hat Text, system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif',
      fontSize: 16,
      flowchart: {
        useMaxWidth: false,
        htmlLabels: true
      },
      class: {
        useMaxWidth: false,
        htmlLabels: true
      },
      sequence: {
        useMaxWidth: false,
        actorFontSize: 17,
        messageFontSize: 16,
        noteFontSize: 16,
        actorFontFamily: 'Red Hat Text, system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif',
        messageFontFamily: 'Red Hat Text, system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif',
        noteFontFamily: 'Red Hat Text, system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif'
      },
      themeVariables: {
        fontFamily: 'Red Hat Text, system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif',
        fontSize: '16px',
        primaryTextColor: '#1f2937',
        secondaryTextColor: '#374151',
        lineColor: '#64748b'
      }
    },
    
    // Base URL for the site (can be changed for different deployments)
    base: '/',
    
    // Register custom theme
    themeConfig: {
        nav: [
            ...topNavSections,
            {text: 'Versions', link: '/versions/', activeMatch: '^/versions(?:/|$)'}
        ],

        sidebar: {
            '/versions/': [
                {
                    text: 'Versioned Docs',
                    items: [
                        {text: 'Versions', link: '/versions/'}
                    ]
                }
            ],
            '/': mainSidebar
        },

      // Add search functionality
      search: {
        provider: 'local'
      },
      
      socialLinks: [
        { icon: 'github', link: 'https://github.com/The-Pipeline-Framework/pipelineframework' }
      ],

      editLink: {
        pattern: 'https://github.com/The-Pipeline-Framework/pipelineframework/edit/main/docs/:path',
        text: 'Edit this page'
      }

    },

    transformPageData: (pageData, { siteConfig }) => {
        // Initialize the `head` frontmatter if it doesn't exist.
        pageData.frontmatter.head ??= []

        // Add basic meta tags to the frontmatter.
        pageData.frontmatter.head.push(
            [
                'meta',
                {
                    property: 'og:title',
                    content:
                        pageData.frontmatter.title || pageData.title || siteConfig.site.title,
                },
            ],
            [
                'meta',
                {
                    name: 'twitter:title',
                    content:
                        pageData.frontmatter.title || pageData.title || siteConfig.site.title,
                },
            ],
            [
                'meta',
                {
                    property: 'og:description',
                    content:
                        pageData.frontmatter.description || pageData.description || siteConfig.site.description,
                },
            ],
            [
                'meta',
                {
                    name: 'twitter:description',
                    content:
                        pageData.frontmatter.description || pageData.description || siteConfig.site.description,
                },
            ],
        )
    },

    cleanUrls: true,
    // Keep historical version snapshots immutable even if they contain stale links.
    // This suppresses dead-link failures for `/versions/**` pages and known
    // relative links that only appear within version snapshots.
    ignoreDeadLinks: [
      /^\/versions\//,
      /^(?:\.\/)?\.\.\/\.\.\/evolve\/annotation-processor-architecture(?:\.md)?$/
    ],

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
      },
      build: {
        // The docs intentionally ship local search and Mermaid support. Keep
        // those large features isolated in named chunks, then set the warning
        // limit to the largest expected feature chunk so routine docs builds
        // stay quiet without hiding accidental growth in page bundles.
        chunkSizeWarningLimit: 1800,
        rollupOptions: {
          output: {
            manualChunks(id) {
              if (!id.includes('node_modules')) {
                return undefined
              }
              if (
                id.includes('/mermaid/') ||
                id.includes('/vitepress-plugin-mermaid/') ||
                id.includes('/@mermaid-js/')
              ) {
                return 'mermaid'
              }
              if (
                id.includes('/d3-') ||
                id.includes('/dagre-d3-es/') ||
                id.includes('/cytoscape/') ||
                id.includes('/cytoscape-cose-bilkent/')
              ) {
                return 'diagram-layout'
              }
              return undefined
            }
          }
        }
      }
    },

    sitemap: {
      hostname: 'https://pipelineframework.org',
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
