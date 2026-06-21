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
        text: 'Design',
        collapsed: true,
        items: [
            {text: 'Overview', link: '/design/'},
            {
                text: 'Pipeline Studio',
                link: '/design/pipeline-studio/',
                collapsed: true,
                items: [
                    {text: 'Quick Start', link: '/design/pipeline-studio/'},
                    {text: 'Canvas Guide', link: '/design/pipeline-studio/canvas-guide'}
                ]
            },
            {text: 'Application Structure', link: '/design/application-structure'},
            {text: 'Common Module Structure', link: '/design/common-module-structure'},
            {text: 'Operators', link: '/design/operators'},
            {text: 'Await Boundaries', link: '/design/await-boundaries'},
            {text: 'Object Ingest', link: '/design/object-ingest'},
            {
                text: 'JPA Query Connector',
                link: '/design/jpa-query-connector/',
                collapsed: true,
                items: [
                    {text: 'Overview', link: '/design/jpa-query-connector/'},
                    {text: 'Setup and YAML', link: '/design/jpa-query-connector/setup'},
                    {text: 'Predicates and Selection', link: '/design/jpa-query-connector/predicates'},
                    {text: 'Capture and Persistence', link: '/design/jpa-query-connector/capture-and-persistence'}
                ]
            },
            {
                text: 'Caching',
                link: '/design/caching/',
                collapsed: true,
                items: [
                    {text: 'Overview', link: '/design/caching/'},
                    {text: 'Configuration', link: '/design/caching/configuration'},
                    {text: 'Policies', link: '/design/caching/policies'},
                    {text: 'Invalidation', link: '/design/caching/invalidation'},
                    {text: 'Replay Walkthrough', link: '/design/caching/replay-walkthrough'},
                    {text: 'Key Strategy', link: '/design/caching/key-strategy'},
                    {text: 'Cache vs Persistence', link: '/design/caching/cache-vs-persistence'}
                ]
            },
            {text: 'Persistence', link: '/design/persistence'},
            {text: 'Field Materialization', link: '/design/materialization'},
            {text: 'Expansion and Reduction', link: '/design/expansion-and-reduction'},
            {text: 'Operator Reuse Strategy', link: '/design/operator-reuse-strategy'},
            {text: 'Runtime Topology Strategy', link: '/design/runtime-topology-strategy'},
            {text: 'Design Best Practices', link: '/design/best-practices'}
        ]
    },
    {
        text: 'Develop',
        collapsed: true,
        items: [
            {text: 'Overview', link: '/develop/'},
            {
                text: 'Pipeline Compilation',
                link: '/develop/pipeline-compilation/',
                collapsed: true,
                items: [
                    {text: 'Overview', link: '/develop/pipeline-compilation/'},
                    {text: 'Generated Artifacts', link: '/develop/pipeline-compilation/generated-artifacts'},
                    {text: 'Module Ownership', link: '/develop/pipeline-compilation/module-ownership'},
                    {text: 'Build Integration', link: '/develop/pipeline-compilation/build-integration'},
                    {text: 'Verification and Troubleshooting', link: '/develop/pipeline-compilation/verification-and-troubleshooting'}
                ]
            },
            {
                text: 'Configuration Reference',
                link: '/develop/configuration/',
                collapsed: true,
                items: [
                    {text: 'All Settings', link: '/develop/configuration/'},
                    {text: 'Application Configuration', link: '/develop/configuration/application'},
                    {text: 'Replay Viewer Parameters', link: '/develop/configuration/replay-viewer-parameters'},
                    {text: 'Lambda-Focused Configuration', link: '/develop/configuration/lambda-focused'}
                ]
            },
            {text: '@PipelineStep Annotation', link: '/develop/pipeline-step'},
            {text: 'Code a Step', link: '/develop/code-a-step'},
            {text: 'Typed Union Outputs', link: '/develop/typed-union-outputs'},
            {text: 'Item Reject Sink', link: '/develop/item-reject-sink'},
            {text: 'Operators', link: '/develop/operators'},
            {text: 'Operator Build Troubleshooting', link: '/develop/operators-build-troubleshooting'},
            {
                text: 'Delegation and Extension',
                collapsed: true,
                items: [
                    {text: 'External Library Delegation', link: '/develop/external-library-delegation'},
                    {text: 'Operator Libraries', link: '/develop/extension/operator-libraries'},
                    {text: 'Client Steps', link: '/develop/extension/client-steps'},
                    {text: 'Orchestrator Runtime Extensions', link: '/develop/extension/orchestrator-runtime'},
                    {text: 'Reactive Services', link: '/develop/extension/reactive-services'},
                    {text: 'REST Resources', link: '/develop/extension/rest-resources'}
                ]
            },
            {text: 'Mappers and DTOs', link: '/develop/mappers-and-dtos'},
            {text: 'Handling File Operations', link: '/develop/handling-file-operations'},
            {text: 'Testing with Testcontainers', link: '/develop/testing'},
            {text: 'Modularity', link: '/develop/modularity'},
            {text: 'Using Plugins', link: '/develop/using-plugins'},
            {text: 'Writing a Plugin', link: '/develop/writing-a-plugin'},
            {text: 'TPFGo Example', link: '/develop/tpfgo-example'},
            {text: 'Performance', link: '/develop/performance'},
            {text: 'Customization Points', link: '/develop/customization-points'}
        ]
    },
    {
        text: 'Deploy',
        collapsed: true,
        items: [
            {text: 'Overview', link: '/deploy/'},
            {
                text: 'Runtime Layouts',
                link: '/deploy/runtime-layouts/',
                collapsed: true,
                items: [
                    {text: 'Overview', link: '/deploy/runtime-layouts/'},
                    {text: 'Using Runtime Mapping', link: '/deploy/runtime-layouts/using-runtime-mapping'},
                    {text: 'Maven Migration Playbook', link: '/deploy/runtime-layouts/maven-migration'},
                    {text: 'POM vs Layout Matrix', link: '/deploy/runtime-layouts/pom-layout-matrix'},
                    {text: 'CSV Payments Pipeline-Runtime', link: '/deploy/runtime-layouts/csv-payments-pipeline-runtime'},
                    {text: 'CSV Payments Monolith', link: '/deploy/runtime-layouts/csv-payments-monolith'}
                ]
            },
            {
                text: 'Orchestrator Runtime',
                link: '/deploy/orchestrator-runtime/',
                collapsed: true,
                items: [
                    {text: 'Overview', link: '/deploy/orchestrator-runtime/'},
                    {text: 'Queue-Async Runtime', link: '/deploy/orchestrator-runtime/queue-async'},
                    {text: 'Checkpoint Handoff', link: '/deploy/orchestrator-runtime/checkpoint-handoff'},
                    {text: 'Await Runtime Setup', link: '/deploy/orchestrator-runtime/await'}
                ]
            },
            {
                text: 'Function Platforms',
                collapsed: true,
                items: [
                    {text: 'AWS Lambda Platform', link: '/deploy/aws-lambda'},
                    {text: 'Azure Functions Platform', link: '/deploy/azure-functions'},
                    {text: 'Google Cloud Run Functions Platform', link: '/deploy/google-cloud-run-functions'},
                    {text: 'Multi-Cloud Function Providers', link: '/deploy/function-providers'},
                    {text: 'Search Lambda Verification Lane', link: '/deploy/search-lambda'},
                    {text: 'Search Azure Functions Testing Guide', link: '/deploy/search-azure-functions'}
                ]
            },
            {text: 'Dependency Management', link: '/deploy/dependency-management'},
            {text: 'Pipeline Parent POM Lifecycle', link: '/deploy/pipeline-parent-pom-lifecycle'},
            {text: 'CSV Payments POM Lifecycle', link: '/deploy/csv-payments-pom-lifecycle'},
            {text: 'Concurrency and Backpressure Sizing', link: '/deploy/concurrency-and-backpressure'}
        ]
    },
    {
        text: 'Operate',
        collapsed: true,
        items: [
            {text: 'Overview', link: '/operate/'},
            {text: 'Await Boundaries', link: '/operate/await-boundaries'},
            {text: 'Error Handling & DLQ', link: '/operate/error-handling'},
            {text: 'In-flight Probe', link: '/operate/in-flight-probe'},
            {text: 'AWS Lambda SnapStart', link: '/operate/aws-lambda-snapstart'},
            {
                text: 'Operators',
                collapsed: true,
                items: [
                    {text: 'Runtime Operations', link: '/operate/operators'},
                    {text: 'Operator Playbook', link: '/operate/operators-playbook'},
                    {text: 'Operator Troubleshooting', link: '/operate/operators-troubleshooting'}
                ]
            },
            {
                text: 'Observability',
                link: '/operate/observability/',
                collapsed: true,
                items: [
                    {text: 'Overview', link: '/operate/observability/'},
                    {text: 'Metrics', link: '/operate/observability/metrics'},
                    {text: 'Tracing', link: '/operate/observability/tracing'},
                    {text: 'Replay & Live Topology', link: '/operate/observability/replay'},
                    {text: 'Replay Viewer', link: '/replay-viewer/'},
                    {text: 'Logging', link: '/operate/observability/logging'},
                    {text: 'Health Checks', link: '/operate/observability/health-checks'},
                    {text: 'Alerting', link: '/operate/observability/alerting'},
                    {text: 'Scalability', link: '/operate/observability/scalability'},
                    {text: 'Best Practices', link: '/operate/observability/best-practices'},
                    {text: 'NewRelic OTel', link: '/operate/observability/newrelic'},
                    {text: 'Using Quarkus LGTM', link: '/operate/observability/lgtm'},
                    {text: 'Security Notes', link: '/operate/observability/security'}
                ]
            },
            {text: 'Best Practices Index', link: '/operate/best-practices'}
        ]
    },
    {
        text: 'Evolve',
        collapsed: true,
        items: [
            {text: 'Overview', link: '/evolve/'},
            {text: 'Architecture', link: '/evolve/architecture'},
            {
                text: 'Operators',
                link: '/evolve/operators/',
                collapsed: true,
                items: [
                    {text: 'Overview', link: '/evolve/operators/'},
                    {text: 'Architecture', link: '/evolve/operators/architecture'},
                    {text: 'Invocation Internals', link: '/evolve/operators/internals'}
                ]
            },
            {
                text: 'Runtime Mapping',
                link: '/evolve/runtime-mapping/',
                collapsed: true,
                items: [
                    {text: 'Overview', link: '/evolve/runtime-mapping/'},
                    {text: 'Schema', link: '/evolve/runtime-mapping/schema'},
                    {text: 'Cheat Sheet', link: '/evolve/runtime-mapping/cheat-sheet'},
                    {text: 'Build Topologies', link: '/evolve/runtime-mapping/build-topologies'},
                    {text: 'Annotation Processing', link: '/evolve/runtime-mapping/annotation-processing'},
                    {text: 'Synthetics', link: '/evolve/runtime-mapping/synthetics'},
                    {text: 'Implementation Plan', link: '/evolve/runtime-mapping/implementation-plan'},
                    {text: 'TDD Plan', link: '/evolve/runtime-mapping/tdd-plan'},
                    {text: 'Validation & Migration Examples', link: '/evolve/runtime-mapping/validation-migration-examples'}
                ]
            },
            {
                text: 'TPFGo',
                link: '/evolve/tpfgo/',
                collapsed: true,
                items: [
                    {text: 'Overview', link: '/evolve/tpfgo/'},
                    {text: 'Design Spectrum', link: '/evolve/tpfgo/design-spectrum'},
                    {text: 'DDD Alignment', link: '/evolve/tpfgo/ddd-alignment'},
                    {text: 'Observer and Tap Contract', link: '/evolve/tpfgo/observer-tap-contract'},
                    {text: 'Roadmap', link: '/evolve/tpfgo/roadmap'}
                ]
            },
            {
                text: 'Durable Coordinator',
                link: '/evolve/durable-coordinator/',
                collapsed: true,
                items: [
                    {text: 'Overview', link: '/evolve/durable-coordinator/'},
                    {text: 'Coordinator And Worker Topology', link: '/evolve/durable-coordinator/coordinator-worker-topology'},
                    {text: 'Worker Protocols', link: '/evolve/durable-coordinator/worker-protocols'},
                    {text: 'Step-Aware Invocation Runtime', link: '/evolve/durable-coordinator/boundary-invocation-model'},
                    {text: 'Bundle Contract', link: '/evolve/durable-coordinator/bundle-contract'},
                    {text: 'Pipeline Contract And Release Model', link: '/evolve/durable-coordinator/pipeline-contract-release-model'},
                    {text: 'Runtime Boundaries And Performance', link: '/evolve/durable-coordinator/runtime-boundaries-performance'},
                    {text: 'Local APIs', link: '/evolve/durable-coordinator/local-apis'},
                    {text: 'Self-Hosted Deployment', link: '/evolve/durable-coordinator/self-hosted-deployment'},
                    {text: 'Self-Hosted HA Roadmap', link: '/evolve/durable-coordinator/self-hosted-ha-roadmap'},
                    {text: 'Self-Hosted Milestone', link: '/evolve/durable-coordinator/self-hosted-milestone'}
                ]
            },
            {
                text: 'Brokered Boundaries',
                link: '/evolve/brokered-boundaries/',
                collapsed: true,
                items: [
                    {text: 'Overview', link: '/evolve/brokered-boundaries/'},
                    {text: 'Boundary Taxonomy', link: '/evolve/brokered-boundaries/boundary-taxonomy'},
                    {text: 'Dispatch Substrates', link: '/evolve/brokered-boundaries/dispatch-substrates'},
                    {text: 'Envelope And Data Policy', link: '/evolve/brokered-boundaries/envelope-and-data-policy'},
                    {text: 'Adoption And Slices', link: '/evolve/brokered-boundaries/adoption-and-slices'}
                ]
            },
            {
                text: 'Framework Portability Assessment',
                link: '/evolve/framework-portability-assessment/',
                collapsed: true,
                items: [
                    {text: 'Guide Overview', link: '/evolve/framework-portability-assessment/'},
                    {text: 'Full Assessment Snapshot', link: '/evolve/framework-portability-assessment'},
                    {text: 'Coupling Inventory', link: '/evolve/framework-portability-assessment/coupling-inventory'},
                    {text: 'Quarkus Coupling', link: '/evolve/framework-portability-assessment/quarkus-coupling'},
                    {text: 'Vert.x Coupling', link: '/evolve/framework-portability-assessment/vertx-coupling'},
                    {text: 'Runtime Split', link: '/evolve/framework-portability-assessment/runtime-split'},
                    {text: 'Reactive Portability', link: '/evolve/framework-portability-assessment/reactive-portability'},
                    {text: 'Persistence Portability', link: '/evolve/framework-portability-assessment/persistence'},
                    {text: 'Annotation Removal', link: '/evolve/framework-portability-assessment/annotation-removal'},
                    {text: 'Code Generation Portability', link: '/evolve/framework-portability-assessment/code-generation'},
                    {text: 'Maven And Scaffolding', link: '/evolve/framework-portability-assessment/maven-and-scaffolding'},
                    {text: 'Roadmap And Guardrails', link: '/evolve/framework-portability-assessment/roadmap-and-guardrails'}
                ]
            },
            {
                text: 'Annotation Processor Guide',
                link: '/evolve/annotation-processor/',
                collapsed: true,
                items: [
                    {text: 'Overview', link: '/evolve/annotation-processor/'},
                    {text: 'Phases and Flow', link: '/evolve/annotation-processor/phases-and-flow'},
                    {text: 'Models and Bindings', link: '/evolve/annotation-processor/models-and-bindings'},
                    {text: 'Generation and Rendering', link: '/evolve/annotation-processor/generation-and-rendering'},
                    {text: 'Current Architecture', link: '/evolve/annotation-processor/current-architecture'}
                ]
            },
            {text: 'Annotation Processor Architecture (Compat)', link: '/evolve/annotation-processor-architecture'},
            {text: 'Compiler Pipeline Architecture', link: '/evolve/compiler-pipeline-architecture'},
            {text: 'Runtime Core Decoupling', link: '/evolve/runtime-core-decoupling'},
            {text: 'I/O Shell Absorption', link: '/evolve/io-shell-absorption'},
            {text: 'Data Types', link: '/evolve/data-types'},
            {text: 'Typed Union Output Contracts', link: '/evolve/typed-union-output-contracts'},
            {text: 'Plugins Architecture', link: '/evolve/plugins-architecture'},
            {text: 'Reference Implementation', link: '/evolve/reference-implementation'},
            {text: 'Template Generator', link: '/evolve/template-generator'},
            {text: 'Publishing', link: '/evolve/publishing'},
            {text: 'CI Guidelines', link: '/evolve/ci-guidelines'},
            {text: 'Testing Guidelines', link: '/evolve/testing-guidelines'},
            {text: 'Gotchas & Pitfalls', link: '/evolve/gotchas-pitfalls'},
            {text: 'Proto Descriptor Integration', link: '/evolve/protobuf-integration-descriptor-res'},
            {text: 'Protobuf-over-HTTP Dispatch Design', link: '/evolve/protobuf-over-http-dispatch-design'},
            {
                text: 'Aspects',
                collapsed: true,
                items: [
                    {text: 'Aspect Semantics', link: '/evolve/aspects/semantics'},
                    {text: 'Aspect Ordering', link: '/evolve/aspects/ordering'},
                    {text: 'Aspect Warnings', link: '/evolve/aspects/warnings'}
                ]
            },
            {
                text: 'Await Unit Runtime',
                link: '/evolve/await-unit-runtime/',
                collapsed: true,
                items: [
                    {text: 'Model', link: '/evolve/await-unit-runtime/'},
                    {text: 'Sequences', link: '/evolve/await-unit-runtime/sequences'},
                    {text: 'Patterns', link: '/evolve/await-unit-runtime/patterns'},
                    {text: 'Limitations And Debt', link: '/evolve/await-unit-runtime/operations-and-debt'}
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

const withCollapsedGuideGroups = (items = []) =>
    items.map((item) => {
        if (!item.items?.length) {
            return item
        }
        return {
            ...item,
            collapsed: item.collapsed ?? true,
            items: withCollapsedGuideGroups(item.items),
        }
    })

const guideSidebar = withCollapsedGuideGroups(mainSidebar)

const isTopNavOverviewItem = (item) => item.text === 'Overview'

const flattenTopNavItems = (items = []) =>
    items.flatMap((item) => {
        if (isTopNavOverviewItem(item)) {
            return []
        }
        if (item.items?.length) {
            return {
                text: item.text,
                items: item.items.flatMap((child) => {
                    if (isTopNavOverviewItem(child)) {
                        return []
                    }
                    if (!child.items?.length) {
                        return {text: child.text, link: child.link}
                    }
                    const childOverview = child.link
                        ? [{text: child.text, link: child.link}]
                        : []
                    return [
                        ...childOverview,
                        ...child.items.map((grandchild) => ({
                            text: `${child.text}: ${grandchild.text}`,
                            link: grandchild.link,
                        }))
                    ]
                }).filter((child) => child.link && !isTopNavOverviewItem(child)),
            }
        }
        const navItem = {text: item.text}
        if (item.link) {
            navItem.link = item.link
        }
        return navItem
    })

const topNavSections = guideSidebar
    .filter((section) => section.text !== ADDITIONAL_RESOURCES_LABEL)
    .map((section) => ({
        text: section.text,
        items: flattenTopNavItems(section.items ?? []),
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
            '/': guideSidebar
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
      transformItems(items) {
        return items.filter((item) => {
          const pathname = new URL(item.url, 'https://pipelineframework.org').pathname
          // Exclude redirect stubs from route migrations; keep their canonical targets.
          return !pathname.startsWith('/guide/')
            && !pathname.startsWith('/design/runtime-layouts')
            && pathname !== '/design/await'
        })
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
