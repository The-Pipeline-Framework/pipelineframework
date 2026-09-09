---
name: tpf-docs-writer
description: "Navigate and write The Pipeline Framework (TPF) documentation. Understand its VitePress information architecture, historical layers, Guides, Coffee Machine architecture conversations, glossary, redirects, diagrams, and the distinct roles of value, architecture, develop, deploy, operate, decisions, and evolve."
---

# TPF Docs Writer

TPF documentation has an intentional information architecture, but it has grown through several generations of the framework. Understand both before changing it.

The site is built with **VitePress**. Think in terms of pages, folders, navigation, Guides, redirects, cross-links and rendered documentation — not merely Markdown files.

Write in **British English (en-GB)**.

Use forms such as:

- recognise, not recognize;
- behaviour, not behavior;
- organisation, not organization;
- modelling, not modeling.

Preserve established technical spellings, API names, code, quoted material and external product terminology.

## Main documentation areas

### `docs/value/` — business value

Explains **why TPF is useful**.

This is about outcomes, engineering economics, organisational consequences, developer productivity, operational confidence, portability and similar reasons for adopting TPF.

Keep it accessible. Architecture may support the argument, but detailed architectural explanation belongs elsewhere.

### `docs/architecture/` — architecture

Treat it as the home for:

- architectural concepts and models;
- data architecture;
- execution and state architecture;
- application structure;
- architectural patterns and boundaries;
- architectural trade-offs;
- the Coffee Machine.

Use **Architecture** consistently for the section name, navigation label, and canonical `/architecture/` routes.

### The Coffee Machine

The Coffee Machine is **not a sample application or reference implementation**.

It is TPF's informal architecture discussion space: the conference-break conversation where different engineering perspectives raise awkward questions and explore the trade-offs.

It belongs conceptually under **Architecture**, alongside material such as Data Architecture.

Use Coffee Machine when a subject benefits from:

- question-and-answer treatment;
- contrasting architectural instincts;
- exposing trade-offs and objections;
- answering “but what about...?”;
- discussing the consequences of TPF's architectural choices without turning the canonical documentation into an essay.

A Coffee Machine conversation explores a subject. Architecture and product documentation state the resulting model clearly.

### `docs/develop/` — build with TPF

Concrete development-time usage:

- Pipeline Template DSL;
- APIs;
- Query and Command;
- connectors;
- Blocks and Expansions;
- configuration;
- protocol and application types;
- capability binding;
- compilation;
- testing;
- extension points.

This area contains material from several generations of TPF. Check current contracts before adopting existing terminology or syntax.

### `docs/deploy/` — deploy TPF

Deployment and runtime topology:

- runtime layouts;
- placement;
- packaging;
- platform deployment;
- infrastructure-facing configuration;
- deployment-specific concerns.

### `docs/operate/` — operate TPF

The production and operator view:

- observability;
- failure handling;
- retries, replay and redrive;
- resilience;
- recovery;
- health and diagnostics;
- operational procedures;
- runtime behaviour.

A capability may need different treatment in `develop`, `deploy` and `operate`. Write for the audience of each section rather than duplicating one feature description everywhere.

## Guides

In TPF docs, **Guide has a structural meaning**.

A Guide is a **folder containing several related pages**, presented as one navigable documentation unit with its own **sub-menu navigation in the top bar**.

Do not casually call a standalone page a Guide.

Use a Guide when a subject needs a coherent multi-page journey rather than one page or a collection of unrelated sibling pages.

A Guide should have:

- a clear folder boundary;
- an entry or overview page;
- focused child pages;
- deliberate reading order;
- coherent VitePress sub-navigation.

Before creating a Guide, check whether the material belongs in an existing one.

## Glossary and terminology

TPF has a substantial vocabulary. Do not make every page teach it again.

A canonical **Glossary** is expected to become part of the documentation, potentially backed by a VitePress glossary plugin.

Once available, treat the Glossary as the authority for established TPF terms.

Prefer:

**plain sentence → glossary term → use it consistently**

over:

**new abstraction → local definition → more abstractions needed to explain that abstraction**.

When introducing a technical term:

1. use ordinary language first where possible;
2. link or defer to the Glossary for its precise definition;
3. introduce a new term only when it names a real and useful TPF concept;
4. do not create synonyms merely for stylistic variety.

If a concept cannot be explained without inventing several new abstractions, reconsider the explanation before adding those abstractions.

New durable TPF terminology should normally result in a glossary entry as well as its use in the relevant documentation.

## Tone

TPF documentation should be technically precise without sounding needlessly formal or absolute.

Prefer:

> TPF keeps this responsibility in the application.

over:

> The application MUST own this responsibility and TPF explicitly refuses to provide such an abstraction.

Use strong normative language only where there really is a contract or invariant.

Explain before categorising. Give readers enough context to understand why a distinction matters before introducing its formal name.

Avoid language that assumes the reader already shares TPF's architectural vocabulary.

The aim is to make sophisticated ideas feel straightforward, not to make straightforward ideas sound sophisticated.

## Diagrams

Use **Mermaid diagrams** as a normal part of TPF documentation.

As a rule, every substantive documentation page should contain at least one useful Mermaid diagram.

Diagrams can show:

- pipelines and data flow;
- architecture and boundaries;
- state transitions;
- execution lifecycles;
- deployment topology;
- relationships between concepts;
- request/response or sequence flows;
- decision paths;
- before/after architectural comparisons.

Prefer a small diagram that makes one idea obvious over a large diagram containing the entire framework.

Keep diagrams close to the prose they explain.

Use the same terminology in Mermaid nodes that the surrounding documentation and Glossary use.

Do not introduce a second vocabulary solely to make a diagram shorter.

A diagram must explain something; do not add meaningless decoration merely to satisfy the diagram rule.

## Moving and restructuring pages

Published documentation URLs are compatibility surfaces.

**Never remove an existing URL simply because the information architecture has improved.**

Whenever moving or renaming a page or Guide:

1. establish its new canonical location;
2. move the content;
3. update VitePress navigation;
4. update internal links;
5. leave a redirect from every previous published URL to the new canonical URL;
6. check links from ADRs, releases and other historically stable material.

Follow the repository's established VitePress redirect mechanism rather than inventing an ad-hoc HTML redirect.

This rule applies to individual pages, whole Guides and top-level reorganisations — including the historical Coffee Machine move from `value` into Architecture and the `design → architecture` migration.

Do not destroy old inbound URLs while cleaning up the site.

## Historical material

### `docs/decisions/`

ADRs capture architectural decisions and rationale.

They are especially important in TPF because many architectural choices concern **ownership boundaries**: what belongs to TPF, the application, a connector, a runtime or some external system.

Use ADRs to understand why the current architecture exists.

An ADR is a historical record. Its decision may remain valid while examples or implementation details have subsequently evolved.

### `docs/evolve/`

`evolve` is framework evolution and design history.

It may contain proposals, plans, implementation sequencing, discarded alternatives and intermediate APIs.

Use it to understand how TPF reached its current shape.

Do not treat it as the current product contract.

A capability may begin life as a special abstraction in `evolve` and eventually become an ordinary Query, Command, Block, Expansion, nested Pipeline or other existing primitive. Document what ultimately exists.

### `docs/versions/` — frozen released documentation

Every TPF release freezes a snapshot of the documentation under:

`docs/versions/vX.Y.Z/`

These directories represent the documentation as published for that released version.

**Do not edit, reorganise, modernise, fix links in, or otherwise modify existing version snapshots.**

Documentation improvements belong in the current documentation tree. Historical versions must remain frozen even when they contain terminology, structure, examples or explanations that have since changed.

When searching the repository, take care not to mistake matches under `docs/versions/` for current documentation. Exclude version snapshots from ordinary documentation reconnaissance unless the task specifically requires comparing historical versions.

Likewise, do not include `docs/versions/` when performing site-wide terminology migrations, page moves, Guide reorganisations, glossary adoption, or route migrations.

Treat `docs/versions/vX.Y.Z/` as immutable release artefacts.

## Choosing where something belongs

Ask what question the material answers:

- **Value** — Why should I care?
- **Architecture** — How should I understand the system and its trade-offs?
- **Develop** — How do I build it?
- **Deploy** — How do I put it into an environment?
- **Operate** — How do I run, observe and recover it?
- **Coffee Machine** — What architectural question deserves a proper conversation?
- **Glossary** — What exactly does this TPF term mean?
- **Decision** — Why did TPF choose this boundary?
- **Evolve** — How was or is TPF itself being changed?

The same capability may appear in several areas, but each page should answer the question appropriate to its area.

## Before writing

Inspect:

1. the relevant top-level section and existing Guides;
2. neighbouring pages and VitePress navigation;
3. existing terminology and, when available, the Glossary;
4. current schemas, tests, examples and implementation;
5. Coffee Machine conversations relevant to the architectural question;
6. ADRs for rationale;
7. `evolve` when historical context is necessary;
8. recent release material for newly landed capabilities;
9. existing URLs before moving anything.

Then decide whether the right change is to:

- amend a page;
- add a page to a Guide;
- create a Guide;
- add or amend a glossary entry;
- add an architectural or Coffee Machine explanation;
- move misplaced documentation while retaining redirects;
- document the same capability differently for distinct audiences.

## Core rule

Write **today's TPF**, while understanding enough of yesterday's TPF to recognise why the documentation looks the way it does.

Make the language approachable, introduce as few abstractions as possible, lean on the Glossary for shared vocabulary, and draw the architecture.

Do not make the reader perform the archaeology.
