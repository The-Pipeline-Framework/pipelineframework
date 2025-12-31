# The Pipeline Framework Documentation

Welcome to the comprehensive documentation for The Pipeline Framework. This site provides everything you need to understand, use, and extend the framework for building reactive pipeline processing systems.

<Callout type="tip" title="Visual Pipeline Designer">
In addition to programmatic development, The Pipeline Framework includes a visual canvas designer at <a href="https://app.pipelineframework.org" target="_blank">https://app.pipelineframework.org</a> that allows you to create and configure your pipelines using an intuitive visual interface. This tool makes it easier to design complex pipeline architectures without writing code.
</Callout>

## About the The Pipeline Framework

The Pipeline Framework is a powerful tool for building reactive pipeline processing systems. It simplifies the development of distributed systems by providing a consistent way to create, configure, and deploy pipeline steps.

### Key Features

- **Reactive Programming**: Built on top of Quarkus, Mutiny and Vert.x for non-blocking operations
- **Immutable Architecture**: No database updates during pipeline execution - only appends/preserves, ensuring data integrity
- **Annotation-Based Configuration**: Simplifies adapter generation with `@PipelineStep`
- **Visual Design Canvas**: Create and configure pipelines with the visual designer at <a href="https://app.pipelineframework.org" target="_blank">https://app.pipelineframework.org</a>
- **gRPC & REST Flexibility**: Automatic adapter generation for fast gRPC or easy REST integration
- **Multiple Processing Patterns**: OneToOne, OneToMany, ManyToOne, ManyToMany, SideEffect and blocking variants
- **Modular Design**: Clear separation between runtime and deployment components
- **Health Monitoring**: Built-in health check capabilities
- **Test Integration**: Built-in support for integration tests with Testcontainers
- **Multiple Persistence Models**: Choose from reactive or virtual thread-based persistence
- **Error Handling**: Comprehensive error handling with dead letter queue (DLQ) support
- **CI/CD Ready**: Pre-configured GitHub Actions workflow for testing and publishing
- **Auto-Generation**: Generates necessary infrastructure at build time
- **Observability**: Built-in metrics, tracing, and logging support
- **Concurrency Control**: Reactive processing with backpressure management

## Complete Documentation

For complete documentation including detailed reference implementation, YAML configuration schema, Canvas designer guide, and publishing information, see:

- [Framework Overview](https://github.com/mbarcia/pipelineframework/blob/main/FRAMEWORK_OVERVIEW.md) - Complete architecture overview
- [Reference Implementation](https://github.com/mbarcia/pipelineframework/blob/main/REFERENCE_IMPLEMENTATION.md) - Complete implementation guide
- [YAML Configuration Schema](https://github.com/mbarcia/pipelineframework/blob/main/YAML_SCHEMA.md) - Complete YAML schema documentation
- [Canvas Designer Guide](https://github.com/mbarcia/pipelineframework/blob/main/CANVAS_GUIDE.md) - Complete Canvas usage guide
- [Publishing to Maven Central](https://github.com/mbarcia/pipelineframework/blob/main/PUBLISHING.md) - Guide to releasing and publishing the framework

## Documentation Structure

### Getting Started
- [Introduction](/): Overview of the framework and its capabilities
- [Getting Started](/guide/getting-started): Setting up the framework in your project
- [Creating Pipeline Steps](/guide/creating-steps): Building your first pipeline steps

### Guides
- [Application Structure](/guide/application-structure): Structuring pipeline applications
- [Backend Services](/guide/backend-services): Creating backend services that implement pipeline steps
- [Orchestrator Services](/guide/orchestrator-services): Building orchestrator services that coordinate pipelines
- [Configuration](/guide/configuration): Configuration options and best practices
- [Error Handling](/guide/error-handling): Managing errors and dead letter queues
- [Observability](/guide/observability): Monitoring and observing pipeline applications
- [Best Practices](/guide/best-practices): Recommended practices for pipeline development
- [Migrations](/guide/migrations): Upgrade notes and breaking changes

### Architecture
- [Architecture](/reference/architecture): Core concepts and architectural patterns
- [Annotation Processor Architecture](/reference/annotation-processor-architecture): Build-time IR, bindings, and renderers
- [Annotations](/annotations/pipeline-step): Detailed annotation reference
- [Pipeline Compilation](/guide/pipeline-compilation): How the annotation processor works
- [Common Module Structure](/guide/common-module-structure): Shared components structure
- [Dependency Management](/guide/dependency-management): Managing dependencies in pipeline applications

## Development

To run the documentation site locally:

```bash
cd docs
npm install
npm run dev
```

The documentation site will be available at `http://localhost:5173`.

## Contributing

If you find issues with the documentation or want to contribute improvements:

1. Fork the repository
2. Make your changes
3. Submit a pull request

All contributions are welcome!

The site will be available at http://localhost:5173

![CodeRabbit Pull Request Reviews](https://img.shields.io/coderabbit/prs/github/mbarcia/pipelineframework)

## Building

To build the static site:

```bash
cd docs
npm run build
```

The built site will be in `docs/.vitepress/dist`

## Mermaid Diagrams

This documentation site supports Mermaid diagrams for visualizing workflows and architectures. The site uses the `vitepress-plugin-mermaid` plugin which enables GitHub-style Mermaid syntax:

````
```mermaid
graph TD
    A[Input] --> B[Process]
    B --> C[Output]
```
````

**Note**: Using Mermaid diagrams significantly increases the bundle size (currently ~4.8MB vs 1.5MB without Mermaid) as the plugin includes many different diagram types and their dependencies in the bundle. This is a trade-off between rich visualization capabilities and site performance.

## Deployment to Cloudflare Pages

### Prerequisites

1. A Cloudflare account
2. The project repository connected to Cloudflare Pages

### Build Configuration

When setting up the project in Cloudflare Pages, use the following Vitepress configuration:

- **Build command**: `npm run build`
- **Build output directory**: `.vitepress/dist`
- **Root directory**: `/`, `docs`

### New Features

The documentation site now includes:

1. **Search Functionality**: Users can search across all documentation pages using the search bar in the top navigation
2. **Multi-Version Support**: Documentation is available for multiple versions of the The Pipeline Framework:
   - v0.9.2 (current)
   - v0.8.0
   - v0.7.0
3. **Mermaid Diagram Support**: Rich diagrams for visualizing workflows (with increased bundle size)

### Configuration Notes

The VitePress configuration includes `ignoreDeadLinks: true` to prevent build failures due to links that are valid in the context of the full project but not within the documentation site. This is necessary because:

1. The documentation site only covers The Pipeline Framework, not the entire project
2. Many markdown files in the project contain links that are valid in the context of the full repository but aren't relevant to the documentation site
3. There are localhost links in various README files that can't be resolved during the build process

This is a common configuration for documentation sites that are part of larger projects.

### Manual Deployment (Optional)

If you prefer to deploy manually, you can use Wrangler:

1. Install Wrangler:
   ```bash
   npm install -g wrangler
   ```

2. Build the documentation:
   ```bash
   cd docs && npm run build
   ```

3. Deploy to Cloudflare Pages:
   ```bash
   wrangler pages deploy docs/.vitepress/dist --project-name=your-project-name
   ```

### Environment Variables

No special environment variables are required for the basic setup.

### Custom Domain

After deployment, you can configure a custom domain through the Cloudflare dashboard.
