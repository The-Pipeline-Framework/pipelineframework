---
search: false
---

# Quick Start

This guide will help you quickly get started with The Pipeline Framework using the visual Canvas designer - the fastest and easiest way to create your first pipeline application!

<Callout type="tip" title="Immutable Architecture">
The Pipeline Framework follows an immutable architecture where no database updates occur during pipeline execution - only appends/preserves. This ensures complete data integrity and provides a complete audit trail of all transformations.
</Callout>

<Callout type="tip" title="Rich Processing Patterns">
The framework supports multiple processing patterns including OneToOne, OneToMany, ManyToOne, ManyToMany, SideEffect and blocking variants. Each step type is optimized for specific data transformation scenarios.
</Callout>

<Callout type="tip" title="Prerequisites">
Before you begin, ensure you have Java 21+, Maven 3.8+, and an IDE with Quarkus Dev Mode support (like IntelliJ IDEA or VS Code) installed on your system for building and running the generated applications.
</Callout>

## Quick Start with Visual Canvas Designer

The fastest way to get started with The Pipeline Framework is by using the visual Canvas designer at [https://app.pipelineframework.org](https://app.pipelineframework.org).

### 1. Access the Canvas Designer

Visit [https://app.pipelineframework.org](https://app.pipelineframework.org) in your web browser.

### 2. Create Your First Pipeline

1. Click the "New Pipeline" button
2. Start adding steps by clicking the "+" button
3. Configure each step by:
   - Setting a descriptive name
   - Choosing the appropriate cardinality (1-1, Expansion, Reduction, or Side-effect)
   - Defining input and output fields with rich Java types

### 3. Download Your Complete Application

Once you've designed your pipeline:
1. Click the "Download Application" button
2. Save the generated ZIP file containing your complete application

### 4. Build and Run Your Application

Extract the ZIP file and navigate to the application directory:

```bash
unzip your-pipeline-app.zip
cd your-pipeline-app
./mvnw clean compile
```

Your application is now ready to run!

## Next Steps

Congratulations! You've successfully created your first pipeline application. Now explore these topics:

- [Canvas Designer Guide](/versions/v0.9.2/CANVAS_GUIDE.html) - Complete guide to using the visual designer
- [Application Structure](/versions/v0.9.2/guide/application-structure) - Learn how the generated application is structured
- [Framework Overview](/versions/v0.9.2/FRAMEWORK_OVERVIEW.html) - Deep dive into the framework architecture

## Need More Control?

If you need more control over the generation process or want to automate the generation, see the [Template Generator Guide](/versions/v0.9.2/guide/using-template-generator) for advanced usage options.