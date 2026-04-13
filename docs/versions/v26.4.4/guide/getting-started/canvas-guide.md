---
search: false
---

# Using the Canvas Designer

## Overview

The Canvas designer (https://app.pipelineframework.org) provides a visual interface for creating and configuring pipeline applications. This document explains how to use it effectively.

## Getting Started

### 1. Access the Canvas
Visit https://app.pipelineframework.org in your web browser.

### 2. Create a New Pipeline
- Click on the "New Pipeline" button
- Or start with a blank canvas to add steps one by one

## Creating Steps

### Adding Steps
- Click the "+" button to add a new step
- Or click the large "+" button that appears when hovering over the canvas
- Select the step type from the dropdown menu:
  - 1-1 (One-to-One): Single input to single output
  - Expansion (1-Many): Single input to multiple outputs
  - Reduction (Many-1): Multiple inputs to single output
  - Side-effect: Input and output types are identical

### Configuring Steps
- Click on a step to open its configuration form
- Fill in the step name and description
- Define the input type name and fields
- Define the output type name and fields
- The form updates in real-time as you make changes

## Types and Fields

### Defining Types
- **Input Type Name**: Name of the type that flows into this step
- **Output Type Name**: Name of the type that flows out of this step
- **Field Name**: Name of a property in the type
- **Field Type**: Data type of the field (String, Integer, Long, Double, Boolean, UUID, BigDecimal, Currency, Path)

### Automatic Type Dependencies
- When steps are connected, the output type of one step becomes the input type of the next
- This creates a natural flow through your pipeline
- Type mismatches and field synchronization are handled automatically

### Field Synchronization
- Connected steps automatically synchronize their field definitions
- Changes to fields in one step propagate to connected steps
- Most type conversions are handled automatically by MapStruct (primitives, UUID, BigDecimal, Java time types, etc.)
- Specialized types (Currency, AtomicInteger, AtomicLong, `List<String>`) use custom converters

## Connecting Steps

### Visual Connections
- Steps are automatically connected in sequence
- Different arrow shapes represent different cardinalities:
  - Solid arrow: 1-1 connection
  - Split arrow: Expansion (1-Many)
  - Merge arrow: Reduction (Many-1)
  - Parallel arrow: Side-effect (1-1 with same I/O)

### Type Flow Visualization
- The canvas shows how types flow from one step to the next
- Input and output types are visually represented
- Field mappings are clearly shown

## Canvas Features

### Visual Design Elements
- **Step Cards**: Each pipeline step appears as a card on the canvas
- **Arrow Connections**: Show the flow of data between steps
- **Field Lists**: Show input and output fields for each step
- **Type Labels**: Identify input and output types

### Interactive Elements
- **Click on Steps**: Opens configuration form with input/output fields
- **Click on Connectors**: Shows shared fields between connected steps
- **Click on Arrows**: Shows combined input/output forms
- **Drag Steps**: Reorganize the visual layout (does not change processing order)

### Real-time Updates
- Changes appear immediately on the canvas
- Type dependencies update automatically
- YAML configuration updates in real-time
- Download button is always available when configuration changes

## Configuration Management

### Download Configuration
- Click the "Download YAML" button to get the pipeline configuration
- The YAML file can be used with the template generator for advanced scenarios
- Configuration includes all steps, types, and field definitions
- Ready to use with the command line generator if needed

### Download Complete Application
- Click the "Download Application" button to get a ZIP file containing your complete generated application
- The ZIP includes all source code, configuration files, and build scripts
- No additional generation step is needed - the application is ready to build and run
- Perfect for immediately starting development

### Upload Configuration
- Click "Upload Configuration" to load an existing YAML file
- The canvas updates to reflect the uploaded pipeline
- Perfect for editing existing configurations

## Best Practices

### 1. Plan Your Pipeline
- Think through your data flow before starting
- Identify required input and output types
- Consider the cardinality of each transformation

### 2. Use Descriptive Names
- Step names should clearly indicate their function
- Type names should be meaningful
- Field names should be self-explanatory

### 3. Iterate Gradually
- Start with a simple pipeline and add complexity
- Test the generated code frequently
- Use the Canvas to visualize your changes

### 4. Validate Types
- Ensure input and output types are compatible
- Verify field types match between connected steps
- Use the visualization to catch potential issues

## Advanced Features

### Two-Column Layout
- Input and output fields are displayed side-by-side
- Easy comparison of field transformations
- Clear visualization of data transformations

### Animated Elements
- New steps briefly animate to indicate their addition
- Visual feedback for user interactions
- Smooth transitions for better UX

### Confirmation Dialogs
- Prevents accidental deletions
- Confirms potentially destructive operations
- Safe editing experience

## Integration with Development Workflow

### 1. Design Phase
- Use Canvas to visualize your pipeline architecture
- Iterate on the design quickly
- Share designs with team members

### 2. Development Phase
- Download YAML configuration from Canvas
- Use with template generator to create codebase
- Implement business logic in generated services

### 3. Testing Phase
- Validate the generated application
- Verify type flows and transformations
- Iterate on design if needed

## Troubleshooting

### Common Issues
- **Type Mismatch**: Check that field types are compatible between steps
- **Cardinality Issues**: Ensure the correct step type is selected
- **Field Synchronization**: Changes may take a moment to propagate

### Error Handling
- Canvas provides real-time validation feedback
- Invalid configurations are highlighted
- Error messages guide you to fix issues

## Export and Import

### Export Options
- **Download Application**: Get a complete ZIP file with your generated application (recommended for most users)
- **Download YAML**: Get the configuration file for advanced usage or template generator
- **Copy configuration to clipboard**: For quick sharing
- **Save for later use**: Keep your design for future reference

### Import Options
- **Load from existing YAML file**: Resume work from saved configuration
- **Share configurations across projects**: Reuse pipeline designs

## Tips and Tricks

- **Use Side-effect Steps**: For logging, metrics, or other side effects
- **Validate Early**: Test generated code frequently
- **Keep Types Simple**: Complex types can make debugging harder
- **Document Your Design**: Add comments to explain complex transformations
- **Version Control**: Save your YAML configurations in version control
