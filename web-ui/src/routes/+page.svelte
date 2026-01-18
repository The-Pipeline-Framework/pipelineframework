<!--
  - Copyright (c) 2023-2025 Mariano Barcia
  -
  - Licensed under the Apache License, Version 2.0 (the "License");
  - you may not use this file except in compliance with the License.
  - You may obtain a copy of the License at
  -
  -     http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing, software
  - distributed under the License is distributed on an "AS IS" BASIS,
  - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  - See the License for the specific language governing permissions and
  - limitations under the License.
  -->

<script>
  import {onMount} from 'svelte';
  import {saveAs} from 'file-saver';
  import {dump, load} from 'js-yaml';
  import JSZip from 'jszip';
  import StepArrow from '$lib/components/StepArrow.svelte';
  import CombinedFieldForm from '$lib/components/CombinedFieldForm.svelte';
  import Connector from '$lib/components/Connector.svelte';
  import ConfirmationDialog from '$lib/components/ConfirmationDialog.svelte';

  // Initialize with an empty configuration
  let config = {
    appName: 'My Pipeline App',
    basePackage: 'com.example.mypipeline',
    steps: [],
    aspects: {}
  };
  const defaultAspectConfigs = {
    persistence: {
      enabled: true,
      scope: 'GLOBAL',
      position: 'AFTER_STEP',
      order: 0,
      config: {
        pluginImplementationClass: 'org.pipelineframework.plugin.persistence.PersistenceService'
      }
    },
    cache: {
      enabled: true,
      scope: 'GLOBAL',
      position: 'AFTER_STEP',
      order: 5,
      config: {
        pluginImplementationClass: 'org.pipelineframework.plugin.cache.CacheService'
      }
    }
  };

  // State for plugin options
  let includePersistencePlugin = false;
  let includeCachePlugin = false;

  // State for field forms
  let showInputForm = false;
  let currentStepIndex = -1;
  let currentFormType = '';
  
  // State for generation
  let isGenerating = false;
  
  // State for confirmation dialog
  let showConfirmationDialog = false;
  let confirmationTitle = '';
  let confirmationMessage = '';
  let confirmationWarning = false;
  let pendingStepIndex = -1;
  let pendingCardinality = '';
  
  // State for app info dialog
  let showAppInfoDialog = false;
  let appInfoAppName = '';
  let appInfoBasePackage = '';
  
  // State for upload confirmation dialog
  let showUploadConfirmDialog = false;
  let pendingUploadFile = null;
  
  // Map to track previous cardinality values
  let prevCardinality = new Map();
  
  // State for animations
  let animatedStepIndex = -1;
  let animationTimeout;
  
  // Cardinality options
  const cardinalityOptions = [
    { value: 'ONE_TO_ONE', label: '1-1 (One-to-One)' },
    { value: 'EXPANSION', label: 'Expansion (1-Many)' },
    { value: 'REDUCTION', label: 'Reduction (Many-1)' },
    { value: 'MANY_TO_MANY', label: 'Many-to-Many (N-M)' },
    { value: 'SIDE_EFFECT', label: 'Side-effect' }
  ];

  function isAspectEnabled(aspects, aspectName) {
    if (!aspects || !Object.prototype.hasOwnProperty.call(aspects, aspectName)) {
      return false;
    }
    const aspectConfig = aspects[aspectName];
    return aspectConfig == null || aspectConfig.enabled !== false;
  }

  function cloneAspectDefaults(aspectName) {
    const defaults = defaultAspectConfigs[aspectName];
    if (!defaults) return null;
    return {
      ...defaults,
      config: { ...defaults.config }
    };
  }

  function setAspectEnabled(aspectName, enabled) {
    if (enabled) {
      const defaults = cloneAspectDefaults(aspectName);
      if (!defaults) return;
      const existing = config.aspects?.[aspectName];
      const merged = existing
        ? {
          ...defaults,
          ...existing,
          enabled: true,
          config: { ...defaults.config, ...(existing.config || {}) }
        }
        : defaults;
      config.aspects = { ...(config.aspects || {}), [aspectName]: merged };
    } else if (config.aspects && config.aspects[aspectName]) {
      const { [aspectName]: _, ...rest } = config.aspects;
      config.aspects = rest;
    }
    config = { ...config };
  }

  function syncAspectTogglesFromConfig() {
    includePersistencePlugin = isAspectEnabled(config.aspects, 'persistence');
    includeCachePlugin = isAspectEnabled(config.aspects, 'cache');
  }

  syncAspectTogglesFromConfig();
  
  
  // Function to check if a type is valid (scalar, Enum, or defined in previous steps)
  function isValidFieldType(type, currentStepIndex, stepsOverride = null) {
    // Trim whitespace from type
    type = typeof type === 'string' ? type.trim() : type;
    
    // Check if it's a Map type (using pattern matching)
    // Handles formats like: Map<String, Integer>, Map<List<String>, Integer>, etc.
    if (typeof type === 'string' && /^Map<.+?,\s*.+>$/.test(type)) {
      // Additional check to ensure the type is properly closed
      const openCount = (type.match(/</g) || []).length;
      const closeCount = (type.match(/>/g) || []).length;
      if (openCount !== closeCount) {
        return false; // Malformed type with mismatched brackets
      }
      return true;
    }
    
    // Check if it's a List type (using pattern matching)
    // Handles formats like: List<String>, List<Map<String, Integer>>, etc.
    if (typeof type === 'string' && /^List<.+>$/.test(type)) {
      // Additional check to ensure the type is properly closed
      const openCount = (type.match(/</g) || []).length;
      const closeCount = (type.match(/>/g) || []).length;
      if (openCount !== closeCount) {
        return false; // Malformed type with mismatched brackets
      }
      return true;
    }
    
    // First check if it's a Java scalar type
    if (isJavaScalarType(type)) {
      return true;
    }
    
    // If not a scalar, check if it's Enum
    if (type === 'Enum') {
      return true;
    }
    
    // If not scalar or Enum, check if it's defined in any previous step as an input or output type
    const stepsToCheck = stepsOverride ?? config.steps;
    for (let i = 0; i < currentStepIndex && i < stepsToCheck.length; i++) {
      const step = stepsToCheck[i];
      if (step && (step.inputTypeName === type || step.outputTypeName === type)) {
        return true;
      }
    }
    
    return false;
  }
  
  // Function to get all available field types including custom message types
  function getAvailableFieldTypes(currentStepIndex) {
    // Start with basic Java scalar types and Enum
    let allTypes = [
      'String', 'Integer', 'Long', 'Double', 'Boolean', 
      'UUID', 'BigDecimal', 'Currency', 'Path',
      'List', 'Map', 'LocalDateTime', 'LocalDate', 'OffsetDateTime', 'ZonedDateTime', 'Instant', 'Duration', 'Period',
      'URI', 'URL', 'File', 'BigInteger', 'AtomicInteger', 'AtomicLong', 'Enum'
    ];
    
    // Add custom message types from previous steps
    if (config && config.steps && Array.isArray(config.steps)) {
      for (let i = 0; i < currentStepIndex && i < config.steps.length; i++) {
        const step = config.steps[i];
        if (step.inputTypeName && !allTypes.includes(step.inputTypeName)) {
          allTypes.push(step.inputTypeName);
        }
        if (step.outputTypeName && !allTypes.includes(step.outputTypeName)) {
          allTypes.push(step.outputTypeName);
        }
      }
    }
    
    return allTypes;
  }

  // Add a new step
  function addStep() {
    const isSideEffect = false; // New steps default to ONE_TO_ONE
    const stepNumber = config.steps.length + 1;
    const stepName = `Step ${stepNumber}`;
    // Convert step name to valid service name (lowercase, hyphens, svc suffix)
    const serviceName = `${stepName.toLowerCase().replace(/\s+/g, '-')}-svc`;
    const serviceNameCamel = toCamelCase(stepName.replace(/\s+/g, ''), { upperFirst: true });
    
    const newStep = {
      name: stepName,
      serviceName: serviceName,
      serviceNameCamel: serviceNameCamel,
      cardinality: 'ONE_TO_ONE',
      stepType: 'StepOneToOne',
      inputTypeName: config.steps.length === 0 ? 'InputType' : config.steps[config.steps.length - 1].outputTypeName, // Pipeline connection
      inputTypeSimpleName: config.steps.length === 0 ? 'InputType' : config.steps[config.steps.length - 1].outputTypeSimpleName,
      inputFields: config.steps.length === 0 ? [] : [...config.steps[config.steps.length - 1].outputFields], // Copy output fields from previous step
      outputTypeName: isSideEffect ? (config.steps.length === 0 ? 'InputType' : config.steps[config.steps.length - 1].outputTypeName) : `OutputType${stepNumber}`,
      outputTypeSimpleName: isSideEffect ? (config.steps.length === 0 ? 'InputType' : config.steps[config.steps.length - 1].outputTypeSimpleName) : `OutputType${stepNumber}`,
      outputFields: isSideEffect ? (config.steps.length === 0 ? [] : [...config.steps[config.steps.length - 1].outputFields]) : [],
      order: stepNumber,
      grpcClientName: `Step${stepNumber}Svc`
    };
    config.steps = [...config.steps, newStep];
    
    // Set up animation for the new step
    const newIndex = config.steps.length - 1;
    animatedStepIndex = newIndex;
    
    // Clear any existing animation timeout
    if (animationTimeout) {
      clearTimeout(animationTimeout);
    }
    
    // Set timeout to remove animation after 2 seconds
    animationTimeout = setTimeout(() => {
      animatedStepIndex = -1;
    }, 2000);
    
    // Automatically open the form for the new step
    currentStepIndex = newIndex;
    currentFormType = 'both';
    showInputForm = true;
  }

  // Remove a step
  function removeStep(index) {
    config.steps = config.steps.filter((_, i) => i !== index);
    // Update order numbers after removal
    config.steps.forEach((step, i) => {
      step.order = i + 1;
      // Update input types for subsequent steps
      if (i > 0) {
        step.inputTypeName = config.steps[i - 1].outputTypeName;
        step.inputTypeSimpleName = config.steps[i - 1].outputTypeSimpleName;
      }
    });
  }

  // Add a field to a step
  function addField(stepIndex, type) {
    const step = config.steps[stepIndex];
    const fields = type === 'input' ? step.inputFields : step.outputFields;
    
    const newField = {
      name: `${type}Field${fields.length + 1}`,
      type: 'String',  // Using Java type
      protoType: javaTypeToProtoType('String')
    };
    
    fields.push(newField);
    
    // Handle Side-Effect steps - keep input and output fields in sync
    if (step.cardinality === 'SIDE_EFFECT') {
      if (type === 'input') {
        step.outputFields.push({ ...newField });
      } else {
        step.inputFields.push({ ...newField });
      }
    }
    
    // Bidirectional synchronization
    if (type === 'output') {
      // If we're adding an output field, also add it to the input fields of the next step
      if (stepIndex < config.steps.length - 1) {
        const nextStep = config.steps[stepIndex + 1];
        nextStep.inputFields.push({ ...newField });
        config.steps[stepIndex + 1] = { ...nextStep };
      }
    } else if (type === 'input' && stepIndex > 0) {
      // If we're adding an input field, also add it to the output fields of the previous step
      const prevStep = config.steps[stepIndex - 1];
      prevStep.outputFields.push({ ...newField });
      config.steps[stepIndex - 1] = { ...prevStep };
    }
    
    // Update the step in the array
    config.steps[stepIndex] = { ...step };
    config = { ...config };
  }

  // Remove a field from a step
  function removeField(stepIndex, type, fieldIndex) {
    const step = config.steps[stepIndex];
    const fields = type === 'input' ? step.inputFields : step.outputFields;
    
    fields.splice(fieldIndex, 1);
    
    // Handle Side-Effect steps - keep input and output fields in sync
    if (step.cardinality === 'SIDE_EFFECT') {
      if (type === 'input') {
        step.outputFields.splice(fieldIndex, 1);
      } else {
        step.inputFields.splice(fieldIndex, 1);
      }
    }
    
    // Bidirectional synchronization
    if (type === 'output') {
      // If we're removing an output field, also remove it from the input fields of the next step
      if (stepIndex < config.steps.length - 1) {
        const nextStep = config.steps[stepIndex + 1];
        if (nextStep.inputFields && nextStep.inputFields[fieldIndex]) {
          nextStep.inputFields.splice(fieldIndex, 1);
          config.steps[stepIndex + 1] = { ...nextStep };
        }
      }
    } else if (type === 'input' && stepIndex > 0) {
      // If we're removing an input field, also remove it from the output fields of the previous step
      const prevStep = config.steps[stepIndex - 1];
      if (prevStep.outputFields && prevStep.outputFields[fieldIndex]) {
        prevStep.outputFields.splice(fieldIndex, 1);
        config.steps[stepIndex - 1] = { ...prevStep };
      }
    }
    
    // Update the step in the array
    config.steps[stepIndex] = { ...step };
    config = { ...config };
  }

  // Update a field's property
  function updateField(stepIndex, type, fieldIndex, property, value) {
    const step = config.steps[stepIndex];
    const fields = type === 'input' ? step.inputFields : step.outputFields;
    
    // Validate type if the property being updated is 'type'
    if (property === 'type' && !isValidFieldType(value, stepIndex)) {
      alert(`Invalid type '${value}'. Valid types are: scalar types (String, Integer, Long, Double, Boolean, UUID, BigDecimal, Currency, Path, LocalDateTime, LocalDate, OffsetDateTime, ZonedDateTime, Instant, Duration, Period, URI, URL, File, BigInteger, AtomicInteger, AtomicLong), Enum, List<T> (e.g. List<String>), Map<K,V> (e.g. Map<String,Integer>), or custom message types defined in previous steps.`);
      return; // Don't proceed with the update
    }
    
    fields[fieldIndex][property] = value;
    
    // Update protoType based on Java type mapping
    if (property === 'type') {
      fields[fieldIndex].protoType = javaTypeToProtoType(value);
    }
    
    // Handle Side-Effect steps - keep input and output fields in sync
    if (step.cardinality === 'SIDE_EFFECT') {
      if (type === 'input') {
        step.outputFields[fieldIndex][property] = value;
        if (property === 'type') {
          step.outputFields[fieldIndex].protoType = javaTypeToProtoType(value);
        }
      } else {
        step.inputFields[fieldIndex][property] = value;
        if (property === 'type') {
          step.inputFields[fieldIndex].protoType = javaTypeToProtoType(value);
        }
      }
    }
    
    // Update the step in the array
    config.steps[stepIndex] = { ...step };
    
    // Bidirectional synchronization
    if (type === 'output') {
      // If we updated an output field, also update the input field of the next step
      if (stepIndex < config.steps.length - 1) {
        const nextStep = config.steps[stepIndex + 1];
        if (nextStep && nextStep.inputFields && nextStep.inputFields[fieldIndex]) {
          nextStep.inputFields[fieldIndex][property] = value;
          if (property === 'type') {
            nextStep.inputFields[fieldIndex].protoType = javaTypeToProtoType(value);
          }
          config.steps[stepIndex + 1] = { ...nextStep };
        }
      }
    } else if (type === 'input' && stepIndex > 0) {
      // If we updated an input field, also update the output field of the previous step
      const prevStep = config.steps[stepIndex - 1];
      if (prevStep && prevStep.outputFields && prevStep.outputFields[fieldIndex]) {
        prevStep.outputFields[fieldIndex][property] = value;
        if (property === 'type') {
          prevStep.outputFields[fieldIndex].protoType = javaTypeToProtoType(value);
        }
        config.steps[stepIndex - 1] = { ...prevStep };
      }
    }
    
    config = { ...config };
  }

  // Check if a type is a Java scalar type (needs protobuf mapping)
  function isJavaScalarType(javaType) {
    const scalarTypes = [
      'string', 'integer', 'int', 'long', 'double', 'boolean', 
      'uuid', 'bigdecimal', 'currency', 'path',
      'list<string>', 'localdatetime', 'localdate', 'offsetdatetime', 'zoneddatetime', 'instant', 'duration', 'period',
      'uri', 'url', 'file', 'biginteger', 'atomicinteger', 'atomic_int', 'atomiclong', 'atomic_long'
    ];
    return scalarTypes.includes(javaType.toLowerCase());
  }
  
  // Check if a type is a List type
  function isListType(javaType) {
    if (!javaType || typeof javaType !== 'string') return false;
    
    // Simple list check (for basic List)
    if (javaType === 'List') return true;
    
    // Pattern check for generic List (e.g. List<String>, List<MyCustomType>)
    return /^List<.+>$/.test(javaType);
  }
  
  // Check if a type is a Map type
  function isMapType(javaType) {
    if (!javaType || typeof javaType !== 'string') return false;
    
    // Simple map check (for basic Map)
    if (javaType === 'Map') return true;
    
    // Pattern check for generic Map (e.g. Map<String,Integer>, Map<MyKey,MyValue>)
    return /^Map<.+?, .+>$/.test(javaType);
  }

  // Java to protobuf type mapping - only for scalar types
  function javaTypeToProtoType(javaType) {
    if (!isJavaScalarType(javaType)) {
      // For non-scalar types (custom message types), return as-is
      return javaType;
    }
    
    // Only map Java scalar types to protobuf types
    switch (javaType.toLowerCase()) {
      case 'string':
        return 'string';
      case 'integer':
      case 'int':
        return 'int32';
      case 'long':
        return 'int64';
      case 'double':
        return 'double';
      case 'boolean':
        return 'bool';
      case 'uuid':
      case 'currency':
      case 'path':
      case 'list<string>':
      case 'localdate':
      case 'localdatetime':
      case 'offsetdatetime':
      case 'zoneddatetime':
      case 'period':
      case 'uri':
      case 'url':
      case 'file':
      case 'bigdecimal':
      case 'biginteger':
        return 'string';
      case 'instant':
        return 'int64';
      case 'duration':
        return 'int64';
      case 'atomicinteger':
      case 'atomic_int':
        return 'int32';
      case 'atomiclong':
      case 'atomic_long':
        return 'int64';
      default:
        // Default to string for any other scalar type
        return 'string';
    }
  }

  // Get the corresponding proto type - now we're using Java types with automatic mapping to protobuf
  function getProtoType(javaType) {
    return javaTypeToProtoType(javaType);
  }

  // Convert string to camelCase (lowerCamelCase or UpperCamelCase/PascalCase)
  function toCamelCase(input, options = {}) {
    const { upperFirst = false } = options;
    const parts = input.trim().split(/\s+/);
    let result = '';
    
    for (let i = 0; i < parts.length; i++) {
      const part = parts[i];
      if (part.length > 0) {
        let processedPart = part.charAt(0) + part.slice(1).toLowerCase();
        if (i === 0) {
          // For the first part, apply upperFirst option if needed
          processedPart = upperFirst 
            ? processedPart.charAt(0).toUpperCase() + processedPart.slice(1)
            : processedPart.charAt(0).toLowerCase() + processedPart.slice(1);
        } else {
          // For subsequent parts, always capitalize the first letter
          processedPart = processedPart.charAt(0).toUpperCase() + processedPart.slice(1);
        }
        result += processedPart;
      }
    }
    
    return result;
  }

  // Generate YAML and download
  function downloadYaml() {
    // For now, create a simple YAML string to avoid complex escaping issues
    let yamlContent = '---\n';
    yamlContent += 'appName: \"' + config.appName + '\"\n';
    yamlContent += 'basePackage: \"' + config.basePackage + '\"\n';
    yamlContent += 'steps:\n';

    for (let i = 0; i < config.steps.length; i++) {
      const step = config.steps[i];
      yamlContent += '  - stepType: \"' + step.stepType + '\"\n';
      yamlContent += '    serviceNameCamel: \"' + step.serviceNameCamel + '\"\n';
      yamlContent += '    serviceName: \"' + step.serviceName + '\"\n';
      yamlContent += '    cardinality: \"' + step.cardinality + '\"\n';
      yamlContent += '    inputFields:\n';

      for (let j = 0; j < step.inputFields.length; j++) {
        const field = step.inputFields[j];
        yamlContent += '    - protoType: \"' + field.protoType + '\"\n';
        yamlContent += '      name: \"' + field.name + '\"\n';
        yamlContent += '      type: \"' + field.type + '\"\n';
      }

      yamlContent += '    outputFields:\n';

      for (let k = 0; k < step.outputFields.length; k++) {
        const field = step.outputFields[k];
        yamlContent += '    - protoType: \"' + field.protoType + '\"\n';
        yamlContent += '      name: \"' + field.name + '\"\n';
        yamlContent += '      type: \"' + field.type + '\"\n';
      }

      yamlContent += '    outputTypeName: \"' + step.outputTypeName + '\"\n';
      yamlContent += '    inputTypeName: \"' + step.inputTypeName + '\"\n';
      yamlContent += '    outputTypeSimpleName: \"' + step.outputTypeSimpleName + '\"\n';
      yamlContent += '    grpcClientName: \"' + step.grpcClientName + '\"\n';
      yamlContent += '    name: \"' + step.name + '\"\n';
      yamlContent += '    inputTypeSimpleName: \"' + step.inputTypeSimpleName + '\"\n';
      yamlContent += '    order: ' + step.order + '\n';
    }
  }

  // Generate YAML configuration content
  function generateYamlConfig() {
    const minimal = {
      appName: config.appName,
      basePackage: config.basePackage,
      steps: config.steps.map((s) => ({
        stepType: s.stepType,
        serviceNameCamel: s.serviceNameCamel,
        serviceName: s.serviceName,
        cardinality: s.cardinality,
        inputFields: s.inputFields,
        outputFields: s.outputFields,
        outputTypeName: s.outputTypeName,
        inputTypeName: s.inputTypeName,
        outputTypeSimpleName: s.outputTypeSimpleName,
        grpcClientName: s.grpcClientName,
        name: s.name,
        inputTypeSimpleName: s.inputTypeSimpleName,
        order: s.order
      }))
    };
    if (config.aspects && Object.keys(config.aspects).length > 0) {
      minimal.aspects = config.aspects;
    }
    return dump(minimal, { lineWidth: -1 });
  }

  // Check if app name and base package are valid
  function isAppInfoValid() {
    return config.appName && config.appName.trim() !== '' && 
           config.basePackage && config.basePackage.trim() !== '';
  }
  
  // Show app info dialog to collect app name and package if not set
  function showAppInfoDialogIfNeeded() {
    if (!isAppInfoValid()) {
      appInfoAppName = config.appName || 'My Pipeline App';
      appInfoBasePackage = config.basePackage || 'com.example.mypipeline';
      showAppInfoDialog = true;
    } else {
      startDownload();
    }
  }
  
  // Start the actual download process
  async function startDownload() {
    // Simple rate limiting to prevent abuse
    const now = Date.now();
    const lastDownload = localStorage.getItem('lastDownloadTime');
    const minInterval = 5000; // 5 seconds minimum between downloads
    
    if (lastDownload && (now - parseInt(lastDownload, 10)) < minInterval) {
      const remaining = Math.ceil((minInterval - (now - parseInt(lastDownload, 10))) / 1000);
      alert(`Please wait ${remaining} seconds before downloading another application.`);
      return;
    }
    
    // Set generating state
    isGenerating = true;
    
    try {
      // First, check if BrowserTemplateEngine is available
      if (typeof BrowserTemplateEngine === 'undefined') {
        alert('Template engine not available. Please ensure all required scripts are loaded.');
        isGenerating = false;
        return;
      }

      // Create a new instance of the browser template engine
      const templateEngine = new BrowserTemplateEngine();
      
      // Prepare a ZIP file to store the generated application
      const zip = new JSZip();
      
      // Create a file callback that adds files to the ZIP
      const fileCallback = (filePath, content) => {
        zip.file(filePath, content);
      };
      
      // Generate the complete application using the template engine
      const stepsCopy = [...config.steps];
      if (templateEngine.generateApplication.length >= 5) {
        await templateEngine.generateApplication(
          config.appName,
          config.basePackage,
          stepsCopy, // Use a copy to avoid potential mutation issues
          config.aspects,
          fileCallback
        );
      } else {
        await templateEngine.generateApplication(
          config.appName,
          config.basePackage,
          stepsCopy, // Use a copy to avoid potential mutation issues
          fileCallback
        );
      }
      
      // Generate the YAML configuration and add it to the ZIP
      const yamlContent = generateYamlConfig();
      const configFileName = config.appName.replace(/\s+/g, '-') + '-canvas-config.yaml';
      zip.file(configFileName, yamlContent);
      
      // Generate the ZIP file
      const zipContent = await zip.generateAsync({ type: 'blob' });
      
      // Trigger the download
      saveAs(
        zipContent, 
        config.appName.replace(/\s+/g, '-') + '-generated-app.zip'
      );
      
      // Store the download time only after successful generation and download
      localStorage.setItem('lastDownloadTime', now.toString());
      
      console.log('Application generated and downloaded successfully!');
    } catch (error) {
      console.error('Error generating application:', error);
      alert('Error generating application: ' + error.message);
    } finally {
      // Reset generating state
      isGenerating = false;
    }
  }
  
  // Download the complete application as a ZIP file
  async function downloadApplication() {
    showAppInfoDialogIfNeeded();
  }
  
  // Handle app info dialog confirm
  function handleAppInfoConfirm() {
    config.appName = appInfoAppName;
    config.basePackage = appInfoBasePackage;
    showAppInfoDialog = false;
    startDownload();
  }
  
  // Handle app info dialog cancel
  function handleAppInfoCancel() {
    showAppInfoDialog = false;
  }

  // Show upload confirmation dialog if there are existing steps
  function showUploadConfirmDialogIfNeeded(file) {
    if (config.steps.length > 0) {
      pendingUploadFile = file;
      showUploadConfirmDialog = true;
    } else {
      // If no existing steps, proceed directly with upload
      processUpload(file);
    }
  }
  
  // Process the upload after confirmation
  async function processUpload(file) {
    const text = await file.text();
    try {
      const data = load(text);
      
      // Validate the data structure
      if (!data.appName || !data.basePackage || !data.steps) {
        throw new Error('Invalid configuration file: missing required fields (appName, basePackage, steps)');
      }
      
      if (!Array.isArray(data.steps)) {
        throw new Error('Invalid configuration file: steps must be an array');
      }
      
      // Validate each step has required user-provided fields
      for (let i = 0; i < data.steps.length; i++) {
        const step = data.steps[i];
        
        if (!step.name) {
          throw new Error(`Invalid configuration file: step ${i+1} is missing required field (name)`);
        }
        
        if (!step.cardinality) {
          throw new Error(`Invalid configuration file: step ${i+1} is missing required field (cardinality)`);
        }
        
        if (!step.inputTypeName) {
          throw new Error(`Invalid configuration file: step ${i+1} is missing required field (inputTypeName)`);
        }
        
        if (!step.outputTypeName) {
          throw new Error(`Invalid configuration file: step ${i+1} is missing required field (outputTypeName)`);
        }
        
        if (!step.inputFields) {
          step.inputFields = [];
        } else if (!Array.isArray(step.inputFields)) {
          throw new Error(`Invalid configuration file: step ${i+1} inputFields must be an array`);
        }
        
        if (!step.outputFields) {
          step.outputFields = [];
        } else if (!Array.isArray(step.outputFields)) {
          throw new Error(`Invalid configuration file: step ${i+1} outputFields must be an array`);
        }
        
        // Compute missing computed fields if not present in the uploaded config
        if (!step.serviceName) {
          step.serviceName = step.name ? step.name.replace(/[^a-zA-Z0-9]/g, '-').toLowerCase() + '-svc' : `step-${i}-svc`;
        }
        
        if (!step.serviceNameCamel) {
          // Extract entity name from step name (e.g., "Process Customer" -> "Customer", "Validate Order" -> "Order")
          let entityName = step.name 
              ? step.name
                  .replace('Process ', '')
                  .replace('Validate ', '')
                  .replace('Enrich ', '')
                  .replace('Transform ', '')
                  .replace('Filter ', '')
                  .replace('Aggregate ', '')
                  .replace('Sort ', '')
                  .trim()
              : `Step${i+1}`;
          entityName = entityName.replace(/[^a-zA-Z0-9]/g, ' ').trim();
          
          // Convert to camelCase
          const capitalizedCamelName = toCamelCase(entityName, { upperFirst: true });
          
          step.serviceNameCamel = capitalizedCamelName;
        }
        
        if (!step.inputFields || !Array.isArray(step.inputFields)) {
          step.inputFields = [];
        }
        
        if (!step.outputFields || !Array.isArray(step.outputFields)) {
          step.outputFields = [];
        }
        
        // Validate fields have required properties and valid types
        for (let j = 0; j < step.inputFields.length; j++) {
          const field = step.inputFields[j];
          if (!field.name || !field.type || !field.protoType) {
            throw new Error(`Invalid configuration file: input field ${j+1} in step ${i+1} is missing required properties (name, type, protoType)`);
          }
          // Validate that field.type is a valid Java scalar type or custom message type from a previous step
          if (!isValidFieldType(field.type, i, data.steps)) {
            // Additional check to log what specifically is wrong with the type
            console.error(`Invalid field type detected: "${field.type}" in step ${i+1}, input field ${j+1}`);
            console.error(`Type length: ${field.type.length}, ends with: "${field.type.slice(-10)}"`);
            throw new Error(`Invalid configuration file: input field ${j+1} in step ${i+1} has invalid type '${field.type}'. Valid types are Java scalar types or custom message types defined in previous steps.`);
          }
        }
        
        for (let j = 0; j < step.outputFields.length; j++) {
          const field = step.outputFields[j];
          if (!field.name || !field.type || !field.protoType) {
            throw new Error(`Invalid configuration file: output field ${j+1} in step ${i+1} is missing required properties (name, type, protoType)`);
          }
          // Validate that field.type is a valid Java scalar type or custom message type from a previous step
          if (!isValidFieldType(field.type, i, data.steps)) {
            // Additional check to log what specifically is wrong with the type
            console.error(`Invalid field type detected: "${field.type}" in step ${i+1}, output field ${j+1}`);
            console.error(`Type length: ${field.type.length}, ends with: "${field.type.slice(-10)}"`);
            throw new Error(`Invalid configuration file: output field ${j+1} in step ${i+1} has invalid type '${field.type}'. Valid types are Java scalar types or custom message types defined in previous steps.`);
          }
        }
        
        // Set stepType based on cardinality
        if (!step.stepType) {
          switch (step.cardinality) {
            case 'EXPANSION':
              step.stepType = 'StepOneToMany';
              break;
            case 'REDUCTION':
              step.stepType = 'StepManyToOne';
              break;
            case 'MANY_TO_MANY':
              step.stepType = 'StepManyToMany';
              break;
            case 'SIDE_EFFECT':
              step.stepType = 'StepSideEffect';
              break;
            default:
              step.stepType = 'StepOneToOne';
              break;
          }
        }
        
        // For Side-Effect steps, ensure input and output types are aligned
        if (step.cardinality === 'SIDE_EFFECT') {
          step.outputTypeName = step.inputTypeName;
          step.outputTypeSimpleName = step.inputTypeSimpleName;
          step.outputFields = [...step.inputFields];
        }
      }
      
      config = {
        ...data,
        aspects: data.aspects || {}
      };
      if (!isAspectEnabled(config.aspects, 'persistence')) {
        delete config.aspects.persistence;
      }
      if (!isAspectEnabled(config.aspects, 'cache')) {
        delete config.aspects.cache;
      }
      syncAspectTogglesFromConfig();
      config = { ...config };
      currentStepIndex = -1;
      showInputForm = false;
    } catch (e) {
      alert('Error parsing YAML file: ' + e.message);
    }
  }
  
  // Handle file upload
  async function handleFileUpload(event) {
    const file = event.target.files[0];
    if (file && (file.name.endsWith('.yaml') || file.name.endsWith('.yml'))) {
      showUploadConfirmDialogIfNeeded(file);
    }
  }
  
  // Handle upload confirmation dialog confirm
  function handleUploadConfirm() {
    showUploadConfirmDialog = false;
    if (pendingUploadFile) {
      processUpload(pendingUploadFile);
      pendingUploadFile = null;
    }
  }
  
  // Handle upload confirmation dialog cancel
  function handleUploadCancel() {
    showUploadConfirmDialog = false;
    pendingUploadFile = null;
  }

  // Update step type when cardinality changes
  function updateStepType(stepIndex, newValue) {
    const step = config.steps[stepIndex];
    const oldValue = step.cardinality;
    prevCardinality.set(stepIndex, oldValue);
    step.cardinality = newValue;
    
    // Check if we're converting to SIDE_EFFECT and there are existing output fields
    if (step.cardinality === 'SIDE_EFFECT' && step.outputFields && step.outputFields.length > 0) {
      // Show confirmation dialog
      pendingStepIndex = stepIndex;
      pendingCardinality = step.cardinality;
      confirmationTitle = 'Convert to Side-Effect Step';
      confirmationMessage = `Converting to a Side-Effect step will synchronize the output type (${step.outputTypeName}) with the input type (${step.inputTypeName}). This will replace all existing output fields with the input fields. Are you sure you want to proceed?`;
      confirmationWarning = true;
      showConfirmationDialog = true;
      return; // Don't proceed with the change yet
    }
    
    // Proceed with the actual change
    applyStepTypeChange(stepIndex, step.cardinality);
  }
  
  // Apply the step type change after confirmation
  function applyStepTypeChange(stepIndex, cardinality) {
    const step = config.steps[stepIndex];
    step.cardinality = cardinality;
    
    switch (cardinality) {
      case 'EXPANSION':
        step.stepType = 'StepOneToMany';
        break;
      case 'REDUCTION':
        step.stepType = 'StepManyToOne';
        break;
      case 'SIDE_EFFECT':
        step.stepType = 'StepSideEffect';
        // For side-effect steps, output type should be the same as input type
        step.outputTypeName = step.inputTypeName;
        step.outputTypeSimpleName = step.inputTypeSimpleName;
        // Copy input fields to output fields for side-effect steps
        step.outputFields = [...step.inputFields];
        break;
      default:
        step.stepType = 'StepOneToOne';
        break;
    }
    config.steps[stepIndex] = { ...step };
    config = { ...config };
  }
  
  // Handle click on step - show combined form with both input and output fields
  function handleStepClick(stepIndex, side) {
    currentStepIndex = stepIndex;
    currentFormType = side; // Will be 'both' for steps
    
    if (side === 'both') {
      showInputForm = true; // Using input form to show both input/output for steps
    }
  }
  
  // Handle click on connector - show shared type form
  function handleConnectorClick(stepIndex, side, sharedType) {
    // The connector represents the connection between step (stepIndex-1) and step (stepIndex)
    // The data flowing through the connector is the output of step (stepIndex-1) which becomes
    // the input of step (stepIndex). We display the form for the step that receives this data,
    // which is step (stepIndex), so we show its input fields (which should match the previous step's output)
    currentStepIndex = stepIndex;  // Show the step that receives the data from the connector
    currentFormType = side; 
    showInputForm = true; // Using input form to represent shared type
  }
  
  // Close forms (we're now using a single combined form)
  function closeInputForm() {
    showInputForm = false;
    currentFormType = '';
  }
  
  // No longer using separate output form
  function closeOutputForm() {
    showInputForm = false; // Using single form now
    currentFormType = '';
  }
  
  // Add field from form (updated for CombinedFieldForm)
  function formAddField(type) {
    addField(currentStepIndex, type);
  }
  
  // Remove field from form (updated for CombinedFieldForm)
  function formRemoveField(type, fieldIndex) {
    removeField(currentStepIndex, type, fieldIndex);
  }
  
  // Update field from form (updated for CombinedFieldForm)
  function formUpdateField(type, fieldIndex, property, value) {
    updateField(currentStepIndex, type, fieldIndex, property, value);
  }
  
  // Update a step type property (for name changes)
  function updateStepTypeProperty(property, value) {
    if (currentStepIndex !== -1) {
      const step = config.steps[currentStepIndex];
      step[property] = value;
      
      // If the property being updated is the step name, also update the service name
      if (property === 'name') {
        // Convert step name to valid service name (lowercase, hyphens, svc suffix)
        step.serviceName = `${value.toLowerCase().replace(/\s+/g, '-')}-svc`;
        step.serviceNameCamel = toCamelCase(value, { upperFirst: true });
      }
      
      config.steps[currentStepIndex] = { ...step };
      config = { ...config };
    }
  }
  // Handle confirmation dialog confirm
  function handleConfirmationConfirm() {
    showConfirmationDialog = false;
    if (pendingStepIndex !== -1 && pendingCardinality) {
      applyStepTypeChange(pendingStepIndex, pendingCardinality);
      pendingStepIndex = -1;
      pendingCardinality = '';
    }
  }
  
  // Handle confirmation dialog cancel
  function handleConfirmationCancel() {
    showConfirmationDialog = false;
    if (pendingStepIndex !== -1) {
      const step = config.steps[pendingStepIndex];
      const old = prevCardinality.get(pendingStepIndex);
      if (old) step.cardinality = old;
      prevCardinality.delete(pendingStepIndex);
      config.steps[pendingStepIndex] = { ...step };
      config = { ...config };
      pendingStepIndex = -1;
      pendingCardinality = '';
    }
  }
  
  // Cleanup function to clear timeouts
  function cleanup() {
    if (animationTimeout) {
      clearTimeout(animationTimeout);
    }
  }
  
  // Set up cleanup for component destruction
  onMount(() => {
    return () => {
      cleanup();
    };
  });
</script>

<main class="flex flex-col h-screen bg-gray-100">
  <!-- App Name and Package Headers -->
  <div class="p-4 bg-white shadow-sm">
    <div class="flex flex-col md:flex-row md:items-center gap-4">
      <div class="flex-1">
        <label class="block text-sm font-medium text-gray-700 mb-1">App Name</label>
        <input 
          type="text" 
          bind:value={config.appName}
          class="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 text-sm"
        />
      </div>
      
      <div class="flex-1">
        <label class="block text-sm font-medium text-gray-700 mb-1">Base Package</label>
        <input 
          type="text" 
          bind:value={config.basePackage}
          class="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 text-sm"
        />
        <div class="mt-2 flex flex-wrap gap-4 text-sm text-gray-700">
          <label class="inline-flex items-center">
            <input
              type="checkbox"
              bind:checked={includePersistencePlugin}
              on:change={() => setAspectEnabled('persistence', includePersistencePlugin)}
              class="h-4 w-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
            />
            <span class="ml-2">Add persistence plugin</span>
          </label>
          <label class="inline-flex items-center">
            <input
              type="checkbox"
              bind:checked={includeCachePlugin}
              on:change={() => setAspectEnabled('cache', includeCachePlugin)}
              class="h-4 w-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
            />
            <span class="ml-2">Add cache plugin</span>
          </label>
        </div>
      </div>
    </div>
  </div>

  <!-- Canvas area -->
  <div class="flex-1 p-4 overflow-auto">
    <div class="flex flex-col items-center min-h-full">
      {#if config.steps.length === 0}
        <div class="flex flex-col items-center justify-center h-full text-gray-500">
          <p class="text-lg mb-4">No steps yet. Add your first step to get started.</p>
          
          <div class="flex flex-col items-center">
            <button 
              on:click={addStep}
              class="w-16 h-16 rounded-full bg-blue-600 text-white text-3xl flex items-center justify-center hover:bg-blue-700 shadow-lg flex-shrink-0 mb-4"
            >
              +
            </button>
            
            <p class="text-gray-500 my-2">-- OR --</p>
            
            <div class="flex flex-col items-center">
              <label class="text-sm text-gray-700 mb-1">Upload Config:</label>
              <input 
                type="file" 
                accept=".yaml,.yml"
                on:change={handleFileUpload}
                class="w-full text-sm"
              />
            </div>
          </div>
        </div>
      {:else}
        <!-- Container for steps - flex row for desktop, flex column for mobile -->
        <div class="w-full flex justify-center">
          <div class="flex md:flex-row flex-col items-center mt-8 w-full max-w-6xl overflow-x-auto pb-4">
            {#each config.steps as step, index}
              <div class="flex flex-col items-center my-2 mx-2 flex-shrink-0">
                <StepArrow 
                  {step} 
                  {index}
                  isAnimating={index === animatedStepIndex}
                  on:clickStep={(e) => handleStepClick(e.detail.index, e.detail.side)}
                />
                
                <!-- Step controls -->
                <div class="mt-1 flex flex-col items-center">
                  <select 
                    value={step.cardinality}
                    on:change={(e) => updateStepType(index, e.currentTarget.value)}
                    class="text-xs px-1 py-1 border border-gray-300 rounded"
                  >
                    {#each cardinalityOptions as option}
                      <option value={option.value}>{option.label}</option>
                    {/each}
                  </select>
                  <div class="flex space-x-1 mt-1">
                    <input 
                      type="text" 
                      bind:value={step.name}
                      class="text-xs w-20 px-1 py-1 border border-gray-300 rounded"
                    />
                    <button 
                      on:click={() => removeStep(index)}
                      class="text-xs px-1.5 py-1 bg-red-600 text-white rounded hover:bg-red-700"
                    >
                      Del
                    </button>
                  </div>
                </div>
              </div>
              
              <!-- Connector between steps -->
              {#if index < config.steps.length - 1}
                <Connector 
                  stepIndex={index + 1}
                  on:clickConnector={(e) => handleConnectorClick(e.detail.stepIndex, e.detail.side, e.detail.sharedType)}
                  sharedType={step.outputTypeName}
                  class="md:mx-4 my-2"
                />
              {:else}
                <!-- Add Step button at the end -->
                <div class="flex flex-col items-center ml-4 my-2">
                  <button 
                    on:click={addStep}
                    class="w-12 h-12 rounded-full bg-blue-600 text-white text-2xl flex items-center justify-center hover:bg-blue-700 shadow-lg flex-shrink-0"
                    title="Add Step"
                  >
                    +
                  </button>
                  <div class="mt-2 flex flex-col items-center">
                    <label class="text-xs text-gray-700 mb-1">Upload Config:</label>
                    <input 
                      type="file" 
                      accept=".yaml,.yml"
                      on:change={handleFileUpload}
                      class="w-full text-xs"
                    />
                  </div>
                </div>
              {/if}
            {/each}
          </div>
        </div>
        
        <!-- Download button below the steps -->
        <div class="mt-8 w-full max-w-4xl flex justify-center">
          <button 
            on:click={downloadApplication}
            class="px-6 py-3 bg-blue-600 text-white rounded-md hover:bg-blue-700 text-base disabled:opacity-50 w-full max-w-xs"
            disabled={isGenerating}
          >
            {isGenerating ? 'Generating...' : 'Download Complete Java Application'}
          </button>
        </div>
      {/if}
    </div>
  </div>

  <!-- Combined Field Form -->
  {#if showInputForm}
    <CombinedFieldForm
      step={config.steps[currentStepIndex]}
      stepIndex={currentStepIndex}
      fieldTypes={getAvailableFieldTypes(currentStepIndex)}
      formType={currentFormType}
      visible={showInputForm}
      on:close={closeInputForm}
      on:addInputField={() => formAddField('input')}
      on:addOutputField={() => formAddField('output')}
      on:addField={(e) => formAddField(e.detail.type)}
      on:removeField={(e) => formRemoveField(e.detail.type, e.detail.index)}
      on:updateField={(e) => formUpdateField(e.detail.type, e.detail.index, e.detail.property, e.detail.value)}
      on:typeChange={(e) => updateStepTypeProperty(e.detail.property, e.detail.value)}
      on:stepNameChange={(e) => updateStepTypeProperty('name', e.detail.name)}
    />
  {/if}
  
  <!-- Confirmation Dialog -->
  <ConfirmationDialog
    visible={showConfirmationDialog}
    title={confirmationTitle}
    message={confirmationMessage}
    warning={confirmationWarning}
    confirmText="Yes, Convert"
    cancelText="Cancel"
    on:confirm={handleConfirmationConfirm}
    on:cancel={handleConfirmationCancel}
  />
  
  <!-- App Info Dialog -->
  <div 
    class="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4"
    class:hidden={!showAppInfoDialog}
  >
    <div class="bg-white rounded-lg shadow-xl w-full max-w-md">
      <div class="p-6">
        <h3 class="text-lg font-medium text-gray-900 mb-4">Application Information</h3>
        <p class="text-gray-600 mb-4">Please provide the required information to generate your application:</p>
        
        <div class="space-y-4">
          <div>
            <label class="block text-sm font-medium text-gray-700 mb-1">App Name</label>
            <input 
              type="text" 
              bind:value={appInfoAppName}
              class="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 text-sm"
              placeholder="My Pipeline App"
            />
          </div>
          
          <div>
            <label class="block text-sm font-medium text-gray-700 mb-1">Base Package</label>
            <input 
              type="text" 
              bind:value={appInfoBasePackage}
              class="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 text-sm"
              placeholder="com.example.mypipeline"
            />
            <div class="mt-2 flex flex-wrap gap-4 text-sm text-gray-700">
              <label class="inline-flex items-center">
                <input
                  type="checkbox"
                  bind:checked={includePersistencePlugin}
                  on:change={() => setAspectEnabled('persistence', includePersistencePlugin)}
                  class="h-4 w-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                />
                <span class="ml-2">Add persistence plugin</span>
              </label>
              <label class="inline-flex items-center">
                <input
                  type="checkbox"
                  bind:checked={includeCachePlugin}
                  on:change={() => setAspectEnabled('cache', includeCachePlugin)}
                  class="h-4 w-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                />
                <span class="ml-2">Add cache plugin</span>
              </label>
            </div>
          </div>
        </div>
        
        <div class="mt-6 flex justify-end space-x-3">
          <button
            on:click={handleAppInfoCancel}
            class="px-4 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 hover:bg-gray-50"
          >
            Cancel
          </button>
          <button
            on:click={handleAppInfoConfirm}
            class="px-4 py-2 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700"
          >
            Confirm
          </button>
        </div>
      </div>
    </div>
  </div>
  
  <!-- Upload Confirmation Dialog -->
  <div 
    class="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4"
    class:hidden={!showUploadConfirmDialog}
  >
    <div class="bg-white rounded-lg shadow-xl w-full max-w-md">
      <div class="p-6">
        <h3 class="text-lg font-medium text-gray-900 mb-2">Confirm Upload</h3>
        <p class="text-gray-600 mb-4">Uploading a new configuration will replace your current pipeline steps. Are you sure you want to continue?</p>
        <p class="text-sm text-gray-500 mb-4">Note: You can save your current configuration before proceeding if needed.</p>
        
        <div class="mt-6 flex justify-end space-x-3">
          <button
            on:click={handleUploadCancel}
            class="px-4 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 hover:bg-gray-50"
          >
            Cancel
          </button>
          <button
            on:click={handleUploadConfirm}
            class="px-4 py-2 bg-red-600 text-white rounded-md text-sm font-medium hover:bg-red-700"
          >
            Yes, Replace Steps
          </button>
        </div>
      </div>
    </div>
  </div>
</main>
