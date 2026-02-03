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

const fs = require('fs-extra');
const path = require('path');
const Handlebars = require('handlebars');
let distTemplates = null;
try {
    distTemplates = require('../dist/templates');
} catch (error) {
    distTemplates = null;
}

// Register helper for replacing characters in strings
Handlebars.registerHelper('replace', function(str, find, repl) {
    if (typeof str !== 'string' || typeof find !== 'string') return str;
    // Literal global replace
    return str.split(find).join(repl);
});

// Register helper for adding numbers
Handlebars.registerHelper('add', function(a, b) {
    const numA = Number(a);
    const numB = Number(b);
    if (isNaN(numA) || isNaN(numB)) return 0;
    return numA + numB;
});

// Register helper for converting to lowercase
Handlebars.registerHelper('lowercase', function(str) {
    return typeof str === 'string' ? str.toLowerCase() : str;
});

// Register helper for gRPC side-effect client names derived from aspect + type names
Handlebars.registerHelper('grpcAspectSideEffectClientName', function(aspectName, typeName) {
    if (typeof aspectName !== 'string' || !aspectName.trim()) {
        return '';
    }
    if (typeof typeName !== 'string' || !typeName.trim()) {
        return '';
    }
    const base = typeName.trim();
    const withBoundaryHyphens = base
        .replace(/([A-Z]+)([A-Z][a-z])/g, '$1-$2')
        .replace(/([a-z0-9])([A-Z])/g, '$1-$2');
    return `observe-${aspectName.trim()}-${withBoundaryHyphens.toLowerCase()}-side-effect`;
});

// Register helper for Observe service names derived from aspect + type names
Handlebars.registerHelper('observeServiceName', function(aspectName, typeName) {
    if (typeof aspectName !== 'string' || !aspectName.trim()) {
        return '';
    }
    if (typeof typeName !== 'string' || !typeName.trim()) {
        return '';
    }
    const aspectParts = aspectName.trim().split(/[^A-Za-z0-9]+/).filter(Boolean);
    const aspectPascal = aspectParts
        .map(part => part.charAt(0).toUpperCase() + part.slice(1).toLowerCase())
        .join('');
    return `Observe${aspectPascal}${typeName.trim()}SideEffectService`;
});

// Register helper for getting the index of an element in an array
Handlebars.registerHelper('indexOf', function(array, value) {
    return Array.isArray(array) ? array.indexOf(value) : -1;
});

// Register helper for checking if a step is the first one
Handlebars.registerHelper('isFirstStep', function(index) {
    return index === 0;
});

// Register helper for checking cardinality types
Handlebars.registerHelper('isExpansion', function(cardinality) {
    return cardinality === 'EXPANSION';
});

Handlebars.registerHelper('isReduction', function(cardinality) {
    return cardinality === 'REDUCTION';
});

Handlebars.registerHelper('isSideEffect', function(cardinality) {
    return cardinality === 'SIDE_EFFECT';
});

// Register helper for checking if a type is a list
Handlebars.registerHelper('isListType', function(type) {
    return type && type.startsWith('List<');
});

// Register helper for extracting list inner type
Handlebars.registerHelper('listInnerType', function(type) {
    if (!type || !type.startsWith('List<') || !type.endsWith('>')) {
        return type;
    }
    return type.substring(5, type.length - 1).trim();
});

// Register helper for checking if a type is a map
Handlebars.registerHelper('isMapType', function(type) {
    if (!type || !type.startsWith('Map<') || !type.endsWith('>')) {
        return false;
    }
    // Check if there's a comma inside the brackets
    const innerContent = type.substring(4, type.length - 1);
    return innerContent.includes(',');
});

// Register helper for extracting map key and value types
// Register helper for extracting map key and value types
Handlebars.registerHelper('mapKeyType', function(type) {
    if (!type || !type.startsWith('Map<') || !type.includes(',') || !type.endsWith('>')) {
        return 'string';
    }
    const parts = type.substring(4, type.length - 1).split(',').map(s => s.trim());
    let keyType = parts[0] || 'string';
    // Convert Java types to protobuf types
    switch(keyType) {
        case 'String':
            return 'string';
        case 'Integer':
            return 'int32';
        case 'Long':
            return 'int64';
        case 'Double':
            return 'double';
        case 'Float':
            return 'float';
        case 'Boolean':
            return 'bool';
        case 'UUID':
            return 'string';
        case 'BigDecimal':
            return 'string';
        case 'Currency':
            return 'string';
        case 'Path':
            return 'string';
        case 'LocalDateTime':
            return 'string';
        case 'LocalDate':
            return 'string';
        case 'OffsetDateTime':
            return 'string';
        case 'ZonedDateTime':
            return 'string';
        case 'Instant':
            return 'string';
        case 'Duration':
            return 'string';
        case 'Period':
            return 'string';
        case 'URI':
            return 'string';
        case 'URL':
            return 'string';
        case 'File':
            return 'string';
        case 'BigInteger':
            return 'string';
        case 'AtomicInteger':
            return 'int32';
        case 'AtomicLong':
            return 'int64';
        case 'List<String>':
            return 'string';
        default:
            // Keys must be scalar; fall back to string
            return 'string';
    }
});

// Register helper for extracting map value type
Handlebars.registerHelper('mapValueType', function(type) {
    if (!type || !type.startsWith('Map<') || !type.endsWith('>')) {
        return 'string';
    }
    // Check if there's a comma inside the brackets
    const innerContent = type.substring(4, type.length - 1);
    if (!innerContent.includes(',')) {
        return 'string';
    }
    const parts = innerContent.split(',').map(s => s.trim());
    let valueType = parts[1] || 'string';
    // Convert Java types to protobuf types
    switch(valueType) {
        case 'String':
            return 'string';
        case 'Integer':
            return 'int32';
        case 'Long':
            return 'int64';
        case 'Double':
            return 'double';
        case 'Float':
            return 'float';
        case 'Boolean':
            return 'bool';
        case 'UUID':
            return 'string';
        case 'BigDecimal':
            return 'string';
        case 'Currency':
            return 'string';
        case 'Path':
            return 'string';
        case 'LocalDateTime':
            return 'string';
        case 'LocalDate':
            return 'string';
        case 'OffsetDateTime':
            return 'string';
        case 'ZonedDateTime':
            return 'string';
        case 'Instant':
            return 'string';
        case 'Duration':
            return 'string';
        case 'Period':
            return 'string';
        case 'URI':
            return 'string';
        case 'URL':
            return 'string';
        case 'File':
            return 'string';
        case 'BigInteger':
            return 'string';
        case 'AtomicInteger':
            return 'int32';
        case 'AtomicLong':
            return 'int64';
        case 'List<String>':
            return 'string';
        default:
            // Preserve message type names as-is
            return valueType;
    }
});

// Register helper for adding import flags
Handlebars.registerHelper('hasDateFields', function(fields) {
    if (!Array.isArray(fields)) return false;
    return fields.some(field => 
        ['LocalDate', 'LocalDateTime', 'OffsetDateTime', 'ZonedDateTime', 'Instant', 'Duration', 'Period'].includes(field.type)
    );
});

Handlebars.registerHelper('hasBigIntegerFields', function(fields) {
    if (!Array.isArray(fields)) return false;
    return fields.some(field => field.type === 'BigInteger');
});

Handlebars.registerHelper('hasBigDecimalFields', function(fields) {
    if (!Array.isArray(fields)) return false;
    return fields.some(field => field.type === 'BigDecimal');
});

Handlebars.registerHelper('hasCurrencyFields', function(fields) {
    if (!Array.isArray(fields)) return false;
    return fields.some(field => field.type === 'Currency');
});

Handlebars.registerHelper('hasPathFields', function(fields) {
    if (!Array.isArray(fields)) return false;
    return fields.some(field => field.type === 'Path');
});

Handlebars.registerHelper('hasNetFields', function(fields) {
    if (!Array.isArray(fields)) return false;
    return fields.some(field => ['URI', 'URL'].includes(field.type));
});

Handlebars.registerHelper('hasIoFields', function(fields) {
    if (!Array.isArray(fields)) return false;
    return fields.some(field => field.type === 'File');
});

Handlebars.registerHelper('hasAtomicFields', function(fields) {
    if (!Array.isArray(fields)) return false;
    return fields.some(field => ['AtomicInteger', 'AtomicLong'].includes(field.type));
});

Handlebars.registerHelper('hasUtilFields', function(fields) {
    if (!Array.isArray(fields)) return false;
    return fields.some(field => field.type === 'List<String>');
});

// Register helper to check if any field is a map type
Handlebars.registerHelper('hasMapFields', function(fields) {
    if (!Array.isArray(fields)) return false;
    return fields.some(field => field && typeof field.type === 'string' && field.type.startsWith('Map<'));
});

Handlebars.registerHelper('hasIdField', function(fields) {
    if (!Array.isArray(fields)) return false;
    return fields.some(field => field.name === 'id');
});

// Register helper to convert base package to path format
Handlebars.registerHelper('toPath', function(basePackage) {
    return basePackage.replace(/\./g, '/');
});

// Register helper to format service name for proto classes
Handlebars.registerHelper('formatForProtoClassName', function(serviceName) {
    // Convert service names like "process-customer-svc" to "ProcessCustomerSvc"
    if (!serviceName) return '';
    const parts = serviceName.split('-');
    return parts.map(part => {
        if (!part) return '';
        return part.charAt(0).toUpperCase() + part.slice(1).toLowerCase();
    }).join('');
});

// Register helper to check if a field is an ID field
Handlebars.registerHelper('isIdField', function(fieldName) {
    return fieldName === 'id';
});

// Register helper for sanitizing Java identifiers
Handlebars.registerHelper('sanitizeJavaIdentifier', function(fieldName) {
    if (typeof fieldName !== 'string') return fieldName;
    
    // Reserved words in Java that need to be escaped
    const reservedWords = [
        'abstract', 'assert', 'boolean', 'break', 'byte', 'case', 'catch', 'char', 'class',
        'const', 'continue', 'default', 'do', 'double', 'else', 'enum', 'extends', 'final',
        'finally', 'float', 'for', 'goto', 'if', 'implements', 'import', 'instanceof', 'int',
        'interface', 'long', 'native', 'new', 'package', 'private', 'protected', 'public',
        'return', 'short', 'static', 'strictfp', 'super', 'switch', 'synchronized', 'this',
        'throw', 'throws', 'transient', 'try', 'void', 'volatile', 'while', 'true', 'false', 'null'
    ];
    
    // Check if it's a reserved word
    if (reservedWords.includes(fieldName.toLowerCase())) {
        return fieldName + '_';  // Append underscore to reserved words
    }
    
    // Replace invalid characters with underscore
    let sanitized = fieldName.replace(/[^a-zA-Z0-9_$]/g, '_');
    
    // Ensure it doesn't start with a number
    if (sanitized.length > 0 && /\d/.test(sanitized[0])) {
        sanitized = '_' + sanitized;
    }
    
    // If it became empty (which shouldn't happen with real input), return a default name
    if (sanitized === '') {
        sanitized = 'field';
    }
    
    return sanitized;
});

// Register helper for unless functionality
Handlebars.registerHelper('unless', function(condition, options) {
    if (!condition) {
        return options.fn(this);
    } else {
        return options.inverse(this);
    }
});

class HandlebarsTemplateEngine {
    constructor(templatesPath = './templates') {
        this.templatesPath = templatesPath;
        this.compiledTemplates = new Map();
        this.loadTemplates();
    }

    loadTemplates() {
        const templateFiles = fs.readdirSync(this.templatesPath);
        
        for (const file of templateFiles) {
            if (file.endsWith('.hbs') || file.endsWith('.handlebars')) {
                const templatePath = path.join(this.templatesPath, file);
                const templateName = path.basename(file, path.extname(file));
                const templateContent = fs.readFileSync(templatePath, 'utf8');
                this.compiledTemplates.set(templateName, Handlebars.compile(templateContent));
            }
        }
        if (distTemplates) {
            for (const [templateName, templateContent] of Object.entries(distTemplates)) {
                if (!this.compiledTemplates.has(templateName)) {
                    this.compiledTemplates.set(templateName, Handlebars.compile(templateContent));
                }
            }
        }
    }

    render(templateName, context) {
        const template = this.compiledTemplates.get(templateName);
        if (!template) {
            throw new Error(`Template ${templateName} not found`);
        }
        return template(context);
    }

    async generateApplication(appName, basePackage, steps, aspects, transport, outputPath) {
        if (typeof aspects === 'string' && outputPath === undefined) {
            outputPath = aspects;
            aspects = {};
            transport = 'GRPC';
        }
        if (typeof transport === 'string' && outputPath === undefined) {
            outputPath = transport;
            transport = 'GRPC';
        }
        const aspectConfig = aspects || {};
        const transportMode = typeof transport === 'string' && transport.trim() ? transport.trim() : 'GRPC';
        const includePersistenceModule = this.isAspectEnabled(aspectConfig, 'persistence');
        const includeCacheInvalidationModule = this.isAspectEnabled(aspectConfig, 'cache-invalidate')
            || this.isAspectEnabled(aspectConfig, 'cache-invalidate-all');
        const aspectDefinitions = this.getAspectDefinitions(aspectConfig);
        // For sequential pipeline, update input types of steps after the first one
        // to match the output type of the previous step
        for (let i = 1; i < steps.length; i++) {
            const currentStep = steps[i];
            const previousStep = steps[i - 1];
            // Set the input type of the current step to the output type of the previous step
            currentStep.inputTypeName = previousStep.outputTypeName;
            currentStep.inputFields = Array.isArray(previousStep.outputFields) 
                ? previousStep.outputFields.map(field => ({...field}))  // Shallow copy each field object to avoid shared references
                : previousStep.outputFields; // Copy input fields from previous step's outputs
        }

        // Create output directory
        await fs.ensureDir(outputPath);

        // Generate parent POM
        await this.generateParentPom(
            appName,
            basePackage,
            steps,
            includePersistenceModule,
            includeCacheInvalidationModule,
            outputPath);

        // Generate common module
        await this.generateCommonModule(appName, basePackage, steps, aspectDefinitions, transportMode, outputPath);

        // Generate each step service
        for (let i = 0; i < steps.length; i++) {
            await this.generateStepService(appName, basePackage, steps[i], outputPath, i, steps, transportMode);
        }

        if (includePersistenceModule) {
            await this.generatePersistenceModule(appName, basePackage, steps, outputPath);
        }

        if (includeCacheInvalidationModule) {
            await this.generateCacheInvalidationModule(appName, basePackage, steps, includePersistenceModule, outputPath);
        }

        // Generate orchestrator
        await this.generateOrchestrator(
            appName,
            basePackage,
            steps,
            includePersistenceModule,
            includeCacheInvalidationModule,
            aspectDefinitions,
            transportMode,
            outputPath);

        // Generate utility scripts
        await this.generateUtilityScripts(outputPath);

        // Generate mvnw files
        await this.generateMvNWFiles(outputPath);

        // Generate Maven wrapper files
        await this.generateMavenWrapperFiles(outputPath);

        // Generate other files
        await this.generateOtherFiles(
            appName,
            basePackage,
            steps,
            includePersistenceModule,
            includeCacheInvalidationModule,
            outputPath);
    }

    async generateParentPom(
        appName,
        basePackage,
        steps,
        includePersistenceModule,
        includeCacheInvalidationModule,
        outputPath) {
        const context = {
            basePackage,
            artifactId: appName.toLowerCase().replace(/[^a-zA-Z0-9]/g, '-'),
            name: appName,
            steps,
            includePersistenceModule,
            includeCacheInvalidationModule
        };

        const rendered = this.render('parent-pom', context);
        const pomPath = path.join(outputPath, 'pom.xml');
        await fs.writeFile(pomPath, rendered);
    }

    async generateCommonModule(appName, basePackage, steps, aspectDefinitions, transport, outputPath) {
        const commonPath = path.join(outputPath, 'common');
        await fs.ensureDir(path.join(commonPath, 'src/main/java', this.toPath(basePackage + '.common.domain')));
        await fs.ensureDir(path.join(commonPath, 'src/main/java', this.toPath(basePackage + '.common.dto')));
        await fs.ensureDir(path.join(commonPath, 'src/main/java', this.toPath(basePackage + '.common.mapper')));
        await fs.ensureDir(path.join(commonPath, 'src/main/resources'));
        const transportMode = typeof transport === 'string' && transport.trim()
            ? transport.trim().toUpperCase()
            : 'GRPC';
        if (transportMode !== 'REST') {
            await fs.ensureDir(path.join(commonPath, 'src/main/proto'));
        }

        // Generate common POM
        await this.generateCommonPom(appName, basePackage, commonPath, transportMode);

        // Generate entities, DTOs, and mappers for each step
        for (let i = 0; i < steps.length; i++) {
            const step = steps[i];
            await this.generateDomainClasses(step, basePackage, commonPath, i);
            await this.generateDtoClasses(step, basePackage, commonPath, i);
            await this.generateMapperClasses(step, basePackage, commonPath, i);
        }

        // Generate base entity
        await this.generateBaseEntity(basePackage, commonPath);

        // Generate common converters
        await this.generateCommonConverters(basePackage, commonPath);

        // Generate application.properties
        await this.generateCommonApplicationProperties(commonPath, transportMode);
    }

    async generatePersistenceModule(appName, basePackage, steps, outputPath) {
        const modulePath = path.join(outputPath, 'persistence-svc');
        const packagePath = this.toPath(basePackage + '.persistence');
        await fs.ensureDir(path.join(modulePath, 'src/main/java', packagePath));
        await fs.ensureDir(path.join(modulePath, 'src/main/resources'));

        const rootProjectName = appName.toLowerCase().replace(/[^a-zA-Z0-9]/g, '-');
        const pomContent = this.render('persistence-svc-pom', { basePackage, rootProjectName });
        await fs.writeFile(path.join(modulePath, 'pom.xml'), pomContent);

        const hostContent = this.render('persistence-plugin-host', { basePackage });
        await fs.writeFile(
            path.join(modulePath, 'src/main/java', packagePath, 'PersistencePluginHost.java'),
            hostContent);

        await this.generatePersistenceApplicationProperties(
            basePackage,
            rootProjectName,
            steps.length + 1,
            modulePath);
        await this.generateModuleDevProperties(modulePath);
        await this.generatePersistenceTestProperties(modulePath);
        await this.generateModuleBeansXml(modulePath);
        await this.copyBinaryResourceFiles(modulePath, false);
    }

    async generateCacheInvalidationModule(appName, basePackage, steps, includePersistenceModule, outputPath) {
        const modulePath = path.join(outputPath, 'cache-invalidation-svc');
        const packagePath = this.toPath(basePackage + '.cache');
        await fs.ensureDir(path.join(modulePath, 'src/main/java', packagePath));
        await fs.ensureDir(path.join(modulePath, 'src/main/resources'));

        const rootProjectName = appName.toLowerCase().replace(/[^a-zA-Z0-9]/g, '-');
        const pomContent = this.render('cache-invalidation-svc-pom', { basePackage, rootProjectName });
        await fs.writeFile(path.join(modulePath, 'pom.xml'), pomContent);

        const hostContent = this.render('cache-invalidation-plugin-host', { basePackage });
        await fs.writeFile(
            path.join(modulePath, 'src/main/java', packagePath, 'CacheInvalidationPluginHost.java'),
            hostContent);

        const hostAllContent = this.render('cache-invalidation-all-plugin-host', { basePackage });
        await fs.writeFile(
            path.join(modulePath, 'src/main/java', packagePath, 'CacheInvalidationAllPluginHost.java'),
            hostAllContent);

        const cacheHostContent = this.render('cache-plugin-host', { basePackage });
        await fs.writeFile(
            path.join(modulePath, 'src/main/java', packagePath, 'CachePluginHost.java'),
            cacheHostContent);

        const baseOffset = steps.length + (includePersistenceModule ? 2 : 1);
        await this.generateCacheInvalidationApplicationProperties(
            basePackage,
            appName.toLowerCase().replace(/[^a-zA-Z0-9]/g, '-'),
            baseOffset,
            modulePath);
        await this.generateModuleDevProperties(modulePath);
        await this.generateCacheInvalidationTestProperties(modulePath);
        await this.generateModuleBeansXml(modulePath);
        await this.copyBinaryResourceFiles(modulePath, false);
    }

    async generateCommonPom(appName, basePackage, commonPath, transport) {
        const transportMode = typeof transport === 'string' && transport.trim()
            ? transport.trim().toUpperCase()
            : 'GRPC';
        const context = {
            basePackage,
            rootProjectName: appName.toLowerCase().replace(/[^a-zA-Z0-9]/g, '-'),
            isRestTransport: transportMode === 'REST',
            isGrpcTransport: transportMode !== 'REST'
        };

        const rendered = this.render('common-pom', context);
        const pomPath = path.join(commonPath, 'pom.xml');
        await fs.writeFile(pomPath, rendered);
    }

    async generateDomainClasses(step, basePackage, commonPath, stepIndex) {
        // Process input domain class only for first step
        if (stepIndex === 0 && step.inputFields && step.inputTypeName) {
            const inputContext = {
                ...step,
                basePackage,
                className: step.inputTypeName,
                fields: step.inputFields,
                hasDateFields: this.hasImportFlag(step.inputFields, ['LocalDate', 'LocalDateTime', 'OffsetDateTime', 'ZonedDateTime', 'Instant', 'Duration', 'Period']),
                hasBigIntegerFields: this.hasImportFlag(step.inputFields, ['BigInteger']),
                hasBigDecimalFields: this.hasImportFlag(step.inputFields, ['BigDecimal']),
                hasCurrencyFields: this.hasImportFlag(step.inputFields, ['Currency']),
                hasPathFields: this.hasImportFlag(step.inputFields, ['Path']),
                hasNetFields: this.hasImportFlag(step.inputFields, ['URI', 'URL']),
                hasIoFields: this.hasImportFlag(step.inputFields, ['File']),
                hasAtomicFields: this.hasImportFlag(step.inputFields, ['AtomicInteger', 'AtomicLong']),
                hasUtilFields: this.hasImportFlag(step.inputFields, ['List<String>']),
                hasMapFields: this.hasMapType(step.inputFields),
                hasIdField: step.inputFields.some(field => field.name === 'id')
            };

            const rendered = this.render('domain', inputContext);
            const inputDomainPath = path.join(commonPath, 'src/main/java', this.toPath(basePackage + '.common.domain'), step.inputTypeName + '.java');
            await fs.writeFile(inputDomainPath, rendered);
        }

        // Process output domain class for all steps
        if (step.outputFields && step.outputTypeName) {
            const outputContext = {
                ...step,
                basePackage,
                className: step.outputTypeName,
                fields: step.outputFields,
                hasDateFields: this.hasImportFlag(step.outputFields, ['LocalDate', 'LocalDateTime', 'OffsetDateTime', 'ZonedDateTime', 'Instant', 'Duration', 'Period']),
                hasBigIntegerFields: this.hasImportFlag(step.outputFields, ['BigInteger']),
                hasBigDecimalFields: this.hasImportFlag(step.outputFields, ['BigDecimal']),
                hasCurrencyFields: this.hasImportFlag(step.outputFields, ['Currency']),
                hasPathFields: this.hasImportFlag(step.outputFields, ['Path']),
                hasNetFields: this.hasImportFlag(step.outputFields, ['URI', 'URL']),
                hasIoFields: this.hasImportFlag(step.outputFields, ['File']),
                hasAtomicFields: this.hasImportFlag(step.outputFields, ['AtomicInteger', 'AtomicLong']),
                hasUtilFields: this.hasImportFlag(step.outputFields, ['List<String>']),
                hasMapFields: this.hasMapType(step.outputFields),
                hasIdField: step.outputFields.some(field => field.name === 'id')
            };

            const rendered = this.render('domain', outputContext);
            const outputDomainPath = path.join(commonPath, 'src/main/java', this.toPath(basePackage + '.common.domain'), step.outputTypeName + '.java');
            await fs.writeFile(outputDomainPath, rendered);
        }
    }

    async generateBaseEntity(basePackage, commonPath) {
        const context = { basePackage };
        const rendered = this.render('base-entity', context);
        const baseEntityPath = path.join(commonPath, 'src/main/java', this.toPath(basePackage + '.common.domain'), 'BaseEntity.java');
        await fs.writeFile(baseEntityPath, rendered);
    }

    async generateDtoClasses(step, basePackage, commonPath, stepIndex) {
        // Process input DTO class only for first step
        if (stepIndex === 0 && step.inputFields && step.inputTypeName) {
            const inputContext = {
                ...step,
                basePackage,
                className: step.inputTypeName + 'Dto',
                fields: step.inputFields,
                hasDateFields: this.hasImportFlag(step.inputFields, ['LocalDate', 'LocalDateTime', 'OffsetDateTime', 'ZonedDateTime', 'Instant', 'Duration', 'Period']),
                hasBigIntegerFields: this.hasImportFlag(step.inputFields, ['BigInteger']),
                hasBigDecimalFields: this.hasImportFlag(step.inputFields, ['BigDecimal']),
                hasCurrencyFields: this.hasImportFlag(step.inputFields, ['Currency']),
                hasPathFields: this.hasImportFlag(step.inputFields, ['Path']),
                hasNetFields: this.hasImportFlag(step.inputFields, ['URI', 'URL']),
                hasIoFields: this.hasImportFlag(step.inputFields, ['File']),
                hasAtomicFields: this.hasImportFlag(step.inputFields, ['AtomicInteger', 'AtomicLong']),
                hasUtilFields: this.hasImportFlag(step.inputFields, ['List<String>']),
                hasMapFields: this.hasMapType(step.inputFields),
                hasIdField: step.inputFields.some(field => field.name === 'id')
            };

            const rendered = this.render('dto', inputContext);
            const inputDtoPath = path.join(commonPath, 'src/main/java', this.toPath(basePackage + '.common.dto'), step.inputTypeName + 'Dto.java');
            await fs.writeFile(inputDtoPath, rendered);
        }

        // Process output DTO class for all steps
        if (step.outputFields && step.outputTypeName) {
            const outputContext = {
                ...step,
                basePackage,
                className: step.outputTypeName + 'Dto',
                fields: step.outputFields,
                hasDateFields: this.hasImportFlag(step.outputFields, ['LocalDate', 'LocalDateTime', 'OffsetDateTime', 'ZonedDateTime', 'Instant', 'Duration', 'Period']),
                hasBigIntegerFields: this.hasImportFlag(step.outputFields, ['BigInteger']),
                hasBigDecimalFields: this.hasImportFlag(step.outputFields, ['BigDecimal']),
                hasCurrencyFields: this.hasImportFlag(step.outputFields, ['Currency']),
                hasPathFields: this.hasImportFlag(step.outputFields, ['Path']),
                hasNetFields: this.hasImportFlag(step.outputFields, ['URI', 'URL']),
                hasIoFields: this.hasImportFlag(step.outputFields, ['File']),
                hasAtomicFields: this.hasImportFlag(step.outputFields, ['AtomicInteger', 'AtomicLong']),
                hasUtilFields: this.hasImportFlag(step.outputFields, ['List<String>']),
                hasMapFields: this.hasMapType(step.outputFields),
                hasIdField: step.outputFields.some(field => field.name === 'id')
            };

            const rendered = this.render('dto', outputContext);
            const outputDtoPath = path.join(commonPath, 'src/main/java', this.toPath(basePackage + '.common.dto'), step.outputTypeName + 'Dto.java');
            await fs.writeFile(outputDtoPath, rendered);
        }
    }

    async generateMapperClasses(step, basePackage, commonPath, stepIndex) {
        // Generate input mapper class only for first step (since other steps reference previous step's output)
        if (stepIndex === 0 && step.inputTypeName) {
            await this.generateMapperClass(step.inputTypeName, step, basePackage, commonPath);
        }

        // Generate output mapper class for all steps
        if (step.outputTypeName) {
            await this.generateMapperClass(step.outputTypeName, step, basePackage, commonPath);
        }
    }

    async generateMapperClass(className, step, basePackage, commonPath) {
        const context = {
            ...step,
            basePackage,
            className,
            domainClass: className.replace('Dto', ''),
            dtoClass: className + 'Dto',
            grpcClass: basePackage + '.grpc.' + this.formatForProtoClassName(step.serviceName)
        };

        const rendered = this.render('mapper', context);
        const mapperPath = path.join(commonPath, 'src/main/java', this.toPath(basePackage + '.common.mapper'), className + 'Mapper.java');
        await fs.writeFile(mapperPath, rendered);
    }

    async generateCommonConverters(basePackage, commonPath) {
        const context = { basePackage };
        const rendered = this.render('common-converters', context);
        const convertersPath = path.join(commonPath, 'src/main/java', this.toPath(basePackage + '.common.mapper'), 'CommonConverters.java');
        await fs.writeFile(convertersPath, rendered);
    }

    async generateCommonApplicationProperties(commonPath, transport) {
        const transportMode = typeof transport === 'string' && transport.trim()
            ? transport.trim().toUpperCase()
            : 'GRPC';
        const context = {
            isRestTransport: transportMode === 'REST',
            isGrpcTransport: transportMode !== 'REST'
        };
        const rendered = this.render('common-application-properties', context);
        const appPropsPath = path.join(commonPath, 'src/main/resources', 'application.properties');
        await fs.writeFile(appPropsPath, rendered);
    }

    async generateUtilityScripts(outputPath) {
        const context = {};
        if (this.compiledTemplates.has('up-local')) {
            const upLocalContent = this.render('up-local', context);
            await fs.writeFile(path.join(outputPath, 'up-local.sh'), upLocalContent);
        }
        if (this.compiledTemplates.has('down-local')) {
            const downLocalContent = this.render('down-local', context);
            await fs.writeFile(path.join(outputPath, 'down-local.sh'), downLocalContent);
        }
    }

    async generateStepService(appName, basePackage, step, outputPath, stepIndex, allSteps, transport) {
        const safeServiceName = String(step.serviceName || '')
          .toLowerCase()
          .replace(/[^a-z0-9\-_]/g, '');
        if (!safeServiceName) throw new Error('Invalid service name');
        const stepPath = path.join(outputPath, safeServiceName);
        // Convert hyphens to underscores for valid Java package names
        const serviceNameForPackage = safeServiceName.replace('-svc', '').replace(/-/g, '_');
        await fs.ensureDir(path.join(stepPath, 'src/main/java', this.toPath(basePackage + '.' + serviceNameForPackage + '.service')));
        await fs.ensureDir(path.join(stepPath, 'src/main/resources'));
        await fs.ensureDir(path.join(stepPath, 'src/main/resources', 'META-INF'));

        // Add rootProjectName to step map
        step.rootProjectName = appName.toLowerCase().replace(/[^a-zA-Z0-9]/g, '-');

        // Generate step POM
        await this.generateStepPom(step, basePackage, stepPath, transport);

        // Generate the service class
        await this.generateStepServiceClass(appName, basePackage, step, stepPath, stepIndex, allSteps);

        step.portOffset = stepIndex + 1;

        // Generate application.properties
        await this.generateApplicationProperties(step, basePackage, stepPath);

        // Generate application-dev.properties
        await this.generateApplicationDevProperties(step, basePackage, stepPath);

        // Generate application-test.properties
        await this.generateApplicationTestProperties(step, basePackage, stepPath);

        // Generate beans.xml
        await this.generateBeansXml(step, basePackage, stepPath);

        // Copy binary resource files (e.g., keystore)
        await this.copyBinaryResourceFiles(stepPath, true);
    }

    async generateStepPom(step, basePackage, stepPath, transport) {
        const transportMode = typeof transport === 'string' && transport.trim()
            ? transport.trim().toUpperCase()
            : 'GRPC';
        const context = {
            ...step,
            basePackage,
            isRestTransport: transportMode === 'REST',
            isGrpcTransport: transportMode !== 'REST'
        };
        const rendered = this.render('step-pom', context);
        const pomPath = path.join(stepPath, 'pom.xml');
        await fs.writeFile(pomPath, rendered);
    }

    async generateStepServiceClass(appName, basePackage, step, stepPath, stepIndex, allSteps) {
        const context = { ...step };
        context.basePackage = basePackage;
        context.serviceName = step.serviceName.replace('-svc', '');
        // Convert hyphens to underscores for valid Java package names
        context.serviceNameForPackage = step.serviceName.replace('-svc', '').replace(/-/g, '_');

        // Format service name for proto-generated class names
        const protoClassName = this.formatForProtoClassName(step.serviceName);
        context.protoClassName = protoClassName;

        // Use the serviceNameCamel field from the configuration to form the gRPC class names
        const serviceNameCamel = step.serviceNameCamel ?? (step.serviceName || '').replace(/-svc$/, '').replace(/-([a-z])/g, (_, c) => c.toUpperCase());
        // Convert camelCase to PascalCase
        const serviceNamePascal = serviceNameCamel
          ? serviceNameCamel.charAt(0).toUpperCase() + serviceNameCamel.slice(1)
          : this.formatForProtoClassName(step.serviceName);

        // Extract the entity name from the PascalCase service name to match proto service names
        const entityName = this.extractEntityName(serviceNamePascal);

        context.serviceNamePascal = serviceNamePascal;
        context.serviceNameFormatted = step.name;

        let reactiveServiceInterface = 'ReactiveService';
        let grpcAdapter = 'GrpcReactiveServiceAdapter';
        let processMethodReturnType = `Uni<${step.outputTypeName}>`;
        let processMethodParamType = step.inputTypeName;
        let returnStatement = 'Uni.createFrom().item(output)';

        if (step.cardinality === 'EXPANSION') {
            reactiveServiceInterface = 'ReactiveStreamingService';
            grpcAdapter = 'GrpcServiceStreamingAdapter';
            processMethodReturnType = `Multi<${step.outputTypeName}>`;
            returnStatement = 'Multi.createFrom().item(output)';
        } else if (step.cardinality === 'REDUCTION') {
            reactiveServiceInterface = 'ReactiveStreamingClientService';
            grpcAdapter = 'GrpcServiceClientStreamingAdapter';
            processMethodParamType = `Multi<${step.inputTypeName}>`;
            returnStatement = 'Uni.createFrom().item(output)';
        } else if (step.cardinality === 'SIDE_EFFECT') {
            reactiveServiceInterface = 'ReactiveService';
            grpcAdapter = 'GrpcReactiveServiceAdapter';
            returnStatement = 'Uni.createFrom().item(input)';
        }

        context.reactiveServiceInterface = reactiveServiceInterface;
        context.grpcAdapter = grpcAdapter;
        context.processMethodReturnType = processMethodReturnType;
        context.processMethodParamType = processMethodParamType;
        context.returnStatement = returnStatement;

        const rendered = this.render('step-service', context);
        const servicePath = path.join(stepPath, 'src/main/java', this.toPath(basePackage + '.' + context.serviceNameForPackage + '.service'), 'Process' + serviceNamePascal + 'Service.java');
        await fs.writeFile(servicePath, rendered);
    }

    async generateOrchestratorApplicationProperties(
        appName,
        basePackage,
        steps,
        includePersistenceModule,
        includeCacheInvalidationModule,
        aspectDefinitions,
        transport,
        orchPath) {
        // Create context for orchestrator properties
        const context = { appName, basePackage, steps, transport };
        const transportMode = typeof transport === 'string' && transport.trim()
            ? transport.trim().toUpperCase()
            : 'GRPC';
        context.isRestTransport = transportMode === 'REST';
        context.isGrpcTransport = !context.isRestTransport;
        context.serviceName = 'orchestrator-svc';
        context.clientStepSuffix = transportMode === 'REST'
            ? 'RestClientStep'
            : 'GrpcClientStep';
        context.rootProjectName = appName.toLowerCase().replace(/[^a-zA-Z0-9]/g, '-');
        context.includePersistenceModule = includePersistenceModule;
        context.includeCacheInvalidationModule = includeCacheInvalidationModule;
        context.hasCacheAspect = (aspectDefinitions || []).some(aspect => aspect.name === 'cache');
        if (includePersistenceModule) {
            context.persistencePortOffset = steps.length + 1;
            const outputTypes = new Set();
            steps.forEach(step => {
                if (step.outputTypeName) {
                    outputTypes.add(step.outputTypeName);
                }
            });
            context.persistenceSideEffectTypes = Array.from(outputTypes);
            context.persistenceAspectNames = (aspectDefinitions || [])
                .filter(aspect => aspect.name === 'persistence')
                .filter(aspect => {
                    const targets = aspect.enabledTargets || [];
                    return targets.includes('CLIENT_STEP') || targets.includes('GRPC_SERVICE');
                })
                .map(aspect => aspect.name);
        }
        if (includeCacheInvalidationModule) {
            const baseOffset = steps.length + (includePersistenceModule ? 2 : 1);
            context.cacheInvalidationPortOffset = baseOffset;
            const inputTypes = new Set();
            steps.forEach(step => {
                if (step.inputTypeName) {
                    inputTypes.add(step.inputTypeName);
                }
            });
            context.cacheInvalidationSideEffectTypes = Array.from(inputTypes);
            context.cacheInvalidationAspectNames = (aspectDefinitions || [])
                .filter(aspect => aspect.name.startsWith('cache-invalidate'))
                .filter(aspect => {
                    const targets = aspect.enabledTargets || [];
                    return targets.includes('CLIENT_STEP') || targets.includes('GRPC_SERVICE');
                })
                .map(aspect => aspect.name);
            const outputTypes = new Set();
            steps.forEach(step => {
                if (step.outputTypeName) {
                    outputTypes.add(step.outputTypeName);
                }
            });
            context.cacheSideEffectTypes = Array.from(outputTypes);
            context.cacheAspectNames = (aspectDefinitions || [])
                .filter(aspect => aspect.name === 'cache')
                .filter(aspect => {
                    const targets = aspect.enabledTargets || [];
                    return targets.includes('CLIENT_STEP') || targets.includes('GRPC_SERVICE');
                })
                .map(aspect => aspect.name);
        }
        // Process steps to add additional properties for template
        context.steps = steps.map((step, index) => ({
            ...step,
            portOffset: index + 1,
            serviceNameForPackage: step.serviceName.replace('-svc', '').replace(/-/g, '_'),
            serviceNameFormatted: this.formatForProtoClassName(step.serviceName),
            serviceNameCamel: step.serviceNameCamel
        }));
        const rendered = this.render('orchestrator-application-properties', context);
        const appPropsPath = path.join(orchPath, 'src/main/resources', 'application.properties');
        await fs.writeFile(appPropsPath, rendered);
    }

    async generateOrchestratorApplicationDevProperties(appName, basePackage, steps, orchPath) {
        const context = { basePackage, steps };
        const rendered = this.render('orchestrator-application-dev-properties', context);
        const appDevPropsPath = path.join(orchPath, 'src/main/resources', 'application-dev.properties');
        await fs.writeFile(appDevPropsPath, rendered);
    }

    async generateApplicationProperties(step, basePackage, stepPath) {
        const context = { ...step, basePackage };
        const rendered = this.render('application-properties', context);
        const appPropsPath = path.join(stepPath, 'src/main/resources', 'application.properties');
        await fs.writeFile(appPropsPath, rendered);
    }

    async generatePersistenceApplicationProperties(basePackage, rootProjectName, portOffset, modulePath) {
        const context = {
            basePackage,
            basePackageSuffix: basePackage.split('.').pop(),
            rootProjectName,
            portOffset,
            serviceName: 'persistence-svc'
        };
        const rendered = this.render('persistence-application-properties', context);
        const appPropsPath = path.join(modulePath, 'src/main/resources', 'application.properties');
        await fs.writeFile(appPropsPath, rendered);
    }

    async generateCacheInvalidationApplicationProperties(basePackage, rootProjectName, portOffset, modulePath) {
        const context = {
            basePackage,
            basePackageSuffix: basePackage.split('.').pop(),
            rootProjectName,
            portOffset,
            serviceName: 'cache-invalidation-svc'
        };
        const rendered = this.render('cache-invalidation-application-properties', context);
        const appPropsPath = path.join(modulePath, 'src/main/resources', 'application.properties');
        await fs.writeFile(appPropsPath, rendered);
    }

    async generateModuleDevProperties(modulePath) {
        const rendered = this.render('module-application-dev-properties', {});
        const appDevPropsPath = path.join(modulePath, 'src/main/resources', 'application-dev.properties');
        await fs.writeFile(appDevPropsPath, rendered);
    }

    async generateModuleTestProperties(modulePath, templateName) {
        const rendered = this.render(templateName, {});
        const testResourcesPath = path.join(modulePath, 'src/test/resources');
        await fs.ensureDir(testResourcesPath);
        const appTestPropsPath = path.join(testResourcesPath, 'application.properties');
        await fs.writeFile(appTestPropsPath, rendered);
    }

    async generatePersistenceTestProperties(modulePath) {
        await this.generateModuleTestProperties(modulePath, 'persistence-application-test-properties');
    }

    async generateCacheInvalidationTestProperties(modulePath) {
        await this.generateModuleTestProperties(modulePath, 'cache-invalidation-application-test-properties');
    }

    async generateModuleBeansXml(modulePath) {
        const rendered = this.render('module-beans-xml', {}).trimEnd();
        const beansXmlPath = path.join(modulePath, 'src/main/resources', 'META-INF', 'beans.xml');
        await fs.ensureDir(path.dirname(beansXmlPath));
        await fs.writeFile(beansXmlPath, rendered);
    }

    async generateApplicationDevProperties(step, basePackage, stepPath) {
        const context = { ...step, basePackage };
        const rendered = this.render('application-dev-properties', context);
        const appDevPropsPath = path.join(stepPath, 'src/main/resources', 'application-dev.properties');
        await fs.writeFile(appDevPropsPath, rendered);
    }

    async generateApplicationTestProperties(step, basePackage, stepPath) {
        const context = { ...step, basePackage };
        const rendered = this.render('application-test-properties', context);
        const testResourcesPath = path.join(stepPath, 'src/test/resources');
        await fs.ensureDir(testResourcesPath);
        const appTestPropsPath = path.join(testResourcesPath, 'application.properties');
        await fs.writeFile(appTestPropsPath, rendered);
    }

    async generateOrchestratorApplicationTestProperties(orchPath) {
        const rendered = this.render('orchestrator-application-test-properties', {});
        const testResourcesPath = path.join(orchPath, 'src/test/resources');
        await fs.ensureDir(testResourcesPath);
        const appTestPropsPath = path.join(testResourcesPath, 'application.properties');
        await fs.writeFile(appTestPropsPath, rendered);
    }

    async generateBeansXml(step, basePackage, stepPath) {
        const context = { ...step, basePackage };
        const rendered = this.render('step-beans-xml', context).trimEnd();
        const beansXmlPath = path.join(stepPath, 'src/main/resources', 'META-INF', 'beans.xml');
        await fs.writeFile(beansXmlPath, rendered);
    }

    // Copy binary files like keystore to the resources directory
    async copyBinaryResourceFiles(stepPath, includeReadme) {
        // This would copy the server-keystore.jks from a default location if available
        const sourceKeystorePath = path.join(__dirname, '../templates/server-keystore.jks');
        const targetKeystorePath = path.join(stepPath, 'src/main/resources', 'server-keystore.jks');

        // Check if source keystore exists before copying
        if (fs.existsSync(sourceKeystorePath)) {
            await fs.copy(sourceKeystorePath, targetKeystorePath);
        }
        if (includeReadme) {
            const noticeContent = `# Keystore File Needed
#
# This application requires a server-keystore.jks file for SSL/TLS functionality.
#
# Please generate or obtain a keystore file and place it in this location:
#
# ${targetKeystorePath}
#
# For development purposes, you can create a self-signed certificate using:
# keytool -genkey -alias server -keyalg RSA -keystore server-keystore.jks -storetype PKCS12
#
# For production, please use a proper certificate from a trusted CA.
`;
            await fs.writeFile(path.join(stepPath, 'src/main/resources', 'keystore-README.txt'), noticeContent);
        }
    }

    // Copy orchestrator-specific binary files like truststore
    async copyOrchestratorBinaryResourceFiles(orchPath) {
        // This would copy the client-truststore.jks from a default location if available
        const sourceTruststorePath = path.join(__dirname, '../templates/client-truststore.jks');
        const targetTruststorePath = path.join(orchPath, 'src/main/resources', 'client-truststore.jks');

        // Check if source truststore exists before copying
        if (fs.existsSync(sourceTruststorePath)) {
            await fs.copy(sourceTruststorePath, targetTruststorePath);
        }
        const noticeContent = `# Truststore File Needed
#
# This application requires a client-truststore.jks file for SSL/TLS client functionality.
#
# Please generate or obtain a truststore file and place it in this location:
#
# ${targetTruststorePath}
#
# For development purposes, you can create a truststore using:
# keytool -import -alias server -file server-cert.pem -keystore client-truststore.jks
#
# For production, please use a proper certificate from a trusted CA.
`;
        await fs.writeFile(path.join(orchPath, 'src/main/resources', 'truststore-README.txt'), noticeContent);
    }

    async generateOrchestrator(
        appName,
        basePackage,
        steps,
        includePersistenceModule,
        includeCacheInvalidationModule,
        aspectDefinitions,
        transport,
        outputPath) {
        const orchPath = path.join(outputPath, 'orchestrator-svc');
        const classPath = path.join(orchPath, 'src/main/java', this.toPath(basePackage + '.orchestrator.service'));
        await fs.ensureDir(classPath);
        await fs.ensureDir(path.join(orchPath, 'src/main/resources'));

        // Generate orchestrator application
        await this.generateOrchestratorApplication(appName, basePackage, classPath);

        // Generate orchestrator POM
        await this.generateOrchestratorPom(
            appName,
            basePackage,
            includePersistenceModule,
            includeCacheInvalidationModule,
            transport,
            orchPath);

        // Generate orchestrator application.properties
        await this.generateOrchestratorApplicationProperties(
            appName,
            basePackage,
            steps,
            includePersistenceModule,
            includeCacheInvalidationModule,
            aspectDefinitions,
            transport,
            orchPath);

        // Generate orchestrator application-dev.properties
        await this.generateOrchestratorApplicationDevProperties(appName, basePackage, steps, orchPath);

        // Copy orchestrator binary resource files (e.g., truststore)
        await this.copyOrchestratorBinaryResourceFiles(orchPath);
        await this.copyBinaryResourceFiles(orchPath, false);
        await this.generateOrchestratorApplicationTestProperties(orchPath);
    }

    async generateOrchestratorPom(
        appName,
        basePackage,
        includePersistenceModule,
        includeCacheInvalidationModule,
        transport,
        orchPath) {
        const transportMode = typeof transport === 'string' && transport.trim()
            ? transport.trim().toUpperCase()
            : 'GRPC';
        const context = {
            basePackage,
            artifactId: 'orchestrator-svc',
            rootProjectName: appName.toLowerCase().replace(/[^a-zA-Z0-9]/g, '-'),
            includePersistenceModule,
            includeCacheInvalidationModule,
            isRestTransport: transportMode === 'REST',
            isGrpcTransport: transportMode !== 'REST'
        };

        const rendered = this.render('orchestrator-pom', context);
        const pomPath = path.join(orchPath, 'pom.xml');
        await fs.writeFile(pomPath, rendered);
    }

    async generateOrchestratorApplication(appName, basePackage, classPath) {
        const context = {
            appName,
            basePackage
        };

        const rendered = this.render('orchestrator-application', context);
        const mainAppPath = path.join(classPath, 'OrchestratorHost.java');
        await fs.writeFile(mainAppPath, rendered);
    }

    async generateMvNWFiles(outputPath) {
        // Create mvnw (Unix)
        const context = {};
        const mvnwContent = this.render('mvnw', context);
        const mvnwPath = path.join(outputPath, 'mvnw');
        await fs.writeFile(mvnwPath, mvnwContent);
        await fs.chmod(mvnwPath, 0o755); // Make executable

        // Create mvnw.cmd (Windows)
        const mvnwCmdContent = this.render('mvnw-cmd', context);
        const mvnwCmdPath = path.join(outputPath, 'mvnw.cmd');
        await fs.writeFile(mvnwCmdPath, mvnwCmdContent);
    }

    async generateMavenWrapperFiles(outputPath) {
        // Create .mvn/wrapper directory
        const wrapperDir = path.join(outputPath, '.mvn', 'wrapper');
        await fs.ensureDir(wrapperDir);

        // Copy maven-wrapper.properties (we'll use a template)
        // In a real implementation, we'd have this file in our templates directory
        const mavenWrapperProperties = `# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
wrapperVersion=3.3.4
distributionType=only-script
distributionUrl=https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/3.9.11/apache-maven-3.9.11-bin.zip
wrapperUrl=https://repo.maven.apache.org/maven2/org/apache/maven/wrapper/maven-wrapper/3.3.4/maven-wrapper-3.3.4.jar
`;
        await fs.writeFile(path.join(wrapperDir, 'maven-wrapper.properties'), mavenWrapperProperties);
    }

    async generateOtherFiles(
        appName,
        basePackage,
        steps,
        includePersistenceModule,
        includeCacheInvalidationModule,
        outputPath) {
        const firstInputTypeName = steps && steps.length ? steps[0].inputTypeName : 'Input';
        const optionsClass = `${firstInputTypeName}Options`;

        // Create README
        const readmeContext = {
            appName,
            basePackage,
            rootProjectName: appName.toLowerCase().replace(/[^a-zA-Z0-9]/g, '-'),
            optionsImport: `${basePackage}.common.util.${optionsClass}`,
            requestTypeName: firstInputTypeName,
            optionsClass
        };
        const readmeContent = this.render('readme', readmeContext);
        const readmePath = path.join(outputPath, 'README.md');
        await fs.writeFile(readmePath, readmeContent);

        // Create .gitignore
        const gitignoreContent = this.render('gitignore', {});
        const gitignorePath = path.join(outputPath, '.gitignore');
        await fs.writeFile(gitignorePath, gitignoreContent);

        // Create formatter config
        const formatterContent = this.render('quarkus-formatter', {});
        const formatterDir = path.join(outputPath, 'ide-config');
        await fs.ensureDir(formatterDir);
        const formatterPath = path.join(formatterDir, 'quarkus-formatter.xml');
        await fs.writeFile(formatterPath, formatterContent);

        const certScriptContext = {
            appName,
            steps,
            includePersistenceModule,
            includeCacheInvalidationModule
        };
        const certScriptContent = this.render('generate-dev-certs', certScriptContext);
        const certScriptPath = path.join(outputPath, 'generate-dev-certs.sh');
        await fs.writeFile(certScriptPath, certScriptContent);
        await fs.chmod(certScriptPath, 0o755);
    }

    // Utility methods
    toPath(packageName) {
        return packageName.replace(/\./g, '/');
    }

    hasImportFlag(fields, types) {
        if (!Array.isArray(fields)) return false;
        return fields.some(field => types.includes(field.type));
    }

    hasMapType(fields) {
        if (!Array.isArray(fields)) return false;
        return fields.some(field => field && typeof field.type === 'string' && field.type.startsWith('Map<'));
    }

    formatForClassName(input) {
        if (!input) return '';
        // Split by spaces and capitalize each word
        const parts = input.split(' ');
        return parts
            .filter(part => part)
            .map(part => part.charAt(0).toUpperCase() + part.slice(1).toLowerCase())
            .join('');
    }

    formatForProtoClassName(input) {
        if (!input) return '';
        // Convert service names like "process-customer-svc" to "ProcessCustomerSvc"
        const parts = input.split('-');
        return parts
            .filter(part => part)
            .map(part => part.charAt(0).toUpperCase() + part.slice(1).toLowerCase())
            .join('');
    }

    simpleTypeName(typeName) {
        if (!typeName || typeof typeName !== 'string') {
            return '';
        }
        const parts = typeName.split('.');
        return parts[parts.length - 1] || '';
    }

    computePipelineStreamingShape(steps) {
        if (!Array.isArray(steps) || steps.length === 0) {
            return { inputStreaming: false, outputStreaming: false };
        }
        const inputStreaming = this.isStreamingInputCardinality(steps[0].cardinality);
        let outputStreaming = inputStreaming;
        for (const step of steps) {
            outputStreaming = this.applyCardinalityToStreaming(step.cardinality, outputStreaming);
        }
        return { inputStreaming, outputStreaming };
    }

    isStreamingInputCardinality(cardinality) {
        return cardinality === 'REDUCTION' || cardinality === 'MANY_TO_MANY';
    }

    applyCardinalityToStreaming(cardinality, currentStreaming) {
        switch (cardinality) {
            case 'EXPANSION':
            case 'MANY_TO_MANY':
                return true;
            case 'REDUCTION':
                return false;
            default:
                return currentStreaming;
        }
    }

    extractEntityName(serviceNamePascal) {
        // If it starts with "Process", return everything after "Process"
        if (serviceNamePascal.startsWith('Process')) {
            return serviceNamePascal.substring('Process'.length);
        }
        // For other cases, we'll default to the whole string
        return serviceNamePascal;
    }

    isAspectEnabled(aspects, aspectName) {
        if (!aspects || !Object.prototype.hasOwnProperty.call(aspects, aspectName)) {
            return false;
        }
        const config = aspects[aspectName];
        return config == null || config.enabled !== false;
    }

    getAspectDefinitions(aspects) {
        if (!aspects || typeof aspects !== 'object') {
            return [];
        }
        const definitions = [];
        for (const [name, config] of Object.entries(aspects)) {
            if (!this.isAspectEnabled(aspects, name)) {
                continue;
            }
            const position = config && typeof config.position === 'string'
                ? config.position
                : 'AFTER_STEP';
            const enabledTargets = config
                && config.config
                && Array.isArray(config.config.enabledTargets)
                ? config.config.enabledTargets.map(target => String(target))
                : [];
            definitions.push({
                name,
                position,
                enabledTargets
            });
        }
        return definitions;
    }
}

module.exports = HandlebarsTemplateEngine;
