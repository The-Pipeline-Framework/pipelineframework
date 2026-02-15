package org.pipelineframework.processor.util;

import java.util.Locale;
import java.util.Map;
import java.util.logging.Logger;
import javax.annotation.processing.ProcessingEnvironment;
import javax.tools.Diagnostic;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.StreamingShape;

/**
 * Resolves REST resource and operation paths for generated REST artifacts.
 */
public final class RestPathResolver {

    public static final String REST_NAMING_STRATEGY_OPTION = "pipeline.rest.naming.strategy";
    private static final String API_BASE_PATH = "/api/v1";
    private static final Logger LOG = Logger.getLogger(RestPathResolver.class.getName());

    private RestPathResolver() {
    }

    /**
     * Resolve the class-level REST path for a pipeline step.
     *
     * @param model pipeline step model
     * @param processingEnv processing environment used to read processor options
     * @return the resolved REST base path
     */
    public static String resolveResourcePath(PipelineStepModel model, ProcessingEnvironment processingEnv) {
        NamingStrategy strategy = resolveNamingStrategy(processingEnv);
        if (strategy == NamingStrategy.LEGACY) {
            return legacyResourcePath(model);
        }
        return resourcefulResourcePath(model);
    }

    /**
     * Resolve the process-operation path segment.
     *
     * @param processingEnv processing environment used to read processor options
     * @return "/process" in LEGACY mode, "/" in RESOURCEFUL mode
     */
    public static String resolveOperationPath(ProcessingEnvironment processingEnv) {
        return resolveNamingStrategy(processingEnv) == NamingStrategy.LEGACY ? "/process" : "/";
    }

    private static NamingStrategy resolveNamingStrategy(ProcessingEnvironment processingEnv) {
        String raw = null;
        if (processingEnv != null) {
            Map<String, String> options = processingEnv.getOptions();
            if (options != null) {
                raw = options.get(REST_NAMING_STRATEGY_OPTION);
            }
        }
        
        // If not found in processing environment options, check system property
        if (raw == null || raw.isBlank()) {
            raw = System.getProperty(REST_NAMING_STRATEGY_OPTION);
        }
        
        if (raw == null || raw.isBlank()) {
            return NamingStrategy.RESOURCEFUL;
        }
        String trimmed = raw.trim();
        if ("LEGACY".equalsIgnoreCase(trimmed)) {
            return NamingStrategy.LEGACY;
        }
        if ("RESOURCEFUL".equalsIgnoreCase(trimmed)) {
            return NamingStrategy.RESOURCEFUL;
        }
        String warning = "Unknown REST naming strategy '" + trimmed + "'; defaulting to RESOURCEFUL.";
        if (processingEnv != null && processingEnv.getMessager() != null) {
            processingEnv.getMessager().printMessage(Diagnostic.Kind.WARNING, warning);
        } else {
            System.err.println(warning);
        }
        return NamingStrategy.RESOURCEFUL;
    }

    private static String resourcefulResourcePath(PipelineStepModel model) {
        String resourceToken = toResourceToken(model);
        if (resourceToken == null || resourceToken.isBlank()) {
            LOG.warning("RESOURCEFUL naming fallback to LEGACY path for model '"
                + modelIdentity(model) + "' due to unresolved resource token.");
            return legacyResourcePath(model);
        }
        String basePath = API_BASE_PATH + "/" + resourceToken;
        if (!isSideEffect(model)) {
            return basePath;
        }

        String pluginToken = toPluginToken(model, resourceToken);
        if (pluginToken != null && !pluginToken.isBlank()) {
            return basePath + "/" + pluginToken;
        }

        String fallbackToken = toFallbackDisambiguationToken(model, resourceToken);
        if (fallbackToken != null && !fallbackToken.isBlank()) {
            return basePath + "/" + fallbackToken;
        }
        return basePath;
    }

    private static String legacyResourcePath(PipelineStepModel model) {
        String className = baseName(model);
        if (className == null || className.isBlank()) {
            return API_BASE_PATH + "/process";
        }
        String normalized = removeSuffix(className, "Service");
        normalized = removeSuffix(normalized, "Reactive");
        String pathPart = toKebabCase(normalized);
        if (pathPart == null || pathPart.isBlank()) {
            pathPart = "process";
        }
        return API_BASE_PATH + "/" + pathPart;
    }

    private static String toResourceToken(PipelineStepModel model) {
        if (model == null) {
            return null;
        }
        TypeName preferred = preferredResourceType(model);
        String preferredName = simpleTypeName(preferred);
        if (preferredName != null && !preferredName.isBlank()) {
            return toKebabCase(preferredName);
        }

        TypeName alternate = alternateResourceType(model);
        String alternateName = simpleTypeName(alternate);
        if (alternateName != null && !alternateName.isBlank()) {
            return toKebabCase(alternateName);
        }
        return null;
    }

    private static TypeName preferredResourceType(PipelineStepModel model) {
        StreamingShape shape = resolvedStreamingShape(model);
        return switch (shape) {
            case UNARY_STREAMING, STREAMING_STREAMING -> model.inboundDomainType();
            default -> model.outboundDomainType();
        };
    }

    private static TypeName alternateResourceType(PipelineStepModel model) {
        StreamingShape shape = resolvedStreamingShape(model);
        return switch (shape) {
            case UNARY_STREAMING, STREAMING_STREAMING -> model.outboundDomainType();
            default -> model.inboundDomainType();
        };
    }

    private static StreamingShape resolvedStreamingShape(PipelineStepModel model) {
        if (model == null || model.streamingShape() == null) {
            return StreamingShape.UNARY_UNARY;
        }
        return model.streamingShape();
    }

    private static String toPluginToken(PipelineStepModel model, String resourceToken) {
        String raw = normalizeStepBaseName(model);
        if (raw == null || raw.isBlank()) {
            return null;
        }

        String inbound = simpleTypeName(model.inboundDomainType());
        String outbound = simpleTypeName(model.outboundDomainType());
        raw = stripTypeSuffix(raw, inbound);
        raw = stripTypeSuffix(raw, outbound);
        if (raw == null || raw.isBlank()) {
            return null;
        }
        String token = toKebabCase(raw);
        if (token == null || token.isBlank()) {
            return null;
        }
        if (token.equals(resourceToken)) {
            return null;
        }
        return token;
    }

    private static String toFallbackDisambiguationToken(PipelineStepModel model, String resourceToken) {
        String raw = normalizeStepBaseName(model);
        if (raw == null || raw.isBlank()) {
            return null;
        }
        String token = toKebabCase(raw);
        if (token == null || token.isBlank() || token.equals(resourceToken)) {
            return "step";
        }
        return token;
    }

    private static String baseName(PipelineStepModel model) {
        if (model == null) {
            return null;
        }
        if (model.generatedName() != null && !model.generatedName().isBlank()) {
            return model.generatedName();
        }
        if (model.serviceName() != null && !model.serviceName().isBlank()) {
            return model.serviceName();
        }
        return model.serviceClassName() != null ? model.serviceClassName().simpleName() : null;
    }

    private static boolean isSideEffect(PipelineStepModel model) {
        return model != null && model.sideEffect();
    }

    private static String normalizeStepBaseName(PipelineStepModel model) {
        String raw = baseName(model);
        if (raw == null || raw.isBlank()) {
            return raw;
        }
        raw = removePrefix(raw, "Observe");
        raw = removeSuffix(raw, "SideEffect");
        raw = removeSuffix(raw, "Service");
        return removeSuffix(raw, "Reactive");
    }

    private static String stripTypeSuffix(String base, String typeName) {
        if (base == null || base.isBlank() || typeName == null || typeName.isBlank()) {
            return base;
        }
        String result = base;
        // Remove repeated trailing type suffixes while ensuring at least one non-type token remains.
        while (result.endsWith(typeName) && result.length() > typeName.length()) {
            result = result.substring(0, result.length() - typeName.length());
        }
        return result;
    }

    private static String simpleTypeName(TypeName typeName) {
        if (typeName == null) {
            return null;
        }
        if (typeName instanceof ClassName className) {
            return className.simpleName();
        }
        String raw = typeName.toString();
        int genericStart = raw.indexOf('<');
        if (genericStart >= 0) {
            raw = raw.substring(0, genericStart);
        }
        while (raw.endsWith("[]")) {
            raw = raw.substring(0, raw.length() - 2);
        }
        int lastDot = raw.lastIndexOf('.');
        return lastDot >= 0 ? raw.substring(lastDot + 1) : raw;
    }

    private static String removePrefix(String value, String prefix) {
        if (value == null || prefix == null || prefix.isBlank()) {
            return value;
        }
        return value.startsWith(prefix) ? value.substring(prefix.length()) : value;
    }

    private static String removeSuffix(String value, String suffix) {
        if (value == null || suffix == null || suffix.isBlank()) {
            return value;
        }
        return value.endsWith(suffix) ? value.substring(0, value.length() - suffix.length()) : value;
    }

    private static String toKebabCase(String value) {
        if (value == null) {
            return null;
        }
        String sanitized = value.trim();
        if (sanitized.isBlank()) {
            return null;
        }
        String boundaryNormalized = sanitized
            .replaceAll("([A-Z]+)([A-Z][a-z])", "$1-$2")
            .replaceAll("([a-z0-9])([A-Z])", "$1-$2")
            .replaceAll("[^A-Za-z0-9]+", "-")
            .replaceAll("-{2,}", "-")
            .toLowerCase(Locale.ROOT);
        boundaryNormalized = boundaryNormalized.replaceAll("^-+", "").replaceAll("-+$", "");
        return boundaryNormalized;
    }

    private enum NamingStrategy {
        LEGACY,
        RESOURCEFUL
    }

    private static String modelIdentity(PipelineStepModel model) {
        if (model == null) {
            return "unknown";
        }
        if (model.generatedName() != null && !model.generatedName().isBlank()) {
            return model.generatedName();
        }
        if (model.serviceName() != null && !model.serviceName().isBlank()) {
            return model.serviceName();
        }
        return model.serviceClassName() == null ? "unknown" : model.serviceClassName().toString();
    }
}
