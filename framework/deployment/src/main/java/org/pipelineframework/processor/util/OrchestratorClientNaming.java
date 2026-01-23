package org.pipelineframework.processor.util;

import java.util.Locale;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;
import org.pipelineframework.processor.ir.PipelineStepModel;

/**
 * Utility methods for orchestrator client naming conventions.
 */
public final class OrchestratorClientNaming {

    private OrchestratorClientNaming() {
    }

    public static String clientNameForModel(PipelineStepModel model) {
        if (model == null) {
            return null;
        }
        if (model.sideEffect()) {
            String aspectName = resolveAspectName(model);
            String typeName = resolveSideEffectTypeName(model);
            String typeKebab = toKebabCase(typeName);
            return "observe-" + aspectName + "-" + typeKebab + "-side-effect";
        }
        String baseName = baseServiceName(model.serviceName());
        if (baseName.isBlank()) {
            return null;
        }
        return "process-" + toKebabCase(baseName);
    }

    public static String resolveAspectName(PipelineStepModel model) {
        if (model == null) {
            return "aspect";
        }
        String serviceName = model.serviceName();
        if (serviceName == null) {
            return "aspect";
        }
        String typeName = resolveSideEffectTypeName(model);
        String trimmed = serviceName;
        if (trimmed.startsWith("Observe")) {
            trimmed = trimmed.substring("Observe".length());
        }
        if (trimmed.endsWith("SideEffectService")) {
            trimmed = trimmed.substring(0, trimmed.length() - "SideEffectService".length());
        }
        if (typeName != null && !typeName.isBlank() && trimmed.endsWith(typeName)) {
            trimmed = trimmed.substring(0, trimmed.length() - typeName.length());
        }
        if (trimmed.isBlank()) {
            return "aspect";
        }
        return toKebabCase(trimmed);
    }

    public static String resolveSideEffectTypeName(PipelineStepModel model) {
        if (model == null || model.inputMapping() == null) {
            return "payload";
        }
        return simpleTypeName(model.inputMapping().domainType());
    }

    public static String baseServiceName(String serviceName) {
        if (serviceName == null) {
            return "";
        }
        String trimmed = serviceName;
        if (trimmed.startsWith("Process")) {
            trimmed = trimmed.substring("Process".length());
        }
        if (trimmed.endsWith("Service")) {
            trimmed = trimmed.substring(0, trimmed.length() - "Service".length());
        }
        return trimmed;
    }

    public static String toKebabCase(String value) {
        if (value == null || value.isBlank()) {
            return "";
        }
        String withBoundaryHyphens = value
            .replaceAll("([A-Z]+)([A-Z][a-z])", "$1-$2")
            .replaceAll("([a-z0-9])([A-Z])", "$1-$2");
        return withBoundaryHyphens.replace('_', '-').toLowerCase(Locale.ROOT);
    }

    private static String simpleTypeName(TypeName type) {
        if (type instanceof ClassName className) {
            return className.simpleName();
        }
        if (type == null) {
            return "payload";
        }
        String name = type.toString();
        int generics = name.indexOf('<');
        if (generics != -1) {
            name = name.substring(0, generics);
        }
        int lastDot = name.lastIndexOf('.');
        String simple = lastDot == -1 ? name : name.substring(lastDot + 1);
        return simple.replace("[]", "");
    }
}
