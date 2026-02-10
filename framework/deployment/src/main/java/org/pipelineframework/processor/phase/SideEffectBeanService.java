package org.pipelineframework.processor.phase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import org.jboss.logging.Logger;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.PipelineStepProcessor;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.GrpcBinding;
import org.pipelineframework.processor.ir.PipelineStepModel;

/**
 * Generates side-effect CDI beans and resolves their observed types.
 */
public class SideEffectBeanService {

    private static final Logger LOG = Logger.getLogger(SideEffectBeanService.class);
    private static final String CACHE_SERVICE_CLASS = "org.pipelineframework.plugin.cache.CacheService";

    private final GenerationPathResolver pathResolver;

    public SideEffectBeanService(GenerationPathResolver pathResolver) {
        this.pathResolver = Objects.requireNonNull(pathResolver, "pathResolver must not be null");
    }

    /**
     * Generate a side-effect bean for a model.
     *
     * @param ctx compilation context
     * @param model step model
     * @param role generated role annotation role
     * @param outputRole role output directory override
     * @param grpcBinding grpc binding for observed type resolution
     */
    public void generateSideEffectBean(
            PipelineCompilationContext ctx,
            PipelineStepModel model,
            DeploymentRole role,
            DeploymentRole outputRole,
            GrpcBinding grpcBinding) {
        Objects.requireNonNull(role, "role must not be null");
        if (model == null || model.serviceClassName() == null) {
            return;
        }
        if (ctx == null || ctx.getProcessingEnv() == null) {
            return;
        }

        TypeName observedType = resolveObservedType(model, role, grpcBinding);
        TypeName parentType = ParameterizedTypeName.get(model.serviceClassName(), observedType);

        String packageName = model.servicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX;
        String serviceClassName = model.serviceName();

        TypeSpec.Builder beanBuilder = TypeSpec.classBuilder(serviceClassName)
            .addModifiers(javax.lang.model.element.Modifier.PUBLIC)
            .superclass(parentType)
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.enterprise.context", "Dependent")).build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("io.quarkus.arc", "Unremovable")).build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("org.pipelineframework.annotation", "GeneratedRole"))
                .addMember("value", "$T.$L",
                    ClassName.get("org.pipelineframework.annotation.GeneratedRole", "Role"),
                    role.name())
                .build());

        javax.lang.model.element.TypeElement pluginElement =
            ctx.getProcessingEnv().getElementUtils().getTypeElement(model.serviceClassName().canonicalName());
        MethodSpec constructor = buildSideEffectConstructor(pluginElement);
        if (constructor != null) {
            beanBuilder.addMethod(constructor);
        }

        TypeSpec beanClass = beanBuilder.build();
        try {
            JavaFile.builder(packageName, beanClass)
                .build()
                .writeTo(pathResolver.resolveRoleOutputDir(ctx, outputRole == null ? role : outputRole));
        } catch (IOException e) {
            ctx.getProcessingEnv().getMessager().printMessage(javax.tools.Diagnostic.Kind.WARNING,
                "Failed to generate side-effect bean for '" + model.serviceName() + "': " + e.getMessage());
        }
    }

    private MethodSpec buildSideEffectConstructor(javax.lang.model.element.TypeElement pluginElement) {
        if (pluginElement == null) {
            return null;
        }

        List<javax.lang.model.element.ExecutableElement> constructors = pluginElement.getEnclosedElements().stream()
            .filter(element -> element.getKind() == javax.lang.model.element.ElementKind.CONSTRUCTOR)
            .map(element -> (javax.lang.model.element.ExecutableElement) element)
            .toList();
        if (constructors.isEmpty()) {
            return null;
        }

        javax.lang.model.element.ExecutableElement selected = selectConstructor(constructors);
        List<? extends javax.lang.model.element.VariableElement> params = selected.getParameters();
        if (params.isEmpty()) {
            return MethodSpec.constructorBuilder()
                .addAnnotation(ClassName.get("jakarta.inject", "Inject"))
                .addModifiers(javax.lang.model.element.Modifier.PUBLIC)
                .addStatement("super()")
                .build();
        }

        MethodSpec.Builder builder = MethodSpec.constructorBuilder()
            .addAnnotation(ClassName.get("jakarta.inject", "Inject"))
            .addModifiers(javax.lang.model.element.Modifier.PUBLIC);

        List<String> argNames = new ArrayList<>();
        int index = 0;
        for (javax.lang.model.element.VariableElement param : params) {
            String name = param.getSimpleName().toString();
            if (name == null || name.isBlank()) {
                name = "arg" + index;
            }
            argNames.add(name);
            builder.addParameter(TypeName.get(param.asType()), name);
            index++;
        }

        builder.addStatement("super($L)", String.join(", ", argNames));
        return builder.build();
    }

    private javax.lang.model.element.ExecutableElement selectConstructor(
            List<javax.lang.model.element.ExecutableElement> constructors) {
        for (javax.lang.model.element.ExecutableElement constructor : constructors) {
            if (constructor.getAnnotation(jakarta.inject.Inject.class) != null) {
                return constructor;
            }
        }
        if (constructors.size() == 1) {
            return constructors.get(0);
        }
        for (javax.lang.model.element.ExecutableElement constructor : constructors) {
            if (constructor.getParameters().isEmpty()) {
                return constructor;
            }
        }
        LOG.warnf(
            "No @Inject or no-arg constructor found for %s; falling back to first constructor out of %d.",
            constructors.get(0).getEnclosingElement(),
            constructors.size());
        return constructors.get(0);
    }

    private TypeName resolveObservedType(PipelineStepModel model, DeploymentRole role, GrpcBinding grpcBinding) {
        TypeName observedType = model.outboundDomainType() != null ? model.outboundDomainType() : model.inboundDomainType();
        if (observedType == null) {
            return ClassName.OBJECT;
        }
        if (!isCachePlugin(model)) {
            return observedType;
        }
        if (role == DeploymentRole.REST_SERVER) {
            return convertDomainToDtoType(observedType);
        }
        if (role == DeploymentRole.PLUGIN_SERVER && grpcBinding != null) {
            try {
                org.pipelineframework.processor.util.GrpcJavaTypeResolver resolver =
                    new org.pipelineframework.processor.util.GrpcJavaTypeResolver();
                var grpcTypes = resolver.resolve(grpcBinding);
                if (grpcTypes != null && grpcTypes.grpcParameterType() != null && grpcTypes.grpcReturnType() != null) {
                    Object methodDescriptorObj = grpcBinding.methodDescriptor();
                    if (methodDescriptorObj == null) {
                        LOG.warnf("Failed to resolve observed gRPC type for %s; missing method descriptor",
                            model.serviceName());
                        return observedType;
                    }
                    if (methodDescriptorObj instanceof com.google.protobuf.Descriptors.MethodDescriptor methodDescriptor) {
                        String inputFullName = methodDescriptor.getInputType().getFullName();
                        String outputFullName = methodDescriptor.getOutputType().getFullName();
                        String inputName = methodDescriptor.getInputType().getName();
                        String outputName = methodDescriptor.getOutputType().getName();
                        String observedTypeName = observedType.toString();

                        if (observedTypeName.equals(inputFullName) || observedTypeName.endsWith("." + inputName)) {
                            return grpcTypes.grpcParameterType();
                        }
                        if (observedTypeName.equals(outputFullName) || observedTypeName.endsWith("." + outputName)) {
                            return grpcTypes.grpcReturnType();
                        }

                        String serviceName = model.serviceName();
                        if (serviceName != null) {
                            if (serviceName.equals(inputFullName) || serviceName.equals(inputName)) {
                                return grpcTypes.grpcParameterType();
                            }
                            if (serviceName.equals(outputFullName) || serviceName.equals(outputName)) {
                                return grpcTypes.grpcReturnType();
                            }
                        }
                    }
                    LOG.warnf(
                        "Observed type '%s' for service '%s' did not match gRPC input/output descriptors; "
                            + "falling back to observed type.",
                        observedType,
                        model.serviceName());
                    return observedType;
                }
            } catch (ClassCastException | IllegalStateException e) {
                LOG.warnf(e, "Failed to resolve observed gRPC type for %s; falling back to domain type",
                    model.serviceName());
                return observedType;
            }
        }
        return observedType;
    }

    private boolean isCachePlugin(PipelineStepModel model) {
        if (model == null || model.serviceClassName() == null) {
            return false;
        }
        return CACHE_SERVICE_CLASS.equals(model.serviceClassName().canonicalName());
    }

    private TypeName convertDomainToDtoType(TypeName domainType) {
        if (domainType == null) {
            return ClassName.OBJECT;
        }
        String domainTypeStr = domainType.toString();
        String dtoTypeStr = replaceFirstPackageSegment(domainTypeStr, "domain", "dto");
        if (dtoTypeStr.equals(domainTypeStr)) {
            dtoTypeStr = replaceFirstPackageSegment(domainTypeStr, "service", "dto");
        }
        return buildDtoClassName(dtoTypeStr);
    }

    private String replaceFirstPackageSegment(String fqcn, String fromSegment, String toSegment) {
        String needle = "." + fromSegment + ".";
        int index = fqcn.indexOf(needle);
        if (index < 0) {
            return fqcn;
        }
        return fqcn.substring(0, index) + "." + toSegment + fqcn.substring(index + fromSegment.length() + 1);
    }

    private ClassName buildDtoClassName(String fqName) {
        int lastDot = fqName.lastIndexOf('.');
        String packageName = lastDot > 0 ? fqName.substring(0, lastDot) : "";
        String simpleName = lastDot > 0 ? fqName.substring(lastDot + 1) : fqName;
        String dtoSimpleName = simpleName.endsWith("Dto") ? simpleName : simpleName + "Dto";
        return ClassName.get(packageName, dtoSimpleName);
    }
}
