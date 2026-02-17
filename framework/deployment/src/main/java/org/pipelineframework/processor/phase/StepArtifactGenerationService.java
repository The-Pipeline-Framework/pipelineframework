package org.pipelineframework.processor.phase;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

import com.google.protobuf.DescriptorProtos;
import com.squareup.javapoet.ClassName;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.GrpcBinding;
import org.pipelineframework.processor.ir.LocalBinding;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.RestBinding;
import org.pipelineframework.processor.renderer.ClientStepRenderer;
import org.pipelineframework.processor.renderer.GenerationContext;
import org.pipelineframework.processor.renderer.GrpcServiceAdapterRenderer;
import org.pipelineframework.processor.renderer.LocalClientStepRenderer;
import org.pipelineframework.processor.renderer.RestClientStepRenderer;
import org.pipelineframework.processor.renderer.RestFunctionHandlerRenderer;
import org.pipelineframework.processor.renderer.RestResourceRenderer;
import org.pipelineframework.processor.util.ResourceNameUtils;
import org.pipelineframework.processor.util.RoleMetadataGenerator;

/**
 * Generates per-step artifacts for enabled generation targets.
 */
class StepArtifactGenerationService {

    private final GenerationPathResolver pathResolver;
    private final GenerationPolicy generationPolicy;
    private final SideEffectBeanService sideEffectBeanService;

    StepArtifactGenerationService(
            GenerationPathResolver pathResolver,
            GenerationPolicy generationPolicy,
            SideEffectBeanService sideEffectBeanService) {
        this.pathResolver = Objects.requireNonNull(pathResolver, "pathResolver");
        this.generationPolicy = Objects.requireNonNull(generationPolicy, "generationPolicy");
        this.sideEffectBeanService = Objects.requireNonNull(sideEffectBeanService, "sideEffectBeanService");
    }

    void generateArtifactsForModel(
            PipelineCompilationContext ctx,
            PipelineStepModel model,
            GrpcBinding grpcBinding,
            RestBinding restBinding,
            LocalBinding localBinding,
            Set<String> generatedSideEffectBeans,
            Set<String> enabledAspects,
            DescriptorProtos.FileDescriptorSet descriptorSet,
            ClassName cacheKeyGenerator,
            RoleMetadataGenerator roleMetadataGenerator,
            GrpcServiceAdapterRenderer grpcRenderer,
            ClientStepRenderer clientRenderer,
            LocalClientStepRenderer localClientRenderer,
            RestClientStepRenderer restClientRenderer,
            RestResourceRenderer restRenderer,
            RestFunctionHandlerRenderer restFunctionHandlerRenderer) throws IOException {
        for (GenerationTarget target : model.enabledTargets()) {
            switch (target) {
                case GRPC_SERVICE -> {
                    if (model.deploymentRole() == DeploymentRole.PLUGIN_SERVER
                        && !generationPolicy.allowPluginServerArtifacts(ctx)) {
                        break;
                    }
                    if (model.sideEffect() && model.deploymentRole() == DeploymentRole.PLUGIN_SERVER) {
                        DeploymentRole sideEffectOutputRole = ctx.isTransportModeLocal()
                            ? DeploymentRole.ORCHESTRATOR_CLIENT
                            : DeploymentRole.PLUGIN_SERVER;
                        if (ctx.isTransportModeLocal()) {
                            String sideEffectBeanKey = model.servicePackage() + ".pipeline." + model.serviceName();
                            if (generatedSideEffectBeans.add(sideEffectBeanKey)) {
                                sideEffectBeanService.generateSideEffectBean(
                                    ctx,
                                    model,
                                    DeploymentRole.PLUGIN_SERVER,
                                    sideEffectOutputRole,
                                    grpcBinding);
                            }
                        } else {
                            sideEffectBeanService.generateSideEffectBean(
                                ctx,
                                model,
                                DeploymentRole.PLUGIN_SERVER,
                                sideEffectOutputRole,
                                grpcBinding);
                        }
                    }
                    if (ctx.isTransportModeLocal()) {
                        break;
                    }
                    if (grpcBinding == null) {
                        ctx.getProcessingEnv().getMessager().printMessage(javax.tools.Diagnostic.Kind.WARNING,
                            "Skipping gRPC service generation for '" + model.generatedName()
                                + "' because no gRPC binding is available.");
                        break;
                    }
                    String grpcClassName = model.servicePackage() + ".pipeline." + model.generatedName() + "GrpcService";
                    DeploymentRole grpcRole = model.deploymentRole();
                    grpcRenderer.render(grpcBinding, new GenerationContext(
                        ctx.getProcessingEnv(),
                        pathResolver.resolveRoleOutputDir(ctx, grpcRole),
                        grpcRole,
                        enabledAspects,
                        cacheKeyGenerator,
                        descriptorSet));
                    roleMetadataGenerator.recordClassWithRole(grpcClassName, grpcRole.name());
                }
                case CLIENT_STEP -> {
                    if (model.deploymentRole() == DeploymentRole.PLUGIN_SERVER && ctx.isPluginHost()) {
                        break;
                    }
                    if (grpcBinding == null) {
                        ctx.getProcessingEnv().getMessager().printMessage(javax.tools.Diagnostic.Kind.WARNING,
                            "Skipping gRPC client step generation for '" + model.generatedName()
                                + "' because no gRPC binding is available.");
                        break;
                    }
                    String clientClassName = model.servicePackage() + ".pipeline."
                        + ResourceNameUtils.normalizeBaseName(model.generatedName()) + "GrpcClientStep";
                    DeploymentRole clientRole = resolveClientRole(model.deploymentRole());
                    clientRenderer.render(grpcBinding, new GenerationContext(
                        ctx.getProcessingEnv(),
                        pathResolver.resolveRoleOutputDir(ctx, clientRole),
                        clientRole,
                        enabledAspects,
                        cacheKeyGenerator,
                        descriptorSet));
                    roleMetadataGenerator.recordClassWithRole(clientClassName, clientRole.name());
                }
                case LOCAL_CLIENT_STEP -> {
                    if (model.deploymentRole() == DeploymentRole.PLUGIN_SERVER && ctx.isPluginHost()) {
                        break;
                    }
                    if (ctx.getProcessingEnv() == null) {
                        break;
                    }
                    if (model.sideEffect()) {
                        String sideEffectBeanKey = model.servicePackage() + ".pipeline." + model.serviceName();
                        if (generatedSideEffectBeans.add(sideEffectBeanKey)) {
                            DeploymentRole sideEffectRole =
                                model.deploymentRole() == null
                                    ? DeploymentRole.ORCHESTRATOR_CLIENT
                                    : model.deploymentRole();
                            sideEffectBeanService.generateSideEffectBean(
                                ctx,
                                model,
                                sideEffectRole,
                                DeploymentRole.ORCHESTRATOR_CLIENT,
                                grpcBinding);
                        }
                    }
                    if (localBinding == null) {
                        ctx.getProcessingEnv().getMessager().printMessage(javax.tools.Diagnostic.Kind.WARNING,
                            "Skipping local client step generation for '" + model.generatedName()
                                + "' because no local binding is available.");
                        break;
                    }
                    String localClientClassName = model.servicePackage() + ".pipeline."
                        + ResourceNameUtils.normalizeBaseName(model.generatedName()) + "LocalClientStep";
                    DeploymentRole localClientRole = resolveClientRole(model.deploymentRole());
                    localClientRenderer.render(localBinding, new GenerationContext(
                        ctx.getProcessingEnv(),
                        pathResolver.resolveRoleOutputDir(ctx, localClientRole),
                        localClientRole,
                        enabledAspects,
                        cacheKeyGenerator,
                        descriptorSet));
                    roleMetadataGenerator.recordClassWithRole(localClientClassName, localClientRole.name());
                }
                case REST_RESOURCE -> {
                    if (model.deploymentRole() == DeploymentRole.PLUGIN_SERVER
                        && !generationPolicy.allowPluginServerArtifacts(ctx)) {
                        break;
                    }
                    if (model.sideEffect() && model.deploymentRole() == DeploymentRole.PLUGIN_SERVER) {
                        sideEffectBeanService.generateSideEffectBean(
                            ctx,
                            model,
                            DeploymentRole.REST_SERVER,
                            DeploymentRole.REST_SERVER,
                            grpcBinding);
                    }
                    if (restBinding == null) {
                        ctx.getProcessingEnv().getMessager().printMessage(javax.tools.Diagnostic.Kind.WARNING,
                            "Skipping REST resource generation for '" + model.generatedName()
                                + "' because no REST binding is available.");
                        break;
                    }
                    String restClassName = model.servicePackage() + ".pipeline."
                        + ResourceNameUtils.normalizeBaseName(model.generatedName()) + "Resource";
                    DeploymentRole restRole = DeploymentRole.REST_SERVER;
                    restRenderer.render(restBinding, new GenerationContext(
                        ctx.getProcessingEnv(),
                        pathResolver.resolveRoleOutputDir(ctx, restRole),
                        restRole,
                        enabledAspects,
                        cacheKeyGenerator,
                        descriptorSet));
                    roleMetadataGenerator.recordClassWithRole(restClassName, restRole.name());

                    if (ctx.isPlatformModeFunction()) {
                        String handlerClassName =
                            RestFunctionHandlerRenderer.handlerFqcn(model.servicePackage(), model.generatedName());
                        restFunctionHandlerRenderer.render(restBinding, new GenerationContext(
                            ctx.getProcessingEnv(),
                            pathResolver.resolveRoleOutputDir(ctx, restRole),
                            restRole,
                            enabledAspects,
                            cacheKeyGenerator,
                            descriptorSet));
                        roleMetadataGenerator.recordClassWithRole(handlerClassName, restRole.name());
                    }
                }
                case REST_CLIENT_STEP -> {
                    if (model.deploymentRole() == DeploymentRole.PLUGIN_SERVER && ctx.isPluginHost()) {
                        break;
                    }
                    if (restBinding == null) {
                        ctx.getProcessingEnv().getMessager().printMessage(javax.tools.Diagnostic.Kind.WARNING,
                            "Skipping REST client step generation for '" + model.generatedName()
                                + "' because no REST binding is available.");
                        break;
                    }
                    String restClientClassName = model.servicePackage() + ".pipeline."
                        + ResourceNameUtils.normalizeBaseName(model.generatedName()) + "RestClientStep";
                    DeploymentRole restClientRole = resolveClientRole(model.deploymentRole());
                    restClientRenderer.render(restBinding, new GenerationContext(
                        ctx.getProcessingEnv(),
                        pathResolver.resolveRoleOutputDir(ctx, restClientRole),
                        restClientRole,
                        enabledAspects,
                        cacheKeyGenerator,
                        descriptorSet));
                    roleMetadataGenerator.recordClassWithRole(restClientClassName, restClientRole.name());
                }
            }
        }
    }

    private DeploymentRole resolveClientRole(DeploymentRole serverRole) {
        if (serverRole == null) {
            return DeploymentRole.ORCHESTRATOR_CLIENT;
        }
        DeploymentRole mapped = generationPolicy.resolveClientRole(serverRole);
        return mapped != null ? mapped : DeploymentRole.ORCHESTRATOR_CLIENT;
    }
}
