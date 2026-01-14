package org.pipelineframework.processor;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;

import com.google.protobuf.DescriptorProtos;
import lombok.Getter;
import lombok.Setter;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineAspectModel;
import org.pipelineframework.processor.ir.PipelineOrchestratorModel;
import org.pipelineframework.processor.ir.PipelineStepModel;

/**
 * Holds the compilation context for the pipeline annotation processing.
 * This class contains all the information needed during the compilation phases.
 */
@Getter
public class PipelineCompilationContext {

    // Getters
    private final ProcessingEnvironment processingEnv;
    private final RoundEnvironment roundEnv;

    // Setters
    // Discovered semantic models
    @Setter
    private List<PipelineStepModel> stepModels;
    @Setter
    private List<PipelineAspectModel> aspectModels;
    @Setter
    private List<PipelineAspectModel> aspectsForExpansion; // Non-cache aspects that should be expanded
    @Setter
    private List<PipelineOrchestratorModel> orchestratorModels;
    @Setter
    private Object pipelineTemplateConfig; // Store as Object to avoid circular dependencies
    
    // Resolved generation targets
    @Setter
    private Set<GenerationTarget> resolvedTargets;
    
    // Renderer-specific bindings (keyed by transport or target)
    @Setter
    private Map<String, Object> rendererBindings;
    
    // Output paths and module information
    @Setter
    private Path generatedSourcesRoot;
    @Setter
    private Path moduleDir;
    
    // Additional compilation flags and state
    @Setter
    private boolean pluginHost;
    @Setter
    private boolean orchestratorGenerated;
    @Setter
    private boolean transportModeGrpc; // true for GRPC, false for REST

    @Setter
    private DescriptorProtos.FileDescriptorSet descriptorSet;
    
    /**
     * Creates a new compilation context for the current annotation processing round.
     *
     * @param processingEnv the processing environment for compiler utilities and messaging
     * @param roundEnv the round environment containing annotated elements
     */
    public PipelineCompilationContext(ProcessingEnvironment processingEnv, RoundEnvironment roundEnv) {
        this.processingEnv = processingEnv;
        this.roundEnv = roundEnv;
        this.stepModels = List.of();
        this.aspectModels = List.of();
        this.aspectsForExpansion = List.of();
        this.orchestratorModels = List.of();
        this.pipelineTemplateConfig = null;
        this.resolvedTargets = Set.of();
        this.rendererBindings = Map.of();
        this.pluginHost = false;
        this.orchestratorGenerated = false;
        this.transportModeGrpc = true; // Default to GRPC
    }

    // Getters for additional properties
    /**
     * Returns the processing environment for this compilation round.
     *
     * @return the processing environment for this compilation round
     */
    public ProcessingEnvironment getProcessingEnv() {
        return processingEnv;
    }

    /**
     * Returns the round environment for the current annotation processing round.
     *
     * @return the round environment for the current annotation processing round
     */
    public RoundEnvironment getRoundEnv() {
        return roundEnv;
    }

    /**
     * Returns the configured root directory for generated sources.
     *
     * @return the configured root directory for generated sources
     */
    public Path getGeneratedSourcesRoot() {
        return generatedSourcesRoot;
    }

    /**
     * Returns the module directory resolved for the current compilation.
     *
     * @return the module directory resolved for the current compilation
     */
    public Path getModuleDir() {
        return moduleDir;
    }

    /**
     * Returns whether the module is a plugin host.
     *
     * @return true when the module is a plugin host
     */
    public boolean isPluginHost() {
        return pluginHost;
    }

    /**
     * Returns whether orchestrator artifacts should be generated.
     *
     * @return true when orchestrator artifacts should be generated
     */
    public boolean isOrchestratorGenerated() {
        return orchestratorGenerated;
    }

    /**
     * Returns whether the transport mode is gRPC.
     *
     * @return true for gRPC transport mode, false for REST transport mode
     */
    public boolean isTransportModeGrpc() {
        return transportModeGrpc;
    }

    /**
     * Returns renderer bindings keyed by transport or target.
     *
     * @return renderer bindings keyed by transport or target
     */
    public Map<String, Object> getRendererBindings() {
        return rendererBindings;
    }

    /**
     * Returns the loaded protobuf descriptor set, if available.
     *
     * @return the loaded protobuf descriptor set, if available
     */
    public DescriptorProtos.FileDescriptorSet getDescriptorSet() {
        return descriptorSet;
    }

    // Setters for additional properties
    /**
     * Sets the root directory for generated sources.
     *
     * @param generatedSourcesRoot the root directory for generated sources
     */
    public void setGeneratedSourcesRoot(Path generatedSourcesRoot) {
        this.generatedSourcesRoot = generatedSourcesRoot;
    }

    /**
     * Sets the module directory resolved for this compilation.
     *
     * @param moduleDir the module directory resolved for this compilation
     */
    public void setModuleDir(Path moduleDir) {
        this.moduleDir = moduleDir;
    }

    /**
     * Sets whether the module is a plugin host.
     *
     * @param pluginHost whether the module is a plugin host
     */
    public void setPluginHost(boolean pluginHost) {
        this.pluginHost = pluginHost;
    }

    /**
     * Sets whether orchestrator artifacts should be generated.
     *
     * @param orchestratorGenerated whether orchestrator artifacts should be generated
     */
    public void setOrchestratorGenerated(boolean orchestratorGenerated) {
        this.orchestratorGenerated = orchestratorGenerated;
    }

    /**
     * Sets whether the transport mode is gRPC.
     *
     * @param transportModeGrpc true for gRPC transport mode, false for REST transport mode
     */
    public void setTransportModeGrpc(boolean transportModeGrpc) {
        this.transportModeGrpc = transportModeGrpc;
    }

    /**
     * Sets renderer bindings keyed by transport or target.
     *
     * @param rendererBindings renderer bindings keyed by transport or target
     */
    public void setRendererBindings(Map<String, Object> rendererBindings) {
        this.rendererBindings = rendererBindings;
    }

    /**
     * Sets the protobuf descriptor set used for binding resolution.
     *
     * @param descriptorSet the protobuf descriptor set to use for binding resolution
     */
    public void setDescriptorSet(DescriptorProtos.FileDescriptorSet descriptorSet) {
        this.descriptorSet = descriptorSet;
    }
}
