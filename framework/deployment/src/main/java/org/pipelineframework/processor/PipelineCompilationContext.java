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
    public ProcessingEnvironment getProcessingEnv() {
        return processingEnv;
    }

    public RoundEnvironment getRoundEnv() {
        return roundEnv;
    }

    public Path getGeneratedSourcesRoot() {
        return generatedSourcesRoot;
    }

    public Path getModuleDir() {
        return moduleDir;
    }

    public boolean isPluginHost() {
        return pluginHost;
    }

    public boolean isOrchestratorGenerated() {
        return orchestratorGenerated;
    }

    public boolean isTransportModeGrpc() {
        return transportModeGrpc;
    }

    public Map<String, Object> getRendererBindings() {
        return rendererBindings;
    }

    public DescriptorProtos.FileDescriptorSet getDescriptorSet() {
        return descriptorSet;
    }

    // Setters for additional properties
    public void setGeneratedSourcesRoot(Path generatedSourcesRoot) {
        this.generatedSourcesRoot = generatedSourcesRoot;
    }

    public void setModuleDir(Path moduleDir) {
        this.moduleDir = moduleDir;
    }

    public void setPluginHost(boolean pluginHost) {
        this.pluginHost = pluginHost;
    }

    public void setOrchestratorGenerated(boolean orchestratorGenerated) {
        this.orchestratorGenerated = orchestratorGenerated;
    }

    public void setTransportModeGrpc(boolean transportModeGrpc) {
        this.transportModeGrpc = transportModeGrpc;
    }

    public void setRendererBindings(Map<String, Object> rendererBindings) {
        this.rendererBindings = rendererBindings;
    }

    public void setDescriptorSet(DescriptorProtos.FileDescriptorSet descriptorSet) {
        this.descriptorSet = descriptorSet;
    }
}
