package org.pipelineframework.processor;

/**
 * Represents a single phase in the pipeline compilation process.
 * Each phase performs a specific transformation or validation step.
 */
public interface PipelineCompilationPhase {
    
    /**
     * Gets the name of this compilation phase.
     * 
     * @return the phase name, never null
     */
    String name();
    
    /**
     * Executes this compilation phase with the given context.
     * 
     * @param ctx the compilation context containing all necessary information
     * @throws Exception if the phase encounters an unrecoverable error
     */
    void execute(PipelineCompilationContext ctx) throws Exception;
}