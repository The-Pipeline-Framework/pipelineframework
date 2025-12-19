package org.pipelineframework.processor.validator;

import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;

import org.pipelineframework.processor.ir.PipelineStepIR;
import org.pipelineframework.processor.ir.TypeMapping;

/**
 * Validates the PipelineStepIR to ensure semantic consistency
 */
public class PipelineStepValidator {

    private final javax.annotation.processing.ProcessingEnvironment processingEnv;

    /**
     * Creates a new PipelineStepValidator.
     *
     * @param processingEnv the processing environment
     */
    public PipelineStepValidator(javax.annotation.processing.ProcessingEnvironment processingEnv) {
        this.processingEnv = processingEnv;
    }

    /**
     * Validates the PipelineStepIR for semantic consistency.
     *
     * @param ir the PipelineStepIR to validate
     * @param serviceClass the service class being processed
     * @return true if validation passes, false otherwise
     */
    public boolean validate(PipelineStepIR ir, TypeElement serviceClass) {
        boolean isValid = true;
        
        // Validate type mappings have required components
        isValid &= validateTypeMapping(ir.getInputMapping(), "input", serviceClass);
        isValid &= validateTypeMapping(ir.getOutputMapping(), "output", serviceClass);
        
        // Validate REST path if REST is enabled
        if (ir.getEnabledTargets().contains(org.pipelineframework.processor.ir.GenerationTarget.REST_RESOURCE) 
            && ir.getRestPath() == null) {
            processingEnv.getMessager().printMessage(
                Diagnostic.Kind.WARNING,
                "REST resource generation enabled but no path specified for " + ir.getServiceName(),
                serviceClass);
        }
        
        // Validate that mappers exist when both domain and gRPC types are specified
        if (ir.getInputMapping().getDomainType() != null && 
            ir.getInputMapping().getGrpcType() != null &&
            !ir.getInputMapping().hasMapper()) {
            processingEnv.getMessager().printMessage(
                Diagnostic.Kind.ERROR,
                "Input mapper is required when both inputType and inputGrpcType are specified for " + ir.getServiceName(),
                serviceClass);
            isValid = false;
        }
        
        if (ir.getOutputMapping().getDomainType() != null && 
            ir.getOutputMapping().getGrpcType() != null &&
            !ir.getOutputMapping().hasMapper()) {
            processingEnv.getMessager().printMessage(
                Diagnostic.Kind.ERROR,
                "Output mapper is required when both outputType and outputGrpcType are specified for " + ir.getServiceName(),
                serviceClass);
            isValid = false;
        }
        
        return isValid;
    }
    
    private boolean validateTypeMapping(TypeMapping mapping, String mappingType, TypeElement serviceClass) {
        // If we have both domain and gRPC types, we need a mapper
        if (mapping.getDomainType() != null && 
            mapping.getGrpcType() != null && 
            !mapping.hasMapper()) {
            return false;
        }
        return true;
    }
}