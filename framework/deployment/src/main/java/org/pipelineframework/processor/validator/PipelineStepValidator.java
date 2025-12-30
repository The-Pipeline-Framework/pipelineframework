package org.pipelineframework.processor.validator;

import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;

import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.TypeMapping;

/**
 * Validates the PipelineStepModel to ensure semantic consistency
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
     * Validates the PipelineStepModel for semantic consistency.
     *
     * @param model the PipelineStepModel to validate
     * @param serviceClass the service class being processed
     * @return true if validation passes, false otherwise
     */
    public boolean validate(PipelineStepModel model, TypeElement serviceClass) {
        boolean isValid = true;

        // Validate type mappings have required components
        isValid &= validateTypeMapping(model.inputMapping());
        isValid &= validateTypeMapping(model.outputMapping());

        // Validate that mappers exist when domain types are specified
        if (model.inputMapping().domainType() != null &&
            !model.inputMapping().hasMapper()) {
            processingEnv.getMessager().printMessage(
                Diagnostic.Kind.ERROR,
                "Input mapper is required when inputType is specified for " + model.serviceName(),
                serviceClass);
            isValid = false;
        }

        if (model.outputMapping().domainType() != null &&
            !model.outputMapping().hasMapper()) {
            processingEnv.getMessager().printMessage(
                Diagnostic.Kind.ERROR,
                "Output mapper is required when outputType is specified for " + model.serviceName(),
                serviceClass);
            isValid = false;
        }

        return isValid;
    }

    private boolean validateTypeMapping(TypeMapping mapping) {
        // If we have a domain type, we need a mapper
        return mapping.domainType() == null || mapping.hasMapper();
    }
}
