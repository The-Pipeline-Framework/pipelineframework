package org.pipelineframework.processor.renderer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.jupiter.api.Test;

import org.pipelineframework.processor.ir.ConnectorOperationSelection;
import org.pipelineframework.processor.ir.PipelineStepModel;

class LocalPipelineInvocationRendererTest {
    @Test void matchesImportedOperationModelsByAuthoredIdentity() {
        PipelineStepModel model = mock(PipelineStepModel.class);
        ConnectorOperationSelection selection = mock(ConnectorOperationSelection.class);
        when(selection.authoredStepName()).thenReturn("Execute GraphQL Query");
        when(model.connectorOperationSelection()).thenReturn(Optional.of(selection));
        when(model.serviceName()).thenReturn("ProcessExecuteGraphqlQueryBlock8f1198adb48ded87Service");

        assertTrue(LocalPipelineInvocationRenderer.matchesAuthoredStep(model, "Execute GraphQL Query"));
        assertFalse(LocalPipelineInvocationRenderer.matchesAuthoredStep(model, "Execute GraphQL Mutation"));
    }

    @Test void sharesTheCompilerNamingPolicyForOrdinaryChildSteps() {
        PipelineStepModel model = mock(PipelineStepModel.class);
        when(model.connectorOperationSelection()).thenReturn(Optional.empty());
        when(model.serviceName()).thenReturn("ProcessCanonicalizeGraphqlQueryService");

        assertTrue(LocalPipelineInvocationRenderer.matchesAuthoredStep(model, "Canonicalize GraphQL Query"));
    }
}
