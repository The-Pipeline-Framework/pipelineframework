package org.pipelineframework.processor.util;

import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.PipelineTransport;

final class ClientStepClassNames {

    private ClientStepClassNames() {
    }

    static String className(PipelineStepModel model, PipelineTransport transportMode) {
        return model.servicePackage() + ".pipeline."
            + stripTrailingService(model.generatedName())
            + suffix(model, transportMode.clientStepSuffix());
    }

    static String suffix(PipelineStepModel model, String defaultSuffix) {
        if (model.enabledTargets().contains(GenerationTarget.AWAIT_CLIENT_STEP)) {
            return "AwaitClientStep";
        }
        if (model.enabledTargets().contains(GenerationTarget.COMMAND_CLIENT_STEP)) {
            return "CommandClientStep";
        }
        if (model.enabledTargets().contains(GenerationTarget.QUERY_CLIENT_STEP)) {
            return "QueryClientStep";
        }
        return defaultSuffix;
    }

    static String stripTrailingService(String generatedName) {
        if (generatedName == null) {
            return "";
        }
        return generatedName.endsWith("Service")
            ? generatedName.substring(0, generatedName.length() - "Service".length())
            : generatedName;
    }
}
