package org.pipelineframework.processor.renderer;

import com.squareup.javapoet.ClassName;

/** Stable runtime API names used only as generated JavaPoet targets. */
final class RuntimeSymbols {
    static final ClassName PIPELINE_RUNNER = type("org.pipelineframework", "PipelineRunner");
    static final ClassName STEP_ONE_TO_ONE = type("org.pipelineframework.step", "StepOneToOne");
    static final ClassName STEP_ONE_TO_MANY = type("org.pipelineframework.step", "StepOneToMany");
    static final ClassName STEP_MANY_TO_ONE = type("org.pipelineframework.step", "StepManyToOne");
    static final ClassName REACTIVE_SERVICE = type("org.pipelineframework.service", "ReactiveService");
    static final ClassName REACTIVE_STREAMING_SERVICE = type("org.pipelineframework.service", "ReactiveStreamingService");
    static final ClassName REACTIVE_STREAMING_CLIENT_SERVICE =
        type("org.pipelineframework.service", "ReactiveStreamingClientService");
    static final ClassName REACTIVE_BIDIRECTIONAL_STREAMING_SERVICE =
        type("org.pipelineframework.service", "ReactiveBidirectionalStreamingService");
    static final ClassName OBJECT_SELECTION_MAPPER =
        type("org.pipelineframework.objectingest", "ObjectSelectionMapper");
    static final ClassName OBJECT_SNAPSHOT = type("org.pipelineframework.objectingest", "ObjectSnapshot");
    static final ClassName PIPELINE_INVOCATION_STEPS =
        type("org.pipelineframework.invocation", "PipelineInvocationSteps");

    private RuntimeSymbols() {
    }

    private static ClassName type(String packageName, String simpleName) {
        return ClassName.get(packageName, simpleName);
    }
}
