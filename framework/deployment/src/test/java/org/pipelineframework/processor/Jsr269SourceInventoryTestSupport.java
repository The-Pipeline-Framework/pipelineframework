package org.pipelineframework.processor;

import java.util.Set;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.element.Element;

import org.pipelineframework.annotation.PipelineOrchestrator;
import org.pipelineframework.annotation.PipelinePlugin;
import org.pipelineframework.annotation.PipelineStep;

/** Test-only adapter for preserving round fixtures while phases consume immutable inventories. */
public final class Jsr269SourceInventoryTestSupport {
    private Jsr269SourceInventoryTestSupport() {
    }

    public static Jsr269SourceInventory snapshot(RoundEnvironment roundEnvironment) {
        if (roundEnvironment == null) {
            return org.pipelineframework.processor.Jsr269SourceInventory.empty();
        }
        Set<? extends Element> steps = roundEnvironment.getElementsAnnotatedWith(PipelineStep.class);
        Set<? extends Element> orchestrators = roundEnvironment.getElementsAnnotatedWith(PipelineOrchestrator.class);
        Set<? extends Element> plugins = roundEnvironment.getElementsAnnotatedWith(PipelinePlugin.class);
        return new Jsr269SourceInventory(steps, orchestrators, plugins, roundEnvironment.getRootElements());
    }
}
