package org.pipelineframework.processor;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import javax.lang.model.element.Element;

/** Immutable snapshot of authored source elements discovered by the JSR-269 host. */
public record Jsr269SourceInventory(
    Set<? extends Element> pipelineStepElements,
    Set<? extends Element> pipelineOrchestratorElements,
    Set<? extends Element> pipelinePluginElements,
    Set<? extends Element> rootElements
) {
    public Jsr269SourceInventory {
        pipelineStepElements = snapshot(pipelineStepElements);
        pipelineOrchestratorElements = snapshot(pipelineOrchestratorElements);
        pipelinePluginElements = snapshot(pipelinePluginElements);
        rootElements = snapshot(rootElements);
    }

    public static Jsr269SourceInventory empty() {
        return new Jsr269SourceInventory(Set.of(), Set.of(), Set.of(), Set.of());
    }

    private static Set<? extends Element> snapshot(Set<? extends Element> elements) {
        return elements == null ? Set.of() : Collections.unmodifiableSet(new LinkedHashSet<>(elements));
    }
}
