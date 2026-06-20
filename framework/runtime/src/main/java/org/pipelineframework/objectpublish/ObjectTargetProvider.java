package org.pipelineframework.objectpublish;

import io.smallrye.mutiny.Uni;

/**
 * Provider SPI for Object Publish targets.
 */
public interface ObjectTargetProvider {
    String providerName();

    Uni<ObjectWriteResult> write(ObjectWriteRequest request);
}
