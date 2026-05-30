package org.pipelineframework.awaitable;

import io.smallrye.mutiny.Multi;

/**
 * Marker for generated await steps that remain unary at the authored model level but can suspend
 * over a streaming upstream by creating one unary await interaction per input item.
 *
 * @param <I> input item type
 * @param <O> output item type
 */
public interface AwaitStreamOneToOneStep<I, O> {

    Multi<O> applyAwaitPerItem(Multi<I> input);
}
