package org.pipelineframework.objectpublish;

import java.util.List;
import java.util.Map;

/**
 * Maps terminal pipeline outputs into grouped object payloads for Object Publish.
 *
 * @param <T> terminal pipeline output item type
 */
public interface ObjectPublishMapper<T> {
    /**
     * Returns the non-blank grouping key used to select the output object for the item.
     *
     * @param item terminal pipeline output item
     * @return non-null, non-blank group key
     */
    String groupKey(T item);

    /**
     * Renders all items in a group into a non-null object payload.
     *
     * @param groupKey non-blank group key returned by {@link #groupKey(Object)}
     * @param items non-empty immutable view of the grouped items
     * @return non-null payload to write to the configured object target
     */
    ObjectPayload render(String groupKey, List<T> items);

    /**
     * Returns optional labels available to key templating and target metadata.
     *
     * @param groupKey non-blank group key returned by {@link #groupKey(Object)}
     * @param items non-empty immutable view of the grouped items
     * @return label map, or an empty map when no labels are needed
     */
    default Map<String, String> labels(String groupKey, List<T> items) {
        return Map.of();
    }
}
