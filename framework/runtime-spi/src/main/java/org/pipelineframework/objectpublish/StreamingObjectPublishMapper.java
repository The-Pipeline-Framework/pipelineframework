package org.pipelineframework.objectpublish;

/**
 * Maps terminal pipeline outputs into grouped object payload streams.
 *
 * @param <T> terminal pipeline output item type
 */
public interface StreamingObjectPublishMapper<T> {
    /**
     * Returns the non-blank grouping key used to select the output object for the item.
     *
     * @param item terminal pipeline output item
     * @return non-null, non-blank group key
     */
    String groupKey(T item);

    /**
     * Opens a renderer for a group. The first item is supplied so renderers can derive labels
     * without retaining a full group.
     *
     * @param groupKey non-blank group key returned by {@link #groupKey(Object)}
     * @param firstItem first item observed for the group
     * @return non-null group renderer
     */
    ObjectPublishGroupRenderer<T> openGroup(String groupKey, T firstItem);
}
