package org.pipelineframework.objectingest;

/**
 * Application projection from a framework object snapshot into the first pipeline input type.
 *
 * @param <T> pipeline domain input type
 */
public interface ObjectSnapshotMapper<T> {

    T map(ObjectSnapshot snapshot);
}
