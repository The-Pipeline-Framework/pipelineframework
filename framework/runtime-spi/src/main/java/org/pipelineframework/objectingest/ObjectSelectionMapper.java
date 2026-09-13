package org.pipelineframework.objectingest;

import java.util.List;

/** Framework-generated projection of one selected object set into a typed pipeline input. */
public interface ObjectSelectionMapper<T> {
    Class<T> outputType();

    T map(List<ObjectSnapshot> snapshots);
}
