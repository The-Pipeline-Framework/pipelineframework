package org.pipelineframework.objectingest;

import java.util.List;
import org.pipelineframework.repository.PayloadReference;

public final class TestObjectSelectionMapper
    implements ObjectSelectionMapper<TestObjectSelectionMapper.SelectedInput> {

    @Override
    public Class<SelectedInput> outputType() {
        return SelectedInput.class;
    }

    @Override
    public SelectedInput map(List<ObjectSnapshot> snapshots) {
        return new SelectedInput(snapshots.stream().map(ObjectSnapshot::contentRef).toList());
    }

    public record SelectedInput(List<PayloadReference> references) {
    }
}
