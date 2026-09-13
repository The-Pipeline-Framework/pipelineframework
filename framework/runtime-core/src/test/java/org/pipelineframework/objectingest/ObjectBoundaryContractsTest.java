package org.pipelineframework.objectingest;

import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.pipelineframework.objectpublish.ObjectTargetProvider;
import org.pipelineframework.objectpublish.ObjectWriteOpenRequest;
import org.pipelineframework.objectpublish.ObjectWriteResult;
import org.pipelineframework.objectpublish.ObjectWriteSession;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ObjectBoundaryContractsTest {

    @Test
    void providerExtensionsRemainInterfaces() {
        assertTrue(ObjectSourceProvider.class.isInterface());
        assertTrue(ObjectTargetProvider.class.isInterface());
        assertTrue(ObjectWriteSession.class.isInterface());
    }

    @Test
    void sourceItemShapeRemainsStableAcrossRuntimeHosts() {
        assertEquals(
            List.of(
                "provider",
                "container",
                "key",
                "versionId",
                "etag",
                "sizeBytes",
                "lastModifiedEpochMs",
                "contentType",
                "metadata",
                "contentRef",
                "localPath"),
            recordComponents(ObjectSourceItem.class));
    }

    @Test
    void targetRequestAndResultShapesRemainStableAcrossRuntimeHosts() {
        assertEquals(
            List.of("targetName", "target", "objectKey", "contentType", "metadata", "idempotencyKey"),
            recordComponents(ObjectWriteOpenRequest.class));
        assertEquals(
            List.of("reference", "bytes", "checksum", "writtenAt"),
            recordComponents(ObjectWriteResult.class));
    }

    private List<String> recordComponents(Class<?> recordType) {
        return Arrays.stream(recordType.getRecordComponents())
            .map(RecordComponent::getName)
            .toList();
    }
}
