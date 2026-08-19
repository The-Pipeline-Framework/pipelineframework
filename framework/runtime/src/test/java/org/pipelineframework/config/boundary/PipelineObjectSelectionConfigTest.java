package org.pipelineframework.config.boundary;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

class PipelineObjectSelectionConfigTest {

    @Test
    void rejectsDuplicateNormalizedFieldNames() {
        Map<String, String> keys = new LinkedHashMap<>();
        keys.put(" Invoice", "invoice.txt");
        keys.put("Invoice ", "invoice-alt.txt");

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
            () -> new PipelineObjectSelectionConfig("together", keys, Optional.empty()));

        assertEquals("input.object.selection.keys contains duplicate field 'Invoice'", exception.getMessage());
    }
}
