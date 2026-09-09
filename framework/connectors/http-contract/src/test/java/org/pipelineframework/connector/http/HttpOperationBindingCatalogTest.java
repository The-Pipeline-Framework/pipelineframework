package org.pipelineframework.connector.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

class HttpOperationBindingCatalogTest {
    @Test
    void roundTripsResolvedBindingsDeterministically() {
        var direct = new HttpOperationRepresentationBinding("http.direct", HttpRepresentationMode.DIRECT,
            Optional.empty(), Optional.empty(), "1".repeat(64));
        var curated = new HttpOperationRepresentationBinding("http.curated", HttpRepresentationMode.CURATED,
            Optional.of("com.example.WireValue"), Optional.of("com.example.ValueMapper"), "2".repeat(64));

        HttpOperationBindingCatalog restored = HttpOperationBindingCatalog.read(
            new HttpOperationBindingCatalog(List.of(direct, curated)).json());

        assertEquals(List.of(curated, direct), restored.bindings());
        assertEquals(Optional.of(curated), restored.find("http.curated"));
    }

    @Test
    void requiresMapperForGeneratedAndCuratedBindings() {
        IllegalArgumentException generated = assertThrows(IllegalArgumentException.class,
            () -> new HttpOperationRepresentationBinding("http.generated", HttpRepresentationMode.GENERATED,
                Optional.empty(), Optional.empty(), "1".repeat(64)));
        IllegalArgumentException curated = assertThrows(IllegalArgumentException.class,
            () -> new HttpOperationRepresentationBinding("http.curated", HttpRepresentationMode.CURATED,
                Optional.empty(), Optional.empty(), "1".repeat(64)));

        assertTrue(generated.getMessage().contains("require representation"));
        assertTrue(curated.getMessage().contains("require representation"));
    }
}
