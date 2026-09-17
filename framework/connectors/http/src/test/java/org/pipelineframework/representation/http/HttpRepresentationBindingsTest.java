package org.pipelineframework.representation.http;

import java.util.List;
import java.util.Optional;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.junit.jupiter.api.Test;
import org.pipelineframework.connector.http.HttpOperationBindingCatalog;
import org.pipelineframework.connector.http.HttpOperationRepresentationBinding;
import org.pipelineframework.connector.http.HttpRepresentationMode;
import static org.junit.jupiter.api.Assertions.*;

class HttpRepresentationBindingsTest {
    @Test
    void directDeserializationRejectsNullAndPreservesPrimitiveBoxing() {
        var bindings = new HttpRepresentationBindings(new HttpOperationBindingCatalog(List.of(
            new HttpOperationRepresentationBinding("http.direct", HttpRepresentationMode.DIRECT,
                Optional.empty(), Optional.empty(), "a".repeat(64)))), getClass().getClassLoader());
        assertEquals("done", bindings.fromWire("http.direct", TextNode.valueOf("done"), String.class));
        assertEquals(7, bindings.fromWire("http.direct", IntNode.valueOf(7), int.class));
        assertThrows(IllegalStateException.class, () -> bindings.fromWire("http.direct", NullNode.instance, String.class));
    }
}
