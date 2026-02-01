package org.pipelineframework.context.rest;

import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;

import org.junit.jupiter.api.Test;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHeaders;
import org.pipelineframework.context.PipelineContextHolder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class PipelineContextClientHeadersFactoryTest {

    @Test
    void usesIncomingHeadersWhenPresent() {
        PipelineContextClientHeadersFactory factory = new PipelineContextClientHeadersFactory();
        MultivaluedMap<String, String> incoming = new MultivaluedHashMap<>();
        MultivaluedMap<String, String> outgoing = new MultivaluedHashMap<>();

        incoming.add(PipelineContextHeaders.VERSION, "v1");
        incoming.add(PipelineContextHeaders.REPLAY, "true");
        incoming.add(PipelineContextHeaders.CACHE_POLICY, "prefer-cache");

        MultivaluedMap<String, String> result = factory.update(incoming, outgoing);

        assertEquals("v1", result.getFirst(PipelineContextHeaders.VERSION));
        assertEquals("true", result.getFirst(PipelineContextHeaders.REPLAY));
        assertEquals("prefer-cache", result.getFirst(PipelineContextHeaders.CACHE_POLICY));
    }

    @Test
    void fallsBackToContextWhenMissing() {
        PipelineContextClientHeadersFactory factory = new PipelineContextClientHeadersFactory();
        MultivaluedMap<String, String> incoming = new MultivaluedHashMap<>();
        MultivaluedMap<String, String> outgoing = new MultivaluedHashMap<>();

        PipelineContextHolder.set(new PipelineContext("v2", "1", "require-cache"));
        try {
            MultivaluedMap<String, String> result = factory.update(incoming, outgoing);
            assertEquals("v2", result.getFirst(PipelineContextHeaders.VERSION));
            assertEquals("1", result.getFirst(PipelineContextHeaders.REPLAY));
            assertEquals("require-cache", result.getFirst(PipelineContextHeaders.CACHE_POLICY));
        } finally {
            PipelineContextHolder.clear();
        }
    }

    @Test
    void ignoresBlankIncomingValues() {
        PipelineContextClientHeadersFactory factory = new PipelineContextClientHeadersFactory();
        MultivaluedMap<String, String> incoming = new MultivaluedHashMap<>();
        MultivaluedMap<String, String> outgoing = new MultivaluedHashMap<>();

        incoming.add(PipelineContextHeaders.VERSION, " ");
        incoming.add(PipelineContextHeaders.REPLAY, "");

        PipelineContextHolder.set(new PipelineContext("v3", "yes", "bypass-cache"));
        try {
            MultivaluedMap<String, String> result = factory.update(incoming, outgoing);
            assertEquals("v3", result.getFirst(PipelineContextHeaders.VERSION));
            assertEquals("yes", result.getFirst(PipelineContextHeaders.REPLAY));
            assertEquals("bypass-cache", result.getFirst(PipelineContextHeaders.CACHE_POLICY));
        } finally {
            PipelineContextHolder.clear();
        }
    }

    @Test
    void usesFirstNonBlankIncomingValueFromList() {
        PipelineContextClientHeadersFactory factory = new PipelineContextClientHeadersFactory();
        MultivaluedMap<String, String> incoming = new MultivaluedHashMap<>();
        MultivaluedMap<String, String> outgoing = new MultivaluedHashMap<>();

        incoming.add(PipelineContextHeaders.VERSION, " ");
        incoming.add(PipelineContextHeaders.VERSION, "v9");

        PipelineContextHolder.set(new PipelineContext("v2", "no", "bypass-cache"));
        try {
            MultivaluedMap<String, String> result = factory.update(incoming, outgoing);
            assertEquals("v9", result.getFirst(PipelineContextHeaders.VERSION));
        } finally {
            PipelineContextHolder.clear();
        }
    }

    @Test
    void usesCaseInsensitiveHeaderWithNonBlankValue() {
        PipelineContextClientHeadersFactory factory = new PipelineContextClientHeadersFactory();
        MultivaluedMap<String, String> incoming = new MultivaluedHashMap<>();
        MultivaluedMap<String, String> outgoing = new MultivaluedHashMap<>();

        String headerName = PipelineContextHeaders.VERSION.toUpperCase();
        incoming.add(headerName, "");
        incoming.add(headerName, "v10");

        PipelineContextHolder.set(new PipelineContext("v2", "no", "bypass-cache"));
        try {
            MultivaluedMap<String, String> result = factory.update(incoming, outgoing);
            assertEquals("v10", result.getFirst(PipelineContextHeaders.VERSION));
        } finally {
            PipelineContextHolder.clear();
        }
    }

    @Test
    void returnsNullWhenOutgoingIsNull() {
        PipelineContextClientHeadersFactory factory = new PipelineContextClientHeadersFactory();
        MultivaluedMap<String, String> incoming = new MultivaluedHashMap<>();

        MultivaluedMap<String, String> result = factory.update(incoming, null);

        assertNull(result);
    }
}
