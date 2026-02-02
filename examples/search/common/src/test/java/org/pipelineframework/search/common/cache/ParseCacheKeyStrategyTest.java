package org.pipelineframework.search.common.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.search.common.domain.ParsedDocument;
import org.pipelineframework.search.common.dto.ParsedDocumentDto;
import org.pipelineframework.search.common.dto.RawDocumentDto;

class ParseCacheKeyStrategyTest {

    @Test
    void resolvesKeyForParsedDocumentDto() {
        ParseCacheKeyStrategy strategy = new ParseCacheKeyStrategy();
        ParsedDocumentDto parsed = ParsedDocumentDto.builder()
            .rawContentHash("raw-hash-1")
            .build();

        Optional<String> resolved = strategy.resolveKey(parsed, new PipelineContext(null, null, null));
        assertTrue(resolved.isPresent());
        assertEquals(ParsedDocument.class.getName() + ":raw-hash-1", resolved.get());
    }

    @Test
    void resolvesKeyForRawDocumentDto() {
        ParseCacheKeyStrategy strategy = new ParseCacheKeyStrategy();
        RawDocumentDto raw = RawDocumentDto.builder()
            .rawContentHash("raw-hash-2")
            .build();

        Optional<String> resolved = strategy.resolveKey(raw, new PipelineContext(null, null, null));
        assertTrue(resolved.isPresent());
        assertEquals(ParsedDocument.class.getName() + ":raw-hash-2", resolved.get());
    }
}
