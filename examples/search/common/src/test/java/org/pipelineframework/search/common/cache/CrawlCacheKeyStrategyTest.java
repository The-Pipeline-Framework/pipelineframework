package org.pipelineframework.search.common.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.search.common.domain.RawDocument;
import org.pipelineframework.search.common.dto.CrawlRequestDto;
import org.pipelineframework.search.common.dto.RawDocumentDto;
import org.pipelineframework.search.common.util.FetchOptionsNormalizer;

class CrawlCacheKeyStrategyTest {

    @Test
    void resolvesKeyForCrawlRequestDto() {
        CrawlCacheKeyStrategy strategy = new CrawlCacheKeyStrategy();
        CrawlRequestDto request = CrawlRequestDto.builder()
            .sourceUrl("https://example.com")
            .fetchMethod("get")
            .accept("text/html")
            .build();

        Optional<String> resolved = strategy.resolveKey(request, new PipelineContext(null, null, null));
        assertTrue(resolved.isPresent());

        String fetchOptions = FetchOptionsNormalizer.normalize(request);
        String expected = RawDocument.class.getName() + ":https://example.com|" + fetchOptions;
        assertEquals(expected, resolved.get());
    }

    @Test
    void resolvesKeyForRawDocumentDto() {
        CrawlCacheKeyStrategy strategy = new CrawlCacheKeyStrategy();
        RawDocumentDto document = RawDocumentDto.builder()
            .sourceUrl("https://example.com")
            .fetchOptions("method=GET")
            .build();

        Optional<String> resolved = strategy.resolveKey(document, new PipelineContext(null, null, null));
        assertTrue(resolved.isPresent());

        String expected = RawDocument.class.getName() + ":https://example.com|method=GET";
        assertEquals(expected, resolved.get());
    }
}
