package org.pipelineframework.search.crawl_source.service;

import org.junit.jupiter.api.Test;
import org.pipelineframework.search.common.domain.CrawlRequest;
import org.pipelineframework.search.common.util.CrawlRequestOptions;
import org.pipelineframework.search.common.util.FetchOptionsNormalizer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class FetchOptionsNormalizerTest {

  @Test
  void normalizesOptionsInDeterministicOrder() {
    CrawlRequest request = CrawlRequestOptions.builder()
        .fetchMethod("get")
        .accept(" text/html ")
        .acceptLanguage(" en-US ")
        .authScope("tenant-42 ")
        .header("X-B", "2")
        .header("x-a", "1")
        .build("https://example.test");

    String normalized = FetchOptionsNormalizer.normalize(request);

    assertEquals(
        "method=GET|accept=text/html|acceptLanguage=en-US|authScope=tenant-42|headers=x-a=1,x-b=2",
        normalized);
  }

  @Test
  void returnsNullWhenNoOptionsPresent() {
    assertNull(FetchOptionsNormalizer.normalize(new CrawlRequest()));
  }
}
