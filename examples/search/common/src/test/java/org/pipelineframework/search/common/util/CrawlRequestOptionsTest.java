package org.pipelineframework.search.common.util;

import org.junit.jupiter.api.Test;
import org.pipelineframework.search.common.domain.CrawlRequest;

import static org.junit.jupiter.api.Assertions.*;

class CrawlRequestOptionsTest {

  @Test
  void appliesOptionsToRequest() {
    CrawlRequestOptions options = CrawlRequestOptions.builder()
        .fetchMethod("GET")
        .accept("text/html")
        .acceptLanguage("en-US")
        .authScope("tenant-42")
        .header("X-Trace", "abc123");

    CrawlRequest request = options.build("https://example.test");

    assertEquals("GET", request.fetchMethod);
    assertEquals("text/html", request.accept);
    assertEquals("en-US", request.acceptLanguage);
    assertEquals("tenant-42", request.authScope);
    assertNotNull(request.fetchHeaders);
    assertEquals("abc123", request.fetchHeaders.get("X-Trace"));
  }

  @Test
  void ignoresBlankHeaders() {
    CrawlRequest request = CrawlRequestOptions.builder()
        .header(" ", "value")
        .header("X-Blank", " ")
        .build("https://example.test");

    assertNull(request.fetchHeaders);
  }
}
