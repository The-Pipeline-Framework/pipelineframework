package org.pipelineframework.search.common.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.pipelineframework.search.common.dto.CrawlRequestDto;
import org.pipelineframework.search.grpc.PipelineTypes;

class CrawlRequestMapperTest {

  @Test
  void mapsFetchHeadersThroughGrpcMapAccessors() {
    UUID docId = UUID.randomUUID();
    CrawlRequestDto dto = CrawlRequestDto.builder()
        .docId(docId)
        .sourceUrl("https://example.test")
        .fetchMethod("GET")
        .accept("text/html")
        .acceptLanguage("en-US")
        .authScope("tenant-1")
        .fetchHeaders(Map.of("X-Trace", "trace-1"))
        .build();

    PipelineTypes.CrawlRequest grpc = CrawlRequestMapper.INSTANCE.toGrpc(dto);
    CrawlRequestDto roundTripped = CrawlRequestMapper.INSTANCE.fromGrpc(grpc);

    assertEquals("trace-1", grpc.getFetchHeadersMap().get("X-Trace"));
    assertEquals(docId, roundTripped.getDocId());
    assertEquals("trace-1", roundTripped.getFetchHeaders().get("X-Trace"));
  }
}
