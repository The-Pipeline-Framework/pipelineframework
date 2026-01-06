package org.pipelineframework.search.parse_document.service;

import java.time.Instant;
import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.mutiny.Uni;
import lombok.Getter;
import org.jboss.logging.Logger;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.search.common.domain.ParsedDocument;
import org.pipelineframework.search.common.domain.RawDocument;
import org.pipelineframework.service.ReactiveService;

@PipelineStep(
    inputType = org.pipelineframework.search.common.domain.RawDocument.class,
    outputType = org.pipelineframework.search.common.domain.ParsedDocument.class,
    stepType = org.pipelineframework.step.StepOneToOne.class,
    backendType = org.pipelineframework.grpc.GrpcReactiveServiceAdapter.class,
    inboundMapper = org.pipelineframework.search.common.mapper.RawDocumentMapper.class,
    outboundMapper = org.pipelineframework.search.common.mapper.ParsedDocumentMapper.class,
    cacheKeyGenerator = org.pipelineframework.cache.DocIdCacheKeyGenerator.class
)
@ApplicationScoped
@Getter
public class ProcessParseDocumentService
    implements ReactiveService<RawDocument, ParsedDocument> {

  @Override
  public Uni<ParsedDocument> process(RawDocument input) {
    Logger logger = Logger.getLogger(getClass());

    if (input == null || input.rawContent == null || input.rawContent.isBlank()) {
      return Uni.createFrom().failure(new IllegalArgumentException("rawContent is required"));
    }

    String title = extractTitle(input.rawContent);
    String content = extractBody(input.rawContent);

    ParsedDocument output = new ParsedDocument();
    output.docId = input.docId;
    output.title = title;
    output.content = content;
    output.extractedAt = Instant.now();

    logger.infof("Parsed doc %s (title=%s, length=%s)", input.docId, title, content.length());
    return Uni.createFrom().item(output);
  }

  private String extractTitle(String rawContent) {
    String[] lines = rawContent.split("\\R", -1);
    for (String line : lines) {
      String trimmed = line.trim();
      if (trimmed.startsWith("Title:")) {
        return trimmed.substring("Title:".length()).trim();
      }
    }
    return "Untitled Document";
  }

  private String extractBody(String rawContent) {
    String body = rawContent.replaceAll("(?i)\\btitle:.*", "").trim();
    body = body.replaceAll("<[^>]+>", " ");
    return body.replaceAll("\\s+", " ").trim();
  }
}
