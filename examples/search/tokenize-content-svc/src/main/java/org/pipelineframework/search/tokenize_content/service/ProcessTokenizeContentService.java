package org.pipelineframework.search.tokenize_content.service;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.mutiny.Uni;
import lombok.Getter;
import org.jboss.logging.Logger;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.search.common.domain.ParsedDocument;
import org.pipelineframework.search.common.domain.TokenBatch;
import org.pipelineframework.service.ReactiveService;

@PipelineStep(
    inputType = org.pipelineframework.search.common.domain.ParsedDocument.class,
    outputType = org.pipelineframework.search.common.domain.TokenBatch.class,
    stepType = org.pipelineframework.step.StepOneToOne.class,
    backendType = org.pipelineframework.grpc.GrpcReactiveServiceAdapter.class,
    inboundMapper = org.pipelineframework.search.common.mapper.ParsedDocumentMapper.class,
    outboundMapper = org.pipelineframework.search.common.mapper.TokenBatchMapper.class,
    cacheKeyGenerator = org.pipelineframework.cache.DocIdCacheKeyGenerator.class
)
@ApplicationScoped
@Getter
public class ProcessTokenizeContentService
    implements ReactiveService<ParsedDocument, TokenBatch> {

  @Override
  public Uni<TokenBatch> process(ParsedDocument input) {
    Logger logger = Logger.getLogger(getClass());

    if (input == null || input.content == null || input.content.isBlank()) {
      return Uni.createFrom().failure(new IllegalArgumentException("content is required"));
    }

    String tokenized = tokenize(input.content);

    TokenBatch output = new TokenBatch();
    output.docId = input.docId;
    output.tokens = tokenized;
    output.tokenizedAt = Instant.now();

    logger.infof("Tokenized doc %s (%s tokens)", input.docId, countTokens(tokenized));
    return Uni.createFrom().item(output);
  }

  private String tokenize(String content) {
    Set<String> stopWords = new HashSet<>(Arrays.asList("a", "an", "and", "the", "of", "to", "in"));
    String normalized = content.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9\\s]", " ");
    StringBuilder builder = new StringBuilder();
    for (String token : normalized.split("\\s+")) {
      if (token.isBlank() || stopWords.contains(token)) {
        continue;
      }
      if (!builder.isEmpty()) {
        builder.append(' ');
      }
      builder.append(token);
    }
    return builder.toString();
  }

  private int countTokens(String tokenized) {
    if (tokenized.isBlank()) {
      return 0;
    }
    return tokenized.split("\\s+").length;
  }
}
