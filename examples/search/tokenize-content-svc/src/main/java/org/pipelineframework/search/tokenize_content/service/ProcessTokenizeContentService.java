package org.pipelineframework.search.tokenize_content.service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.mutiny.Multi;
import lombok.Getter;
import org.jboss.logging.Logger;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.search.common.domain.ParsedDocument;
import org.pipelineframework.search.common.domain.TokenBatch;
import org.pipelineframework.search.common.util.HashingUtils;
import org.pipelineframework.service.ReactiveStreamingService;

@PipelineStep(
    inputType = org.pipelineframework.search.common.domain.ParsedDocument.class,
    outputType = org.pipelineframework.search.common.domain.TokenBatch.class,
    stepType = org.pipelineframework.step.StepOneToMany.class,
    backendType = org.pipelineframework.grpc.GrpcServiceStreamingAdapter.class,
    inboundMapper = org.pipelineframework.search.common.mapper.ParsedDocumentMapper.class,
    outboundMapper = org.pipelineframework.search.common.mapper.TokenBatchMapper.class,
    cacheKeyGenerator = org.pipelineframework.search.tokenize_content.cache.TokenizeContentCacheKeyGenerator.class
)
@ApplicationScoped
@Getter
public class ProcessTokenizeContentService
    implements ReactiveStreamingService<ParsedDocument, TokenBatch> {

  // Keep batches small so downstream fan-in services receive steady chunks rather than large payload spikes.
  private static final int TOKENS_PER_BATCH = 4;
  private static final Set<String> STOP_WORDS = Set.of("a", "an", "and", "the", "of", "to", "in");
  private static final Logger LOGGER = Logger.getLogger(ProcessTokenizeContentService.class);

  @Override
  public Multi<TokenBatch> process(ParsedDocument input) {
    if (input == null || input.content == null || input.content.isBlank()) {
      return Multi.createFrom().failure(new IllegalArgumentException("content is required"));
    }
    if (input.docId == null) {
      return Multi.createFrom().failure(new IllegalArgumentException("docId is required"));
    }

    List<String> tokenBatches = tokenizeIntoBatches(input.content, TOKENS_PER_BATCH);
    Instant now = Instant.now();

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debugf(
          "Tokenized doc %s into %s batches (%s tokens)",
          input.docId,
          tokenBatches.size(),
          countTokensFromBatches(tokenBatches));
    }
    if (tokenBatches.isEmpty()) {
      return Multi.createFrom().empty();
    }
    return Multi.createFrom()
        .range(0, tokenBatches.size())
        .onItem()
        .transform(batchIndex -> toTokenBatch(input, tokenBatches.get(batchIndex), now, batchIndex));
  }

  private TokenBatch toTokenBatch(ParsedDocument input, String tokens, Instant tokenizedAt, int batchIndex) {
    TokenBatch output = new TokenBatch();
    output.docId = input.docId;
    output.batchIndex = batchIndex;
    output.tokens = tokens;
    output.tokensHash = HashingUtils.sha256Base64Url(tokens);
    output.contentHash = input.contentHash;
    output.tokenizedAt = tokenizedAt;
    return output;
  }

  private List<String> tokenizeIntoBatches(String content, int batchSize) {
    if (batchSize <= 0) {
      throw new IllegalArgumentException("batchSize must be > 0");
    }
    String normalized = content.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9\\s]", " ");
    ArrayList<String> tokens = new ArrayList<>();
    for (String token : normalized.split("\\s+")) {
      if (token.isBlank() || STOP_WORDS.contains(token)) {
        continue;
      }
      tokens.add(token);
    }

    if (tokens.isEmpty()) {
      return List.of();
    }

    ArrayList<String> batches = new ArrayList<>();
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < tokens.size(); i++) {
      if (builder.length() > 0) {
        builder.append(' ');
      }
      builder.append(tokens.get(i));
      boolean endOfBatch = (i + 1) % batchSize == 0;
      boolean lastToken = i == tokens.size() - 1;
      if (endOfBatch || lastToken) {
        batches.add(builder.toString());
        builder.setLength(0);
      }
    }
    return batches;
  }

  private int countTokensFromBatches(List<String> tokenBatches) {
    return tokenBatches.stream()
        .filter(batch -> batch != null && !batch.isBlank())
        .mapToInt(batch -> batch.trim().split("\\s+").length)
        .sum();
  }
}
