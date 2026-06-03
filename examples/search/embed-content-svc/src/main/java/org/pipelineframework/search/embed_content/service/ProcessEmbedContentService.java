package org.pipelineframework.search.embed_content.service;

import java.time.Duration;
import java.time.Instant;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import io.smallrye.mutiny.Uni;
import lombok.Getter;
import org.jboss.logging.Logger;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.search.common.domain.EmbeddedChunk;
import org.pipelineframework.search.common.domain.TokenBatch;
import org.pipelineframework.search.common.util.HashingUtils;
import org.pipelineframework.service.ReactiveService;

@PipelineStep(cacheKeyGenerator = org.pipelineframework.search.embed_content.cache.EmbedContentCacheKeyGenerator.class)
@ApplicationScoped
@Getter
public class ProcessEmbedContentService implements ReactiveService<TokenBatch, EmbeddedChunk> {

  private static final Logger LOGGER = Logger.getLogger(ProcessEmbedContentService.class);

  private final long delayMs;
  private final String vectorVersion;

  @Inject
  public ProcessEmbedContentService(
      @ConfigProperty(name = "search.embed.delay-ms", defaultValue = "0") long delayMs,
      @ConfigProperty(name = "search.embed.vector-version", defaultValue = "v1") String vectorVersion) {
    this.delayMs = Math.max(0L, delayMs);
    this.vectorVersion = vectorVersion == null || vectorVersion.isBlank() ? "v1" : vectorVersion.trim();
  }

  @Override
  public Uni<EmbeddedChunk> process(TokenBatch input) {
    Uni<EmbeddedChunk> result = Uni.createFrom().item(() -> embed(input));
    if (delayMs > 0) {
      result = result.onItem().delayIt().by(Duration.ofMillis(delayMs));
    }
    return result;
  }

  private EmbeddedChunk embed(TokenBatch input) {
    if (input == null) {
      throw new IllegalArgumentException("token batch is required");
    }
    if (input.docId == null) {
      throw new IllegalArgumentException("docId is required");
    }
    if (input.batchIndex == null || input.batchIndex < 0) {
      throw new IllegalArgumentException("batchIndex must be >= 0");
    }
    if (input.tokenCount == null || input.tokenCount <= 0) {
      throw new IllegalArgumentException("tokenCount must be > 0");
    }
    if (input.tokens == null || input.tokens.isBlank()) {
      throw new IllegalArgumentException("tokens are required");
    }
    if (input.tokensHash == null || input.tokensHash.isBlank()) {
      throw new IllegalArgumentException("tokensHash is required");
    }

    EmbeddedChunk output = new EmbeddedChunk();
    output.docId = input.docId;
    output.batchIndex = input.batchIndex;
    output.tokenCount = input.tokenCount;
    output.tokens = input.tokens;
    output.tokensHash = input.tokensHash;
    output.contentHash = input.contentHash;
    output.vectorVersion = vectorVersion;
    output.vectorHash = vectorHash(input);
    output.embeddedAt = Instant.now();

    LOGGER.debugf(
        "Embedded doc %s batch %s with vectorVersion=%s",
        output.docId,
        output.batchIndex,
        output.vectorVersion);
    return output;
  }

  private String vectorHash(TokenBatch input) {
    return HashingUtils.sha256Base64Url(String.join("",
        lengthPrefixed(vectorVersion),
        lengthPrefixed(input.docId.toString()),
        lengthPrefixed(String.valueOf(input.batchIndex)),
        lengthPrefixed(input.tokensHash.trim()),
        lengthPrefixed(input.tokens.trim())));
  }

  private String lengthPrefixed(String value) {
    return value.length() + ":" + value;
  }
}
