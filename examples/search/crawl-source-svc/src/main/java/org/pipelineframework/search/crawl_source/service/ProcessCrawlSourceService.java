package org.pipelineframework.search.crawl_source.service;

import java.time.Instant;
import java.util.UUID;
import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.mutiny.Uni;
import lombok.Getter;
import org.jboss.logging.Logger;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.search.common.domain.CrawlRequest;
import org.pipelineframework.search.common.domain.RawDocument;
import org.pipelineframework.search.common.util.FetchOptionsNormalizer;
import org.pipelineframework.search.common.util.HashingUtils;
import org.pipelineframework.service.ReactiveService;

@PipelineStep(
    inputType = org.pipelineframework.search.common.domain.CrawlRequest.class,
    outputType = org.pipelineframework.search.common.domain.RawDocument.class,
    stepType = org.pipelineframework.step.StepOneToOne.class,
    backendType = org.pipelineframework.grpc.GrpcReactiveServiceAdapter.class,
    inboundMapper = org.pipelineframework.search.common.mapper.CrawlRequestMapper.class,
    outboundMapper = org.pipelineframework.search.common.mapper.RawDocumentMapper.class,
    cacheKeyGenerator = org.pipelineframework.search.crawl_source.cache.CrawlRequestCacheKeyGenerator.class
)
@ApplicationScoped
@Getter
public class ProcessCrawlSourceService
    implements ReactiveService<CrawlRequest, RawDocument> {

  @Override
  public Uni<RawDocument> process(CrawlRequest input) {
    Logger logger = Logger.getLogger(getClass());

    if (input == null || input.sourceUrl == null || input.sourceUrl.isBlank()) {
      return Uni.createFrom().failure(new IllegalArgumentException("sourceUrl is required"));
    }

    UUID docId = input.docId != null ? input.docId : UUID.randomUUID();
    String normalizedUrl = input.sourceUrl.trim();

    String fetchOptions = FetchOptionsNormalizer.normalize(input);

    RawDocument output = new RawDocument();
    output.docId = docId;
    output.sourceUrl = normalizedUrl;
    output.fetchOptions = fetchOptions;
    output.rawContent = buildRawContent(normalizedUrl, docId);
    output.rawContentHash = HashingUtils.sha256Base64Url(output.rawContent);
    output.fetchedAt = Instant.now();

    if (fetchOptions != null) {
      logger.debugf("Fetch options for %s: %s", normalizedUrl, fetchOptions);
    }
    logger.infof("Fetched %s (%s bytes)", normalizedUrl, output.rawContent.length());
    return Uni.createFrom().item(output);
  }

  private String buildRawContent(String sourceUrl, UUID docId) {
    return "Title: Example content for " + sourceUrl + "\n"
        + "DocId: " + docId + "\n"
        + "Body: This is a simulated crawl result with headers, metadata, and content.";
  }
}
