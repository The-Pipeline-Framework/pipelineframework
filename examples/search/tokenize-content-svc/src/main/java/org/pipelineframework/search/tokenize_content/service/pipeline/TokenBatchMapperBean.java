package org.pipelineframework.search.tokenize_content.service.pipeline;

import java.time.Instant;
import java.util.UUID;
import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.search.common.domain.TokenBatch;
import org.pipelineframework.search.common.dto.TokenBatchDto;
import org.pipelineframework.search.common.mapper.TokenBatchMapper;
import org.pipelineframework.search.grpc.TokenizeContentSvc;

@ApplicationScoped
public class TokenBatchMapperBean implements TokenBatchMapper {

  @Override
  public TokenBatchDto toDto(TokenBatch entity) {
    if (entity == null) {
      return null;
    }
    return TokenBatchDto.builder()
        .docId(entity.docId)
        .tokens(entity.tokens)
        .tokensHash(entity.tokensHash)
        .contentHash(entity.contentHash)
        .tokenizedAt(entity.tokenizedAt)
        .build();
  }

  @Override
  public TokenBatch fromDto(TokenBatchDto dto) {
    if (dto == null) {
      return null;
    }
    TokenBatch entity = new TokenBatch();
    entity.docId = dto.getDocId();
    entity.tokens = dto.getTokens();
    entity.tokensHash = dto.getTokensHash();
    entity.contentHash = dto.getContentHash();
    entity.tokenizedAt = dto.getTokenizedAt();
    return entity;
  }

  @Override
  public TokenizeContentSvc.TokenBatch toGrpc(TokenBatchDto dto) {
    if (dto == null) {
      return null;
    }
    return TokenizeContentSvc.TokenBatch.newBuilder()
        .setDocId(toStringValue(dto.getDocId()))
        .setTokens(defaultString(dto.getTokens()))
        .setTokensHash(defaultString(dto.getTokensHash()))
        .setContentHash(defaultString(dto.getContentHash()))
        .setTokenizedAt(toStringValue(dto.getTokenizedAt()))
        .build();
  }

  @Override
  public TokenBatchDto fromGrpc(TokenizeContentSvc.TokenBatch grpc) {
    if (grpc == null) {
      return null;
    }
    return TokenBatchDto.builder()
        .docId(parseUuid(grpc.getDocId()))
        .tokens(defaultString(grpc.getTokens()))
        .tokensHash(defaultString(grpc.getTokensHash()))
        .contentHash(defaultString(grpc.getContentHash()))
        .tokenizedAt(parseInstant(grpc.getTokenizedAt()))
        .build();
  }

  private static String defaultString(String value) {
    return value == null ? "" : value;
  }

  private static String toStringValue(UUID value) {
    return value == null ? "" : value.toString();
  }

  private static String toStringValue(Instant value) {
    return value == null ? "" : value.toString();
  }

  private static UUID parseUuid(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    return UUID.fromString(value);
  }

  private static Instant parseInstant(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    return Instant.parse(value);
  }
}
