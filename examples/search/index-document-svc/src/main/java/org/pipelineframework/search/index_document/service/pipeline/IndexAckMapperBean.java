package org.pipelineframework.search.index_document.service.pipeline;

import java.time.Instant;
import java.util.UUID;
import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.search.common.domain.IndexAck;
import org.pipelineframework.search.common.dto.IndexAckDto;
import org.pipelineframework.search.common.mapper.IndexAckMapper;
import org.pipelineframework.search.grpc.IndexDocumentSvc;

@ApplicationScoped
public class IndexAckMapperBean implements IndexAckMapper {

  @Override
  public IndexAckDto toDto(IndexAck entity) {
    if (entity == null) {
      return null;
    }
    return IndexAckDto.builder()
        .docId(entity.docId)
        .indexVersion(entity.indexVersion)
        .tokensHash(entity.tokensHash)
        .indexedAt(entity.indexedAt)
        .success(entity.success)
        .build();
  }

  @Override
  public IndexAck fromDto(IndexAckDto dto) {
    if (dto == null) {
      return null;
    }
    IndexAck entity = new IndexAck();
    entity.docId = dto.getDocId();
    entity.indexVersion = dto.getIndexVersion();
    entity.tokensHash = dto.getTokensHash();
    entity.indexedAt = dto.getIndexedAt();
    entity.success = dto.getSuccess();
    return entity;
  }

  @Override
  public IndexDocumentSvc.IndexAck toGrpc(IndexAckDto dto) {
    if (dto == null) {
      return null;
    }
    return IndexDocumentSvc.IndexAck.newBuilder()
        .setDocId(toStringValue(dto.getDocId()))
        .setIndexVersion(defaultString(dto.getIndexVersion()))
        .setTokensHash(defaultString(dto.getTokensHash()))
        .setIndexedAt(toStringValue(dto.getIndexedAt()))
        .setSuccess(Boolean.TRUE.equals(dto.getSuccess()))
        .build();
  }

  @Override
  public IndexAckDto fromGrpc(IndexDocumentSvc.IndexAck grpc) {
    if (grpc == null) {
      return null;
    }
    return IndexAckDto.builder()
        .docId(parseUuid(grpc.getDocId()))
        .indexVersion(defaultString(grpc.getIndexVersion()))
        .tokensHash(defaultString(grpc.getTokensHash()))
        .indexedAt(parseInstant(grpc.getIndexedAt()))
        .success(grpc.getSuccess())
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
