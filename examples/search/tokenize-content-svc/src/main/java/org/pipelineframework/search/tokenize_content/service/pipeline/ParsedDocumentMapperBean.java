package org.pipelineframework.search.tokenize_content.service.pipeline;

import java.time.Instant;
import java.util.UUID;
import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.search.common.domain.ParsedDocument;
import org.pipelineframework.search.common.dto.ParsedDocumentDto;
import org.pipelineframework.search.common.mapper.ParsedDocumentMapper;
import org.pipelineframework.search.grpc.ParseDocumentSvc;

@ApplicationScoped
public class ParsedDocumentMapperBean implements ParsedDocumentMapper {

  @Override
  public ParsedDocumentDto toDto(ParsedDocument entity) {
    if (entity == null) {
      return null;
    }
    return ParsedDocumentDto.builder()
        .docId(entity.docId)
        .title(entity.title)
        .content(entity.content)
        .contentHash(entity.contentHash)
        .rawContentHash(entity.rawContentHash)
        .extractedAt(entity.extractedAt)
        .build();
  }

  @Override
  public ParsedDocument fromDto(ParsedDocumentDto dto) {
    if (dto == null) {
      return null;
    }
    ParsedDocument entity = new ParsedDocument();
    entity.docId = dto.getDocId();
    entity.title = dto.getTitle();
    entity.content = dto.getContent();
    entity.contentHash = dto.getContentHash();
    entity.rawContentHash = dto.getRawContentHash();
    entity.extractedAt = dto.getExtractedAt();
    return entity;
  }

  @Override
  public ParseDocumentSvc.ParsedDocument toGrpc(ParsedDocumentDto dto) {
    if (dto == null) {
      return null;
    }
    return ParseDocumentSvc.ParsedDocument.newBuilder()
        .setDocId(toStringValue(dto.getDocId()))
        .setTitle(defaultString(dto.getTitle()))
        .setContent(defaultString(dto.getContent()))
        .setContentHash(defaultString(dto.getContentHash()))
        .setRawContentHash(defaultString(dto.getRawContentHash()))
        .setExtractedAt(toStringValue(dto.getExtractedAt()))
        .build();
  }

  @Override
  public ParsedDocumentDto fromGrpc(ParseDocumentSvc.ParsedDocument grpc) {
    if (grpc == null) {
      return null;
    }
    return ParsedDocumentDto.builder()
        .docId(parseUuid(grpc.getDocId()))
        .title(defaultString(grpc.getTitle()))
        .content(defaultString(grpc.getContent()))
        .contentHash(defaultString(grpc.getContentHash()))
        .rawContentHash(defaultString(grpc.getRawContentHash()))
        .extractedAt(parseInstant(grpc.getExtractedAt()))
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
