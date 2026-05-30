package org.pipelineframework.search.common.mapper;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;
import org.mapstruct.factory.Mappers;
import org.pipelineframework.search.common.domain.CrawlRequest;
import org.pipelineframework.search.common.dto.CrawlRequestDto;
import org.pipelineframework.search.grpc.PipelineTypes;

@SuppressWarnings("unused")
@Mapper(
    componentModel = "cdi",
    uses = {CommonConverters.class},
    unmappedTargetPolicy = ReportingPolicy.WARN)
public interface CrawlRequestMapper extends org.pipelineframework.mapper.Mapper<CrawlRequest, CrawlRequestDto> {

  CrawlRequestMapper INSTANCE = Mappers.getMapper( CrawlRequestMapper.class );

  // Domain ↔ DTO
  CrawlRequestDto toDto(CrawlRequest entity);

  CrawlRequest fromDto(CrawlRequestDto dto);

  // DTO ↔ gRPC
  default PipelineTypes.CrawlRequest toGrpc(CrawlRequestDto dto) {
    if (dto == null) {
      return null;
    }

    PipelineTypes.CrawlRequest.Builder builder = PipelineTypes.CrawlRequest.newBuilder();
    if (dto.getDocId() != null) {
      builder.setDocId(dto.getDocId().toString());
    }
    if (dto.getSourceUrl() != null) {
      builder.setSourceUrl(dto.getSourceUrl());
    }
    if (dto.getFetchMethod() != null) {
      builder.setFetchMethod(dto.getFetchMethod());
    }
    if (dto.getAccept() != null) {
      builder.setAccept(dto.getAccept());
    }
    if (dto.getAcceptLanguage() != null) {
      builder.setAcceptLanguage(dto.getAcceptLanguage());
    }
    if (dto.getAuthScope() != null) {
      builder.setAuthScope(dto.getAuthScope());
    }
    Map<String, String> fetchHeaders = dto.getFetchHeaders();
    if (fetchHeaders != null) {
      builder.putAllFetchHeaders(fetchHeaders);
    }
    return builder.build();
  }

  default CrawlRequestDto fromGrpc(PipelineTypes.CrawlRequest grpc) {
    if (grpc == null) {
      return null;
    }

    CrawlRequestDto.CrawlRequestDtoBuilder builder = CrawlRequestDto.builder();
    String docId = grpc.getDocId();
    if (docId != null && !docId.isBlank()) {
      builder.docId(UUID.fromString(docId));
    }
    builder.sourceUrl(grpc.getSourceUrl());
    builder.fetchMethod(grpc.getFetchMethod());
    builder.accept(grpc.getAccept());
    builder.acceptLanguage(grpc.getAcceptLanguage());
    builder.authScope(grpc.getAuthScope());
    builder.fetchHeaders(new LinkedHashMap<>(grpc.getFetchHeadersMap()));
    return builder.build();
  }

  @Override
  default CrawlRequest fromExternal(CrawlRequestDto external) {
    return fromDto(external);
  }

  @Override
  default CrawlRequestDto toExternal(CrawlRequest domain) {
    return toDto(domain);
  }

  default PipelineTypes.CrawlRequest toDtoToGrpc(CrawlRequest domain) {
    return toGrpc(toDto(domain));
  }

  default CrawlRequest fromGrpcFromDto(PipelineTypes.CrawlRequest grpc) {
    return fromDto(fromGrpc(grpc));
  }
}
