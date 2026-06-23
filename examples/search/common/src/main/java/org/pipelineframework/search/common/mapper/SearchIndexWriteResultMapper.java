package org.pipelineframework.search.common.mapper;

import org.mapstruct.BeanMapping;
import org.mapstruct.BeforeMapping;
import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;
import org.pipelineframework.search.common.domain.SearchIndexWriteResult;
import org.pipelineframework.search.common.dto.SearchIndexWriteResultDto;

@SuppressWarnings("unused")
@Mapper(
    componentModel = "cdi",
    uses = {CommonConverters.class},
    unmappedTargetPolicy = ReportingPolicy.WARN)
public interface SearchIndexWriteResultMapper
    extends org.pipelineframework.mapper.Mapper<SearchIndexWriteResult, SearchIndexWriteResultDto> {

  SearchIndexWriteResultDto toDto(SearchIndexWriteResult entity);

  SearchIndexWriteResult fromDto(SearchIndexWriteResultDto dto);

  @BeanMapping(unmappedTargetPolicy = ReportingPolicy.IGNORE)
  org.pipelineframework.search.grpc.PipelineTypes.SearchIndexWriteResult toGrpc(SearchIndexWriteResultDto dto);

  SearchIndexWriteResultDto fromGrpc(org.pipelineframework.search.grpc.PipelineTypes.SearchIndexWriteResult grpc);

  @BeforeMapping
  default void validateDto(SearchIndexWriteResultDto dto) {
    if (dto == null) {
      return;
    }
    if (dto.getDocId() == null) {
      throw new IllegalArgumentException("SearchIndexWriteResultDto.docId must not be null");
    }
    if (dto.getExternalId() == null || dto.getExternalId().isBlank()) {
      throw new IllegalArgumentException("SearchIndexWriteResultDto.externalId must not be blank");
    }
    if (dto.getCommandId() == null || dto.getCommandId().isBlank()) {
      throw new IllegalArgumentException("SearchIndexWriteResultDto.commandId must not be blank");
    }
    if (dto.getIndexName() == null || dto.getIndexName().isBlank()) {
      throw new IllegalArgumentException("SearchIndexWriteResultDto.indexName must not be blank");
    }
  }

  @Override
  default SearchIndexWriteResult fromExternal(SearchIndexWriteResultDto external) {
    return fromDto(external);
  }

  @Override
  default SearchIndexWriteResultDto toExternal(SearchIndexWriteResult domain) {
    return toDto(domain);
  }

  default org.pipelineframework.search.grpc.PipelineTypes.SearchIndexWriteResult toDtoToGrpc(SearchIndexWriteResult domain) {
    return toGrpc(toDto(domain));
  }

  default SearchIndexWriteResult fromGrpcFromDto(org.pipelineframework.search.grpc.PipelineTypes.SearchIndexWriteResult grpc) {
    SearchIndexWriteResultDto dto = fromGrpc(grpc);
    validateDto(dto);
    return fromDto(dto);
  }
}
