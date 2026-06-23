package org.pipelineframework.search.common.mapper;

import org.mapstruct.BeanMapping;
import org.mapstruct.BeforeMapping;
import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;
import org.pipelineframework.search.common.domain.SearchIndexDocument;
import org.pipelineframework.search.common.dto.SearchIndexDocumentDto;

@SuppressWarnings("unused")
@Mapper(
    componentModel = "cdi",
    uses = {CommonConverters.class},
    unmappedTargetPolicy = ReportingPolicy.WARN)
public interface SearchIndexDocumentMapper
    extends org.pipelineframework.mapper.Mapper<SearchIndexDocument, SearchIndexDocumentDto> {

  SearchIndexDocumentDto toDto(SearchIndexDocument entity);

  SearchIndexDocument fromDto(SearchIndexDocumentDto dto);

  @BeanMapping(unmappedTargetPolicy = ReportingPolicy.IGNORE)
  org.pipelineframework.search.grpc.PipelineTypes.SearchIndexDocument toGrpc(SearchIndexDocumentDto dto);

  SearchIndexDocumentDto fromGrpc(org.pipelineframework.search.grpc.PipelineTypes.SearchIndexDocument grpc);

  @BeforeMapping
  default void validateDto(SearchIndexDocumentDto dto) {
    if (dto == null) {
      return;
    }
    if (dto.getDocId() == null) {
      throw new IllegalArgumentException("SearchIndexDocumentDto.docId must not be null");
    }
    if (dto.getExternalId() == null || dto.getExternalId().isBlank()) {
      throw new IllegalArgumentException("SearchIndexDocumentDto.externalId must not be blank");
    }
    if (dto.getBatchIndex() == null || dto.getBatchIndex() < 0) {
      throw new IllegalArgumentException("SearchIndexDocumentDto.batchIndex must be >= 0");
    }
    if (dto.getTokensHash() == null || dto.getTokensHash().isBlank()) {
      throw new IllegalArgumentException("SearchIndexDocumentDto.tokensHash must not be blank");
    }
    if (dto.getVectorHash() == null || dto.getVectorHash().isBlank()) {
      throw new IllegalArgumentException("SearchIndexDocumentDto.vectorHash must not be blank");
    }
    if (dto.getVectorVersion() == null || dto.getVectorVersion().isBlank()) {
      throw new IllegalArgumentException("SearchIndexDocumentDto.vectorVersion must not be blank");
    }
    if (dto.getIndexName() == null || dto.getIndexName().isBlank()) {
      throw new IllegalArgumentException("SearchIndexDocumentDto.indexName must not be blank");
    }
  }

  @Override
  default SearchIndexDocument fromExternal(SearchIndexDocumentDto external) {
    return fromDto(external);
  }

  @Override
  default SearchIndexDocumentDto toExternal(SearchIndexDocument domain) {
    return toDto(domain);
  }

  default org.pipelineframework.search.grpc.PipelineTypes.SearchIndexDocument toDtoToGrpc(SearchIndexDocument domain) {
    return toGrpc(toDto(domain));
  }

  default SearchIndexDocument fromGrpcFromDto(org.pipelineframework.search.grpc.PipelineTypes.SearchIndexDocument grpc) {
    SearchIndexDocumentDto dto = fromGrpc(grpc);
    validateDto(dto);
    return fromDto(dto);
  }
}
