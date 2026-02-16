package com.example.ai.sdk.mapper;

import com.example.ai.sdk.dto.DocumentDto;
import com.example.ai.sdk.entity.Document;
import com.example.ai.sdk.grpc.DocumentChunkingSvc;
import org.mapstruct.Mapping;
import org.mapstruct.factory.Mappers;
import org.pipelineframework.mapper.Mapper;

/**
 * MapStruct mapper for Document conversion between gRPC, DTO, and Entity layers.
 */
@org.mapstruct.Mapper
public interface DocumentChunkingMapper extends Mapper<DocumentChunkingSvc.Document, DocumentDto, Document> {

    DocumentChunkingMapper INSTANCE = Mappers.getMapper(DocumentChunkingMapper.class);

    /**
     * Converts a gRPC Document into a DocumentDto.
     *
     * @param grpc the gRPC Document to convert
     * @return the resulting DocumentDto with its id and content set from the source
     */
    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "content", source = "content")
    DocumentDto fromGrpc(DocumentChunkingSvc.Document grpc);

    /**
     * Convert a DocumentDto into its gRPC Document representation.
     *
     * @param dto the DTO to convert; its `id` and `content` fields are mapped to the resulting gRPC Document
     * @return the gRPC Document populated from the given DTO
     */
    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "content", source = "content")
    DocumentChunkingSvc.Document toGrpc(DocumentDto dto);

    /**
     * Converts a DocumentDto into a domain Document.
     *
     * @param dto the source DTO containing `id` and `content`
     * @return the domain Document populated with the DTO's `id` and `content`
     */
    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "content", source = "content")
    Document fromDto(DocumentDto dto);

    /**
     * Converts a Document entity into its Data Transfer Object representation.
     *
     * @param domain the Document entity to convert
     * @return the corresponding DocumentDto with matching id and content
     */
    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "content", source = "content")
    DocumentDto toDto(Document domain);
}