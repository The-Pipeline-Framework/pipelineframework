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

    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "content", source = "content")
    DocumentDto fromGrpc(DocumentChunkingSvc.Document grpc);

    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "content", source = "content")
    DocumentChunkingSvc.Document toGrpc(DocumentDto dto);

    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "content", source = "content")
    Document fromDto(DocumentDto dto);

    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "content", source = "content")
    DocumentDto toDto(Document domain);
}