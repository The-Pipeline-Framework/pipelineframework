package com.example.ai.sdk.dto;

/**
 * DTO for Chunk entity.
 */
public record ChunkDto(String id, String documentId, String content, int position) {
}