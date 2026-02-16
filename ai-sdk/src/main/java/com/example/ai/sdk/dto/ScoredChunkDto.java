package com.example.ai.sdk.dto;

/**
 * DTO for ScoredChunk entity.
 */
public record ScoredChunkDto(ChunkDto chunk, float score) {
}