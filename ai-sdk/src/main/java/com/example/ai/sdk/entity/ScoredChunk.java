package com.example.ai.sdk.entity;

/**
 * Represents a chunk with similarity score.
 */
public record ScoredChunk(Chunk chunk, float score) {
}