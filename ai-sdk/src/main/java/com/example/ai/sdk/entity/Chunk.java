package com.example.ai.sdk.entity;

/**
 * Represents a chunk of a document.
 */
public record Chunk(String id, String documentId, String content, int position) {
}