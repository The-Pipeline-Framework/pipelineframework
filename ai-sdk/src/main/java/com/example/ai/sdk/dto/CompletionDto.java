package com.example.ai.sdk.dto;

/**
 * DTO for Completion entity.
 */
public record CompletionDto(String id, String content, String model, long timestamp) {
}