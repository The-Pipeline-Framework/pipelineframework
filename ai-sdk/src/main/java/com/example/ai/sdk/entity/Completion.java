package com.example.ai.sdk.entity;

/**
 * Represents a completion result from LLM.
 */
public record Completion(String id, String content, String model, long timestamp) {
}