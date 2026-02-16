package com.example.ai.sdk.entity;

/**
 * Represents a prompt for LLM completion.
 */
public record Prompt(String id, String content, double temperature) {
}