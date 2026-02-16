package com.example.ai.sdk.entity;

/**
 * Result of storing a vector in the store.
 */
public record StoreResult(String id, boolean success, String message) {
}