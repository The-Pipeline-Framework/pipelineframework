/*
 * Copyright (c) 2023-2025 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.plugin.persistence;

import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.parallelism.OrderingRequirement;
import org.pipelineframework.parallelism.ParallelismHints;
import org.pipelineframework.parallelism.ThreadSafety;
import org.pipelineframework.service.ReactiveSideEffectService;
import org.pipelineframework.step.NonRetryableException;

/**
 * A general-purpose persistence plugin that can persist any entity that has a corresponding
 * PersistenceProvider configured in the system.
 */
public class PersistenceService<T> implements ReactiveSideEffectService<T>, ParallelismHints {
    private final Logger logger = Logger.getLogger(PersistenceService.class);

    @Inject
    PersistenceManager persistenceManager;

    @Inject
    PersistenceConfig config;

    @Override
    public Uni<T> process(T item) {
        logger.debugf("PersistenceService.process() called with item: %s (class: %s)",
            item != null ? item.toString() : "null",
            item != null ? item.getClass().getName() : "null");
        if (item == null) {
            logger.debug("Received null item to persist, returning null");
            return Uni.createFrom().nullItem();
        }

        logger.debugf("Using persistenceManager: %s to persist item of type: %s",
            persistenceManager != null ? persistenceManager.getClass().getName() : "null",
            item.getClass().getName());

        if (persistenceManager == null) {
            return Uni.createFrom().failure(new IllegalStateException("PersistenceManager is not available"));
        }
        return persistenceManager.persist(item)
            .onFailure(this::isDuplicateKeyError)
            .recoverWithUni(failure -> handleDuplicateKey(item, failure))
            .onItem().invoke(result -> logger.debugf("Successfully persisted entity: %s", result != null ? result.getClass().getName() : "null"))
            .onFailure().invoke(failure -> logger.error("Failed to persist entity", failure))
            .onFailure().transform(failure -> isTransientDbError(failure)
                ? failure
                : new NonRetryableException("Non-transient persistence error", failure))
            .replaceWith(item); // Return the original item as it was just persisted (side effect)
    }

    private Uni<T> handleDuplicateKey(T item, Throwable failure) {
        String policyValue = config != null ? config.duplicateKey() : null;
        DuplicateKeyPolicy policy = DuplicateKeyPolicy.fromConfig(policyValue);
        return switch (policy) {
            case IGNORE -> Uni.createFrom().item(item);
            case UPSERT -> persistenceManager.persistOrUpdate(item).replaceWith(item);
            case FAIL -> Uni.createFrom().failure(failure);
        };
    }

    private enum DuplicateKeyPolicy {
        FAIL,
        IGNORE,
        UPSERT;

        static DuplicateKeyPolicy fromConfig(String value) {
            if (value == null || value.isBlank()) {
                return FAIL;
            }
            String normalized = value.trim().replace('-', '_').toUpperCase();
            for (DuplicateKeyPolicy policy : values()) {
                if (policy.name().equals(normalized)) {
                    return policy;
                }
            }
            return FAIL;
        }
    }

    /**
     * Determines whether a Throwable represents a transient database connectivity issue.
     *
     * @param failure the throwable to inspect; walks the cause chain checking each message and type for transient DB indicators
     * @return {@code true} if any exception in the cause chain has a message containing "connection refused", "connection closed",
     * "timeout", "connection reset", "communications link failure" (case-insensitive) or is of a known transient exception type,
     * {@code false} otherwise
     */
    protected boolean isTransientDbError(Throwable failure) {
        Throwable current = failure;
        while (current != null) {
            // Check if the current exception is of a known transient type
            if (isKnownTransientExceptionType(current)) {
                return true;
            }

            // Check for transient indicators in the message (case-insensitive)
            String msg = current.getMessage();
            if (msg != null) {
                String lowerMsg = msg.toLowerCase();
                if (lowerMsg.contains("connection refused")
                    || lowerMsg.contains("connection closed")
                    || lowerMsg.contains("timeout")
                    || lowerMsg.contains("connection reset")
                    || lowerMsg.contains("communications link failure")) {
                    return true;
                }
            }

            // Move to the cause
            current = current.getCause();

            // Prevent infinite loops if there's a circular cause
            if (current == failure) {
                break;
            }
        }

        return false;
    }

    /**
     * Determines if the given exception is of a type that indicates a transient database error.
     *
     * @param throwable the exception to check
     * @return true if the exception type is known to indicate transient database errors
     */
    private boolean isKnownTransientExceptionType(Throwable throwable) {
        // SQL transient exceptions
        if (throwable instanceof java.sql.SQLTransientException) {
            return true;
        }

        // Hibernate Reactive specific transient exceptions (if they exist)
        // Check for common Hibernate and database driver transient exceptions
        String throwableClassName = throwable.getClass().getName();
        if (throwableClassName.contains("hibernate") &&
            (throwableClassName.toLowerCase().contains("transient")
                || throwableClassName.toLowerCase().contains("connection")
                || throwableClassName.toLowerCase().contains("timeout"))) {
            return true;
        }

        // PostgreSQL-specific connection-related exceptions
        if (throwableClassName.equals("org.postgresql.util.PSQLException")) {
            // Check for SQL state codes that indicate connection issues
            // 08xxx = Connection Exception
            try {
                java.lang.reflect.Method getSQLStateMethod = throwable.getClass().getMethod("getSQLState");
                Object result = getSQLStateMethod.invoke(throwable);
                if (result != null) {
                    String sqlState = result.toString();
                    if (sqlState != null && sqlState.startsWith("08")) {
                        return true;
                    }
                }
            } catch (Exception e) {
                // If we can't access the SQL state through reflection, fall back to message inspection
                String message = throwable.getMessage();
                if (message != null) {
                    String lowerMessage = message.toLowerCase();
                    // Check for connection-related keywords in PostgreSQL exception messages
                    if (lowerMessage.contains("connection refused") ||
                        lowerMessage.contains("connection closed") ||
                        lowerMessage.contains("connection lost") ||
                        lowerMessage.contains("terminating connection") ||
                        lowerMessage.contains("connection timeout")) {
                        return true;
                    }
                }
            }
            return false; // Only return true for actual connection-related PSQLExceptions
        }

        // MySQL-specific connection exceptions (more specific than just checking package name)
        if (throwableClassName.startsWith("com.mysql.cj.exceptions.")) {
            // Check for specific MySQL connection-related exception types
            return throwableClassName.contains("CommunicationsException") ||
                    throwableClassName.contains("ConnectionException") ||
                    throwableClassName.contains("MySQLTimeoutException") ||
                    throwableClassName.contains("SSLException");// Only return true for specific connection-related MySQL exceptions
        }

        // Oracle-specific connection exceptions
        if (throwableClassName.startsWith("oracle.jdbc")) {
            // Check for Oracle connection-related exceptions
            return throwableClassName.contains("OracleConnection") ||
                    throwableClassName.contains("SQLRecoverableException");// Only return true for connection-related Oracle exceptions
        }

        // Microsoft SQL Server exceptions
        if (throwableClassName.startsWith("com.microsoft.sqlserver.jdbc")) {
            // Check for SQL Server connection-related exceptions
            if (throwableClassName.contains("SQLServerException")) {
                // Check if the message indicates a connection issue
                String message = throwable.getMessage();
                if (message != null) {
                    String lowerMessage = message.toLowerCase();
                    // Common connection-related messages in SQL Server exceptions
                    return lowerMessage.contains("connection timed out") ||
                            lowerMessage.contains("connection reset") ||
                            lowerMessage.contains("the connection is closed") ||
                            lowerMessage.contains("tcp provider") ||
                            lowerMessage.contains("connection was terminated");
                }
            }
            return false; // Only return true for connection-related SQL Server exceptions
        }

        return false;
    }

    private boolean isDuplicateKeyError(Throwable failure) {
        Throwable current = failure;
        while (current != null) {
            if (current instanceof java.sql.SQLException sqlException) {
                if ("23505".equals(sqlException.getSQLState())) {
                    return true;
                }
            } else {
                try {
                    java.lang.reflect.Method getSqlState = current.getClass().getMethod("getSQLState");
                    Object result = getSqlState.invoke(current);
                    if (result != null && "23505".equals(result.toString())) {
                        return true;
                    }
                } catch (Exception ignored) {
                    // ignore reflection failures
                }
            }

            String msg = current.getMessage();
            if (msg != null) {
                String lowerMsg = msg.toLowerCase();
                if (lowerMsg.contains("duplicate key") || lowerMsg.contains("unique constraint")) {
                    return true;
                }
            }

            current = current.getCause();
            if (current == failure) {
                break;
            }
        }

        return false;
    }

    @Override
    public OrderingRequirement orderingRequirement() {
        if (persistenceManager == null) {
            return OrderingRequirement.RELAXED;
        }
        return persistenceManager.orderingRequirement();
    }

    @Override
    public ThreadSafety threadSafety() {
        if (persistenceManager == null) {
            return ThreadSafety.UNSAFE;
        }
        return persistenceManager.threadSafety();
    }
}
