package org.pipelineframework.checkout.deliverorder.common.mapper;

public class ReadyOrderMapperException extends IllegalArgumentException {
    /**
     * Creates a ReadyOrderMapperException with no detail message or cause.
     */
    public ReadyOrderMapperException() {
    }

    /**
     * Creates a ReadyOrderMapperException with the specified detail message.
     *
     * @param message the detail message describing the cause of the exception
     */
    public ReadyOrderMapperException(String message) {
        super(message);
    }

    /**
     * Constructs a ReadyOrderMapperException with the specified cause.
     *
     * @param cause the underlying cause of this exception
     */
    public ReadyOrderMapperException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a ReadyOrderMapperException with the specified detail message and cause.
     *
     * @param message the detail message explaining the reason for the exception
     * @param cause the underlying cause of this exception
     */
    public ReadyOrderMapperException(String message, Throwable cause) {
        super(message, cause);
    }
}