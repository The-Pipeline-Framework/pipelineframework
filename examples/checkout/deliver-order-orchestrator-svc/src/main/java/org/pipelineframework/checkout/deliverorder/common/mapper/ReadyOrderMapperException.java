package org.pipelineframework.checkout.deliverorder.common.mapper;

public class ReadyOrderMapperException extends IllegalArgumentException {
    public ReadyOrderMapperException() {
    }

    public ReadyOrderMapperException(String message) {
        super(message);
    }

    public ReadyOrderMapperException(Throwable cause) {
        super(cause);
    }

    public ReadyOrderMapperException(String message, Throwable cause) {
        super(message, cause);
    }
}
