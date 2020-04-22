package io.kafka.common.exception;

import io.kafka.common.ErrorMapping;

public class RpcRuntimeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public RpcRuntimeException() {
    }

    public RpcRuntimeException(String message) {
        super(message);
    }

    public RpcRuntimeException(Throwable cause) {
        super(cause);
    }

    public RpcRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

}