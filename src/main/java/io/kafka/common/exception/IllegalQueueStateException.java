package io.kafka.common.exception;

public class IllegalQueueStateException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public IllegalQueueStateException() {
        super();
    }

    public IllegalQueueStateException(String message) {
        super(message);
    }

}
