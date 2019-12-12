package io.kafka.common.exception;

public class AsyncProducerInterruptedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public AsyncProducerInterruptedException() {
        super();
    }

    public AsyncProducerInterruptedException(String message) {
        super(message);
    }

}
