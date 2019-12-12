package io.kafka.common.exception;

public class QueueClosedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public QueueClosedException() {
        super();
    }

    public QueueClosedException(String message) {
        super(message);
    }

}
