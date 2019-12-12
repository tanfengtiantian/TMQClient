package io.kafka.transaction;

public class LongSequenceGenerator {

    private long lastSequenceId;


    public synchronized long getNextSequenceId() {
        return ++this.lastSequenceId;
    }


    public synchronized long getLastSequenceId() {
        return this.lastSequenceId;
    }


    public synchronized void setLastSequenceId(final long l) {
        this.lastSequenceId = l;
    }
}
