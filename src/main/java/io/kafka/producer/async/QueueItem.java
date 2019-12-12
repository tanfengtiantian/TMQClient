package io.kafka.producer.async;

/**
 *
 * @author tf
 * @since 1.0
 */
public class QueueItem<T> {

    public final T data;

    public final int partition;

    public final String topic;

    public QueueItem(T data, int partition, String topic) {
        super();
        this.data = data;
        this.partition = partition;
        this.topic = topic;
    }

    @Override
    public String toString() {
        return String.format("QueueItem [data=%s, partition=%s, topic=%s]", data, partition, topic);
    }
}
