package io.kafka.producer.async;

import io.kafka.producer.SyncProducer;
import io.kafka.producer.serializer.Encoder;

import java.io.Closeable;
import java.util.List;
import java.util.Properties;

public interface EventHandler<T> extends Closeable {
    /**
     * Initializes the event handler using a Properties object
     *
     * @param properties the properties used to initialize the event
     *        handler
     */
    void init(Properties properties);

    /**
     * Callback to dispatch the batched data and send it to a Jafka server
     *
     * @param events the data sent to the producer
     * @param producer the low-level producer used to send the data
     * @param encoder data encoder
     */
    void handle(List<QueueItem<T>> events, SyncProducer producer, Encoder<T> encoder);

    /**
     * Cleans up and shuts down the event handler
     */
    void close();
}
