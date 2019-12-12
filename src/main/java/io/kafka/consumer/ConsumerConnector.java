package io.kafka.consumer;

import io.kafka.producer.serializer.Decoder;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author tf
 * @version 创建时间：2019年1月31日 下午2:34:38
 * @ClassName ConsumerConnector
 */
public interface ConsumerConnector extends Closeable{

	/**
     * Create a list of {@link MessageStream} for each topic
     * @param <T> message type
     * @param topicCountMap a map of (topic,#streams) pair
     * @param decoder message decoder
     * @return a map of (topic,list of MessageStream) pair. The number of
     *         items in the list is #streams. Each MessageStream supports
     *         an iterator of messages.
     */
    <T> Map<String, List<MessageStream<T>>> createMessageStreams(//
            Map<String, Integer> topicCountMap, Decoder<T> decoder);

    /**
     * Commit the offsets of all broker partitions connected by this
     * connector
     */
    void commitOffsets();

    /**
     * Shut down the connector
     */
    public void close() throws IOException;
}
