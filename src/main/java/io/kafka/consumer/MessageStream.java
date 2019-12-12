package io.kafka.consumer;

import io.kafka.producer.serializer.Decoder;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author tf
 * @version 创建时间：2019年1月31日 下午2:36:09
 * @ClassName MessageStream
 */
public class MessageStream<T> implements Iterable<T> {

	final String topic;

    final BlockingQueue<FetchedDataChunk> queue;

    final int consumerTimeoutMs;

    final Decoder<T> decoder;

    private final ConsumerIterator<T> consumerIterator;

    public MessageStream(String topic, BlockingQueue<FetchedDataChunk> queue, int consumerTimeoutMs, Decoder<T> decoder) {
        super();
        this.topic = topic;
        this.queue = queue;
        this.consumerTimeoutMs = consumerTimeoutMs;
        this.decoder = decoder;
        this.consumerIterator = new ConsumerIterator<T>(topic, queue, consumerTimeoutMs, decoder);
    }

    public Iterator<T> iterator() {
        return consumerIterator;
    }

    /**
     *	此方法清除在使用者期间迭代的队列
     *  再平衡。这主要是为了减少重复的次数
     *  由消费者接收
     */
    public void clear() {
        consumerIterator.clearCurrentChunk();
    }
}
