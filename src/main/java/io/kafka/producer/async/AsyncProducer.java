package io.kafka.producer.async;

import io.kafka.common.exception.AsyncProducerInterruptedException;
import io.kafka.common.exception.QueueClosedException;
import io.kafka.common.exception.QueueFullException;
import io.kafka.producer.SyncProducer;
import io.kafka.producer.config.ProducerConfig;
import io.kafka.producer.serializer.Encoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author tf
 * @since 异步Producer
 */
public class AsyncProducer<T> implements Closeable {

    private final static Logger logger = LoggerFactory.getLogger(AsyncProducer.class);

    private static final Random random = new Random();
    private final int asyncProducerID = AsyncProducer.random.nextInt();

    private final SyncProducer producer;

    private final CallbackHandler<T> callbackHandler;

    private final int enqueueTimeoutMs;

    private final LinkedBlockingQueue<QueueItem<T>> queue;

    private final ProducerSendThread<T> sendThread;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    public AsyncProducer(AsyncProducerConfig asyncProducerConfig, SyncProducer syncProducer, Encoder<T> serializer, EventHandler<T> eventHandler,
                         Properties eventHandlerProperties, CallbackHandler<T> callbackHandler, Properties callbackHandlerProperties) {
        this.producer = syncProducer;
        this.callbackHandler = callbackHandler;
        this.enqueueTimeoutMs = asyncProducerConfig.getEnqueueTimeoutMs();
        this.queue = new LinkedBlockingQueue<>(asyncProducerConfig.getQueueSize());
        //
        if (eventHandler != null) {
            eventHandler.init(eventHandlerProperties);
        }
        this.sendThread = new ProducerSendThread<T>("ProducerSendThread-" + asyncProducerID,
                queue, //
                serializer,//
                producer, //
                eventHandler != null ? eventHandler//
                        : new DefaultEventHandler<T>(new ProducerConfig(asyncProducerConfig.getProperties()), callbackHandler), //
                callbackHandler, //
                asyncProducerConfig.getQueueTime(), //
                asyncProducerConfig.getBatchSize());
    }

    public void start() {
        sendThread.start();
    }

    public void send(String topic, T event, int partition) {
        if (closed.get()) {
            throw new QueueClosedException("Attempt to add event to a closed queue.");
        }

        QueueItem<T> data = new QueueItem<T>(event, partition, topic);
        boolean added = false;
        if (data != null) {
            try {
                if (enqueueTimeoutMs == 0) {
                    added = queue.offer(data);
                } else if (enqueueTimeoutMs < 0) {
                    queue.put(data);
                    added = true;
                } else {
                    added = queue.offer(data, enqueueTimeoutMs, TimeUnit.MILLISECONDS);
                }
            } catch (InterruptedException e) {
                throw new AsyncProducerInterruptedException(e.getMessage());
            }
        }

        if (!added) {
            throw new QueueFullException("Event queue is full of unsent messages, could not send event: " + event);
        }
    }

    @Override
    public void close() throws IOException {
        if (this.callbackHandler != null) {
            callbackHandler.close();
        }
        closed.set(true);
        sendThread.shutdown();
        sendThread.awaitShutdown();
        producer.close();
        logger.info("Closed AsyncProducer");
    }
}
