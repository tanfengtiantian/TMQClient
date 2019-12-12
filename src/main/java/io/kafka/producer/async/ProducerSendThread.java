package io.kafka.producer.async;

import io.kafka.common.exception.IllegalQueueStateException;
import io.kafka.producer.SyncProducer;
import io.kafka.producer.serializer.Encoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public class ProducerSendThread<T> extends Thread {

    final String threadName;

    final BlockingQueue<QueueItem<T>> queue;

    final Encoder<T> serializer;

    final SyncProducer underlyingProducer;

    final EventHandler<T> eventHandler;

    final CallbackHandler<T> callbackHandler;

    final long queueTime;

    final int batchSize;

    private final static Logger logger = LoggerFactory.getLogger(ProducerSendThread.class);

    /////////////////////////////////////////////////////////////////////
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    private volatile boolean shutdown = false;

    public ProducerSendThread(String threadName, //
                              BlockingQueue<QueueItem<T>> queue, //
                              Encoder<T> serializer, //
                              SyncProducer underlyingProducer,//
                              EventHandler<T> eventHandler, //
                              CallbackHandler<T> callbackHandler, //
                              long queueTime, //
                              int batchSize) {
        super();
        this.threadName = threadName;
        this.queue = queue;
        this.serializer = serializer;
        this.underlyingProducer = underlyingProducer;
        this.eventHandler = eventHandler;
        this.callbackHandler = callbackHandler;
        this.queueTime = queueTime;
        this.batchSize = batchSize;
    }

    @Override
    public void run() {
        try {
            //关闭后，未batchSize数量的任务
            List<QueueItem<T>> remainingEvents = processEvents();
            //handle remaining events
            if (remainingEvents.size() > 0) {
                logger.debug(format("Dispatching last batch of %d events to the event handler", remainingEvents.size()));
                tryToHandle(remainingEvents);
            }
        } catch (Exception e) {
            logger.error("Error in sending events: ", e);
        } finally {
            shutdownLatch.countDown();
        }
    }

    public void awaitShutdown() {
        try {
            shutdownLatch.await();
        } catch (InterruptedException e) {
            logger.warn(e.getMessage());
        }
    }

    public void shutdown() {
        shutdown = true;
        eventHandler.close();
        logger.info("Shutdown thread complete");
    }

    private List<QueueItem<T>> processEvents() {
        long lastSend = System.currentTimeMillis();
        final List<QueueItem<T>> events = new ArrayList<>();
        boolean full = false;
        while (!shutdown) {
            try {
                QueueItem<T> item = queue.poll(Math.max(0, (lastSend + queueTime) - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
                long elapsed = System.currentTimeMillis() - lastSend;
                boolean expired = item == null;
                if (item != null) {
                    events.add(item);
                    //
                    full = events.size() >= batchSize;
                }
                if (full || expired) {
                    if (logger.isDebugEnabled()) {
                        if (expired) {
                            logger.debug(elapsed + " ms elapsed. Queue time reached. Sending..");
                        } else {
                            logger.debug(format("Batch(%d) full. Sending..", batchSize));
                        }
                    }
                    tryToHandle(events);
                    lastSend = System.currentTimeMillis();
                    events.clear();
                }
            } catch (InterruptedException e) {
                logger.warn(e.getMessage(), e);
            }
        }
        if (queue.size() > 0) {
            throw new IllegalQueueStateException("Invalid queue state! After queue shutdown, " + queue.size() + " remaining items in the queue");
        }
        return events;
    }

    private void tryToHandle(List<QueueItem<T>> events) {
        if (logger.isDebugEnabled()) {
            logger.debug("handling " + events.size() + " events");
        }
        if (events.size() > 0) {
            try {
                this.eventHandler.handle(events, underlyingProducer, serializer);
            } catch (RuntimeException e) {
                logger.error("Error in handling batch of " + events.size() + " events", e);
            }
        }
    }
}
