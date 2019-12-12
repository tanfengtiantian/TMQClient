package io.kafka.consumer;

import static java.lang.String.format;
import io.kafka.api.FetchRequest;
import io.kafka.api.MultiFetchResponse;
import io.kafka.cluster.Broker;
import io.kafka.cluster.Partition;
import io.kafka.common.ErrorMapping;
import io.kafka.message.ByteBufferMessageSet;
import io.kafka.utils.Closer;
import io.kafka.utils.zookeeper.ZkGroupTopicDirs;
import io.kafka.utils.zookeeper.ZkUtils;

import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.zkclient.ZkClient;

/**
 * @author tf
 * @version 创建时间：2019年2月12日 上午9:49:19
 * @ClassName FetcherRunnable
 */
public class FetcherRunnable extends Thread {

    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    private final SimpleConsumer simpleConsumer;

    private volatile boolean stopped = false;

    private final ConsumerConfig config;

    private final Broker broker;

    private final ZkClient zkClient;

    private final List<PartitionTopicInfo> partitionTopicInfos;

    private final Logger logger = LoggerFactory.getLogger(FetcherRunnable.class);

    private final static AtomicInteger threadIndex = new AtomicInteger(0);

    public FetcherRunnable(String name,//
                           ZkClient zkClient,//
                           ConsumerConfig config,//
                           Broker broker,//
                           List<PartitionTopicInfo> partitionTopicInfos) {
        super(name + "-" + threadIndex.getAndIncrement());
        this.zkClient = zkClient;
        this.config = config;
        this.broker = broker;
        this.partitionTopicInfos = partitionTopicInfos;
        this.simpleConsumer = new SimpleConsumer(broker.host, broker.port, config.getSocketTimeoutMs(),
                config.getSocketBufferSize());
    }

    public void shutdown() throws InterruptedException {
        logger.info("shutdown the fetcher " + getName());
        stopped = true;
        interrupt();
        shutdownLatch.await(5, TimeUnit.SECONDS);
    }

    @Override
    public void run() {
        StringBuilder buf = new StringBuilder("[");
        for (PartitionTopicInfo pti : partitionTopicInfos) {
            buf.append(format("%s-%d-%d,", pti.topic, pti.partition.brokerId, pti.partition.partId));
        }
        buf.append(']');
        logger.info(String.format("%s comsume  %s:%d ------ %s", getName(), broker.host, broker.port, buf.toString()));
        //
        try {
            final long maxFetchBackoffMs = config.getMaxFetchBackoffMs();
            long fetchBackoffMs = config.getFetchBackoffMs();
            while (!stopped) {
                if (fetchOnce() == 0) {//read empty bytes
                    if (logger.isDebugEnabled()) {
                        logger.debug("backing off " + fetchBackoffMs + " ms");
                    }
                    Thread.sleep(fetchBackoffMs);
                    if (fetchBackoffMs < maxFetchBackoffMs) {
                        fetchBackoffMs += fetchBackoffMs / 10;
                    }
                } else {
                    fetchBackoffMs = config.getFetchBackoffMs();
                }
            }
        } catch (ClosedByInterruptException cbie) {
            logger.info("FetcherRunnable " + this + " interrupted");
        } catch (Exception e) {
            if (stopped) {
                logger.info("FetcherRunnable " + this + " interrupted");
            } else {
                logger.error("error in FetcherRunnable ", e);
            }
        }
        //
        logger.debug("stopping fetcher " + getName() + " to broker " + broker);
        Closer.closeQuietly(simpleConsumer);
        shutdownComplete();
    }

    private long fetchOnce() throws IOException, InterruptedException {
        List<FetchRequest> fetches = new ArrayList<FetchRequest>();
        for (PartitionTopicInfo info : partitionTopicInfos) {
            fetches.add(new FetchRequest(info.topic, info.partition.partId, info.getFetchedOffset(), config
                    .getFetchSize()));
        }
        MultiFetchResponse response = simpleConsumer.multifetch(fetches);
        int index = 0;
        long read = 0L;
        for (ByteBufferMessageSet messages : response) {
            PartitionTopicInfo info = partitionTopicInfos.get(index);
            //
            try {
                read += processMessages(messages, info);
            } catch (IOException e) {
                throw e;
            } catch (InterruptedException e) {
                if (!stopped) {
                    logger.error("error in FetcherRunnable for " + info, e);
                    info.enqueueError(e, info.getFetchedOffset());
                }
                throw e;
            } catch (RuntimeException e) {
                if (!stopped) {
                    logger.error("error in FetcherRunnable for " + info, e);
                    info.enqueueError(e, info.getFetchedOffset());
                }
                throw e;
            }

            //
            index++;
        }
        return read;
    }

    private long processMessages(ByteBufferMessageSet messages, PartitionTopicInfo info) throws IOException,
            InterruptedException {
        boolean done = false;
        if (messages.getErrorCode() == ErrorMapping.OffsetOutOfRangeCode) {
            logger.warn("" + info + " 偏移超出范围.");
        }
        if (!done) {
            return info.enqueue(messages, info.getFetchedOffset());
        }
        return 0;
    }

    private void shutdownComplete() {
        this.shutdownLatch.countDown();
    }
}