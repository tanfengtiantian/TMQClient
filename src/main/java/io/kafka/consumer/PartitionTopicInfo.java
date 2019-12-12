package io.kafka.consumer;

import static java.lang.String.format;
import io.kafka.cluster.Partition;
import io.kafka.common.ErrorMapping;
import io.kafka.message.ByteBufferMessageSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tf
 * @version 创建时间：2019年1月31日 下午2:39:33
 * @ClassName PartitionTopicInfo
 */
public class PartitionTopicInfo {

    private static final Logger logger = LoggerFactory.getLogger(PartitionTopicInfo.class);

    public final String topic;

    public final int brokerId;

    private final BlockingQueue<FetchedDataChunk> chunkQueue;

    private final AtomicLong consumedOffset;

    private final AtomicLong fetchedOffset;

    private final AtomicLong consumedOffsetChanged = new AtomicLong(0);

    final Partition partition;

    public PartitionTopicInfo(String topic, //
            Partition partition,//
            BlockingQueue<FetchedDataChunk> chunkQueue, //
            AtomicLong consumedOffset, //
            AtomicLong fetchedOffset) {
        super();
        this.topic = topic;
        this.partition = partition;
        this.brokerId = partition.brokerId;
        this.chunkQueue = chunkQueue;
        this.consumedOffset = consumedOffset;
        this.fetchedOffset = fetchedOffset;
    }

    public long getConsumedOffset() {
        return consumedOffset.get();
    }

    public AtomicLong getConsumedOffsetChanged() {
        return consumedOffsetChanged;
    }

    public boolean resetComsumedOffsetChanged(long lastChanged) {
        return consumedOffsetChanged.compareAndSet(lastChanged, 0);
    }

    public long getFetchedOffset() {
        return fetchedOffset.get();
    }

    public void resetConsumeOffset(long newConsumeOffset) {
        consumedOffset.set(newConsumeOffset);
        consumedOffsetChanged.incrementAndGet();
    }

    public void resetFetchOffset(long newFetchOffset) {
        fetchedOffset.set(newFetchOffset);
    }

    public long enqueue(ByteBufferMessageSet messages, long fetchOffset) throws InterruptedException {
        long size = messages.getValidBytes();
        if (size > 0) {
            final long oldOffset = fetchedOffset.get();
            chunkQueue.put(new FetchedDataChunk(messages, this, fetchOffset));
            long newOffset = fetchedOffset.addAndGet(size);
            if (logger.isDebugEnabled()) {
                logger.debug(format("updated fetchset (origin+size=newOffset) => %d + %d = %d", oldOffset, size, newOffset));
            }
        }
        return size;
    }

    @Override
    public String toString() {
        return topic + "-" + partition + ", fetched/consumed offset: " + fetchedOffset.get() + "/" + consumedOffset.get();
    }

    public void enqueueError(Exception e, long fetchOffset) throws InterruptedException {
        ByteBufferMessageSet messages = new ByteBufferMessageSet(ErrorMapping.EMPTY_BUFFER, 0);
        chunkQueue.put(new FetchedDataChunk(messages, this, fetchOffset));
    }
}