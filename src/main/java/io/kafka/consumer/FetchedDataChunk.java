package io.kafka.consumer;

import io.kafka.message.ByteBufferMessageSet;


/**
 * @author tf
 * @version 创建时间：2019年1月31日 下午2:38:51
 * @ClassName FetchedDataChunk
 */
public class FetchedDataChunk {

    public final ByteBufferMessageSet messages;

    public final PartitionTopicInfo topicInfo;

    public final long fetchOffset;

    public FetchedDataChunk(ByteBufferMessageSet messages, PartitionTopicInfo topicInfo, long fetchOffset) {
        super();
        this.messages = messages;
        this.topicInfo = topicInfo;
        this.fetchOffset = fetchOffset;
    }

}
