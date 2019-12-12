package io.kafka.api;

import io.kafka.cluster.Partition;
import io.kafka.common.ErrorMapping;
import io.kafka.message.ByteBufferMessageSet;
import io.kafka.network.request.Request;
import io.kafka.producer.SendResult;
import io.kafka.utils.Utils;
import java.nio.ByteBuffer;


/**
 * @author tf
 * @version 创建时间：2019年1月18日 下午2:23:29
 * @ClassName 服务提供请求类
 * message producer request
 * <p>
 * Producer format:
 * <pre>
 * size + type + Len(topic) + topic + partition + ttl + messageSize + message
 * =====================================
 * size		  : size(4bytes)
 * type		  : type(2bytes)
 * Len(topic) : Len(2bytes)
 * topic	  : size(2bytes) + data(utf-8 bytes)
 * partition  : int(4bytes)
 * ttl        : long(8bytes)
 * messageSize: int(4bytes)
 * message: bytes
 */
public class ProducerTTLRequest implements Request {

	/**
	 * 分区
	 */
	public static final int RandomPartition = -1;

    /**
     * request messages
     */
    public final ByteBufferMessageSet messages;

    /**
     * topic partition
     */
    public int partition;

    /**
     * topic name
     */
    public final String topic;
    /**
     * topic offset
     */
    public long offset;
    /**
     * topic brokerId
     */
    public int brokerId;

    /**
     * topic ttl
     */
    public final long ttl;

    public ProducerTTLRequest(String topic, int partition, long ttl, ByteBufferMessageSet messages) {
        this.topic = topic;
        this.partition = partition;
        this.ttl = ttl;
        this.messages = messages;
    }

    public String getTopic() {
        return topic;
    }

    public static SendResult deserializeProducer(ByteBuffer buffer, ErrorMapping errorcode) {
        String topic = Utils.readShortString(buffer);
        int brokerId = buffer.getInt();
        int partition = buffer.getInt();
        long offset =buffer.getLong();
        return new SendResult(topic
                ,new Partition(brokerId,partition)
                ,offset
                ,errorcode);
    }
	@Override
	public RequestKeys getRequestKey() {
		return RequestKeys.TTLPRODUCE;
	}

    @Override
    public void writeTo(ByteBuffer buffer) {
        Utils.writeShortString(buffer, topic);
        buffer.putInt(partition);
        buffer.putLong(ttl);
        final ByteBuffer sourceBuffer = messages.serialized();
        buffer.putInt(sourceBuffer.limit());
        buffer.put(sourceBuffer);
        sourceBuffer.rewind();
    }

    public int getSizeInBytes() {
        return  Utils.caculateShortString(topic) //Len(topic) : Len(2bytes)  + topic : bytes
                + 4 //partition : 4bytes
                + 8 //ttl : 8bytes
                + 4 //messageSize: int(4bytes)
                + (int)messages.getSizeInBytes();//messageSize:: int(4bytes)  +  message : bytes
    }
	@Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("ProducerTTLRequest(");
        buf.append(topic).append(',').append(partition).append(',');
        buf.append(messages.getSizeInBytes()).append(')');
        return buf.toString();
    }

}
