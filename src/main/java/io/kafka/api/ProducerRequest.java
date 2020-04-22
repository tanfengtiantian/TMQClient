package io.kafka.api;

import java.nio.ByteBuffer;
import io.kafka.cluster.Partition;
import io.kafka.common.ErrorMapping;
import io.kafka.common.exception.ErrorMappingException;
import io.kafka.common.exception.RpcRuntimeException;
import io.kafka.message.ByteBufferMessageSet;
import io.kafka.network.request.Request;
import io.kafka.producer.SendResult;
import io.kafka.utils.Utils;


/**
 * @author tf
 * @version 创建时间：2019年1月18日 下午2:23:29
 * @ClassName 服务提供请求类
 * message producer request
 * <p>
 * Producer format:
 * <pre>
 * size + type + Len(topic) + topic + partition + messageSize + message
 * =====================================
 * size		  : size(4bytes)
 * type		  : type(2bytes)
 * Len(topic) : Len(2bytes)
 * topic	  : size(2bytes) + data(utf-8 bytes)
 * partition  : int(4bytes)
 * messageSize: int(4bytes)
 * message: bytes
 */
public class ProducerRequest implements Request {

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

    public ProducerRequest(String topic, int partition, ByteBufferMessageSet messages) {
        this.topic = topic;
        this.partition = partition;
        this.messages = messages;
    }

    public static SendResult deserializeProducer(ByteBuffer buffer, ErrorMapping errorcode) {
        if(errorcode == ErrorMapping.NoError){
            String topic = Utils.readShortString(buffer);
            int brokerId = buffer.getInt();
            int partition = buffer.getInt();
            long offset =buffer.getLong();
            return new SendResult(topic
                    ,new Partition(brokerId,partition)
                    ,offset
                    ,errorcode);
        }else{
            throw new ErrorMappingException("ErrorMapping failure:" + errorcode.toString());
        }
    }
	
	@Override
	public int getSizeInBytes() {
		 return (int) (Utils.caculateShortString(topic) + 4 + 4 + messages.getSizeInBytes());
	}

	@Override
	public RequestKeys getRequestKey() {
		return RequestKeys.PRODUCE;
	}

	@Override
	public void writeTo(ByteBuffer buffer) {
		Utils.writeShortString(buffer, topic);
        buffer.putInt(partition);
        final ByteBuffer sourceBuffer = messages.serialized();
        buffer.putInt(sourceBuffer.limit());
        buffer.put(sourceBuffer);
        sourceBuffer.rewind();
	}
	
	@Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("ProducerRequest(");
        buf.append(topic).append(',').append(partition).append(',');
        buf.append(messages.getSizeInBytes()).append(')');
        return buf.toString();
    }

}
