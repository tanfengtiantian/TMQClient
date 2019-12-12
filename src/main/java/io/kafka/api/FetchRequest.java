package io.kafka.api;

import java.nio.ByteBuffer;
import io.kafka.network.request.Request;
import io.kafka.utils.Utils;

/**
 * @author tf
 * @version 创建时间：2019年1月24日 上午9:49:51
 * @ClassName FetchRequest
 * fetch request
 * <p>
 * fetch format:
 * <pre>
 * size + type + Len(topic) + topic + partition + offset + maxSize
 * =====================================
 * size		  : size(4bytes)
 * type		  : type(2bytes)
 * Len(topic) : Len(2bytes)
 * topic	  : data(utf-8 bytes)
 * partition  : int(4bytes)
 * offset	  : long(8bytes)
 * maxSize    : int(4bytes)
 */
public class FetchRequest implements Request {

	/**
     * message topic
     */
    public final String topic;

    /**
     * partition
     */
    public final int partition;

    /**
     * offset topic(log file)
     */
    public final long offset;

    /**
     * 此请求的最大数据大小(bytes) 
     */
    public final int maxSize;

    public FetchRequest(String topic, int partition, long offset) {
        this(topic, partition, offset, 1024 * 1024);//64KB
    }
    
	public FetchRequest(String topic, int partition, long offset, int maxSize) {
        this.topic = topic;
        if (topic == null) {
            throw new IllegalArgumentException("no topic");
        }
        this.partition = partition;
        this.offset = offset;
        this.maxSize = maxSize;
    }

	@Override
	public int getSizeInBytes() {
		 return Utils.caculateShortString(topic)/** len(top)+top **/
				 + 4 /** partition **/
				 + 8 /** offset **/
				 + 4;/** maxSize **/
	}

	@Override
	public RequestKeys getRequestKey() {
		return RequestKeys.FETCH;
	}

	 /**
     * 从缓冲区读取获取请求
     * 
     * @param buffer  data
     * @return FetchRequest
     */
    public static FetchRequest readFrom(ByteBuffer buffer) {
        String topic = Utils.readShortString(buffer);
        int partition = buffer.getInt();
        long offset = buffer.getLong();
        int size = buffer.getInt();
        return new FetchRequest(topic, partition, offset, size);
    }
    
	@Override
	public void writeTo(ByteBuffer buffer) {
		//Len(topic) + topic
		Utils.writeShortString(buffer, topic);
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(maxSize);
	}
	
	@Override
    public String toString() {
        return "FetchRequest(topic:" + topic + ", part:" + partition + " offset:" + offset + " maxSize:" + maxSize + ")";
    }

}
