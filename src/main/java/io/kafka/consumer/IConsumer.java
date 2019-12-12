package io.kafka.consumer;

import io.kafka.api.FetchRequest;
import io.kafka.message.ByteBufferMessageSet;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author tf
 * @version 创建时间：2019年1月25日 上午9:22:02
 * @ClassName 消费端
 */
public interface IConsumer extends Closeable {

	/**
     * FetchRequest broker
     * 
     * @param request 
     * @return bytebuffer 
     * @throws IOException 
     */
    ByteBufferMessageSet fetch(FetchRequest request) throws IOException;
}
