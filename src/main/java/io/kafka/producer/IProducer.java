package io.kafka.producer;

import io.kafka.common.exception.InvalidPartitionException;
import io.kafka.common.exception.NoBrokersForPartitionException;
import io.kafka.producer.data.ProducerData;
import io.kafka.producer.serializer.Encoder;

import java.io.Closeable;

/**
 * @author tf
 * @version 创建时间：2019年1月19日 上午10:35:36
 * @ClassName Producer interface
 */
public interface IProducer <K, V> extends Closeable {

	/**
     * Send messages
     * 
     * @param data message data
     * @throws NoBrokersForPartitionException  broker 没有 topic
     * @throws InvalidPartitionException partition 范围越界
     */
	SendResult send(ProducerData<K, V> data) throws Exception;

    /**
     * get message encoder
     * 
     */
    Encoder<V> getEncoder();

    /**
     * get partition(分区)
     * 
     * @return partition
     */
    IPartitioner<K> getPartitioner();
}
