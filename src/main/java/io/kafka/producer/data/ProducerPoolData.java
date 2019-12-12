package io.kafka.producer.data;

import io.kafka.cluster.Partition;

import java.util.List;

/**
 * @author tf
 * @version 创建时间：2019年1月1日 上午12:44:01
 * @ClassName message topic and partition
 */
public class ProducerPoolData<V> {

	public final String topic;

    public final Partition partition;

    public final long ttl;

    public final List<V> data;

    public ProducerPoolData(String topic, Partition partition, long ttl, List<V> data) {
        super();
        this.topic = topic;
        this.partition = partition;
        this.ttl=ttl;
        this.data = data;
    }

    @Override
    public String toString() {
        return "ProducerPoolData [topic=" + topic + ", partition=" + partition + ", data size=" + (data != null ? data.size() : -1) + "]";
    }
}
