package io.kafka.producer.data;

import java.util.ArrayList;
import java.util.List;

/**
 * @author tf
 * @version 创建时间：2019年1月19日 下午4:59:58
 * @ClassName string data
 */
public class StringProducerData extends ProducerData<String, String> {

	public StringProducerData(String topic, String key, List<String> data) {
        super(topic, key, data);
    }

    public StringProducerData(String topic, List<String> data,long ttl) {
        super(topic, data, ttl);
    }

    public StringProducerData(String topic, String data) {
        super(topic, data);
    }

    public StringProducerData(String topic) {
        this(topic, new ArrayList<String>(),-1);
    }

    /**
     *
     * @param topic
     * @param seconds ttl延迟消息
     */
    public StringProducerData(String topic,long seconds) {
        this(topic, new ArrayList<String>(),seconds);
    }

    public StringProducerData add(String message) {
        getData().add(message);
        return this;
    }

}
