package io.kafka.producer.data;

import java.util.ArrayList;
import java.util.List;

/**
 * @author tf
 * @version 创建时间：2019年1月19日 上午11:22:34
 * @ClassName 使用生产者发送API发送的数据
 */
public class ProducerData <K, V> {

    private String topic;

    /** 分区程序用于选择代理分区的键 */
    private K key;

    private long ttl =-1;

    /** 主题下要作为消息发布的可变长度数据 */
    private List<V> data;

    public ProducerData(String topic, K key, List<V> data) {
        super();
        this.topic = topic;
        this.key = key;
        this.data = data;
    }

    public ProducerData(String topic, List<V> data, long ttl) {
        this(topic, null, data);
        this.ttl = ttl;
    }

    public ProducerData(String topic, List<V> data) {
        this(topic, null, data);
    }

    public ProducerData(String topic, V data) {
        this.topic = topic;
        getData().add(data);
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public K getKey() {
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public List<V> getData() {
        if (data == null) {
            data = new ArrayList<V>();
        }
        return data;
    }

    public long getTtl() {
        return ttl;
    }

    public void setData(List<V> data) {
        this.data = data;
    }
    
    

}