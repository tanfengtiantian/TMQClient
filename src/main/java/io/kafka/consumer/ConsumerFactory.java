package io.kafka.consumer;


/**
 * @author tf
 * @version 创建时间：2019年1月31日 下午2:30:45
 * @ClassName ConsumerFactory
 */
public class ConsumerFactory {

	/**
     * create  ConsumerConnector
     * 
     * @param ConsumerConnector
     * 
     * <pre>
     *  必填项
     *  groupid:  consumer group name
     *  zk.connect: zookeeper connection string
     * </pre>
     * @return  zookeeper consumer connector
     */
    public static ConsumerConnector create(ConsumerConfig config) {
        ConsumerConnector consumerConnector = new ZookeeperConsumerConnector(config);
        return consumerConnector;
    }
}
