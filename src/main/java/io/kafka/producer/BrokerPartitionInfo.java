package io.kafka.producer;

import io.kafka.cluster.Broker;
import io.kafka.cluster.Partition;

import java.io.Closeable;
import java.util.Map;
import java.util.SortedSet;

/**
 * @author tf
 * @version 创建时间：2019年1月19日 下午2:06:40
 * @ClassName Broker-Partition
 */
public interface BrokerPartitionInfo extends Closeable{

	/**
	*  返回一个(brokerId, numpartition)序列。
	*
	* @param topic是返回此信息的主题
	* @return (brokerId, numpartition)序列。返回一个如果没有代理可用，则为零长度序列。
	*/
    SortedSet<Partition> getBrokerPartitionInfo(String topic);

    /**
     * 为标识的代理生成主机和端口信息给定的代理id
     * 
     * @param brokerId是返回信息的代理
     * @return 主机和brokerId端口
     */
    Broker getBrokerInfo(int brokerId);

    /**
     * 为所有代理id生成到主机和端口的映射
     * brokers
     * 
     * @return 映射从id到所有代理的主机和端口
     */
    Map<Integer, Broker> getAllBrokerInfo();

    /**
     * Cleanup
     */
    void close();

    public interface Callback {

        void producerCbk(int bid, String host, int port,boolean autocreated);
    }
}
