package io.kafka.producer.config;

import io.kafka.cluster.Broker;
import io.kafka.cluster.Partition;
import io.kafka.producer.BrokerPartitionInfo;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;


/**
 * @author tf
 * @version 创建时间：2019年1月19日 下午2:56:05
 * @ClassName 使用给定的代理列表发送消息
 */
public class ConfigBrokerPartitionInfo implements BrokerPartitionInfo {

	private ProducerConfig producerConfig;
	
	private final SortedSet<Partition> brokerPartitions = new TreeSet<Partition>();
	
	private final Map<Integer, Broker> allBrokers = new HashMap<Integer, Broker>();

	public ConfigBrokerPartitionInfo(ProducerConfig producerConfig) {
        this.producerConfig = producerConfig;
        try {
            init();
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("非法的broker.list", e);
        }
    }
	private void init() {
		String[] brokerInfoList = producerConfig.getBrokerList().split(",");
        for (String bInfo : brokerInfoList) {
            final String[] idHostPort = bInfo.split(":");
            final int id = Integer.parseInt(idHostPort[0]);
            final String host = idHostPort[1];
            final int port = Integer.parseInt(idHostPort[2]);
            final int partitions = idHostPort.length > 3 ? Integer.parseInt(idHostPort[3]) : 1;
            final boolean autocreated = idHostPort.length>4?Boolean.valueOf(idHostPort[4]):true;
            allBrokers.put(id, new Broker(id, host, host, port,autocreated));
            for (int i = 0; i < partitions; i++) {
                this.brokerPartitions.add(new Partition(id, i));
            }
        }
	}
	@Override
	public SortedSet<Partition> getBrokerPartitionInfo(String topic) {
		return brokerPartitions;
	}

	@Override
	public Broker getBrokerInfo(int brokerId) {
		return allBrokers.get(brokerId);
	}

	@Override
	public Map<Integer, Broker> getAllBrokerInfo() {
		return allBrokers;
	}

	@Override
	public void close() {
		brokerPartitions.clear();
        allBrokers.clear();
	}

}
