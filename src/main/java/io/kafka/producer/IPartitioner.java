package io.kafka.producer;
/**
 * @author tf
 * @version 创建时间：2019年1月19日 上午10:36:54
 * @ClassName 向特殊代理（服务器）发送的某些消息
 */
public interface IPartitioner<T> {

	 /**
     * 使用键计算分区存储桶ID，以便将数据路由到适当的
     * broker partition
     * @param key 分区名
     * @param numPartitions 分区数
     * @return 范围 between 0 and numPartitions 分区数
     */
    int partition(T key, int numPartitions);
}
