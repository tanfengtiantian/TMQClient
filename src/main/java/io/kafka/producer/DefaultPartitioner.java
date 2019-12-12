package io.kafka.producer;

import java.util.Random;

/**
 * @author tf
 * @version 创建时间：2019年1月28日 上午11:11:11
 * @ClassName 获取top下分区
 */
public class DefaultPartitioner<T> implements IPartitioner<T> {

	private final Random random = new Random();

    public int partition(T key, int numPartitions) {
        if (key == null) {
            return random.nextInt(numPartitions);
        }
        return Math.abs(key.hashCode()) % numPartitions;
    }

}
