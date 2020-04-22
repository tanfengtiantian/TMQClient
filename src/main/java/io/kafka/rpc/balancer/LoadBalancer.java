package io.kafka.rpc.balancer;

import io.kafka.rpc.Channel;

/**
 * 软负载策略
 */
public interface LoadBalancer {

    Channel select(Channel[] channels);
}
