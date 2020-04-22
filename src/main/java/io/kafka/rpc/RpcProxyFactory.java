package io.kafka.rpc;

import io.kafka.rpc.cluster.ClusterInvoker;
import io.kafka.rpc.invoker.AsyncInvoker;
import io.kafka.rpc.invoker.InvokeType;
import io.kafka.rpc.invoker.SyncInvoker;
import io.kafka.utils.Proxies;
import io.kafka.utils.Utils;

public class RpcProxyFactory {

    private final RpcProducerConfig config;

    private ClusterInvoker.Strategy strategy = ClusterInvoker.Strategy.getDefault();

    private InvokeType invokeType = InvokeType.getDefault();

    public RpcProxyFactory(RpcProducerConfig config) {
        this.config = config;
    }

    public <T> T proxyRemote(Class<T> serviceClass, String bean) {

        RemotingClient remotingClient = new RemotingClient(config);
        // 软负载
        if(config.getSerializerClass() != null) {
            remotingClient.setLoadBalancer(Utils.getObject(config.getSerializerClass()));
        }
        // 集群策略
        if(config.getClusterInvokerStrategy() == null) {
            strategy = ClusterInvoker.Strategy.parse(config.getClusterInvokerStrategy());
        }
        // 调用方式
        if(config.getInvokeType() != null) {
            invokeType = InvokeType.parse(config.getInvokeType());
        }

        Object handler;
        switch (invokeType) {
            case SYNC:
                handler = new SyncInvoker(remotingClient,strategy,bean);
                break;
            case ASYNC://未实现
                handler = new AsyncInvoker(remotingClient,strategy);
                break;
            default:
                throw new UnsupportedOperationException("invokeType: " + invokeType);
        }

        return Proxies.getDefault().newProxy(serviceClass, handler);
    }
}
