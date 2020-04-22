package io.kafka.rpc.invoker;

import io.kafka.rpc.RemotingClient;
import io.kafka.rpc.cluster.ClusterInvoker;
import io.kafka.rpc.cluster.ClusterStrategyBridging;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

public class AsyncInvoker extends ClusterStrategyBridging implements InvocationHandler {

    public AsyncInvoker(RemotingClient client, ClusterInvoker.Strategy strategy) {
        super(client, strategy);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        return null;
    }
}
