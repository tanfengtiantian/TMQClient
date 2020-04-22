package io.kafka.rpc.invoker;

import io.kafka.api.RpcRequest;
import io.kafka.rpc.RemotingClient;
import io.kafka.rpc.cluster.ClusterInvoker;
import io.kafka.rpc.cluster.ClusterStrategyBridging;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

public class SyncInvoker extends ClusterStrategyBridging implements InvocationHandler {

    private final String bean;

    public SyncInvoker(RemotingClient client, ClusterInvoker.Strategy strategy,String bean){
        super(client,strategy);
        this.bean = bean;
    }

    public Object invoke(Object proxy, Method method, Object[] args){
        RpcRequest request = new RpcRequest(bean, method.getName(), args);
        String methodName = request.getClassName()+"."+request.getMethodName();
        ClusterInvoker invoker = getClusterInvoker(methodName);
        return invoker.invoke(request);
    }
}
