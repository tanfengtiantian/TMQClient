package io.kafka.rpc.cluster;

import io.kafka.rpc.RemotingClient;
import java.util.HashMap;
import java.util.Map;

public abstract class ClusterStrategyBridging {

    private final ClusterInvoker defaultClusterInvoker;

    private final Map<String, ClusterInvoker> methodSpecialClusterInvokerMapping;


    public ClusterStrategyBridging(RemotingClient client, ClusterInvoker.Strategy strategy) {
        this.defaultClusterInvoker = createClusterInvoker(client, strategy);
        this.methodSpecialClusterInvokerMapping = new HashMap<>();

    }

    public ClusterInvoker getClusterInvoker(String methodName) {
        ClusterInvoker invoker = methodSpecialClusterInvokerMapping.get(methodName);
        return invoker != null ? invoker : defaultClusterInvoker;
    }

    private ClusterInvoker createClusterInvoker(RemotingClient client,ClusterInvoker.Strategy strategy) {
        switch (strategy) {
            case FAIL_FAST:
                //快速失败, 只发起一次调用, 失败立即报错
                return new FailFastClusterInvoker(client);
            //case FAIL_OVER:
            //case FAIL_SAFE:
            default:
                throw new UnsupportedOperationException("strategy: " + strategy);
        }
    }
}
