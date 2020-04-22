package io.kafka.rpc;

import io.kafka.api.RpcRequest;
import io.kafka.cluster.Broker;
import io.kafka.producer.Callback;
import io.kafka.rpc.balancer.LoadBalancer;
import io.kafka.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class RemotingClient {

    private static Logger logger = LoggerFactory.getLogger(RemotingClient.class);

    private final RpcProducerConfig config;

    private LoadBalancer loadBalancer;

    private CopyOnWriteArrayList<Channel> channelsList = new CopyOnWriteArrayList<>();

    public RemotingClient(RpcProducerConfig config) {
        this.config = config;
        this.loadBalancer = getLoadBalancer();
        initChannels(config.getBrokers());

    }

    private void initChannels(Map<Integer, Broker> map) {
        for (Map.Entry<Integer, Broker> entry : map.entrySet()) {
            Channel channel = new Channel(entry.getValue(),config);
            channelsList.add(channel);
        }
    }

    public Channel selectChannel() {
        // 通过软负载均衡选择一个channel
        Channel channel = loadBalancer.select(channelsList.toArray(new Channel[channelsList.size()]));
        return channel;
    }

    public void removeChannel(Channel c) {
        channelsList.remove(c);
    }

    public RpcResponse invoke(Channel channel,RpcRequest request, Callback back) {
        //调用失败 启用集群策略
        return channel.invoke(request, (c, o) ->
                errorCallBack(c,(RpcRequest)o,back)
        );
    }

    private void errorCallBack(Channel channel, RpcRequest request, Callback back) {
        if(back != null) {
            back.call(channel,request);
        }
    }

    public void setLoadBalancer(LoadBalancer loadBalancer){
        this.loadBalancer = loadBalancer;
    }

    public LoadBalancer getLoadBalancer() {
        return loadBalancer == null ? (LoadBalancer) Utils.getObject(config.getSerializerClass()) : loadBalancer;
    }
}
