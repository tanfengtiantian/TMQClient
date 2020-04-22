package io.kafka.rpc;

import io.kafka.cluster.Broker;
import io.kafka.rpc.invoker.InvokeType;
import io.kafka.utils.Utils;
import java.util.*;

public class RpcProducerConfig {

    public int bufferSize;
    public int socketTimeoutMs;
    public int maxMessageSize;
    protected final Properties props;
    private int connectTimeoutMs;
    private int reconnectInterval;
    private int reconnectTimeInterval;
    private final Map<Integer, Broker> allBrokers = new HashMap<Integer, Broker>();


    public RpcProducerConfig(Properties props) {
        this.props = props;
        try {
            init();
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("非法的rpc.list", e);
        }
        this.bufferSize = Utils.getInt(props, "buffer.size", 100 * 1024);
        this.connectTimeoutMs = Utils.getInt(props, "connect.timeout.ms", 5000);
        this.socketTimeoutMs = Utils.getInt(props, "socket.timeout.ms", 30000);
        this.reconnectInterval = Utils.getInt(props, "reconnect.interval", 100000);
        this.reconnectTimeInterval = Utils.getInt(props, "reconnect.time.interval.ms", 1000 * 1000 * 10);
        this.maxMessageSize = Utils.getInt(props, "max.message.size", 1024 * 1024);//1MB
    }

    private void init() {
        String[] brokerInfoList = Utils.getString(props, "rpc.list", null).split(",");
        for (String bInfo : brokerInfoList) {
            final String[] idHostPort = bInfo.split(":");
            final int id = Integer.parseInt(idHostPort[0]);
            final String host = idHostPort[1];
            final int port = Integer.parseInt(idHostPort[2]);
            allBrokers.put(id, new Broker(id, host, host, port,false));
        }
    }

    public Properties getProperties() {
        return props;
    }

    public Map<Integer, Broker> getBrokers() {
        return allBrokers;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public int getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    public int getSocketTimeoutMs() {
        return socketTimeoutMs;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public String getSerializerClass() {
        return Utils.getString(props, "loadBalancer.class",null);
    }

    public String getClusterInvokerStrategy() {
        return Utils.getString(props, "clusterInvoker.Strategy",null);
    }

    public String getInvokeType() {
        return Utils.getString(props, "invokeType",null);
    }
}
