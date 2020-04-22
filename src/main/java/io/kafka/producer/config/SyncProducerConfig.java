package io.kafka.producer.config;

import io.kafka.utils.Utils;
import java.util.Properties;
/**
 * @author tf
 * @version 创建时间：2019年1月19日 下午9:42:02
 * @ClassName Sync同步生产配置
 */
public class SyncProducerConfig {

	public int bufferSize;
    public int socketTimeoutMs;
    public int maxMessageSize;
    protected final Properties props;
    private int connectTimeoutMs;
    private int reconnectInterval;
    private int reconnectTimeInterval;

    public SyncProducerConfig(Properties props) {
        this.props = props;
        this.bufferSize = Utils.getInt(props, "buffer.size", 100 * 1024);
        this.connectTimeoutMs = Utils.getInt(props, "connect.timeout.ms", 5000);
        this.socketTimeoutMs = Utils.getInt(props, "socket.timeout.ms", 30000);
        this.reconnectInterval = Utils.getInt(props, "reconnect.interval", 100000);
        this.reconnectTimeInterval = Utils.getInt(props, "reconnect.time.interval.ms", 1000 * 1000 * 10);
        this.maxMessageSize = Utils.getInt(props, "max.message.size", 1024 * 1024);//1MB
    }

    public String getHost() {
        return Utils.getString(props, "host");
    }

    public int getPort() {
        return Utils.getInt(props, "port");
    }

    public Properties getProperties() {
        return props;
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


}
