package io.kafka.producer.config;

import io.kafka.common.CompressionCodec;
import io.kafka.producer.serializer.imp.StringEncoder;
import io.kafka.utils.Utils;

import java.util.Map;
import java.util.Properties;

/**
 * @author tf
 * @version 创建时间：2019年1月19日 上午10:34:22
 * @ClassName Producer配置类
 */
public class ProducerConfig {

	protected final Properties props;
	
	public ProducerConfig(Properties props) {
		this.props = props;
	}

	/**
     * broker Format-
     * <pre>
     *      brokerid1:host1:port1[:partitions[:autocreatetopic]], brokerid2:host2:port2[:partitions[:autocreatetopic]]
     * </pre>
     * @return broker list
     */
    public String getBrokerList() {
        return Utils.getString(props, "broker.list", null);
    }

    public CompressionCodec getCompressionCodec() {
        return CompressionCodec.valueOf(Utils.getInt(props, "compression.codec", 0));
    }

    public Properties getProperties() {
        return props;
    }

    public String getSerializerClass() {
        return Utils.getString(props, "serializer.class", StringEncoder.class.getName());
    }

    public int getNumRetries() {
        return Utils.getInt(props, "num.retries", 0);
    }
}
