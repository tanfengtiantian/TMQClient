package io.kafka.producer.async;

import io.kafka.producer.config.SyncProducerConfig;
import static io.kafka.utils.Utils.getInt;
import static io.kafka.utils.Utils.getString;
import static io.kafka.utils.Utils.getProps;
import java.util.Properties;

public class AsyncProducerConfig extends SyncProducerConfig {

    public AsyncProducerConfig(Properties props) {
        super(props);
    }

    public int getQueueTime() {
        return getInt(props, "queue.time", 5000);
    }

    public int getQueueSize() {
        return getInt(props, "queue.size", 10000);
    }

    public int getEnqueueTimeoutMs() {
        return getInt(props, "queue.enqueueTimeout.ms", 0);
    }

    public int getBatchSize() {
        return getInt(props, "batch.size", 200);
    }


    public String getCbkHandler() {
        return getString(props, "callback.handler", null);
    }

    public Properties getCbkHandlerProperties() {
        return getProps(props, "callback.handler.props", null);
    }

    public String getEventHandler() {
        return getString(props, "event.handler", null);
    }

    public Properties getEventHandlerProperties() {
        return getProps(props, "event.handler.props", null);
    }
}
