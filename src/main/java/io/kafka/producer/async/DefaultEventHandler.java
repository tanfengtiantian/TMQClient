package io.kafka.producer.async;

import io.kafka.api.ProducerRequest;
import io.kafka.common.CompressionCodec;
import io.kafka.message.ByteBufferMessageSet;
import io.kafka.message.Message;
import io.kafka.producer.SyncProducer;
import io.kafka.producer.config.ProducerConfig;
import io.kafka.producer.serializer.Encoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class DefaultEventHandler<T> implements EventHandler<T> {

    private final CallbackHandler<T> callbackHandler;

    private final Set<String> compressedTopics;

    private final CompressionCodec codec;

    private final static Logger logger = LoggerFactory.getLogger(DefaultEventHandler.class);

    private final int numRetries;

    public DefaultEventHandler(ProducerConfig producerConfig, CallbackHandler<T> callbackHandler) {
        this.callbackHandler = callbackHandler;
        this.compressedTopics = new HashSet<>();
        this.codec = producerConfig.getCompressionCodec();
        this.numRetries = producerConfig.getNumRetries();
    }

    @Override
    public void init(Properties properties) {

    }

    @Override
    public void handle(List<QueueItem<T>> events, SyncProducer producer, Encoder<T> encoder) {
        List<QueueItem<T>> processedEvents = events;
        if (this.callbackHandler != null) {
            processedEvents = this.callbackHandler.beforeSendingData(events);
        }
        send(collate(processedEvents, encoder), producer);
    }

    private void send(List<ProducerRequest> produces, SyncProducer syncProducer) {
        if (produces.isEmpty()) {
            return;
        }
        final int maxAttempts = 1 + numRetries;
        for (int i = 0; i < maxAttempts; i++) {
            try {
                syncProducer.multiSend(produces);
                break;
            } catch (RuntimeException e) {
                logger.warn("error sending message, attempts times: " + i, e);
                if (i == maxAttempts - 1) {
                    throw e;
                }
            }
        }
    }

    private List<ProducerRequest> collate(List<QueueItem<T>> events, Encoder<T> encoder) {
        if(events == null || events.isEmpty()){
            return Collections.emptyList();
        }
        final Map<String, Map<Integer, List<Message>>> topicPartitionData = new HashMap<String, Map<Integer, List<Message>>>();
        for (QueueItem<T> event : events) {
            Map<Integer, List<Message>> partitionData = topicPartitionData.get(event.topic);
            if (partitionData == null) {
                partitionData = new HashMap<>();
                topicPartitionData.put(event.topic, partitionData);
            }
            List<Message> data = partitionData.get(event.partition);
            if (data == null) {
                data = new ArrayList<Message>();
                partitionData.put(event.partition, data);
            }
            data.add(encoder.toMessage(event.data));
        }
        //
        final List<ProducerRequest> requests = new ArrayList<ProducerRequest>();
        for (Map.Entry<String, Map<Integer, List<Message>>> e : topicPartitionData.entrySet()) {
            final String topic = e.getKey();
            for (Map.Entry<Integer, List<Message>> pd : e.getValue().entrySet()) {
                final Integer partition = pd.getKey();
                requests.add(new ProducerRequest(topic, partition, convert(topic, pd.getValue())));
            }
        }
        return requests;
    }

    private ByteBufferMessageSet convert(String topic, List<Message> messages) {
        //compress condition:
        if (codec != CompressionCodec.NoCompressionCodec//
                && (compressedTopics.isEmpty() || compressedTopics.contains(topic))) {
            return new ByteBufferMessageSet(codec, messages.toArray(new Message[messages.size()]));
        }
        return new ByteBufferMessageSet(CompressionCodec.NoCompressionCodec, messages.toArray(new Message[messages
                .size()]));
    }

    @Override
    public void close() {

    }
}
