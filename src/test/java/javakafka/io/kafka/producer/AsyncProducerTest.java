package javakafka.io.kafka.producer;

import io.kafka.producer.Producer;
import io.kafka.producer.SendResult;
import io.kafka.producer.async.CallbackHandler;
import io.kafka.producer.async.QueueItem;
import io.kafka.producer.config.ProducerConfig;
import io.kafka.producer.data.StringProducerData;
import io.kafka.producer.serializer.imp.StringEncoder;

import java.util.List;
import java.util.Properties;

public class AsyncProducerTest {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        //                          brokerId:host:port[:partitions(分区)[:autocreatetopic(自动创建topic)]]
        //props.put("broker.list", "0:192.168.3.191:9091:2,1:192.168.3.192:9091:2,2:192.168.3.181:9091:2");
        props.put("broker.list", "1:127.0.0.1:9092");
        props.put("serializer.class", StringEncoder.class.getName());
        //
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<>(config, null, new CallbackHandler<String>() {
            @Override
            public List<QueueItem<String>> beforeSendingData(List<QueueItem<String>> data) {
                return data;
            }

            @Override
            public void close() {

            }
        });
        for (int i = 0; i < 202; i++) {
            StringProducerData data = new StringProducerData("test");
            data.add("tanfeng-------------");
            SendResult result = producer.send(data);
        }
        producer.close();


        //public Producer(ProducerConfig config,EventHandler<V> eventHandler, CallbackHandler<V> cbkHandler) {
    }
}
