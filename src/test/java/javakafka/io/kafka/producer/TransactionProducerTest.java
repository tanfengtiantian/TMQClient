package javakafka.io.kafka.producer;


import io.kafka.producer.Producer;
import io.kafka.producer.SendResult;
import io.kafka.producer.config.ProducerConfig;
import io.kafka.producer.data.StringProducerData;
import io.kafka.producer.serializer.imp.StringEncoder;

import java.io.IOException;
import java.util.Properties;

public class TransactionProducerTest {

	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		//                          brokerId:host:port[:partitions(分区)[:autocreatetopic(自动创建topic)]]
	    //props.put("broker.list", "0:192.168.3.191:9091:2,1:192.168.3.192:9091:2,2:192.168.3.181:9091:2");
		props.put("broker.list", "5:127.0.0.1:9092");
	    props.put("serializer.class", StringEncoder.class.getName());
	    //
	    ProducerConfig config = new ProducerConfig(props);
	    Producer<String, String> producer = new Producer<String, String>(config);
		for (int t = 0; t < 1; t++) {
			try {
				producer.beginTransaction();
				long start = System.currentTimeMillis();
				for (int i = 0; i < 10; i++) {
					StringProducerData data = new StringProducerData("tf");
					data.add("#tanfeng----------"+i);
					SendResult result = producer.send(data);
					System.err.println("brokerId="+result.getPartition().brokerId+" topic="+result.getTopic()
							+" partId="+result.getPartition().partId +" offset="+result.getOffset());
					if(i == 9){
						//throw new Exception("test");
					}
				}
				producer.commit();
				long cost = System.currentTimeMillis() - start;
				System.out.println("send------------------------------------------ cost: "+cost+" ms");
			} catch (Exception e) {
				producer.rollback();
				e.printStackTrace();
			} finally {
				System.out.println("send begin over ");
				//producer.close();
				System.out.println("send end over");
			}
		}

	}
}
