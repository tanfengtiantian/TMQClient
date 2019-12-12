package javakafka.io.kafka.producer;


import io.kafka.producer.Producer;
import io.kafka.producer.SendResult;
import io.kafka.producer.config.ProducerConfig;
import io.kafka.producer.data.StringProducerData;
import io.kafka.producer.serializer.imp.StringEncoder;

import java.io.IOException;
import java.util.Properties;

public class ProducerTest {

	public static void main(String[] args) throws IOException, InterruptedException {
		Properties props = new Properties();
		//                          brokerId:host:port[:partitions(分区)[:autocreatetopic(自动创建topic)]]
	    //props.put("broker.list", "0:192.168.3.191:9091:2,1:192.168.3.192:9091:2,2:192.168.3.181:9091:2");
		props.put("broker.list", "1:127.0.0.1:9092");
	    props.put("serializer.class", StringEncoder.class.getName());
	    //
	    ProducerConfig config = new ProducerConfig(props);
	    Producer<String, String> producer = new Producer<String, String>(config);
	    /*
	    StringProducerData data = new StringProducerData("demo10");
	    for(int i=0;i<100;i++) {
	    	//Thread.sleep(1000);
	        data.add("demo10 #"+i);
	    }
	    */
	    try {
	        long start = System.currentTimeMillis();
	        for (int i = 0; i < 100; i++) {
	        	StringProducerData data = new StringProducerData("lcc");
	        	data.add("tf"+i);
	        	SendResult result = producer.send(data);
	        	System.err.println("brokerId="+result.getPartition().brokerId+" topic="+result.getTopic()
	        			+" partId="+result.getPartition().partId +" offset="+result.getOffset());
	            //Thread.sleep(1000);
	            //System.err.println(i);
	        }
	        long cost = System.currentTimeMillis() - start;
	        System.out.println("send cost: "+cost+" ms");
	    } catch (Exception e) {
			e.printStackTrace();
		} finally {
	        producer.close();
	    }
	}
}
