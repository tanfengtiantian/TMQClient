package javakafka.io.kafka.consumer;


import io.kafka.api.FetchRequest;
import io.kafka.consumer.SimpleConsumer;
import io.kafka.message.ByteBufferMessageSet;
import io.kafka.message.MessageAndOffset;
import io.kafka.utils.Utils;
import java.io.IOException;



public class SimpleConsumerTest {

	public static void main(String[] args) throws IOException, InterruptedException {
		SimpleConsumer consumer = new SimpleConsumer("127.0.0.1", 9092);
		long offset = 0;//2250;
		
		while (true) {
		    FetchRequest request = new FetchRequest("lcc", 0, offset);
		    for (MessageAndOffset msg : consumer.fetch(request)) {
		        System.out.println(Utils.toString(msg.message.payload(), "UTF-8")+" offset = "+msg.offset);
		        offset = msg.offset;

		    }
		}
		
		/*
		long offset = -1;
        int cnt = 0;
        for (int i = 0; i < 5; i++) {
            FetchRequest request = new FetchRequest("demo10", i, 0, 1000 * 1000);
            ByteBufferMessageSet messages = consumer.fetch(request);
            for (MessageAndOffset msg : messages) {
                System.out.printf("%s ", Utils.toString(msg.message.payload(), "UTF-8"));
                System.out.println();
                offset = Math.max(offset, msg.offset);
                cnt++;
            }
        }
        System.out.println("receive message count: " + cnt);
        */
		/*
		for (int i = 0; i < 5; i++) {
			FetchRequest request = new FetchRequest("demo3", 0, offset);
		    for (MessageAndOffset msg : consumer.fetch(request)) {
		        System.out.println(Utils.toString(msg.message.payload(), "UTF-8")+" offset = "+msg.offset);
		        offset = msg.offset;
		        
		    }
		}*/
		

	
	}
}
