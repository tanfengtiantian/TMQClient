package javakafka.io.kafka.consumer;


import io.kafka.api.FetchRequest;
import io.kafka.api.MultiFetchResponse;
import io.kafka.consumer.SimpleConsumer;
import io.kafka.message.ByteBufferMessageSet;
import io.kafka.message.MessageAndOffset;
import io.kafka.utils.Utils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

/**
 * @author tf
 * @version 创建时间：2019年2月11日 下午3:58:57
 * @ClassName SimpleConsumerTest2
 */
public class SimpleConsumerTest2 {

	public static void main(String[] args) throws IOException {
		
		SimpleConsumer consumer = new SimpleConsumer("127.0.0.1", 9092);
		long offsetdemo1 = 0;
		long offsetdemo2 = 0;
        int count =0;
		while (true){
            FetchRequest request1 = new FetchRequest("test", 0, offsetdemo1);
            FetchRequest request2 = new FetchRequest("test", 1, offsetdemo2);

            MultiFetchResponse responses = consumer.multifetch(Arrays.asList(request1, request2));
            Iterator<ByteBufferMessageSet> iter = responses.iterator();
            ByteBufferMessageSet demo1Messages = iter.next();
            for (MessageAndOffset msg : demo1Messages) {
                offsetdemo1 = msg.offset;
                count++;
                System.out.println(Utils.toString(msg.message.payload(), "UTF-8")+" offset = "+msg.offset +" count="+count);
            }
            ByteBufferMessageSet demo2Messages = iter.next();
            for (MessageAndOffset msg : demo2Messages) {
                offsetdemo2 = msg.offset;
                count++;
                System.out.println(Utils.toString(msg.message.payload(), "UTF-8")+" offset = "+msg.offset+" count="+count);
            }
            //System.out.println("count="+count);
        }

	}
}
