package javakafka.io.kafka.consumer;

import io.kafka.consumer.ConsumerConfig;
import io.kafka.consumer.ConsumerConnector;
import io.kafka.consumer.ConsumerFactory;
import io.kafka.consumer.MessageStream;
import io.kafka.producer.serializer.imp.StringDecoder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author tf
 * @version 创建时间：2019年1月31日 下午2:53:06
 * @ClassName ZookeeperConsumerConnectorTest
 */
public class ZookeeperConsumerConnectorTest {

	
	public static void main(String[] args) {
		Properties props = new Properties();
		//指明zookeeper地址
		props.setProperty("zk.connect","localhost:2181");
		//指明consumer group的名字
		props.setProperty("groupid","tf");
		ConsumerConfig config = new ConsumerConfig(props);
		//创建了ZookeeperConsumerConnector，连接zookeeper，获取当前topic的数据信息
		ConsumerConnector connector = ConsumerFactory.create(config);
		//指明每一个topic的消费线程数
		Map<String,Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put("tf",2);
		//创建消费消息流，key为topic，value为MessageStream的list，大小为上面map中指定的大小
		Map<String,List<MessageStream<String>>> streams = connector.createMessageStreams(topicCountMap,new StringDecoder());
		List<MessageStream<String>> messageStreamList = streams.get("tf");
		final AtomicInteger count = new AtomicInteger(0);
		final AtomicInteger streamCount = new AtomicInteger(0);
		//创建线程池，该线程池数目必须不小于上面所有的消费线程数
		ExecutorService executor = Executors.newFixedThreadPool(2);
		//提交消费任务，开始消费消息
		for(final MessageStream<String> stream:messageStreamList){
		executor.execute(new Runnable() {
		   @Override
		   public void run() {
		        int threadNum = streamCount.incrementAndGet();
		        //从stream中获取消息，此处为阻塞式消费，即当没有新消息到来时，阻塞直到新消息到来或者线程结束
		        //通过BlockingQueue实现
		        for(String msg:stream){
		        	System.out.println("stream#"+threadNum+":msg#"+count.incrementAndGet()+"=>"+msg);
		        }
		     }
		   });
		}
	}
}
