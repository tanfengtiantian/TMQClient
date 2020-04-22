package javakafka.io.kafka.producer;

import io.kafka.api.rpc.IHello;
import io.kafka.rpc.balancer.RandomLoadBalancer;
import io.kafka.rpc.RpcProducerConfig;
import io.kafka.rpc.RpcProxyFactory;
import java.util.Properties;

public class RpcTest {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("rpc.list", "1:127.0.0.1:9091,2:127.0.0.1:9092,3:127.0.0.1:9093");
		//props.put("rpc.list", "1:127.0.0.1:9092,2:127.0.0.1:9093");
		//序列化/反序列化方式
	    props.put("serializer.class", "");
	    //软负载均衡类型
		props.put("loadBalancer.class", RandomLoadBalancer.class.getName());
		//调用方式 [同步, 异步]
		props.put("invokeType", "SYNC");
		//集群容错策略
		props.put("clusterInvoker.Strategy", "FAIL_FAST");
		//failover重试次数
		props.put("failoverRetries", 2);
		//
		RpcProducerConfig config = new RpcProducerConfig(props);

	    try {
	        long start = System.currentTimeMillis();
			RpcProxyFactory factory = new RpcProxyFactory(config);
			IHello hello = factory.proxyRemote(IHello.class,"hello");
	        for (int i = 0; i < 100; i++) {
				System.out.println(hello.sayHello("tf", 10000));
				Thread.sleep(1000);
	        }
	        long cost = System.currentTimeMillis() - start;
	        System.out.println("send cost: "+cost+" ms");
	    } catch (Exception e) {
			e.printStackTrace();
		} finally {
			System.out.println("send close");
	    }
	}
}
