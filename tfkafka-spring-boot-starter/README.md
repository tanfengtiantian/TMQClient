# Tfkafka Spring Boot Starter

Spring Boot with tfkafka support,help you simplify tfkafka config in Spring Boot.

## 如何使用
1. 在 Spring Boot 项目中加入```tfkafka-spring-boot-starter```依赖 ([点击查询最新版本](http://192.168.12.95/apm-team/tfkafka-client))
```
<dependency>
    <groupId>com.zxcs</groupId>
    <artifactId>tfkafka-spring-boot-starter</artifactId>
    <version>1.0.2</version>
</dependency>
```
2. 添加配置
```
tfkafka:
    service:
    enabled: true
    zkConnect: 192.168.3.192:2181
    groupid: myConsumer
    brokerList: 0:192.168.3.192:39091
```
3. 如果找不到依赖，添加二方库配置：
```
<repositories>
    <repository>
        <id>releases</id>
        <url>http://192.168.12.96:8081/repository/maven-public/</url>
    </repository>
</repositories>
```

## 入门示例
1. 消费端-消费消息
```
/**
    * Created by zfh on 2019/06/02
    */
@SpringBootApplication
public class AppAplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(AppAplication.class, args);
        ConsumerConnector connector = context.getBean(ConsumerConnector.class);

        //指明每一个topic的消费线程数
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put("zxcity_agent", 1);
        //创建消费消息流，key为topic，value为MessageStream的list，大小为上面map中指定的大小
        Map<String, List<MessageStream<String>>> streams = connector.createMessageStreams(topicCountMap, new StringDecoder());
        List<MessageStream<String>> messageStreamList = streams.get("zxcity_agent");
        final AtomicInteger count = new AtomicInteger(0);
        final AtomicInteger streamCount = new AtomicInteger(0);
        //创建线程池，该线程池数目必须不小于上面所有的消费线程数
        ExecutorService executor = Executors.newFixedThreadPool(2);
        //提交消费任务，开始消费消息
        for (final MessageStream<String> stream : messageStreamList) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    //从stream中获取消息，此处为阻塞式消费，即当没有新消息到来时，阻塞直到新消息到来或者线程结束
                    //通过BlockingQueue实现
                    for (String msg : stream) {
                        System.out.println(msg);
                    }
                }
            });
        }
    }
}
```
2. 生产端-发送消息
```
/**
    * Created by zfh on 2019/06/02
    */
@SpringBootApplication
public class AppAplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(AppAplication.class, args);

        Producer producer = context.getBean(Producer.class);
        StringProducerData data = new StringProducerData("zxcity_agent");
        data.setKey("trace");
        data.add("hello dazuo " + LocalDateTime.now().toString());

        try {
            producer.send(data);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```
## 参考
[tfkafka Wiki](http://192.168.12.95/zxcity/tfkafka/blob/master/README.md)

[Spring Boot Reference](http://docs.spring.io/spring-boot/docs/current/reference/htmlsingle/)