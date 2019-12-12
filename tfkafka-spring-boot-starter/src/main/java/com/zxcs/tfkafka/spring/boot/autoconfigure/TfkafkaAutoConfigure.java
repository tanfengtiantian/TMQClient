package com.zxcs.tfkafka.spring.boot.autoconfigure;

import io.kafka.consumer.ConsumerConfig;
import io.kafka.consumer.ConsumerConnector;
import io.kafka.consumer.ConsumerFactory;
import io.kafka.producer.Producer;
import io.kafka.producer.config.ProducerConfig;
import io.kafka.producer.serializer.imp.StringEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.util.Properties;

/**
 * Tfkafka Autoconfigure
 * <p>
 * Created by zfh on 2019/08/01
 */
@Configuration
@ConditionalOnProperty(prefix = "tfkafka.service", value = "enabled", havingValue = "true")
@EnableConfigurationProperties(StarterServiceConfig.class)
public class TfkafkaAutoConfigure {

    private static final Logger logger = LoggerFactory.getLogger(TfkafkaAutoConfigure.class);

    @Autowired
    private StarterServiceConfig serviceConfig;

    @PostConstruct
    public void init () {
        logger.info("【tfkafka-starter】初始化配置 {}", serviceConfig);
    }

    @Bean
    public ConsumerConnector consumerConnector () {
        logger.info("【tfkafka-starter】实例化 ConsumerConnector");
        Properties props = new Properties();
        props.setProperty("zk.connect", serviceConfig.getZkConnect());
        props.setProperty("groupid", serviceConfig.getGroupid());
        ConsumerConfig config = new ConsumerConfig(props);
        return ConsumerFactory.create(config);
    }

    @Bean
    public Producer producer () {
        logger.info("【tfkafka-starter】实例化 Producer");
        Properties props = new Properties();
        props.put("broker.list", serviceConfig.getBrokerList());
        props.put("serializer.class", StringEncoder.class.getName());
        ProducerConfig config = new ProducerConfig(props);
        return new Producer<>(config);
    }
}
