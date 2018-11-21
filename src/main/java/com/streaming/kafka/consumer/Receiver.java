package com.streaming.kafka.consumer;

import com.streaming.kafka.LogManager;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Service
public class Receiver implements ApplicationListener<ListenerContainerIdleEvent> {

    private static final LogManager logger = new LogManager(Receiver.class);

    int recordCountPerIteration=0;

    public void onApplicationEvent(ListenerContainerIdleEvent event) {
        Set<TopicPartition> paused = event.getConsumer().paused();

        System.out.println("Checking paused: "+paused);

        if(paused.size() > 0){
            System.out.println("Idle Consumer: "+event.toString());
            System.out.println("Resuming Idle Consumer: "+paused);
            event.getConsumer().resume(paused);
        }else{
            System.out.println("No paused Partitions.Container idle.");
        }
    }

    @KafkaListener(topics = "${app.topic.foo}",containerFactory = "kafkaListenerContainerFactory")
    public void listen(ConsumerRecord<?, ?> consumerRecord, Consumer<?, ?> consumer, @Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) throws InterruptedException {
        System.out.println("received message='{}'"+ message);
        if(++recordCountPerIteration % 5 == 0 ){
            System.out.println("Pausing Consumer For Partition: ["+partition+"]");
            consumer.pause(Collections.singleton(new TopicPartition("kafka.topic", partition)));
        }
        logger.logInfo("received message='{}'", message);
        //resume
        //Thread.sleep(1000);
        System.out.println("Trying to resume consumer");

    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.getContainerProperties().setIdleEventInterval(15000L);
        factory.setConcurrency(3);
        factory.getContainerProperties().setPollTimeout(3000);
        return factory;
    }
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "foo");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"5");
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,"5000");

        return props;
    }

    public ConsumerFactory<String, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }
}
