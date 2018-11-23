package com.streaming.kafka.consumer;

import com.streaming.kafka.LogManager1;


import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.event.ConsumerPausedEvent;
import org.springframework.kafka.event.KafkaEvent;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class Receiver implements ApplicationListener<ListenerContainerIdleEvent>{

    private static final LogManager1 logger = new LogManager1(Receiver.class);

    int recordCountPerIteration=0;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Override
    public void onApplicationEvent(ListenerContainerIdleEvent event) {
        Set<TopicPartition> paused = event.getConsumer().paused();

        System.out.println("Checking paused: "+paused);

        if(paused.size() > 0){
            event.getConsumer().resume(paused);
            System.out.println("Resuming Idle Consumer: "+paused);
        }else{
            System.out.println("No paused Partitions.Container idle.");
        }


    }

    @KafkaListener(id = "pause.resume",topics = "${app.topic.foo}",containerFactory = "kafkaListenerContainerFactory")
    public void processMessage(String message,Consumer<?, ?> consumer,
                               @Header(KafkaHeaders.RECEIVED_PARTITION_ID) List<Integer> partitions,
                               @Header(KafkaHeaders.RECEIVED_TOPIC) List<String> topics,
                               @Header(KafkaHeaders.OFFSET) List<Long> offsets) {
        if (++recordCountPerIteration % 5 == 0) {
            //System.out.printf("%s-%d[%d] \"%s\"\n", topics.get(0), partitions.get(0), offsets.get(0), message);
            consumer.pause(Collections.singleton(new TopicPartition("kafka.topic", 0)));
            //registry.getListenerContainer("pause.resume").pause();
        }
        System.out.printf("%s-%d[%d] \"%s\"\n", topics.get(0), partitions.get(0), offsets.get(0), message);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer, ConsumerFactory<Object,Object> kafkaConsumerFactory){
        ConcurrentKafkaListenerContainerFactory<Object,Object> factory = new ConcurrentKafkaListenerContainerFactory();
        configurer.configure(factory, kafkaConsumerFactory);
        factory.getContainerProperties().setIdleEventInterval(15_000L);

        return factory;

    }
}
