package com.streaming.kafka.consumer;

import com.streaming.kafka.LogManager;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Set;

@Service
public class Receiver implements ApplicationListener<ListenerContainerIdleEvent> {

    private static final LogManager logger = new LogManager(Receiver.class);

    int recordCountPerIteration;

    @Override
    public void onApplicationEvent(ListenerContainerIdleEvent event) {
        Set<TopicPartition> paused = event.getConsumer().paused();

        System.out.println("Checking paused: "+paused);

        if(paused.size() > 0){
            event.getConsumer().resume(paused);
            System.out.println("Idle Consumer: "+event.toString());
            System.out.println("Resuming Idle Consumer: "+paused);
        }else{
            System.out.println("No paused Partitions.Container idle.");
        }

    }
    @KafkaListener(topics = "${app.topic.foo}",containerFactory = "kafkaListenerContainerFactory")
    public void listen(ConsumerRecord<?, ?> consumerRecord, Consumer<?, ?> consumer, @Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        System.out.println("received message='{}'"+ message);
        if(++recordCountPerIteration % 5 == 0 ){
            System.out.println("Pausing Consumer For Partition: ["+partition+"]");
            consumer.pause(Collections.singleton(new TopicPartition("kafka.topic", partition)));
        }
        logger.logInfo("received message='{}'", message);
    }
    @Bean
    public ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(ConcurrentKafkaListenerContainerFactoryConfigurer configurer, ConsumerFactory<Object,Object> kafkaConsumerFactory){
        ConcurrentKafkaListenerContainerFactory<Object,Object> factory = new ConcurrentKafkaListenerContainerFactory();
        configurer.configure(factory, kafkaConsumerFactory);
        factory.getContainerProperties().setIdleEventInterval(15_000L);

        return factory;

    }

}
