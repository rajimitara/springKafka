package com.paypal.sre.snap.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.event.KafkaEvent;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import com.paypal.sre.snap.LogManager1;

import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.event.ConsumerPausedEvent;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import com.paypal.sre.snap.consumer.iSnapKafkaConsumer;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class Receiver implements ApplicationListener<KafkaEvent> {

    private static final LogManager1 logger = new LogManager1(Receiver.class);

    int recordCountPerIteration = 0;
    @Autowired
    public KafkaListenerEndpointRegistry registry;
    
    
    DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    public void onApplicationEvent(KafkaEvent event) {
        if (registry.getListenerContainer("pause.resume").isContainerPaused()) {
            System.out.println("Resuming Idle Consumer : "+dateFormat.format( new Date()));
            System.out.println("Container state :"+registry.getListenerContainer("pause.resume").isPauseRequested());
            registry.getListenerContainer("pause.resume").resume();
        }
    }

    public iSnapKafkaConsumer snapKafkaConsumer;
    
    
    @KafkaListener(id = "pause.resume", topics = "${app.topic.foo}", containerFactory = "kafkaListenerContainerFactory")
    public void processMessage(ConsumerRecord<String, String> record,
                               @Header(KafkaHeaders.RECEIVED_PARTITION_ID) List<Integer> partitions,
                               @Header(KafkaHeaders.RECEIVED_TOPIC) List<String> topics,
                               @Header(KafkaHeaders.OFFSET) List<Long> offsets) {
        if (++recordCountPerIteration % 5 == 0) {
            System.out.println("Trying to pause container :" + dateFormat.format(new Date()));
            System.out.println("Container state : "+registry.getListenerContainer("pause.resume").isPauseRequested());
            registry.getListenerContainer("pause.resume").pause();
            //send to changeFetcher
            //snapKafkaConsumer.consume(messages);
            //(message);
            
        }
        snapKafkaConsumer.consume(record.value());
        System.out.printf("%s-%d[%d] \"%s\"\n", topics.get(0), partitions.get(0), offsets.get(0), record.value());
    }

   /* @Bean
    public ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer, ConsumerFactory<Object,Object> kafkaConsumerFactory){
        ConcurrentKafkaListenerContainerFactory<Object,Object> factory = new ConcurrentKafkaListenerContainerFactory();
        configurer.configure(factory, kafkaConsumerFactory);
        factory.getContainerProperties().setIdleEventInterval(15000L);

        return factory;
    }*/
    /*
   is not thread safe.
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
        //System.out.printf("%s-%d[%d] \"%s\"\n", topics.get(0), partitions.get(0), offsets.get(0), message);
        //consumer.pause(Collections.singleton(new TopicPartition("kafka.topic", 0)));


    }*/

	public iSnapKafkaConsumer getSnapKafkaConsumer() {
		return snapKafkaConsumer;
	}

	public void setSnapKafkaConsumer(iSnapKafkaConsumer snapKafkaConsumer) {
		this.snapKafkaConsumer = snapKafkaConsumer;
	}
}
