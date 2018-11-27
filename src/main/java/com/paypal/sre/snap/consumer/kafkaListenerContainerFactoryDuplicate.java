package com.paypal.sre.snap.consumer;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;

@Configuration
public class kafkaListenerContainerFactoryDuplicate {

	
	@Bean
	@ConditionalOnMissingBean(name = "kafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer, ConsumerFactory<Object,Object> kafkaConsumerFactory){
        ConcurrentKafkaListenerContainerFactory<Object,Object> factory = new ConcurrentKafkaListenerContainerFactory();
        configurer.configure(factory, kafkaConsumerFactory);
        //factory.getContainerProperties().setIdleEventInterval(15000L);

        return factory;
    }
}
