package com.streaming.kafka;

import org.springframework.aop.ThrowsAdvice;
import org.springframework.aop.framework.adapter.ThrowsAdviceInterceptor;

public class ExceptionHandlerAdvice implements ThrowsAdvice {
    public void afterThrowing(KafkaException e){
        System.out.println("Some Kafka Exception thrown mainly not a valid json");
    }

}
