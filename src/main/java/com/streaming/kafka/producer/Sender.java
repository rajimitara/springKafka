package com.streaming.kafka.producer;

import com.streaming.kafka.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.apache.commons.lang3.StringUtils;

@Service
public class Sender {

    private static final LogManager logger = new LogManager(Sender.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Value("${app.topic.foo}")
    private String topic;
    int runNumber = 0;

    @Scheduled(fixedDelay = 100000)
    public void runkafka() throws Exception{

        System.out.println("Starting runKafka method for runNumber: ["+runNumber+"]");

        for(int i=0;i<15;i++){
            send( runNumber+ ": Hello world:"+i);

            System.out.println("Sent message:"+i);
        }
        runNumber++;
    }
    public void send(String message){
        logger.logInfo("sending message='{}' to topic='{}'", message, topic);
        kafkaTemplate.send(topic, message);
    }
}
