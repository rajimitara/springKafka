package com.streaming.kafka.producer;

import com.streaming.kafka.LogManager1;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

@Service
public class Sender {

    private static final LogManager1 logger = new LogManager1(Sender.class);

   @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
   DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    @Value("${app.topic.foo}")
    private String topic;
    int runNumber = 0;

    @Scheduled(fixedDelay = 100000)
    public void runkafka() throws Exception{

        System.out.println("Starting runKafka method for runNumber: ["+runNumber+"]");
        System.out.println("Sending messages to Topic : "+dateFormat.format( new Date()));
        for(int i=0;i<15;i++){
            send( runNumber+ ": Hello world:"+i);

            System.out.println("Sent message:"+i);
        }
        runNumber++;
    }
   /* public void send(String message){
        logger.logInfo("sending message='{}' to topic='{}'", message, topic);
        kafkaTemplate.send(topic, message);
    }*/

   /* private final KafkaTemplate<String, String> kafkaTemplate;

    Sender(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }*/

    public void send(String message) {
        this.kafkaTemplate.send(topic, message);
        System.out.println("Sent sample message [" + message + "] to " + topic);
    }
}
