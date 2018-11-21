package com.streaming.kafka;

import com.streaming.kafka.producer.Sender;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ImportResource;

@SpringBootApplication
@ComponentScan("com.streaming.kafka.*")
@ImportResource({"classpath:kafka-streams.xml"})
public class KafkaApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaApplication.class, args);
	}
	/*@Autowired
	private Sender sender;

	public void run(String... strings) throws Exception {
		sender.send("Spring Kafka Producer and Consumer Example");
	}*/
}
