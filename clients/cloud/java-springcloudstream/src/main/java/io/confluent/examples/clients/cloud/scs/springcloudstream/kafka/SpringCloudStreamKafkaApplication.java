package io.confluent.examples.clients.cloud.scs.springcloudstream.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;

import java.util.function.Supplier;

import io.confluent.examples.clients.cloud.DataRecordAvro;

@SpringBootApplication
public class SpringCloudStreamKafkaApplication {

  public static void main(String[] args) {
    SpringApplication.run(SpringCloudStreamKafkaApplication.class, args);
  }

  @Bean
  Supplier<Message<DataRecordAvro>> sendDataRecord() {
    return () -> MessageBuilder.withPayload(new DataRecordAvro(42L))
        .setHeader(KafkaHeaders.MESSAGE_KEY, "alice").build();
  }
}
	


