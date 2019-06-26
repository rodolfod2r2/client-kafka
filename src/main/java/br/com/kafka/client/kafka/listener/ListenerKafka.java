package br.com.kafka.client.kafka.listener;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import java.util.List;
import java.util.Optional;

@Slf4j
public class ListenerKafka {


    @KafkaListener(topics = "${spring.kafka.topics.name1}", containerFactory = "kafkaListenerContainerFactory")
    public void Listener(@Payload List<Object> record, Acknowledgment ack,
                         @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) List<String> keys,
                         @Header(KafkaHeaders.RECEIVED_PARTITION_ID) List<Long> partitions,
                         @Header(KafkaHeaders.RECEIVED_TOPIC) List<String> topics,
                         @Header(KafkaHeaders.OFFSET) List<Long> offsets) {

        for (int i = 0; i < record.size(); i++) {
            Optional<?> messages = Optional.ofNullable(record.get(i));
            if (messages.isPresent()) {
                Object msg = messages.get();
                log.info("Topic: '{}' Key : '{}' Offset: '{}' Partition : '{}' Message: '{}'", topics.get(i), keys.get(i), offsets.get(i), partitions.get(i), msg);
            }

        }
        ack.acknowledge();


    }
}
