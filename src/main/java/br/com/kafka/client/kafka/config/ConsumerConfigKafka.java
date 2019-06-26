package br.com.kafka.client.kafka.config;


import br.com.kafka.client.kafka.listener.ListenerKafka;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;


@Slf4j
@Configuration
@EnableKafka
public class ConsumerConfigKafka {

    @Autowired
    PropertyKafka propertyKafka;

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {

        Map<String, Object> config = new HashMap<>();

        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, propertyKafka.getBootstrapServers());
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, propertyKafka.getEnableAutoCommit());
        config.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, propertyKafka.getAutoCommitInterval());
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, propertyKafka.getSessionTimeout());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, propertyKafka.getKeyDeserializer());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, propertyKafka.getValueDeserializer());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, propertyKafka.getGroupId());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, propertyKafka.getAutoOffsetReset());
        config.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, propertyKafka.getFetchMaxWait());
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, propertyKafka.getMaxPollRecords());
        config.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "72428800");
        config.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,"4048576");

        return new DefaultKafkaConsumerFactory<>(config, new StringDeserializer(), new JsonDeserializer<>(Object.class));
    }


    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, Object>> kafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setBatchListener(true);
        factory.setBatchErrorHandler(new BatchLoggingErrorHandler());
        factory.setBatchErrorHandler(new BatchErrorHandler() {
            @Override
            public void handle(Exception e, ConsumerRecords<?, ?> consumerRecords) {

            }
            @Override
            public void handle(Exception thrownException, ConsumerRecords<?, ?> data, Consumer<?, ?> consumer, MessageListenerContainer container) {
                String s = thrownException.getMessage().split("Error deserializing key/value for partition ")[1].split(". If needed, please seek past the record to continue consumption.")[0];
                String topics = s.split("-")[0];
                int offset = Integer.valueOf(s.split("offset ")[1]);
                int partition = Integer.valueOf(s.split("-")[1].split(" at")[0]);

                TopicPartition topicPartition = new TopicPartition(topics, partition);
                log.error("Message: Error deserializing key/value for partition, Topic: '{}' Partition: '{}' Offset: '{}'" ,topics, partition, offset );
                consumer.seek(topicPartition, offset + 1);
            }
        });
        factory.setConcurrency(5);
        factory.getContainerProperties().setPollTimeout(4000);
        factory.setBatchErrorHandler(new SeekToCurrentBatchErrorHandler());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        return factory;
    }


    @Bean
    public ListenerKafka kafkaListeners() {
        return new ListenerKafka();
    }

}
