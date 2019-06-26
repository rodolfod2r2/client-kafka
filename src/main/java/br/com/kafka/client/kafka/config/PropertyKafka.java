package br.com.kafka.client.kafka.config;

import lombok.Getter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

@Getter
@Component
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "spring.kafka.consumer")
public class PropertyKafka {

    private String bootstrapServers;
    private String groupId;
    private String autoOffsetReset;
    private String keyDeserializer;
    private String valueDeserializer;
    private String autoCommitInterval;
    private Boolean enableAutoCommit;
    private String sessionTimeout;
    private String fetchMaxWait;
    private String maxPollRecords;


}