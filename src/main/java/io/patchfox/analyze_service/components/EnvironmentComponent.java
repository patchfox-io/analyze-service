package io.patchfox.analyze_service.components;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import lombok.Getter;


@Getter
@Component
public class EnvironmentComponent {
    
    @Value("${spring.application.name}")
    String serviceName;

    @Value("${spring.kafka.request-topic}")
    String kafkaRequestTopicName;

    @Value("${spring.kafka.response-topic}")
    String kafkaResponseTopicName;

    @Value("${spring.kafka.request.client-id-prefix}")
    String kafkaRequestClientIdPrefix;

    @Value("${spring.kafka.response.client-id-prefix}")
    String kafkaResponseClientIdPrefix;

    @Value("${spring.kafka.group-name}")
    String kafkaGroupName;

    @Value("${patchfox.analyze-service.max-tabulate-cache-size}")
    Integer maxTabulateCacheSize;

    @Value("${patchfox.tabulate.package-index-batch-size:500}")
    Integer packageIndexBatchSize;
}
