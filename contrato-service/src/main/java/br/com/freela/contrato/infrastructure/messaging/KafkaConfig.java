package br.com.freela.contrato.infrastructure.messaging;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableScheduling
public class KafkaConfig {
    /** 3 partições: contratos distintos processam em paralelo; mesma chave (contratoId) = mesma partição. */
    @Bean
    NewTopic contratosEventosTopic() {
        return TopicBuilder.name(KafkaTopics.CONTRATOS_EVENTOS).partitions(3).replicas(1).build();
    }
}
