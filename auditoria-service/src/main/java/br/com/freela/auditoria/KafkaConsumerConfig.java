package br.com.freela.auditoria;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConsumerConfig {
    static final String DLT = "contratos.eventos.v1.auditoria.DLT";

    /** 3 tentativas com 1s de intervalo; depois a mensagem vai para o Dead Letter Topic e a partição segue. */
    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<Object, Object> template) {
        var recoverer = new DeadLetterPublishingRecoverer(template, (r, e) -> new TopicPartition(DLT, -1));
        return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3L));
    }

    @Bean
    NewTopic auditoriaDlt() { return TopicBuilder.name(DLT).partitions(1).replicas(1).build(); }
}
