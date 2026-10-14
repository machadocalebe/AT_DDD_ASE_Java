package br.com.freela.contrato.infrastructure.messaging;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * Lê mensagens PENDING do outbox em ordem de gravação e publica no Kafka usando contratoId como chave.
 * Em caso de falha interrompe o lote para não furar a ordem; tenta de novo no próximo ciclo.
 */
@Component
public class OutboxPublisher {
    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);
    private final OutboxRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OutboxPublisher(OutboxRepository repository, KafkaTemplate<String, String> kafkaTemplate) {
        this.repository = repository; this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelayString = "${outbox.publisher.fixed-delay-ms:1000}")
    @Transactional
    public void publicarPendentes() {
        for (OutboxMessage m : repository.findTop100ByStatusOrderBySeqAsc(OutboxMessage.PENDING)) {
            try {
                var record = new ProducerRecord<>(KafkaTopics.CONTRATOS_EVENTOS, m.getAggregateId().toString(), m.getPayload());
                record.headers().add(new RecordHeader("eventId", m.getEventId().toString().getBytes(StandardCharsets.UTF_8)));
                record.headers().add(new RecordHeader("eventType", m.getEventType().getBytes(StandardCharsets.UTF_8)));
                if (m.getCorrelationId() != null)
                    record.headers().add(new RecordHeader("X-Correlation-Id", m.getCorrelationId().getBytes(StandardCharsets.UTF_8)));
                var result = kafkaTemplate.send(record).get(10, TimeUnit.SECONDS);
                m.marcarPublicada();
                log.info("contrato.outbox.publicado contratoId={} eventId={} eventType={} correlationId={} topic={} partition={} offset={}",
                        m.getAggregateId(), m.getEventId(), m.getEventType(), m.getCorrelationId(),
                        KafkaTopics.CONTRATOS_EVENTOS, result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            } catch (Exception e) {
                if (e instanceof InterruptedException) Thread.currentThread().interrupt();
                log.error("contrato.outbox.falha eventId={} contratoId={} erro={}", m.getEventId(), m.getAggregateId(), e.toString());
                return;
            }
        }
    }
}
