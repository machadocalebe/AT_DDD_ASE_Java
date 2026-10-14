package br.com.freela.auditoria;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;

@Component
public class ContratoEventListener {
    private static final Logger log = LoggerFactory.getLogger(ContratoEventListener.class);
    private final AuditoriaService service;
    private final ObjectMapper mapper;

    public ContratoEventListener(AuditoriaService service, ObjectMapper mapper) {
        this.service = service; this.mapper = mapper;
    }

    @KafkaListener(topics = "${app.kafka.topic:contratos.eventos.v1}", groupId = "auditoria-group")
    public void onMessage(ConsumerRecord<String, String> record) {
        Header h = record.headers().lastHeader("X-Correlation-Id");
        String correlationId = h != null ? new String(h.value(), StandardCharsets.UTF_8) : null;
        if (correlationId != null) MDC.put("correlationId", correlationId);
        try {
            var envelope = mapper.readValue(record.value(), ContratoEventEnvelope.class);
            MDC.put("eventId", String.valueOf(envelope.eventId()));
            MDC.put("contratoId", String.valueOf(envelope.contratoId()));
            log.info("auditoria.evento.recebido eventId={} contratoId={} eventType={} key={} partition={} offset={}",
                    envelope.eventId(), envelope.contratoId(), envelope.eventType(), record.key(), record.partition(), record.offset());
            service.processar(envelope, correlationId);
        } catch (RuntimeException ex) {
            log.error("auditoria.processamento.falha topic={} partition={} offset={} erro={}",
                    record.topic(), record.partition(), record.offset(), ex.toString());
            throw ex;
        } finally {
            MDC.remove("correlationId"); MDC.remove("eventId"); MDC.remove("contratoId");
        }
    }
}
