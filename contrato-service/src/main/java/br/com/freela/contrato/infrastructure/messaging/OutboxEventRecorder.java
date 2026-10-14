package br.com.freela.contrato.infrastructure.messaging;

import br.com.freela.contrato.domain.shared.DomainEvent;
import br.com.freela.contrato.domain.shared.DomainEventRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

/**
 * Registra eventos de domínio na tabela outbox. Deve ser chamado dentro da transação que persiste o Aggregate.
 */
@Component
public class OutboxEventRecorder implements DomainEventRecorder {
    private static final Logger log = LoggerFactory.getLogger(OutboxEventRecorder.class);
    private final OutboxRepository repository;
    private final ObjectMapper mapper;

    public OutboxEventRecorder(OutboxRepository repository, ObjectMapper mapper) {
        this.repository = repository; this.mapper = mapper;
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void registrar(DomainEvent event, String correlationId) {
        var envelope = new EventEnvelope(event.eventId(), event.eventType(), event.contratoId(),
                event.occurredAt(), correlationId, event);
        String json = mapper.writeValueAsString(envelope);
        repository.save(new OutboxMessage(event.eventId(), event.contratoId(), event.eventType(), json, correlationId));
        log.info("contrato.outbox.registrado contratoId={} eventId={} eventType={} correlationId={}",
                event.contratoId(), event.eventId(), event.eventType(), correlationId);
    }
}
