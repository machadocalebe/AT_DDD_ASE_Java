package br.com.freela.contrato.infrastructure.messaging;

import java.time.Instant;
import java.util.UUID;

/**
 * Envelope canônico das mensagens publicadas no Kafka (contrato de comunicação entre microsserviços).
 */
public record EventEnvelope(UUID eventId,
                            String eventType,
                            UUID contratoId,
                            Instant occurredAt,
                            String correlationId,
                            Object payload) {
}
