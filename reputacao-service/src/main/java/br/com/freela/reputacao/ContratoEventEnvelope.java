package br.com.freela.reputacao;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/** Envelope das mensagens do tópico contratos.eventos.v1 (contrato de comunicação com o contrato-service). */
public record ContratoEventEnvelope(UUID eventId, String eventType, UUID contratoId, Instant occurredAt,
                                    String correlationId, Payload payload) {
    public record Payload(UUID clienteId, UUID freelancerId, String titulo, BigDecimal valor) {}
}
