package br.com.freela.auditoria;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/** Envelope das mensagens do tópico contratos.eventos.v1; payload mantido genérico para auditar o conteúdo original. */
public record ContratoEventEnvelope(UUID eventId, String eventType, UUID contratoId, Instant occurredAt,
                                    String correlationId, Map<String, Object> payload) {}
