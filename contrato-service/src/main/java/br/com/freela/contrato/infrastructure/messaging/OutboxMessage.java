package br.com.freela.contrato.infrastructure.messaging;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

/**
 * Registro do Transactional Outbox: gravado na mesma transação do Aggregate e publicado depois no Kafka.
 */
@Entity
@Table(name = "outbox_messages", indexes = @Index(name = "idx_outbox_status_seq", columnList = "status,seq"))
public class OutboxMessage {
    public static final String PENDING = "PENDING";
    public static final String PUBLISHED = "PUBLISHED";

    @Id @GeneratedValue(strategy = GenerationType.IDENTITY) private Long seq;
    @Column(nullable = false, unique = true) private UUID eventId;
    @Column(nullable = false) private UUID aggregateId;
    @Column(nullable = false, length = 60) private String eventType;
    @Column(nullable = false, columnDefinition = "text") private String payload;
    @Column(length = 120) private String correlationId;
    @Column(nullable = false, length = 20) private String status;
    @Column(nullable = false) private Instant createdAt;
    private Instant publishedAt;

    protected OutboxMessage() {}

    public OutboxMessage(UUID eventId, UUID aggregateId, String eventType, String payload, String correlationId) {
        this.eventId = eventId; this.aggregateId = aggregateId; this.eventType = eventType;
        this.payload = payload; this.correlationId = correlationId;
        this.status = PENDING; this.createdAt = Instant.now();
    }

    public void marcarPublicada() { this.status = PUBLISHED; this.publishedAt = Instant.now(); }

    public Long getSeq() { return seq; }
    public UUID getEventId() { return eventId; }
    public UUID getAggregateId() { return aggregateId; }
    public String getEventType() { return eventType; }
    public String getPayload() { return payload; }
    public String getCorrelationId() { return correlationId; }
    public String getStatus() { return status; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getPublishedAt() { return publishedAt; }
}
