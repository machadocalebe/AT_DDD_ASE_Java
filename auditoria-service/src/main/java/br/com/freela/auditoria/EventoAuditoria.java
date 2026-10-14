package br.com.freela.auditoria;
import jakarta.persistence.*; import java.time.Instant; import java.util.UUID;
@Entity @Table(name="auditoria_eventos", uniqueConstraints=@UniqueConstraint(name="uk_auditoria_event_id", columnNames="eventId"))
class EventoAuditoria {
 @Id UUID id; UUID eventId; UUID aggregateId; String eventType; String correlationId; @Column(columnDefinition="text") String payload; Instant occurredAt; Instant recebidoEm;
 protected EventoAuditoria(){} EventoAuditoria(UUID eventId,UUID aggregateId,String eventType,String correlationId,String payload,Instant occurredAt){this.id=UUID.randomUUID();this.eventId=eventId;this.aggregateId=aggregateId;this.eventType=eventType;this.correlationId=correlationId;this.payload=payload;this.occurredAt=occurredAt;this.recebidoEm=Instant.now();}
 public UUID getId(){return id;} public UUID getEventId(){return eventId;} public UUID getAggregateId(){return aggregateId;} public String getEventType(){return eventType;}
 public String getCorrelationId(){return correlationId;} public String getPayload(){return payload;} public Instant getOccurredAt(){return occurredAt;} public Instant getRecebidoEm(){return recebidoEm;}
}
