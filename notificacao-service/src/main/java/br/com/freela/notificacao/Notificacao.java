package br.com.freela.notificacao;
import jakarta.persistence.*; import java.time.Instant; import java.util.UUID;
@Entity @Table(name="notificacoes")
class Notificacao {
 @Id UUID id; UUID contratoId; UUID eventId; UUID destinatarioId; String tipo; @Column(length=500) String mensagem; Instant criadaEm;
 protected Notificacao(){} Notificacao(UUID contratoId, UUID eventId, UUID destinatarioId, String tipo, String mensagem){this.id=UUID.randomUUID();this.contratoId=contratoId;this.eventId=eventId;this.destinatarioId=destinatarioId;this.tipo=tipo;this.mensagem=mensagem;this.criadaEm=Instant.now();}
 public UUID getId(){return id;} public UUID getContratoId(){return contratoId;} public UUID getEventId(){return eventId;} public UUID getDestinatarioId(){return destinatarioId;}
 public String getTipo(){return tipo;} public String getMensagem(){return mensagem;} public Instant getCriadaEm(){return criadaEm;}
}
