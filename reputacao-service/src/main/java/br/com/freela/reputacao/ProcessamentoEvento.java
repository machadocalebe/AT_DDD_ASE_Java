package br.com.freela.reputacao;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

/** Registro de eventos já processados: garante idempotência (eventId é a chave primária). */
@Entity
@Table(name = "eventos_processados")
class ProcessamentoEvento {
    @Id UUID eventId;
    @Column(length = 60) String eventType;
    Instant processadoEm;
    protected ProcessamentoEvento() {}
    ProcessamentoEvento(UUID eventId, String eventType) { this.eventId = eventId; this.eventType = eventType; this.processadoEm = Instant.now(); }
}
