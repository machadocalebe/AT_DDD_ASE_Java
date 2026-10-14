package br.com.freela.contrato.domain.shared;

/** Porta: registra eventos de domínio para publicação (implementada pelo Transactional Outbox). */
public interface DomainEventRecorder {
    void registrar(DomainEvent event, String correlationId);
}
