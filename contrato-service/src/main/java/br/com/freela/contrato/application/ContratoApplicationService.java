package br.com.freela.contrato.application;

import br.com.freela.contrato.domain.model.Contrato;
import br.com.freela.contrato.domain.repository.ContratoRepository;
import br.com.freela.contrato.domain.shared.DomainEvent;
import br.com.freela.contrato.domain.shared.DomainEventRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
public class ContratoApplicationService {
    private static final Logger log = LoggerFactory.getLogger(ContratoApplicationService.class);
    private final ContratoRepository repository;
    private final DomainEventRecorder eventRecorder;

    public ContratoApplicationService(ContratoRepository repository, DomainEventRecorder eventRecorder) {
        this.repository = repository;
        this.eventRecorder = eventRecorder;
    }

    @Transactional
    public Contrato criar(CriarContratoCommand cmd) {
        log.info("contrato.criacao.inicio clienteId={} freelancerId={} titulo={} valor={}",
                cmd.clienteId(), cmd.freelancerId(), cmd.titulo(), cmd.valor());
        Contrato contrato = Contrato.criar(cmd.clienteId(), cmd.freelancerId(), cmd.titulo(), cmd.valor());
        log.info("contrato.dominio.criado contratoId={} status={} domainEvents={}",
                contrato.id(), contrato.status(), contrato.domainEvents().size());
        Contrato salvo = repository.salvar(contrato);
        registrarEventos(contrato);
        log.info("contrato.criacao.sucesso contratoId={} clienteId={} freelancerId={} status={}",
                salvo.id(), salvo.clienteId(), salvo.freelancerId(), salvo.status());
        return salvo;
    }

    @Transactional
    public Contrato registrarEntrega(UUID id) { return transicionar(id, "entrega", Contrato::registrarEntrega); }

    @Transactional
    public Contrato concluir(UUID id) { return transicionar(id, "conclusao", Contrato::concluir); }

    @Transactional
    public Contrato cancelar(UUID id) { return transicionar(id, "cancelamento", Contrato::cancelar); }

    private Contrato transicionar(UUID id, String operacao, java.util.function.Consumer<Contrato> acao) {
        log.info("contrato.{}.inicio contratoId={}", operacao, id);
        Contrato contrato = repository.buscarPorId(id)
                .orElseThrow(() -> new IllegalArgumentException("Contrato não encontrado: " + id));
        acao.accept(contrato);
        Contrato salvo = repository.salvar(contrato);
        // Aggregate e evento são gravados na MESMA transação (Transactional Outbox).
        registrarEventos(contrato);
        log.info("contrato.{}.sucesso contratoId={} status={}", operacao, salvo.id(), salvo.status());
        return salvo;
    }

    private void registrarEventos(Contrato contrato) {
        String correlationId = MDC.get("correlationId");
        for (DomainEvent event : contrato.pullDomainEvents()) {
            eventRecorder.registrar(event, correlationId);
            log.info("contrato.evento.pendente contratoId={} eventId={} eventType={} occurredAt={}",
                    contrato.id(), event.eventId(), event.eventType(), event.occurredAt());
        }
    }

    @Transactional(readOnly = true)
    public Contrato buscar(UUID id) {
        log.info("contrato.busca.inicio contratoId={}", id);
        var contrato = repository.buscarPorId(id).orElseThrow(() -> new IllegalArgumentException("Contrato não encontrado: " + id));
        log.info("contrato.busca.sucesso contratoId={} status={}", id, contrato.status());
        return contrato;
    }

    @Transactional(readOnly = true)
    public List<Contrato> listar() {
        log.info("contrato.listagem.inicio");
        var contratos = repository.listar();
        log.info("contrato.listagem.sucesso quantidade={}", contratos.size());
        return contratos;
    }
}
