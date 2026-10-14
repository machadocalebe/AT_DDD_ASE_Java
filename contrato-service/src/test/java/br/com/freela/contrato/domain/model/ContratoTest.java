package br.com.freela.contrato.domain.model;

import br.com.freela.contrato.domain.shared.DomainEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class ContratoTest {

    private Contrato novo() {
        return Contrato.criar(UUID.randomUUID(), UUID.randomUUID(), "API de pagamentos", new BigDecimal("3500.00"));
    }

    @Test
    void criarEmiteContratoCriadoEFicaAtivo() {
        var c = novo();
        assertEquals(StatusContrato.ATIVO, c.status());
        assertEquals(List.of("CONTRATO_CRIADO"), c.domainEvents().stream().map(DomainEvent::eventType).toList());
    }

    @Test
    void cicloCompletoEmiteEventosNaOrdemEComMesmoContratoId() {
        var c = novo();
        c.registrarEntrega();
        c.concluir();
        var eventos = c.pullDomainEvents();
        assertEquals(List.of("CONTRATO_CRIADO", "ENTREGA_REGISTRADA", "CONTRATO_CONCLUIDO"),
                eventos.stream().map(DomainEvent::eventType).toList());
        assertTrue(eventos.stream().allMatch(e -> e.contratoId().equals(c.id())));
        assertEquals(3, eventos.stream().map(DomainEvent::eventId).distinct().count(), "cada evento tem eventId único");
        assertTrue(c.domainEvents().isEmpty(), "pullDomainEvents esvazia a lista");
    }

    @Test
    void cancelarEmiteContratoCancelado() {
        var c = novo();
        c.pullDomainEvents();
        c.cancelar();
        assertEquals(StatusContrato.CANCELADO, c.status());
        assertEquals("CONTRATO_CANCELADO", c.domainEvents().get(0).eventType());
    }

    @Test
    void transicoesInvalidasNaoEmitemEventos() {
        var c = novo();
        c.pullDomainEvents();
        assertThrows(IllegalStateException.class, c::concluir);
        assertTrue(c.domainEvents().isEmpty());

        c.registrarEntrega(); c.concluir(); c.pullDomainEvents();
        assertThrows(IllegalStateException.class, c::cancelar);
        assertTrue(c.domainEvents().isEmpty());
    }
}
