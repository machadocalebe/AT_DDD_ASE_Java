package br.com.freela.reputacao;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.math.BigDecimal;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Producer -> broker embarcado -> consumer: o mesmo eventId entregue duas vezes NÃO pode alterar a reputação duas vezes.
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:reputacao_it;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "eureka.client.enabled=false",
        "management.tracing.sampling.probability=0.0"
})
@EmbeddedKafka(partitions = 3, topics = "contratos.eventos.v1",
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class IdempotenciaReputacaoTest {

    @Autowired KafkaTemplate<String, String> template;
    @Autowired ReputacaoRepository reputacoes;
    @Autowired ProcessamentoEventoRepository processados;
    @Autowired ReputacaoService service;

    private String json(UUID eventId, String tipo, UUID contratoId, UUID freelancerId, String valor) {
        return """
                {"eventId":"%s","eventType":"%s","contratoId":"%s","occurredAt":"2026-10-11T15:30:00Z","correlationId":"corr-it",
                 "payload":{"clienteId":"%s","freelancerId":"%s","titulo":"Teste","valor":%s}}
                """.formatted(eventId, tipo, contratoId, UUID.randomUUID(), freelancerId, valor);
    }

    private void aguardarProcessado(UUID eventId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline && !processados.existsById(eventId)) Thread.sleep(100);
        assertTrue(processados.existsById(eventId), "evento " + eventId + " deveria ter sido processado");
    }

    @Test
    void mensagemDuplicadaViaKafkaIncrementaReputacaoApenasUmaVez() throws Exception {
        UUID contrato = UUID.randomUUID(), freelancer = UUID.randomUUID(), eventId = UUID.randomUUID(), sentinela = UUID.randomUUID();
        String concluido = json(eventId, "CONTRATO_CONCLUIDO", contrato, freelancer, "100.00");

        template.send("contratos.eventos.v1", contrato.toString(), concluido);
        template.send("contratos.eventos.v1", contrato.toString(), concluido); // duplicata (mesmo eventId)
        // sentinela com a mesma chave => mesma partição => só é processada depois das duas mensagens acima
        template.send("contratos.eventos.v1", contrato.toString(), json(sentinela, "ENTREGA_REGISTRADA", contrato, freelancer, "100.00"));
        aguardarProcessado(sentinela);

        var rep = reputacoes.findById(freelancer).orElseThrow();
        assertEquals(1, rep.getContratosConcluidos(), "duplicata não pode incrementar o contador");
        assertEquals(0, new BigDecimal("100.00").compareTo(rep.getValorTotal()), "duplicata não pode somar o valor de novo");
    }

    @Test
    void processarDiretamenteRetornaFalsoParaEventoJaProcessado() {
        UUID contrato = UUID.randomUUID(), freelancer = UUID.randomUUID(), eventId = UUID.randomUUID();
        var envelope = new ContratoEventEnvelope(eventId, "CONTRATO_CONCLUIDO", contrato, java.time.Instant.now(), "c",
                new ContratoEventEnvelope.Payload(UUID.randomUUID(), freelancer, "x", new BigDecimal("50.00")));

        assertTrue(service.processar(envelope));
        assertFalse(service.processar(envelope));
        assertFalse(service.processar(envelope));

        var rep = reputacoes.findById(freelancer).orElseThrow();
        assertEquals(1, rep.getContratosConcluidos());
        assertEquals(0, new BigDecimal("50.00").compareTo(rep.getValorTotal()));
    }

    @Test
    void cancelamentoEhRegistradoUmaUnicaVez() {
        UUID freelancer = UUID.randomUUID();
        var envelope = new ContratoEventEnvelope(UUID.randomUUID(), "CONTRATO_CANCELADO", UUID.randomUUID(), java.time.Instant.now(), "c",
                new ContratoEventEnvelope.Payload(UUID.randomUUID(), freelancer, "x", new BigDecimal("10.00")));
        service.processar(envelope);
        service.processar(envelope);
        assertEquals(1, reputacoes.findById(freelancer).orElseThrow().getContratosCancelados());
    }
}
