package br.com.freela.auditoria;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:auditoria_it;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "eureka.client.enabled=false",
        "management.tracing.sampling.probability=0.0"
})
@EmbeddedKafka(partitions = 3, topics = "contratos.eventos.v1",
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class AuditoriaConsumoTest {

    @Autowired KafkaTemplate<String, String> template;
    @Autowired EventoAuditoriaRepository repository;

    private String json(UUID eventId, String tipo, UUID contratoId) {
        return """
                {"eventId":"%s","eventType":"%s","contratoId":"%s","occurredAt":"2026-10-11T15:30:00Z","correlationId":"corpo-corr",
                 "payload":{"clienteId":"%s","freelancerId":"%s","titulo":"API","valor":100.00}}
                """.formatted(eventId, tipo, contratoId, UUID.randomUUID(), UUID.randomUUID());
    }

    private void enviar(UUID contrato, String json, String correlationId) {
        var record = new ProducerRecord<>("contratos.eventos.v1", contrato.toString(), json);
        record.headers().add(new RecordHeader("X-Correlation-Id", correlationId.getBytes(StandardCharsets.UTF_8)));
        template.send(record);
    }

    private void aguardar(UUID eventId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline && !repository.existsByEventId(eventId)) Thread.sleep(100);
        assertTrue(repository.existsByEventId(eventId));
    }

    @Test
    void registraEventoComCamposMinimosEIgnoraDuplicata() throws Exception {
        UUID contrato = UUID.randomUUID(), eventId = UUID.randomUUID(), sentinela = UUID.randomUUID();
        String msg = json(eventId, "CONTRATO_CRIADO", contrato);

        enviar(contrato, msg, "corr-header-77");
        enviar(contrato, msg, "corr-header-77"); // duplicata
        enviar(contrato, json(sentinela, "ENTREGA_REGISTRADA", contrato), "corr-header-77");
        aguardar(sentinela);

        var registros = repository.findAll().stream().filter(e -> e.getAggregateId().equals(contrato)).toList();
        assertEquals(2, registros.size(), "um registro por eventId (duplicata descartada)");
        var criado = registros.stream().filter(e -> e.getEventId().equals(eventId)).findFirst().orElseThrow();
        assertEquals("CONTRATO_CRIADO", criado.getEventType());
        assertEquals("corr-header-77", criado.getCorrelationId());
        assertNotNull(criado.getOccurredAt());
        assertTrue(criado.getPayload().contains("freelancerId"));
    }
}
