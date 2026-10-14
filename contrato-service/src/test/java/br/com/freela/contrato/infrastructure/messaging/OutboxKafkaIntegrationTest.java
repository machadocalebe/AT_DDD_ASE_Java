package br.com.freela.contrato.infrastructure.messaging;

import br.com.freela.contrato.application.ContratoApplicationService;
import br.com.freela.contrato.application.CriarContratoCommand;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integração com broker Kafka embarcado: Outbox -> Kafka com chave = contratoId, ordem preservada e headers de correlação.
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:contrato_it;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "eureka.client.enabled=false",
        "management.tracing.sampling.probability=0.0",
        "outbox.publisher.fixed-delay-ms=200"
})
@EmbeddedKafka(partitions = 3, topics = KafkaTopics.CONTRATOS_EVENTOS,
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class OutboxKafkaIntegrationTest {

    @Autowired ContratoApplicationService service;
    @Autowired OutboxRepository outbox;
    @Autowired EmbeddedKafkaBroker broker;

    @AfterEach
    void limparMdc() { MDC.clear(); }

    private CriarContratoCommand cmd() {
        return new CriarContratoCommand(UUID.randomUUID(), UUID.randomUUID(), "Contrato de teste", new BigDecimal("100.00"));
    }

    @Test
    void eventosDoMesmoContratoChegamNaMesmaParticaoEmOrdemComChaveEHeaders() {
        MDC.put("correlationId", "corr-teste-001");
        var c = service.criar(cmd());
        service.registrarEntrega(c.id());
        service.concluir(c.id());
        // outro contrato em paralelo, para provar que a chave separa contratos
        var outro = service.criar(cmd());

        var recebidos = consumir(c.id().toString(), 3);

        assertEquals(List.of("CONTRATO_CRIADO", "ENTREGA_REGISTRADA", "CONTRATO_CONCLUIDO"),
                recebidos.stream().map(r -> header(r, "eventType")).toList(), "ordem cronológica preservada");
        assertEquals(1, recebidos.stream().map(ConsumerRecord::partition).distinct().count(),
                "mesma chave (contratoId) => mesma partição");
        assertTrue(recebidos.stream().allMatch(r -> r.key().equals(c.id().toString())));
        assertTrue(recebidos.stream().allMatch(r -> "corr-teste-001".equals(header(r, "X-Correlation-Id"))),
                "correlationId propagado para os headers Kafka");
        assertTrue(recebidos.get(0).value().contains("\"eventId\"") && recebidos.get(0).value().contains(c.id().toString()),
                "envelope contém eventId e contratoId");
        assertNotNull(outro.id());
    }

    @Test
    void outboxEhMarcadoComoPublicadoAposEnvio() throws Exception {
        var c = service.criar(cmd());
        consumir(c.id().toString(), 1);
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline
                && outbox.findAll().stream().anyMatch(m -> m.getAggregateId().equals(c.id()) && !OutboxMessage.PUBLISHED.equals(m.getStatus()))) {
            Thread.sleep(100);
        }
        assertTrue(outbox.findAll().stream().filter(m -> m.getAggregateId().equals(c.id()))
                .allMatch(m -> OutboxMessage.PUBLISHED.equals(m.getStatus())));
    }

    @Test
    void falhaDeRegraDeNegocioNaoGravaEventoNoOutbox() {
        var c = service.criar(cmd());
        long antes = outbox.findAll().stream().filter(m -> m.getAggregateId().equals(c.id())).count();
        assertThrows(IllegalStateException.class, () -> service.concluir(c.id())); // sem entrega registrada
        long depois = outbox.findAll().stream().filter(m -> m.getAggregateId().equals(c.id())).count();
        assertEquals(antes, depois, "transação revertida: nenhum evento órfão");
    }

    private List<ConsumerRecord<String, String>> consumir(String chave, int esperados) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "teste-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        List<ConsumerRecord<String, String>> acumulado = new ArrayList<>();
        try (var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(List.of(KafkaTopics.CONTRATOS_EVENTOS));
            long deadline = System.currentTimeMillis() + 20_000;
            while (acumulado.size() < esperados && System.currentTimeMillis() < deadline) {
                for (var r : consumer.poll(Duration.ofMillis(300))) if (chave.equals(r.key())) acumulado.add(r);
            }
        }
        assertEquals(esperados, acumulado.size(), "mensagens esperadas para a chave " + chave);
        return acumulado;
    }

    private String header(ConsumerRecord<String, String> r, String nome) {
        var h = r.headers().lastHeader(nome);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }
}
