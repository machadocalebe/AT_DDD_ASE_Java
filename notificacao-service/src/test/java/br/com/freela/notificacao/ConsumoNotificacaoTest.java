package br.com.freela.notificacao;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:notificacao_it;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "eureka.client.enabled=false",
        "management.tracing.sampling.probability=0.0"
})
@EmbeddedKafka(partitions = 3, topics = "contratos.eventos.v1",
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class ConsumoNotificacaoTest {

    @Autowired KafkaTemplate<String, String> template;
    @Autowired NotificacaoRepository notificacoes;
    @Autowired ProcessamentoEventoRepository processados;
    @Autowired EmbeddedKafkaBroker broker;

    private String json(UUID eventId, String tipo, UUID contratoId, UUID cliente, UUID freelancer) {
        return """
                {"eventId":"%s","eventType":"%s","contratoId":"%s","occurredAt":"2026-10-11T15:30:00Z","correlationId":"corr-it",
                 "payload":{"clienteId":"%s","freelancerId":"%s","titulo":"API","valor":100.00}}
                """.formatted(eventId, tipo, contratoId, cliente, freelancer);
    }

    private void aguardarProcessado(UUID eventId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline && !processados.existsById(eventId)) Thread.sleep(100);
        assertTrue(processados.existsById(eventId));
    }

    @Test
    void eventoCriadoGeraUmaNotificacaoPorDestinatarioMesmoQuandoDuplicado() throws Exception {
        UUID contrato = UUID.randomUUID(), cliente = UUID.randomUUID(), freelancer = UUID.randomUUID();
        UUID eventId = UUID.randomUUID(), sentinela = UUID.randomUUID();
        String msg = json(eventId, "CONTRATO_CRIADO", contrato, cliente, freelancer);

        template.send("contratos.eventos.v1", contrato.toString(), msg);
        template.send("contratos.eventos.v1", contrato.toString(), msg); // duplicada
        template.send("contratos.eventos.v1", contrato.toString(), json(sentinela, "ENTREGA_REGISTRADA", contrato, cliente, freelancer));
        aguardarProcessado(sentinela);

        var doContrato = notificacoes.findAll().stream().filter(n -> n.getContratoId().equals(contrato)).toList();
        long criadas = doContrato.stream().filter(n -> "CONTRATO_CRIADO".equals(n.getTipo())).count();
        assertEquals(2, criadas, "cliente + freelancer, sem repetir por causa da duplicata");
        assertTrue(doContrato.stream().anyMatch(n -> n.getDestinatarioId().equals(cliente)));
        assertTrue(doContrato.stream().anyMatch(n -> n.getDestinatarioId().equals(freelancer)));
    }

    @Test
    void mensagemInvalidaVaiParaDeadLetterTopicSemBloquearAsDemais() throws Exception {
        UUID contrato = UUID.randomUUID(), sentinela = UUID.randomUUID();
        String veneno = "isto-nao-e-json-" + UUID.randomUUID();
        template.send("contratos.eventos.v1", contrato.toString(), veneno);
        template.send("contratos.eventos.v1", contrato.toString(),
                json(sentinela, "ENTREGA_REGISTRADA", contrato, UUID.randomUUID(), UUID.randomUUID()));

        aguardarProcessado(sentinela); // mensagem válida seguinte foi processada após esgotar as tentativas da inválida

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "teste-dlt-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        boolean achou = false;
        try (var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(List.of(KafkaConsumerConfig.DLT));
            long deadline = System.currentTimeMillis() + 20_000;
            while (!achou && System.currentTimeMillis() < deadline) {
                for (var r : consumer.poll(Duration.ofMillis(300))) if (veneno.equals(r.value())) achou = true;
            }
        }
        assertTrue(achou, "mensagem inválida deve estar disponível no DLT para diagnóstico");
    }
}
