# Arquitetura Orientada a Eventos — Freela Marketplace

Documentação da evolução do marketplace para comunicação assíncrona via Apache Kafka.

## 1. Visão geral

```text
Cliente --HTTP--> API Gateway (:8080, gera/propaga X-Correlation-Id + trace)
                      |
                      v
              contrato-service (:8081)
   [1 transação: salva Contrato + grava evento em outbox_messages]
                      |
          OutboxPublisher (@Scheduled, ordem de gravação)
                      |  key = contratoId
                      v
        Kafka  tópico  contratos.eventos.v1  (3 partições)
          |                 |                    |
          v                 v                    v
 notificacao-service  reputacao-service   auditoria-service
 (notificacao-group)  (reputacao-group)   (auditoria-group)
   idempotente          idempotente          idempotente
   retry x3 -> DLT      retry x3 -> DLT      retry x3 -> DLT
```

Não há chamadas HTTP entre `contrato-service` e os consumidores. O fluxo é assíncrono porque o contrato responde `201` assim que persiste a transação local; notificação, reputação e auditoria acontecem depois, no ritmo de cada consumidor, e um consumidor fora do ar não impede a criação do contrato (a mensagem espera no Kafka).

## 2. Tópicos Kafka

| Tópico | Partições | Produtor | Consumidores (group) | Chave |
|---|---|---|---|---|
| `contratos.eventos.v1` | 3 | contrato-service | notificacao-group, reputacao-group, auditoria-group | `contratoId` |
| `contratos.eventos.v1.notificacao.DLT` | 1 | notificacao-service | (diagnóstico/reprocessamento) | — |
| `contratos.eventos.v1.reputacao.DLT` | 1 | reputacao-service | (diagnóstico/reprocessamento) | — |
| `contratos.eventos.v1.auditoria.DLT` | 1 | auditoria-service | (diagnóstico/reprocessamento) | — |

## 3. Eventos

Todos usam o mesmo envelope e o tópico `contratos.eventos.v1`, chave = `contratoId`.

| `eventType` | Disparado por | Consumidores | Efeito |
|---|---|---|---|
| `CONTRATO_CRIADO` | `POST /api/contratos` | notificacao, auditoria | notifica cliente e freelancer; audita |
| `ENTREGA_REGISTRADA` | `POST /api/contratos/{id}/entregar` | notificacao, auditoria | notifica cliente; audita |
| `CONTRATO_CONCLUIDO` | `POST /api/contratos/{id}/concluir` | notificacao, reputacao, auditoria | notifica ambos; **+1 concluído e soma valor**; audita |
| `CONTRATO_CANCELADO` | `POST /api/contratos/{id}/cancelar` | notificacao, reputacao, auditoria | notifica ambos; **+1 cancelado**; audita |

### Envelope (campos obrigatórios: todos, exceto `correlationId` quando a origem não o informa)

| Campo | Tipo | Descrição |
|---|---|---|
| `eventId` | UUID | identificador único do evento (chave de idempotência) |
| `eventType` | string | tipo do evento |
| `contratoId` | UUID | contrato relacionado (também é a **chave** da mensagem) |
| `occurredAt` | ISO-8601 | quando ocorreu |
| `correlationId` | string | correlação da operação (vem do `X-Correlation-Id`) |
| `payload.clienteId` / `freelancerId` | UUID | partes |
| `payload.titulo` / `valor` | string / decimal | dados do contrato |

Exemplo:

```json
{
  "eventId": "64672413-a379-44f8-9759-a5a5058ec03a",
  "eventType": "CONTRATO_CONCLUIDO",
  "contratoId": "68456620-6b08-4913-a47c-9c3f6c69975c",
  "occurredAt": "2026-10-11T16:08:13.260324100Z",
  "correlationId": "demo-001",
  "payload": {
    "eventId": "64672413-a379-44f8-9759-a5a5058ec03a",
    "occurredAt": "2026-10-11T16:08:13.260324100Z",
    "contratoId": "68456620-6b08-4913-a47c-9c3f6c69975c",
    "clienteId": "11111111-1111-1111-1111-111111111111",
    "freelancerId": "22222222-2222-2222-2222-222222222222",
    "titulo": "Construção de API de pagamentos",
    "valor": 3500.00
  }
}
```

Headers Kafka: `eventId`, `eventType`, `X-Correlation-Id` (+ headers de trace do Micrometer/Brave).

## 4. Particionamento e ordenação

- A chave da mensagem é o `contratoId` (`OutboxPublisher`). O Kafka garante ordem **dentro da partição**; a mesma chave sempre cai na mesma partição, então `CONTRATO_CRIADO → ENTREGA_REGISTRADA → CONTRATO_CONCLUIDO` chega nessa ordem.
- Contratos diferentes se espalham pelas 3 partições e são processados em paralelo: `spring.kafka.listener.concurrency: 3` (uma thread por partição). Cada partição é lida por uma única thread, o que não quebra a ordem.
- O publisher lê o outbox por `seq` crescente e interrompe o lote se uma publicação falhar, para não passar um evento à frente de outro.
- Producer com `acks=all` e `enable.idempotence=true`.

## 5. Publicação transacional (Transactional Outbox)

`ContratoApplicationService` salva o Aggregate e chama `DomainEventRecorder.registrar(...)`, que grava em `outbox_messages` **na mesma transação** (`Propagation.MANDATORY`). Se a regra de negócio falhar, nada é gravado (o teste `falhaDeRegraDeNegocioNaoGravaEventoNoOutbox` cobre isso). `OutboxPublisher` publica as pendentes e as marca `PUBLISHED`. Se o Kafka estiver fora, as mensagens ficam `PENDING` e saem depois, garantindo entrega *at-least-once*.

## 6. Idempotência

Como a entrega é *at-least-once*, o mesmo evento pode chegar mais de uma vez.

- `notificacao-service` e `reputacao-service`: tabela `eventos_processados` com `eventId` como chave primária. O handler checa `existsById(eventId)`; se existir, descarta com log `*.evento.duplicado`. A regra de negócio e o registro do `eventId` ocorrem na mesma transação.
- `auditoria-service`: coluna `eventId` com restrição de unicidade e checagem `existsByEventId`.
- Se duas threads processarem o mesmo `eventId` ao mesmo tempo, a chave primária/unicidade impede o segundo commit; o erro cai no retry e na tentativa seguinte o evento é visto como duplicado.

## 7. Tratamento de falhas

`DefaultErrorHandler` com `FixedBackOff(1000ms, 3 tentativas)`. Esgotadas as tentativas, `DeadLetterPublishingRecoverer` publica a mensagem original no `<tópico>.<serviço>.DLT` e a partição segue. Uma mensagem inválida não bloqueia as demais, e fica no DLT para diagnóstico e reprocessamento (coberto por `mensagemInvalidaVaiParaDeadLetterTopicSemBloquearAsDemais`).

## 8. Logs, correlação e tracing

- **Correlation ID:** o Gateway gera/propaga `X-Correlation-Id`. No `contrato-service` o `CorrelationIdFilter` o coloca no MDC; o evento o grava no envelope e no header Kafka; os consumidores o recolocam no MDC. Aparece em todas as linhas de log (`correlationId=...`).
- **Formato de log:** `service=… traceId=… correlationId=… thread=… logger=… - evento.nome contratoId=… eventId=…`. Dados sensíveis não são logados.
- **Centralização:** cada serviço envia logs ao **Grafana Loki** (`loki-logback-appender`, ver `logback-spring.xml`). Consulte em http://localhost:3000 (Explore → Loki), por exemplo:
  - `{app=~".+"} |= "demo-001"` (correlationId)
  - `{app=~".+"} |= "<contratoId>"` ou `|= "<eventId>"`
- **Tracing:** Micrometer Tracing (Brave) + Zipkin (http://localhost:9411), `sampling.probability=1.0`. `spring.kafka.template.observation-enabled` e `spring.kafka.listener.observation-enabled` propagam o contexto pelos headers Kafka.

## 9. Infraestrutura e execução

```bash
cd infra
docker compose up -d        # postgres, kafka, kafka-ui, zipkin, loki, grafana
```

| Componente | URL |
|---|---|
| Kafka UI | http://localhost:8090 |
| Zipkin | http://localhost:9411 |
| Grafana (Loki) | http://localhost:3000 |

Suba os serviços na ordem: `eureka-server`, `api-gateway`, `contrato-service`, `notificacao-service`, `reputacao-service`, `auditoria-service` (`mvn -pl <modulo> spring-boot:run`).

## 10. Exemplos de chamadas

```bash
# 1) criar
curl -X POST http://localhost:8080/api/contratos -H 'Content-Type: application/json' \
  -H 'X-Correlation-Id: demo-001' \
  -d '{"clienteId":"11111111-1111-1111-1111-111111111111","freelancerId":"22222222-2222-2222-2222-222222222222","titulo":"API de pagamentos","valor":3500.00}'
# 2) entregar, 3) concluir (ou cancelar)
curl -X POST http://localhost:8080/api/contratos/{id}/entregar -H 'X-Correlation-Id: demo-001'
curl -X POST http://localhost:8080/api/contratos/{id}/concluir -H 'X-Correlation-Id: demo-001'
# consultas
curl http://localhost:8080/api/notificacoes
curl http://localhost:8080/api/reputacoes
curl http://localhost:8080/api/auditoria
```

O script `scripts/demo-fluxo.ps1` executa esse fluxo e mostra os resultados.

## 11. Testes (`mvn test`)

Rodam sem Docker: Kafka embarcado (`@EmbeddedKafka`) + H2.

| Teste | Cobre |
|---|---|
| `ContratoTest` | regras do Aggregate e eventos emitidos |
| `OutboxKafkaIntegrationTest` | producer + outbox + broker; **ordem**, mesma partição por `contratoId`, headers de correlação, atomicidade |
| `IdempotenciaReputacaoTest` | **duplicidade** via Kafka e direta; reputação não é alterada duas vezes |
| `ConsumoNotificacaoTest` | consumer, duplicidade (sem notificações repetidas) e **DLT** |
| `AuditoriaConsumoTest` | consumer, campos auditados e duplicidade |

## 12. Evidências para o relatório

| Evidência | Onde obter |
|---|---|
| Requisição no Gateway | log `gateway.request.inicio/fim`; saída do `curl` |
| Persistência no contrato-service | `GET /api/contratos/{id}`; tabela `outbox_messages` |
| Evento no Kafka (chave, headers, payload) | Kafka UI → tópico `contratos.eventos.v1` |
| Consumo e persistência | logs `*.evento.recebido`; `GET /api/notificacoes`, `/api/reputacoes`, `/api/auditoria` |
| Duplicidade | teste `IdempotenciaReputacaoTest`; log `*.evento.duplicado` |
| Ordem | `OutboxKafkaIntegrationTest`; partição/offset nos logs `contrato.outbox.publicado` |
| Logs centralizados | Grafana → Loki, filtrando por `correlationId` |
| Trace | Zipkin, buscando por serviço ou `traceId` |
