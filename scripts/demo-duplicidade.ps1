# Simula a chegada de uma mensagem duplicada no Kafka para comprovar Idempotência.
# Pré-requisito: o fluxo principal já deve ter sido executado pelo menos uma vez.
param(
    [string]$BootstrapServer = "localhost:9092",
    [string]$Topic = "contratos.eventos.v1"
)

Write-Host "Buscando o último evento concluído no banco de dados para retransmitir..." -ForegroundColor Cyan
$event = docker exec freela-postgres psql -U freela -d contrato_db -t -A -c "SELECT aggregate_id || '#' || payload FROM outbox_messages WHERE status = 'PUBLISHED' ORDER BY seq DESC LIMIT 1;"

if (-not $event) {
    Write-Host "Nenhum evento encontrado no outbox. Execute primeiro o scripts/demo-fluxo.ps1." -ForegroundColor Red
    exit 1
}

Write-Host "Retransmitindo mensagem já processada ao Kafka (tópico: $Topic)..." -ForegroundColor Yellow
$event | docker exec -i freela-kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server $BootstrapServer --topic $Topic --property "parse.key=true" --property "key.separator=#"

Write-Host "`nMensagem duplicada injetada com sucesso no Kafka!" -ForegroundColor Green
Write-Host "Verifique os terminais de 'reputacao-service', 'notificacao-service' e 'auditoria-service'." -ForegroundColor Yellow
Write-Host "Você verá a linha: *.evento.duplicado ... resultado=DESCARTADO`n" -ForegroundColor Yellow
