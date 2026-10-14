# Demonstra o fluxo assíncrono completo. Pré-requisitos: docker compose up -d (infra) e os 6 serviços no ar.
param(
    [string]$Gateway = "http://localhost:8080",
    [string]$CorrelationId = "demo-$(Get-Date -Format 'HHmmss')"
)

[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$h = @{ "X-Correlation-Id" = $CorrelationId }
$freelancer = "22222222-2222-2222-2222-222222222222"
$body = @{ clienteId = "11111111-1111-1111-1111-111111111111"; freelancerId = $freelancer; titulo = "API de pagamentos"; valor = 3500.00 } | ConvertTo-Json

Write-Host "== correlationId: $CorrelationId ==" -ForegroundColor Cyan
$c = Invoke-RestMethod -Method Post -Uri "$Gateway/api/contratos" -Headers $h -ContentType "application/json" -Body $body
Write-Host "Contrato criado: $($c.id) status=$($c.status)"
Invoke-RestMethod -Method Post -Uri "$Gateway/api/contratos/$($c.id)/entregar" -Headers $h | Out-Null
Invoke-RestMethod -Method Post -Uri "$Gateway/api/contratos/$($c.id)/concluir" -Headers $h | Out-Null
Write-Host "Entrega registrada e contrato concluído. Aguardando consumidores..."
Start-Sleep -Seconds 4

# Atribuir a variável antes de filtrar: no PowerShell 5.1 o pipe direto do Invoke-RestMethod não enumera o array.
Write-Host "`n-- Notificações do contrato --" -ForegroundColor Yellow
$notificacoes = Invoke-RestMethod "$Gateway/api/notificacoes"
$notificacoes | Where-Object { $_.contratoId -eq $c.id } | Format-Table tipo, destinatarioId, mensagem -AutoSize

Write-Host "-- Reputação do freelancer --" -ForegroundColor Yellow
$reputacoes = Invoke-RestMethod "$Gateway/api/reputacoes"
$reputacoes | Where-Object { $_.freelancerId -eq $freelancer } | Format-Table contratosConcluidos, contratosCancelados, valorTotal, freelancerId -AutoSize

Write-Host "-- Auditoria do contrato --" -ForegroundColor Yellow
$auditoria = Invoke-RestMethod "$Gateway/api/auditoria"
$auditoria | Where-Object { $_.aggregateId -eq $c.id } | Sort-Object occurredAt | Format-Table eventType, correlationId, occurredAt -AutoSize

Write-Host "Kafka UI:  http://localhost:8090 (tópico contratos.eventos.v1)"
Write-Host "Zipkin:    http://localhost:9411"
Write-Host "Grafana:   http://localhost:3000  -> Loki: {app=~`".+`"} |= `"$CorrelationId`""
