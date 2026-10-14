package br.com.freela.reputacao;
import org.slf4j.*; import org.springframework.stereotype.Service; import org.springframework.transaction.annotation.Transactional; import java.math.BigDecimal; import java.util.UUID;
@Service
public class ReputacaoService {
 private static final Logger log=LoggerFactory.getLogger(ReputacaoService.class); private final ReputacaoRepository repository; private final ProcessamentoEventoRepository processados;
 public ReputacaoService(ReputacaoRepository r, ProcessamentoEventoRepository p){this.repository=r; this.processados=p;}
 @Transactional public void registrarContratoConcluido(UUID contratoId,UUID freelancerId,BigDecimal valor){
  log.info("reputacao.atualizacao.inicio contratoId={} freelancerId={} valor={}",contratoId,freelancerId,valor);
  var r=repository.findById(freelancerId).orElseGet(()->new ReputacaoFreelancer(freelancerId)); r.registrarContrato(valor); repository.save(r);
  log.info("reputacao.atualizacao.sucesso contratoId={} freelancerId={} contratosConcluidos={} valorTotal={}",contratoId,freelancerId,r.contratosConcluidos,r.valorTotal);
 }
 @Transactional public void registrarContratoCancelado(UUID contratoId,UUID freelancerId){
  log.info("reputacao.cancelamento.inicio contratoId={} freelancerId={}",contratoId,freelancerId);
  var r=repository.findById(freelancerId).orElseGet(()->new ReputacaoFreelancer(freelancerId)); r.registrarCancelamento(); repository.save(r);
  log.info("reputacao.cancelamento.sucesso contratoId={} freelancerId={} contratosCancelados={}",contratoId,freelancerId,r.contratosCancelados);
 }

 /**
  * Processa o evento de forma idempotente: o mesmo eventId nunca altera a reputação duas vezes.
  * Atualização e marcação do eventId ocorrem na MESMA transação.
  * @return true se processou, false se era duplicado.
  */
 @Transactional public boolean processar(ContratoEventEnvelope e){
  if(processados.existsById(e.eventId())){
   log.warn("reputacao.evento.duplicado eventId={} contratoId={} eventType={} resultado=DESCARTADO",e.eventId(),e.contratoId(),e.eventType());
   return false;
  }
  switch(e.eventType()){
   case "CONTRATO_CONCLUIDO" -> registrarContratoConcluido(e.contratoId(),e.payload().freelancerId(),e.payload().valor());
   case "CONTRATO_CANCELADO" -> registrarContratoCancelado(e.contratoId(),e.payload().freelancerId());
   default -> { log.info("reputacao.evento.ignorado eventId={} eventType={} motivo=sem-impacto-na-reputacao",e.eventId(),e.eventType()); }
  }
  processados.save(new ProcessamentoEvento(e.eventId(),e.eventType()));
  log.info("reputacao.processamento.sucesso eventId={} contratoId={} eventType={} resultado=PROCESSADO",e.eventId(),e.contratoId(),e.eventType());
  return true;
 }
}
