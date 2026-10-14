package br.com.freela.notificacao;
import org.slf4j.*; import org.springframework.stereotype.Service; import org.springframework.transaction.annotation.Transactional; import java.util.UUID;
@Service
public class NotificacaoService {
 private static final Logger log=LoggerFactory.getLogger(NotificacaoService.class); private final NotificacaoRepository repository; private final ProcessamentoEventoRepository processados;
 public NotificacaoService(NotificacaoRepository r, ProcessamentoEventoRepository p){this.repository=r; this.processados=p;}
 @Transactional public void registrar(UUID contratoId, UUID eventId, UUID destinatarioId, String tipo, String mensagem){
  log.info("notificacao.registro.inicio contratoId={} eventId={} destinatarioId={} tipo={}",contratoId,eventId,destinatarioId,tipo);
  var n=repository.save(new Notificacao(contratoId,eventId,destinatarioId,tipo,mensagem));
  log.info("notificacao.registro.sucesso notificacaoId={} contratoId={} destinatarioId={}",n.id,contratoId,destinatarioId);
 }

 /**
  * Processa o evento de forma idempotente: se o eventId já foi processado, nada é feito.
  * Notificações e marcação do eventId acontecem na MESMA transação.
  * @return true se processou, false se era duplicado.
  */
 @Transactional public boolean processar(ContratoEventEnvelope e){
  if(processados.existsById(e.eventId())){
   log.warn("notificacao.evento.duplicado eventId={} contratoId={} eventType={} resultado=DESCARTADO",e.eventId(),e.contratoId(),e.eventType());
   return false;
  }
  var p=e.payload(); String titulo=p==null?"":p.titulo();
  UUID cliente=p==null?null:p.clienteId(), freelancer=p==null?null:p.freelancerId();
  switch(e.eventType()){
   case "CONTRATO_CRIADO" -> { registrar(e.contratoId(),e.eventId(),cliente,e.eventType(),"Contrato '"+titulo+"' criado com sucesso.");
                              registrar(e.contratoId(),e.eventId(),freelancer,e.eventType(),"Você foi contratado para '"+titulo+"'."); }
   case "ENTREGA_REGISTRADA" -> registrar(e.contratoId(),e.eventId(),cliente,e.eventType(),"O freelancer registrou a entrega de '"+titulo+"'. Valide para concluir.");
   case "CONTRATO_CONCLUIDO" -> { registrar(e.contratoId(),e.eventId(),cliente,e.eventType(),"Contrato '"+titulo+"' concluído.");
                                 registrar(e.contratoId(),e.eventId(),freelancer,e.eventType(),"Contrato '"+titulo+"' concluído. Pagamento liberado."); }
   case "CONTRATO_CANCELADO" -> { registrar(e.contratoId(),e.eventId(),cliente,e.eventType(),"Contrato '"+titulo+"' cancelado.");
                                 registrar(e.contratoId(),e.eventId(),freelancer,e.eventType(),"Contrato '"+titulo+"' cancelado."); }
   default -> log.warn("notificacao.evento.tipo-desconhecido eventId={} eventType={}",e.eventId(),e.eventType());
  }
  processados.save(new ProcessamentoEvento(e.eventId(),e.eventType()));
  log.info("notificacao.processamento.sucesso eventId={} contratoId={} eventType={} resultado=PROCESSADO",e.eventId(),e.contratoId(),e.eventType());
  return true;
 }
}
