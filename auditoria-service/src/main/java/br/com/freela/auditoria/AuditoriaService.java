package br.com.freela.auditoria;
import org.slf4j.*; import org.springframework.stereotype.Service; import org.springframework.transaction.annotation.Transactional; import tools.jackson.databind.ObjectMapper; import java.time.Instant; import java.util.UUID;
@Service
public class AuditoriaService {
 private static final Logger log=LoggerFactory.getLogger(AuditoriaService.class); private final EventoAuditoriaRepository repository; private final ObjectMapper mapper;
 public AuditoriaService(EventoAuditoriaRepository r, ObjectMapper mapper){this.repository=r; this.mapper=mapper;}
 @Transactional public void registrar(UUID eventId,UUID aggregateId,String eventType,String correlationId,String payload,Instant occurredAt){
  log.info("auditoria.registro.inicio eventId={} aggregateId={} eventType={} correlationId={}",eventId,aggregateId,eventType,correlationId);
  var e=repository.save(new EventoAuditoria(eventId,aggregateId,eventType,correlationId,payload,occurredAt));
  log.info("auditoria.registro.sucesso auditoriaId={} eventId={} aggregateId={} eventType={}",e.id,eventId,aggregateId,eventType);
 }

 /** Registra o evento de forma idempotente (mesmo eventId não gera segundo registro). @return true se registrou. */
 @Transactional public boolean processar(ContratoEventEnvelope e, String correlationIdHeader){
  if(repository.existsByEventId(e.eventId())){
   log.warn("auditoria.evento.duplicado eventId={} contratoId={} eventType={} resultado=DESCARTADO",e.eventId(),e.contratoId(),e.eventType());
   return false;
  }
  String correlationId = correlationIdHeader!=null ? correlationIdHeader : e.correlationId();
  registrar(e.eventId(),e.contratoId(),e.eventType(),correlationId,mapper.writeValueAsString(e.payload()),e.occurredAt());
  return true;
 }
}
