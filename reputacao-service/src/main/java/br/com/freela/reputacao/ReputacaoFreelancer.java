package br.com.freela.reputacao;
import jakarta.persistence.*; import java.math.BigDecimal; import java.util.UUID;
@Entity @Table(name="reputacoes")
class ReputacaoFreelancer {
 @Id UUID freelancerId; int contratosConcluidos; BigDecimal valorTotal=BigDecimal.ZERO;
 @Column(columnDefinition="integer not null default 0") int contratosCancelados;
 protected ReputacaoFreelancer(){} ReputacaoFreelancer(UUID id){this.freelancerId=id;}
 void registrarContrato(BigDecimal valor){contratosConcluidos++; valorTotal=valorTotal.add(valor);}
 void registrarCancelamento(){contratosCancelados++;}
 public UUID getFreelancerId(){return freelancerId;} public int getContratosConcluidos(){return contratosConcluidos;} public BigDecimal getValorTotal(){return valorTotal;} public int getContratosCancelados(){return contratosCancelados;}
}
