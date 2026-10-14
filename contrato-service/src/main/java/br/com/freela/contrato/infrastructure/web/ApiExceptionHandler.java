package br.com.freela.contrato.infrastructure.web;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ApiExceptionHandler {
    @ExceptionHandler(IllegalStateException.class)
    ProblemDetail conflito(IllegalStateException e) { return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, e.getMessage()); }

    @ExceptionHandler(IllegalArgumentException.class)
    ProblemDetail invalido(IllegalArgumentException e) {
        var status = e.getMessage() != null && e.getMessage().startsWith("Contrato não encontrado") ? HttpStatus.NOT_FOUND : HttpStatus.BAD_REQUEST;
        return ProblemDetail.forStatusAndDetail(status, e.getMessage());
    }
}
