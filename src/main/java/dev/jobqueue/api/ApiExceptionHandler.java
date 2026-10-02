package dev.jobqueue.api;

import dev.jobqueue.core.DeadLetterNotFoundException;
import dev.jobqueue.core.JobNotFoundException;
import dev.jobqueue.core.JobStateConflictException;
import dev.jobqueue.core.UnknownJobTypeException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.validation.method.ParameterErrors;
import org.springframework.validation.method.ParameterValidationResult;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.method.annotation.HandlerMethodValidationException;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/** Every error leaves the API as an RFC 7807 {@code application/problem+json} body. */
@RestControllerAdvice
class ApiExceptionHandler extends ResponseEntityExceptionHandler {

  private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

  @ExceptionHandler(JobNotFoundException.class)
  ProblemDetail notFound(JobNotFoundException e) {
    return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.getMessage());
  }

  @ExceptionHandler(DeadLetterNotFoundException.class)
  ProblemDetail dlqNotFound(DeadLetterNotFoundException e) {
    return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.getMessage());
  }

  @ExceptionHandler(UnknownJobTypeException.class)
  ProblemDetail unknownType(UnknownJobTypeException e) {
    return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
  }

  @ExceptionHandler(JobStateConflictException.class)
  ProblemDetail conflict(JobStateConflictException e) {
    return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, e.getMessage());
  }

  @ExceptionHandler(Exception.class)
  ProblemDetail unexpected(Exception e) {
    log.error("unhandled error", e);
    return ProblemDetail.forStatusAndDetail(
        HttpStatus.INTERNAL_SERVER_ERROR, "Unexpected server error");
  }

  @Override
  protected ResponseEntity<Object> handleMethodArgumentNotValid(
      MethodArgumentNotValidException ex,
      HttpHeaders headers,
      HttpStatusCode status,
      WebRequest request) {
    Map<String, String> errors = new LinkedHashMap<>();
    for (FieldError fe : ex.getBindingResult().getFieldErrors()) {
      errors.putIfAbsent(fe.getField(), fe.getDefaultMessage());
    }
    ex.getBindingResult()
        .getGlobalErrors()
        .forEach(ge -> errors.putIfAbsent(ge.getObjectName(), ge.getDefaultMessage()));
    ProblemDetail pd =
        ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, "Validation failed");
    pd.setProperty("errors", errors);
    return ResponseEntity.badRequest().body(pd);
  }

  @Override
  protected ResponseEntity<Object> handleHandlerMethodValidationException(
      HandlerMethodValidationException ex,
      HttpHeaders headers,
      HttpStatusCode status,
      WebRequest request) {
    Map<String, String> errors = new LinkedHashMap<>();
    for (ParameterValidationResult r : ex.getParameterValidationResults()) {
      if (r instanceof ParameterErrors pe) {
        // @Valid request body: report each failing field by its own name.
        pe.getFieldErrors()
            .forEach(fe -> errors.putIfAbsent(fe.getField(), fe.getDefaultMessage()));
        pe.getGlobalErrors()
            .forEach(ge -> errors.putIfAbsent(ge.getObjectName(), ge.getDefaultMessage()));
      } else {
        // Constraint on a simple parameter (header, query param): report by parameter name.
        r.getResolvableErrors()
            .forEach(
                err ->
                    errors.putIfAbsent(
                        r.getMethodParameter().getParameterName(), err.getDefaultMessage()));
      }
    }
    ProblemDetail pd =
        ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, "Validation failed");
    pd.setProperty("errors", errors);
    return ResponseEntity.badRequest().body(pd);
  }
}
