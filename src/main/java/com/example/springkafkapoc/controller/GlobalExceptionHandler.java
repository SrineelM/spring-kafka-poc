package com.example.springkafkapoc.controller;

import java.util.HashMap;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * <b>Global REST Error Handler</b>
 *
 * <p><b>TUTORIAL:</b> In a robust API, you don't want to leak stack traces to the user. This class
 * catches exceptions globally and translates them into clean, developer-friendly JSON messages.
 *
 * <p><b>Key Architecture Tip:</b> Notice the {@link MethodArgumentNotValidException} handler. This
 * automatically turns cryptic Java validation errors into a simple Map of "Field -> Error Message",
 * which is exactly what a Frontend developer needs to show error hints on a UI form.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

  /**
   * Handles validation errors (e.g. @NotNull, @Positive) triggered by @Valid.
   *
   * <p>WHY: Spring throws MethodArgumentNotValidException when validation fails. By default, this
   * returns a 400 but with a messy body. We intercept it to return a clean JSON map of field names
   * to error messages, so the API client knows exactly what to fix.
   */
  @ExceptionHandler(MethodArgumentNotValidException.class)
  public ResponseEntity<Map<String, String>> handleValidationExceptions(
      MethodArgumentNotValidException ex) {
    Map<String, String> errors = new HashMap<>();
    ex.getBindingResult()
        .getAllErrors()
        .forEach(
            (error) -> {
              String fieldName = ((FieldError) error).getField();
              String errorMessage = error.getDefaultMessage();
              errors.put(fieldName, errorMessage);
            });
    return new ResponseEntity<>(errors, HttpStatus.BAD_REQUEST);
  }
}
