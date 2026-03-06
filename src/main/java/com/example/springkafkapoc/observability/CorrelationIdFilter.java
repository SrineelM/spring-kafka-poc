package com.example.springkafkapoc.observability;

import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * <b>HTTP Correlation ID Filter</b>
 *
 * <p>The first point of entry for tracking a request.
 *
 * <p>Tutorial Tip: In a microservices architecture, you need a way to trace a single business
 * transaction across multiple services. We do this by attaching a <b>Correlation ID</b> to every
 * log statement.
 *
 * <p><b>WHY @Order(Ordered.HIGHEST_PRECEDENCE):</b> This ensures the ID is generated before any
 * other logic or filters run. If a database or security error occurs early, we already have an ID
 * for it.
 */
@Slf4j
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class CorrelationIdFilter implements Filter {

  private static final String CORRELATION_ID_HEADER = "X-Correlation-ID";

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {

    if (request instanceof HttpServletRequest httpRequest) {
      // 1. Try to get ID from incoming header (from gateway or user)
      String correlationId = httpRequest.getHeader(CORRELATION_ID_HEADER);

      // 2. Generate new if missing
      if (correlationId == null || correlationId.isBlank()) {
        correlationId = UUID.randomUUID().toString();
      }

      // 3. Set in ThreadLocal & MDC for logging and downstream use
      CorrelationIdContext.setCorrelationId(correlationId);

      log.trace("Correlation ID attached to request: {}", correlationId);
    }

    try {
      // 4. Continue with the rest of the application
      chain.doFilter(request, response);
    } finally {
      // 5. CRITICAL: Clear the context in 'finally'!
      // Threads in Tomcat/Netty are REUSED for other users.
      // If you don't clear, the NEXT user's logs will have THIS user's ID.
      CorrelationIdContext.clear();
    }
  }
}
