package com.example.springkafkapoc.observability;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;

/**
 * <b>Tracking Context Storage</b>
 *
 * <p>Central hub for the Request's Trace ID (Correlation ID).
 *
 * <p>Tutorial Tip: This class is <b>Thread-Scoped</b>. Every business request (REST or Kafka) gets
 * its own isolated bucket to store ID strings. We use {@link ThreadLocal} so that multiple
 * concurrent requests never "bleed" their ID into each other.
 *
 * <p><b>MDC Integration:</b> By setting {@code MDC.put("correlationId", ...)}, SLF4J automatically
 * prepends the ID to every single log line. No more logging like {@code log.info("ID {}: Message",
 * id)}. It's done at the logger level!
 */
@Slf4j
public class CorrelationIdContext {

  private static final String MDC_KEY = "correlationId";
  private static final ThreadLocal<String> CONTEXT = new ThreadLocal<>();

  /** Sets the Tracking Context for the current thread. */
  public static void setCorrelationId(String id) {
    if (id == null) {
      clear();
      return;
    }

    // 1. Storage in memory for programmatic check
    CONTEXT.set(id);

    // 2. Storage in SLF4J MDC for logging prepends
    MDC.put(MDC_KEY, id);
  }

  /** Retrieves the ID currently associated with this execution thread. */
  public static String getCorrelationId() {
    return CONTEXT.get();
  }

  /** Wipes the context. Mandatory to call at the end of every request cycle. */
  public static void clear() {
    log.trace("Clearing tracking context for thread.");
    CONTEXT.remove();
    MDC.remove(MDC_KEY);
  }
}
