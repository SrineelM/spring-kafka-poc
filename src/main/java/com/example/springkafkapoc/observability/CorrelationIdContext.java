package com.example.springkafkapoc.observability;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;

/**
 * <b>Correlation ID Context — Thread-Scoped Trace Storage</b>
 *
 * <p><b>TUTORIAL — What is a Correlation ID?</b><br>
 * In a microservices/event-driven system, a single business operation (e.g., "process payment")
 * spans multiple services, consumer threads, and Kafka topics. Without a shared identifier, it's
 * nearly impossible to reconstruct the full timeline of a request from logs. A Correlation ID is a
 * UUID that is:
 *
 * <ol>
 *   <li>Generated when the HTTP request first enters the system ({@link CorrelationIdFilter}).
 *   <li>Stored in this class so all code on the same thread can read it without it being passed as
 *       a method parameter everywhere.
 *   <li>Injected into every Kafka record's headers ({@link KafkaCorrelationIdInterceptor}).
 *   <li>Extracted from every consumed Kafka record's headers ({@link KafkaCorrelationIdExtractor}).
 *   <li>Prepended to every log line via the SLF4J MDC (Mapped Diagnostic Context).
 * </ol>
 *
 * <p><b>TUTORIAL — ThreadLocal:</b><br>
 * {@link ThreadLocal} is a per-thread storage mechanism. Each thread has its own isolated copy of
 * the stored value. When Thread A sets {@code correlationId = "abc"}, Thread B (which might be
 * processing a completely different request simultaneously) sees its own value, not "abc."
 *
 * <p><b>Memory Leak Warning:</b><br>
 * {@code ThreadLocal} values survive as long as the thread lives. In a thread-pooled environment
 * (Tomcat, Spring async), threads are reused across requests. ALWAYS call {@code clear()} in a
 * finally block or a Servlet filter's {@code afterCompletion()} to prevent a previous request's ID
 * from leaking into the next request on the same thread.
 *
 * <p><b>MDC Integration:</b><br>
 * Setting {@code MDC.put("correlationId", id)} causes SLF4J to prepend the value to every log line
 * produced by this thread — as long as your logging pattern includes {@code %X{correlationId}}.
 * This eliminates the need to explicitly pass the ID to every {@code log.info()} call.
 */
@Slf4j
public class CorrelationIdContext {

  /** The MDC key used in the logging pattern (e.g., {@code logback.xml %X{correlationId}}). */
  private static final String MDC_KEY = "correlationId";

  /**
   * Per-thread storage for the correlation ID.
   *
   * <p>Using {@code ThreadLocal} rather than a method parameter means business code (services,
   * consumers) can read the correlation ID without it being threaded through every method signature
   * — a form of cross-cutting concern handled invisibly.
   */
  private static final ThreadLocal<String> CONTEXT = new ThreadLocal<>();

  /**
   * Sets the correlation ID for the current thread and registers it with the SLF4J MDC.
   *
   * <p>A {@code null} ID is treated as a clear request to avoid storing null in the MDC, which
   * would cause log patterns to print "null" instead of omitting the field.
   *
   * @param id the correlation ID to associate with this thread (typically a UUIDv7)
   */
  public static void setCorrelationId(String id) {
    if (id == null) {
      clear(); // Treat null as a "clear" operation to keep MDC clean
      return;
    }
    CONTEXT.set(id); // Store for programmatic access via getCorrelationId()
    MDC.put(MDC_KEY, id); // Register with SLF4J — auto-prepended to every log line
  }

  /**
   * Retrieves the correlation ID for the current thread.
   *
   * <p>Returns {@code null} if no ID has been set (e.g., for async threads not spawned from an HTTP
   * request). Callers should handle null gracefully rather than throwing.
   */
  public static String getCorrelationId() {
    return CONTEXT.get();
  }

  /**
   * Clears the correlation ID from both the ThreadLocal and the SLF4J MDC.
   *
   * <p><b>CRITICAL:</b> Always call this at the end of request processing. In a thread-pool
   * environment, threads are reused — failing to clear means the next request on this thread will
   * inherit the previous request's correlation ID, causing log cross-contamination and making log
   * traces misleading.
   *
   * <p>In HTTP filters: call in {@code afterCompletion()}. In Kafka listeners: call in a {@code
   * finally} block after processing.
   */
  public static void clear() {
    log.trace("Clearing correlation ID from thread context.");
    CONTEXT.remove(); // Remove from ThreadLocal — releases the reference for GC
    MDC.remove(MDC_KEY); // Remove from MDC — stops prepending the ID to future log lines
  }
}
