package com.example.springkafkapoc.domain.model;

/**
 * <b>Audit Event Registry (The Vocabulary)</b>
 *
 * <p><b>TUTORIAL:</b> Using an Enum for event types is a <b>Best Practice</b>.
 *
 * <p>Why not just use a String?
 *
 * <ul>
 *   <li><b>Type Safety:</b> You can't accidentally type "PROCESS_ERROR" if only "SENT_TO_DLQ" is
 *       defined.
 *   <li><b>Performance:</b> Comparing enums is a simple memory address check, much faster than
 *       String-based {@code equals()}.
 *   <li><b>IDE Support:</b> You get full autocomplete whenever you want to record an audit event.
 * </ul>
 */
public enum AuditEventType {
  RECEIVED,
  PROCESSED,
  RETRY_ATTEMPTED,
  SENT_TO_DLQ,
  ANALYTICS_AGGREGATED,
  BIGQUERY_WRITTEN
}
